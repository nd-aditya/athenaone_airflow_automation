from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from shared_models import LLMRequest, LLMResponse, LLMStreamChunk
from llm_models.models import LLMManager
import json
from typing import Iterator

router = APIRouter()
llm_manager = LLMManager()

@router.post("/generate", response_model=LLMResponse)
async def generate_text(request: LLMRequest):
    tool = llm_manager.get_tool(request.model_name)
    if not tool:
        raise HTTPException(status_code=404, detail=f"Model '{request.model_name}' not found.")
    try:
        output = tool.invoke({
            "input_text": request.input_text,
            "model_param": request.model_param,
            "num_workers": request.num_workers,
            "stream": request.stream
        })
        return LLMResponse(output=output)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/generate/stream")
async def generate_text_stream(request: LLMRequest):
    """Stream text generation responses - supports both single and batch streaming"""
    if not request.stream:
        request.stream = True
    
    tool = llm_manager.get_tool(request.model_name)
    if not tool:
        raise HTTPException(status_code=404, detail=f"Model '{request.model_name}' not found.")
    
    def generate():
        try:
            result = tool.invoke({
                "input_text": request.input_text,
                "model_param": request.model_param,
                "num_workers": request.num_workers,
                "stream": True
            })
            
            # Handle single streaming (Iterator[str])
            if hasattr(result, '__iter__') and not isinstance(result, list):
                print(f"🔄 [API] Single streaming mode")
                for chunk in result:
                    chunk_data = LLMStreamChunk(content=chunk, finished=False)
                    yield f"data: {chunk_data.json()}\n\n"
                
                # Send final chunk to indicate completion
                final_chunk = LLMStreamChunk(content="", finished=True)
                yield f"data: {final_chunk.json()}\n\n"
            
            # Handle batch streaming (List[Iterator[str]])
            elif isinstance(result, list):
                print(f"🔄 [API] Batch streaming mode - {len(result)} iterators")
                import threading
                import queue
                import time
                
                # Create a queue to collect chunks from all iterators
                chunk_queue = queue.Queue()
                active_iterators = len(result)
                
                def process_iterator(idx: int, iterator):
                    """Process one iterator and put chunks in the queue"""
                    try:
                        for chunk in iterator:
                            chunk_queue.put((idx, chunk, False))  # (prompt_idx, content, finished)
                        chunk_queue.put((idx, "", True))  # Mark this iterator as finished
                    except Exception as e:
                        chunk_queue.put((idx, f"Error: {str(e)}", True))
                
                # Start threads for each iterator
                threads = []
                for i, iterator in enumerate(result):
                    thread = threading.Thread(target=process_iterator, args=(i, iterator))
                    thread.daemon = True
                    thread.start()
                    threads.append(thread)
                
                finished_iterators = set()
                
                # Stream chunks as they arrive
                while len(finished_iterators) < active_iterators:
                    try:
                        # Get chunk with timeout
                        prompt_idx, content, is_finished = chunk_queue.get(timeout=1.0)
                        
                        if is_finished:
                            finished_iterators.add(prompt_idx)
                            if content:  # Only send error messages
                                chunk_data = LLMStreamChunk(
                                    content=content, 
                                    finished=False,
                                    metadata={"prompt_index": prompt_idx, "error": True}
                                )
                                yield f"data: {chunk_data.json()}\n\n"
                        else:
                            # Send chunk with prompt index
                            chunk_data = LLMStreamChunk(
                                content=content, 
                                finished=False,
                                metadata={"prompt_index": prompt_idx}
                            )
                            yield f"data: {chunk_data.json()}\n\n"
                        
                    except queue.Empty:
                        # Timeout - continue waiting
                        continue
                
                # Send final completion signal
                final_chunk = LLMStreamChunk(
                    content="", 
                    finished=True,
                    metadata={"batch_complete": True, "total_prompts": len(result)}
                )
                yield f"data: {final_chunk.json()}\n\n"
            
            else:
                # Fallback for unexpected return type
                error_chunk = LLMStreamChunk(content=f"Unexpected return type: {type(result)}", finished=True)
                yield f"data: {error_chunk.json()}\n\n"
            
        except Exception as e:
            error_chunk = LLMStreamChunk(content=f"Error: {str(e)}", finished=True)
            yield f"data: {error_chunk.json()}\n\n"
    
    return StreamingResponse(generate(), media_type="text/plain")

# 
