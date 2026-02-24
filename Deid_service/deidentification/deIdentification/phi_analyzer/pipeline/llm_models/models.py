import time
from typing import Any, Dict, Optional, Union, List, Iterator, AsyncIterator
import os 
import asyncio
import aiohttp
from tqdm.asyncio import tqdm
import requests
import json
import queue
import threading

import openai
from openai import OpenAIError
import google.generativeai as genai
from pydantic import BaseModel, Field

from langchain_core.tools import BaseTool
from langgraph.prebuilt import ToolNode
from langchain_core.tools import BaseTool
from phi_analyzer.pipeline.llm_models.config import model_configs 
# from config import model_configs
# from langgraph.prebuilt import ToolInvocation
from langchain_core.messages import AIMessage


import re
from ollama import Client


class StreamingQueue:
    """Thread-safe queue-based iterator for streaming responses"""
    def __init__(self):
        self.queue = queue.Queue()
        self.finished = False
    
    def put(self, item: str):
        """Add an item to the stream"""
        self.queue.put(item)
    
    def finish(self):
        """Mark the stream as finished"""
        self.finished = True
        self.queue.put(None)  # Sentinel value
    
    def error(self, error_msg: str):
        """Add error and finish the stream"""
        self.queue.put(f"Error: {error_msg}")
        self.finish()
    
    def __iter__(self):
        return self
    
    def __next__(self):
        while True:
            try:
                item = self.queue.get(timeout=1.0)
                if item is None:  # Sentinel value
                    raise StopIteration
                return item
            except queue.Empty:
                if self.finished:
                    raise StopIteration
                continue


class LMStudioTool(BaseTool, BaseModel):
    """
    Tool wrapping LM studio endpoint for use with LangGraph.
    """
    name: str = "lmstudio_tool"
    description: str = "Send chat requests to LM studio endpoint."
    
    url: Optional[str] = None
    model_names: Optional[List[str]] = None
    enable_thinking: Optional[bool] = None
    
    # Flag to prevent multiple initializations
    _initialized: bool = False

    def __init__(self, **kwargs):
        # Store important values before BaseModel init
        model_name_val = kwargs.get("model_name")
        url_val = kwargs.get("url")
        enable_thinking_val = kwargs.get("enable_thinking")
        
        # Initialize core classes
        BaseTool.__init__(self)
        BaseModel.__init__(self, **kwargs)
        
        # Skip if already initialized
        if self._initialized:
            return
            
        # Set values from constructor params
        if model_name_val:
            # Handle both single string and list of model names
            if isinstance(model_name_val, list):
                self.model_names = model_name_val
            else:
                self.model_names = [model_name_val]
        if url_val:
            self.url = url_val
        if enable_thinking_val is not None:
            self.enable_thinking = enable_thinking_val
            
        # Set defaults if not provided
        if not self.model_names:
            self.model_names = ["qwen3-8b"]
        if not self.url:
            self.url = "http://localhost:1234/v1/chat/completions"
        if self.enable_thinking is None:
            self.enable_thinking = False
            
        # Mark as initialized
        self._initialized = True
        
        print(f"LMStudioTool initialized with models: {self.model_names}, url: '{self.url}'")

    def _preprocess_text(self, content: str) -> str:
        """Remove thinking tags and clean up extra newlines"""
        # Remove <think>...</think> blocks and any newlines immediately following
        cleaned = re.sub(r"<think>.*?</think>\n*", "", content, flags=re.DOTALL)
        return cleaned.strip()

    def _run(
        self,
        input_text: Union[str, List[str]],
        model_param: Optional[Dict[str, Any]] = None,
        num_workers: int = 50,
        stream: bool = False,
    ) -> Union[str, List[str], Iterator[str], List[Iterator[str]]]:
        """
        Send chat message(s) to LM Studio and return the assistant response(s).
        
        Args:
            input_text: Single string or list of strings to process
            model_param: Model parameters
            num_workers: Number of concurrent workers for batch processing
            stream: Whether to stream the response
            
        Returns:
            - Single string if input was string and stream=False
            - Iterator[str] if input was string and stream=True  
            - List[str] if input was list and stream=False
            - List[Iterator[str]] if input was list and stream=True (batch streaming)
        """
        print(f"🎯 [LMStudioTool._run] Called with:")
        print(f"   • input_text type: {type(input_text).__name__}")
        print(f"   • input_text length: {len(input_text) if isinstance(input_text, list) else 'N/A (single string)'}")
        print(f"   • stream: {stream}")
        print(f"   • num_workers: {num_workers}")
        
        # Handle single string input
        if isinstance(input_text, str):
            print(f"🔸 [LMStudioTool._run] Taking SINGLE input path")
            if stream:
                print(f"🔸 [LMStudioTool._run] → Single streaming")
                return self._run_single_stream(input_text, model_param)
            else:
                print(f"🔸 [LMStudioTool._run] → Single non-streaming")
                return self._run_single(input_text, model_param)
        
        # Handle list input - both streaming and non-streaming
        print(f"🔹 [LMStudioTool._run] Taking LIST input path")
        if stream:
            # Batch streaming
            print(f"🔹 [LMStudioTool._run] → List streaming (BATCH STREAMING)")
            return self._run_batch_stream(input_text, model_param, num_workers)
        else:
            # Regular batch processing
            try:
                # Check if we're already in an event loop
                asyncio.get_running_loop()
                # If we're in an event loop, run in a separate thread with new loop
                import threading
                import concurrent.futures
                
                def run_in_thread():
                    # Create a new event loop for this thread
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        return new_loop.run_until_complete(self._run_batch_async(input_text, model_param, num_workers))
                    finally:
                        new_loop.close()
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_in_thread)
                    return future.result()
                    
            except RuntimeError:
                # No event loop running, safe to use asyncio.run()
                return asyncio.run(self._run_batch_async(input_text, model_param, num_workers))

    def _run_single(
        self,
        text: str,
        model_param: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Process a single text input"""
        
        user_content = text
        if not self.enable_thinking:
            user_content = user_content.rstrip() + "\n/no_think"

        # Use the first model for single requests
        model_name = self.model_names[0]

        payload = {
            "model": model_name,
            "messages": [{"role": "user", "content": user_content}],
            "stream": False
        }

        # Add model parameters if provided
        if model_param:
            do_sample = model_param.get("do_sample", True)
            if "max_tokens" in model_param:
                payload["max_tokens"] = model_param["max_tokens"]
            if "temperature" in model_param:
                payload["temperature"] = 0.0 if not do_sample else model_param["temperature"]
            if "top_p" in model_param:
                payload["top_p"] = model_param["top_p"]

        try:
            response = requests.post(self.url, json=payload)
            if response.status_code == 200:
                data = response.json()
                raw_content = data["choices"][0]["message"]["content"]
                return self._preprocess_text(raw_content)
            else:
                return f"Error: {response.status_code}"
        except Exception as e:
            return f"Exception: {e}"

    def _run_single_stream(
        self,
        text: str,
        model_param: Optional[Dict[str, Any]] = None,
    ) -> Iterator[str]:
        """Process a single text input with streaming"""
        
        user_content = text
        if not self.enable_thinking:
            user_content = user_content.rstrip() + "\n/no_think"

        # Use the first model for single requests
        model_name = self.model_names[0]

        payload = {
            "model": model_name,
            "messages": [{"role": "user", "content": user_content}],
            "stream": True
        }

        # Add model parameters if provided
        if model_param:
            do_sample = model_param.get("do_sample", True)
            if "max_tokens" in model_param:
                payload["max_tokens"] = model_param["max_tokens"]
            if "temperature" in model_param:
                payload["temperature"] = 0.0 if not do_sample else model_param["temperature"]
            if "top_p" in model_param:
                payload["top_p"] = model_param["top_p"]

        try:
            response = requests.post(self.url, json=payload, stream=True)
            if response.status_code == 200:
                for line in response.iter_lines():
                    if line:
                        line = line.decode('utf-8')
                        if line.startswith('data: '):
                            line = line[6:]  # Remove 'data: ' prefix
                            if line == '[DONE]':
                                break
                            try:
                                data = json.loads(line)
                                if 'choices' in data and len(data['choices']) > 0:
                                    delta = data['choices'][0].get('delta', {})
                                    content = delta.get('content', '')
                                    if content:
                                        yield content
                            except json.JSONDecodeError:
                                continue
            else:
                yield f"Error: {response.status_code}"
        except Exception as e:
            yield f"Exception: {e}"

    def _run_batch_stream(
        self,
        input_texts: List[str],
        model_param: Optional[Dict[str, Any]] = None,
        num_workers: int = 50,
    ) -> List[Iterator[str]]:
        """
        Process batch of texts with streaming for each input.
        
        Returns:
            List of iterators, one for each input text
        """
        n = len(input_texts)
        print(f"🏗️  [LMStudioTool] Setting up batch streaming:")
        print(f"   • Creating {n} streaming queues...")
        print(f"   • Using {num_workers} workers")
        
        # Create a streaming queue for each input
        streaming_queues = [StreamingQueue() for _ in range(n)]
        print(f"✅ [LMStudioTool] Created {len(streaming_queues)} StreamingQueue objects")
        
        def run_async_streaming():
            try:
                # Check if we're already in an event loop
                asyncio.get_running_loop()
                # If we're in an event loop, run in a separate thread with new loop
                import threading
                import concurrent.futures
                
                def run_in_thread():
                    # Create a new event loop for this thread
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        return new_loop.run_until_complete(
                            self._run_batch_stream_async(input_texts, streaming_queues, model_param, num_workers)
                        )
                    finally:
                        new_loop.close()
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_in_thread)
                    future.result()
                    
            except RuntimeError:
                # No event loop running, safe to use asyncio.run()
                asyncio.run(self._run_batch_stream_async(input_texts, streaming_queues, model_param, num_workers))
        
        # Start the async processing in a separate thread
        threading.Thread(target=run_async_streaming, daemon=True).start()
        
        # Return the iterators immediately
        return streaming_queues

    async def _run_batch_stream_async(
        self,
        input_texts: List[str],
        streaming_queues: List[StreamingQueue], 
        model_param: Optional[Dict[str, Any]] = None,
        num_workers: int = 50
    ):
        """Async batch streaming implementation"""
        n = len(input_texts)
        print(f"🚀 [LMStudioTool] Starting async batch streaming:")
        print(f"   • Processing {n} texts")
        print(f"   • Available models: {self.model_names}")
        print(f"   • Workers: {num_workers}")
        
        # Build queue of indices
        queue = asyncio.Queue()
        for i in range(n):
            queue.put_nowait(i)
        
        print(f"📋 [LMStudioTool] Created work queue with {queue.qsize()} items")

        # Create progress bar
        pbar = tqdm(total=n, desc="Processing LM Studio streaming requests", unit="req")

        async with aiohttp.ClientSession() as session:
            # Worker function for streaming
            async def worker(model_name: str):
                worker_processed = 0
                while not queue.empty():
                    try:
                        idx = await queue.get()
                        await self._send_stream_request_async(
                            session, input_texts[idx], streaming_queues[idx], idx, model_name, model_param
                        )
                        worker_processed += 1
                        pbar.update(1)
                        queue.task_done()
                    except asyncio.QueueEmpty:
                        break
                print(f"⚡ [LMStudioTool] Worker '{model_name}' processed {worker_processed} requests")

            # Start workers - one per model for load balancing
            print(f"👷 [LMStudioTool] Starting {len(self.model_names)} async workers...")
            tasks = [asyncio.create_task(worker(model_name)) for model_name in self.model_names]
            
            start = time.time()
            await asyncio.gather(*tasks)
            pbar.close()
            elapsed = time.time() - start
            print(f"✅ [LMStudioTool] All async workers completed in {elapsed:.2f} seconds")
            print(f"   • Processed {n} requests")
            print(f"   • Average: {n/elapsed:.1f} requests/second")

    async def _send_stream_request_async(
        self, 
        session, 
        text: str, 
        stream_queue: StreamingQueue, 
        idx: int, 
        model_name: str, 
        model_param: Optional[Dict[str, Any]] = None
    ):
        """Send async streaming request to LM Studio endpoint"""
        try:
            user_content = text
            if not self.enable_thinking:
                user_content = user_content.rstrip() + "\n/no_think"

            payload = {
                "model": model_name,
                "messages": [{"role": "user", "content": user_content}],
                "stream": True
            }

            # Add model parameters if provided
            if model_param:
                do_sample = model_param.get("do_sample", True)
                if "max_tokens" in model_param:
                    payload["max_tokens"] = model_param["max_tokens"]
                if "temperature" in model_param:
                    payload["temperature"] = 0.0 if not do_sample else model_param["temperature"]
                if "top_p" in model_param:
                    payload["top_p"] = model_param["top_p"]

            async with session.post(self.url, json=payload) as resp:
                if resp.status == 200:
                    async for line in resp.content:
                        line = line.decode('utf-8').strip()
                        if line.startswith('data: '):
                            line = line[6:]  # Remove 'data: ' prefix
                            if line == '[DONE]':
                                break
                            try:
                                data = json.loads(line)
                                if 'choices' in data and len(data['choices']) > 0:
                                    delta = data['choices'][0].get('delta', {})
                                    content = delta.get('content', '')
                                    if content:
                                        stream_queue.put(content)
                            except json.JSONDecodeError:
                                continue
                    stream_queue.finish()
                else:
                    stream_queue.error(f"HTTP {resp.status}")
        except Exception as e:
            stream_queue.error(str(e))

    async def _send_request_async(self, session, text: str, results: List, idx: int, model_name: str, model_param: Optional[Dict[str, Any]] = None):
        """Send async request to LM Studio endpoint"""
        try:
            user_content = text
            if not self.enable_thinking:
                user_content = user_content.rstrip() + "\n/no_think"

            payload = {
                "model": model_name,
                "messages": [{"role": "user", "content": user_content}],
                "stream": False
            }

            # Add model parameters if provided
            if model_param:
                do_sample = model_param.get("do_sample", True)
                if "max_tokens" in model_param:
                    payload["max_tokens"] = model_param["max_tokens"]
                if "temperature" in model_param:
                    payload["temperature"] = 0.0 if not do_sample else model_param["temperature"]
                if "top_p" in model_param:
                    payload["top_p"] = model_param["top_p"]

            async with session.post(self.url, json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    raw_content = data["choices"][0]["message"]["content"]
                    results[idx] = self._preprocess_text(raw_content)
                    # print(f"[DEBUG] Successfully processed request {idx} with model {model_name}")
                else:
                    results[idx] = f""
                    print(f"[ERROR] Request {idx} failed with status {resp.status} for model {model_name}")
        except Exception as e:
            results[idx] = f""
            print(f"[ERROR] Exception for request {idx} with model {model_name}: {e}")

    async def _run_batch_async(
        self, 
        input_texts: List[str], 
        model_param: Optional[Dict[str, Any]] = None,
        num_workers: int = 50
    ) -> List[str]:
        """Process batch of texts with async load balancing across multiple models"""
        n = len(input_texts)
        results = [None] * n
        
        # Build queue of indices
        queue = asyncio.Queue()
        for i in range(n):
            queue.put_nowait(i)

        # Create progress bar
        pbar = tqdm(total=n, desc="Processing LM Studio requests", unit="req")

        async with aiohttp.ClientSession() as session:
            # Worker function for load balancing across models
            async def worker(model_name: str):
                while not queue.empty():
                    try:
                        idx = await queue.get()
                        # print(f"[DEBUG] Model {model_name} processing request {idx}")
                        await self._send_request_async(session, input_texts[idx], results, idx, model_name, model_param)
                        pbar.update(1)
                        queue.task_done()
                    except asyncio.QueueEmpty:
                        break

            # Start workers - one per model for load balancing
            tasks = [asyncio.create_task(worker(model_name)) for model_name in self.model_names]
            
            start = time.time()
            await asyncio.gather(*tasks)
            pbar.close()
            print(f"[INFO] Batch processing completed in {time.time() - start:.2f} seconds.")

        return results

    async def _arun(
        self,
        input_text: Union[str, List[str]],
        model_param: Optional[Dict[str, Any]] = {"max_tokens": 4096},
        num_workers: int = 50,
        stream: bool = False,
    ) -> Union[str, List[str], Iterator[str], List[Iterator[str]]]:
        """Async wrapper around `_run`."""
        return self._run(input_text, model_param, num_workers, stream)

    


class OllamaTool(BaseTool, BaseModel):
    """
    Tool wrapping Ollama endpoint for use with LangGraph.
    """
    name: str = "ollama_tool"
    description: str = "Send chat requests to Ollama endpoint."
    
    # Parameters without defaults
    endpoint: Optional[str] = None
    model_name: Optional[str] = None
    enable_thinking: Optional[bool] = None
    client: Any = None
    
    # Flag to prevent multiple initializations
    _initialized: bool = False

    def __init__(self, **kwargs):
        # Store important values before BaseModel init
        model_name_val = kwargs.get("model_name")
        endpoint_val = kwargs.get("endpoint")
        enable_thinking_val = kwargs.get("enable_thinking")
        
        # Initialize core classes
        BaseTool.__init__(self)
        BaseModel.__init__(self, **kwargs)
        
        # Skip if already initialized
        if self._initialized:
            return
            
        # Set values from constructor params
        if model_name_val:
            self.model_name = model_name_val
        if endpoint_val:
            self.endpoint = endpoint_val
        if enable_thinking_val is not None:
            self.enable_thinking = enable_thinking_val
            
        # Set defaults if not provided
        if not self.model_name:
            self.model_name = "qwen3:32b"
        if not self.endpoint:
            self.endpoint = "http://localhost:11434"
        if self.enable_thinking is None:
            self.enable_thinking = False
            
        # Initialize client
        self.client = Client(host=self.endpoint)
        
        # Mark as initialized
        self._initialized = True
        
        print(f"OllamaTool initialized with model: '{self.model_name}', endpoint: '{self.endpoint}'")

    def _preprocess_text(self, response: dict) -> str:
        content = response["message"]["content"]
        return re.sub(r"<think>.*?</think>\n?", "", content, flags=re.DOTALL).strip()

    def _run(
        self,
        input_text: Union[str, List[str]],
        model_param: Optional[Dict[str, Any]] = None,
        num_workers: int = 50,
        stream: bool = False,
    ) -> Union[str, List[str], Iterator[str], List[Iterator[str]]]:
        """
        Send chat message(s) to Ollama and return the assistant response(s).
        
        Args:
            input_text: Single string or list of strings to process
            model_param: Model parameters
            num_workers: Number of concurrent workers for batch processing
            stream: Whether to stream the response
            
        Returns:
            - Single string if input was string and stream=False
            - Iterator[str] if input was string and stream=True  
            - List[str] if input was list and stream=False
            - List[Iterator[str]] if input was list and stream=True (batch streaming)
        """
        # Handle single string input
        if isinstance(input_text, str):
            if stream:
                return self._run_single_stream(input_text, model_param)
            else:
                return self._run_single(input_text, model_param)
        
        # Handle list input - both streaming and non-streaming
        if stream:
            # Batch streaming
            return self._run_batch_stream(input_text, model_param, num_workers)
        else:
            # Regular batch processing
            try:
                # Check if we're already in an event loop
                asyncio.get_running_loop()
                # If we're in an event loop, run in a separate thread with new loop
                import threading
                import concurrent.futures
                
                def run_in_thread():
                    # Create a new event loop for this thread
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        return new_loop.run_until_complete(self._run_batch_async(input_text, model_param, num_workers))
                    finally:
                        new_loop.close()
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_in_thread)
                    return future.result()
                    
            except RuntimeError:
                # No event loop running, safe to use asyncio.run()
                return asyncio.run(self._run_batch_async(input_text, model_param, num_workers))

    def _run_single(
        self,
        text: str,
        model_param: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Process a single text input"""
        user_content = text
        if not self.enable_thinking:
            user_content = user_content.rstrip() + " /no_think"

        messages = [{"role": "user", "content": user_content}]

        opts = {}
        if model_param:
            do_sample = model_param.get("do_sample", True)
            for key, value in model_param.items():
                if key == "max_new_tokens":
                    opts["num_predict"] = value
                elif key == "temperature":
                    opts["temperature"] = 0.0 if not do_sample else value
                else:
                    opts[key] = value

        response = self.client.chat(
            model=self.model_name,
            messages=messages,
            options=opts
        )
        return self._preprocess_text(response)

    def _run_single_stream(
        self,
        text: str,
        model_param: Optional[Dict[str, Any]] = None,
    ) -> Iterator[str]:
        """Process a single text input with streaming"""
        user_content = text
        if not self.enable_thinking:
            user_content = user_content.rstrip() + " /no_think"

        messages = [{"role": "user", "content": user_content}]

        opts = {}
        if model_param:
            do_sample = model_param.get("do_sample", True)
            for key, value in model_param.items():
                if key == "max_new_tokens":
                    opts["num_predict"] = value
                elif key == "temperature":
                    opts["temperature"] = 0.0 if not do_sample else value
                else:
                    opts[key] = value

        try:
            stream = self.client.chat(
                model=self.model_name,
                messages=messages,
                options=opts,
                stream=True
            )
            for chunk in stream:
                if 'message' in chunk and 'content' in chunk['message']:
                    content = chunk['message']['content']
                    if content:
                        yield content
        except Exception as e:
            yield f"Exception: {e}"

    async def _send_request_async(self, session, text: str, results: List, idx: int, model_param: Optional[Dict[str, Any]] = None):
        """Send async request to Ollama endpoint"""
        try:
            user_content = text
            if not self.enable_thinking:
                user_content = user_content.rstrip() + " /no_think"

            opts = {}
            if model_param:
                do_sample = model_param.get("do_sample", True)
                for key, value in model_param.items():
                    if key == "max_new_tokens":
                        opts["num_predict"] = value
                    elif key == "temperature":
                        opts["temperature"] = 0.0 if not do_sample else value
                    else:
                        opts[key] = value

            payload = {
                "model": self.model_name,
                "messages": [{"role": "user", "content": user_content}],
                "options": opts,
                "stream": False
            }

            # Extract base URL from endpoint
            chat_url = f"{self.endpoint}/api/chat"
            
            async with session.post(chat_url, json=payload) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    results[idx] = self._preprocess_text(data)
                    # print(f"[DEBUG] Successfully processed request {idx}")
                else:
                    results[idx] = f""
                    print(f"[ERROR] Request {idx} failed with status {resp.status}")
        except Exception as e:
            results[idx] = f""
            print(f"[ERROR] Exception for request {idx}: {e}")

    async def _run_batch_async(
        self, 
        input_texts: List[str], 
        model_param: Optional[Dict[str, Any]] = None,
        num_workers: int = 50
    ) -> List[str]:
        """Process batch of texts with async load balancing"""
        n = len(input_texts)
        results = [None] * n
        
        # Build queue of indices
        queue = asyncio.Queue()
        for i in range(n):
            queue.put_nowait(i)

        # Create progress bar
        pbar = tqdm(total=n, desc="Processing Ollama requests", unit="req")

        async with aiohttp.ClientSession() as session:
            # Worker function for load balancing
            async def worker(worker_id: int):
                while not queue.empty():
                    try:
                        idx = await queue.get()
                        # print(f"[DEBUG] Worker {worker_id} processing request {idx}")
                        await self._send_request_async(session, input_texts[idx], results, idx, model_param)
                        pbar.update(1)
                        queue.task_done()
                    except asyncio.QueueEmpty:
                        break

            # Start workers (limit to reasonable number)
            actual_workers = min(num_workers, n)
            tasks = [asyncio.create_task(worker(i)) for i in range(actual_workers)]
            
            start = time.time()
            await asyncio.gather(*tasks)
            pbar.close()
            print(f"[INFO] Batch processing completed in {time.time() - start:.2f} seconds.")

        return results

    async def _arun(
        self,
        input_text: Union[str, List[str]],
        model_param: Optional[Dict[str, Any]] = {"max_tokens": 4096},
        num_workers: int = 50,
        stream: bool = False,
    ) -> Union[str, List[str], Iterator[str]]:
        """Async wrapper around `_run`."""
        return self._run(input_text, model_param, num_workers, stream)

class VLLMTool(BaseTool, BaseModel):
    """
    Tool wrapping a vLLM endpoint via OpenAI‐compatible API.
    """
    name: str = "vllm_tool"
    description: str = "Send chat requests to a vLLM endpoint (OpenAI‐compatible)."

    # Parameters without defaults
    endpoint: Optional[str] = None
    model_name: Optional[str] = None
    
    # Flag to prevent multiple initializations
    _initialized: bool = False

    def __init__(self, **kwargs):
        # Store values before BaseModel init
        model_name_val = kwargs.get("model_name")
        endpoint_val = kwargs.get("endpoint")
        
        # Initialize core classes
        BaseTool.__init__(self)
        BaseModel.__init__(self, **kwargs)
        
        # Skip if already initialized
        if self._initialized:
            return
        
        # Set values from constructor params
        if model_name_val:
            self.model_name = model_name_val
        if endpoint_val:
            self.endpoint = endpoint_val
            
        # Set defaults if not provided
        if not self.model_name:
            self.model_name = "google/gemma-2-9b-it"
        if not self.endpoint:
            self.endpoint = "http://localhost:6000/v1"
            
        # Mark as initialized
        self._initialized = True
        
        print(f"VLLMTool initialized with model: '{self.model_name}', endpoint: '{self.endpoint}'")

    def _run(
        self,
        input_text: Union[str, List[str]],
        model_param: Optional[Dict[str, Any]] = None,
        num_workers: int = 50,
        stream: bool = False,
    ) -> Union[str, List[str], Iterator[str]]:
        """
        Send chat message(s) to vLLM and return the assistant response(s).
        
        Args:
            input_text: Single string or list of strings to process
            model_param: Model parameters
            num_workers: Number of concurrent workers for batch processing
            stream: Whether to stream the response
            
        Returns:
            Single string if input was string, list of strings if input was list, or Iterator if streaming
        """
        # Handle single string input
        if isinstance(input_text, str):
            if stream:
                return self._run_single_stream(input_text, model_param)
            else:
                return self._run_single(input_text, model_param)
        
        # For batch processing, streaming doesn't make sense
        if stream:
            raise ValueError("Streaming is not supported for batch processing")
            
        # Handle list input with async load balancing
        try:
            # Check if we're already in an event loop
            asyncio.get_running_loop()
            # If we're in an event loop, run in a separate thread with new loop
            import threading
            import concurrent.futures
            
            def run_in_thread():
                # Create a new event loop for this thread
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    return new_loop.run_until_complete(self._run_batch_async(input_text, model_param, num_workers))
                finally:
                    new_loop.close()
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_in_thread)
                return future.result()
                
        except RuntimeError:
            # No event loop running, safe to use asyncio.run()
            return asyncio.run(self._run_batch_async(input_text, model_param, num_workers))

    def _run_single(
        self,
        text: str,
        model_param: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Process a single text input"""
        openai.api_key = "EMPTY"
        openai.api_base = self.endpoint

        params: Dict[str, Any] = {
            "model": self.model_name,
            "messages": [{"role": "user", "content": text}],
        }
        if model_param:
            for key in (
                "temperature",
                "max_tokens",
                "top_p",
                "n",
                "stop",
                "frequency_penalty",
                "presence_penalty",
            ):
                if key in model_param:
                    params[key] = model_param[key]

        response = openai.chat.completions.create(**params)
        return response.choices[0].message.content.strip()

    def _run_single_stream(
        self,
        text: str,
        model_param: Optional[Dict[str, Any]] = None,
    ) -> Iterator[str]:
        """Process a single text input with streaming"""
        openai.api_key = "EMPTY"
        openai.api_base = self.endpoint

        params: Dict[str, Any] = {
            "model": self.model_name,
            "messages": [{"role": "user", "content": text}],
            "stream": True
        }
        if model_param:
            for key in (
                "temperature",
                "max_tokens",
                "top_p",
                "n",
                "stop",
                "frequency_penalty",
                "presence_penalty",
            ):
                if key in model_param:
                    params[key] = model_param[key]

        try:
            stream = openai.chat.completions.create(**params)
            for chunk in stream:
                if chunk.choices[0].delta.content is not None:
                    yield chunk.choices[0].delta.content
        except Exception as e:
            yield f"Exception: {e}"

    async def _send_request_async(self, session, text: str, results: List, idx: int, model_param: Optional[Dict[str, Any]] = None):
        """Send async request to vLLM endpoint"""
        try:
            params = {
                "model": self.model_name,
                "messages": [{"role": "user", "content": text}],
            }
            if model_param:
                for key in (
                    "temperature",
                    "max_tokens",
                    "top_p",
                    "n",
                    "stop",
                    "frequency_penalty",
                    "presence_penalty",
                ):
                    if key in model_param:
                        params[key] = model_param[key]

            # Make async HTTP request to vLLM endpoint
            chat_url = f"{self.endpoint}/chat/completions"
            headers = {"Authorization": "Bearer EMPTY", "Content-Type": "application/json"}
            
            async with session.post(chat_url, json=params, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    results[idx] = data["choices"][0]["message"]["content"].strip()
                    # print(f"[DEBUG] Successfully processed request {idx}")
                else:
                    results[idx] = f""
                    print(f"[ERROR] Request {idx} failed with status {resp.status}")
        except Exception as e:
            results[idx] = f""
            print(f"[ERROR] Exception for request {idx}: {e}")

    async def _run_batch_async(
        self, 
        input_texts: List[str], 
        model_param: Optional[Dict[str, Any]] = None,
        num_workers: int = 50
    ) -> List[str]:
        """Process batch of texts with async load balancing"""
        n = len(input_texts)
        results = [None] * n
        
        # Build queue of indices
        queue = asyncio.Queue()
        for i in range(n):
            queue.put_nowait(i)

        # Create progress bar
        pbar = tqdm(total=n, desc="Processing vLLM requests", unit="req")

        async with aiohttp.ClientSession() as session:
            # Worker function for load balancing
            async def worker(worker_id: int):
                while not queue.empty():
                    try:
                        idx = await queue.get()
                        # print(f"[DEBUG] Worker {worker_id} processing request {idx}")
                        await self._send_request_async(session, input_texts[idx], results, idx, model_param)
                        pbar.update(1)
                        queue.task_done()
                    except asyncio.QueueEmpty:
                        break

            # Start workers (limit to reasonable number)
            actual_workers = min(num_workers, n)
            tasks = [asyncio.create_task(worker(i)) for i in range(actual_workers)]
            
            start = time.time()
            await asyncio.gather(*tasks)
            pbar.close()
            print(f"[INFO] Batch processing completed in {time.time() - start:.2f} seconds.")

        return results

    async def _arun(
        self,
        input_text: Union[str, List[str]],
        model_param: Optional[Dict[str, Any]] = {"max_tokens": 4096, "temperature": 0.7, "top_p": 0.95, "top_k": 40},
        num_workers: int = 50,
        stream: bool = False,
    ) -> Union[str, List[str], Iterator[str]]:
        """Async wrapper around `_run`."""
        return self._run(input_text, model_param, num_workers, stream)


class OpenAITool(BaseTool, BaseModel):
    """
    Tool wrapping OpenAI's official ChatCompletion API (with retry/backoff).
    """
    name: str = "openai_tool"
    description: str = "Send chat requests to OpenAI's ChatCompletion endpoint."

    # Parameters without defaults
    model_name: Optional[str] = None
    endpoint: Optional[str] = None
    api_key: Optional[str] = None
    
    # Flag to prevent multiple initializations
    _initialized: bool = False

    def __init__(self, **kwargs):
        # Store values before BaseModel init
        model_name_val = kwargs.get("model_name")
        endpoint_val = kwargs.get("endpoint")
        api_key_val = kwargs.get("api_key")
        
        # Initialize core classes
        BaseTool.__init__(self)
        BaseModel.__init__(self, **kwargs)
        
        # Skip if already initialized
        if self._initialized:
            return
            
        # Set values from constructor params
        if model_name_val:
            self.model_name = model_name_val
        if endpoint_val:
            self.endpoint = endpoint_val
        if api_key_val:
            self.api_key = api_key_val
            
        # Set defaults if not provided
        if not self.model_name:
            self.model_name = "gpt-4o"
        if not self.endpoint:
            self.endpoint = "https://api.openai.com/v1"
        if not self.api_key:
            self.api_key = os.environ.get("open_ai_key")
            
        # Mark as initialized
        self._initialized = True
        
        print(f"OpenAITool initialized with model: '{self.model_name}', endpoint: '{self.endpoint}'")

    def _run(
        self,
        input_text: Union[str, List[str]],
        model_param: Optional[Dict[str, Any]] = {"max_tokens": 4096, "temperature": 0.7, "top_p": 0.95, "top_k": 40},
        max_retries: int = 3,
        backoff_time: float = 10.0,
        num_workers: int = 50,
        stream: bool = False,
    ) -> Union[str, List[str], Iterator[str], List[Iterator[str]]]:
        """
        Send chat message(s) to OpenAI and return the assistant response(s).
        Retries with exponential backoff on failure.
        
        Args:
            input_text: Single string or list of strings to process
            model_param: Model parameters
            max_retries: Maximum retry attempts
            backoff_time: Initial backoff time for retries
            num_workers: Number of concurrent workers for batch processing
            stream: Whether to stream the response
            
        Returns:
            - Single string if input was string and stream=False
            - Iterator[str] if input was string and stream=True  
            - List[str] if input was list and stream=False
            - List[Iterator[str]] if input was list and stream=True (batch streaming)
        """
        print(f"🎯 [OpenAITool._run] Called with:")
        print(f"   • input_text type: {type(input_text).__name__}")
        print(f"   • input_text length: {len(input_text) if isinstance(input_text, list) else 'N/A (single string)'}")
        print(f"   • stream: {stream}")
        print(f"   • num_workers: {num_workers}")
        
        # Handle single string input
        if isinstance(input_text, str):
            print(f"🔸 [OpenAITool._run] Taking SINGLE input path")
            if stream:
                print(f"🔸 [OpenAITool._run] → Single streaming")
                return self._run_single_stream(input_text, model_param, max_retries, backoff_time)
            else:
                print(f"🔸 [OpenAITool._run] → Single non-streaming")
                return self._run_single(input_text, model_param, max_retries, backoff_time)
        
        # Handle list input - both streaming and non-streaming
        print(f"🔹 [OpenAITool._run] Taking LIST input path")
        if stream:
            # Batch streaming - NEW FUNCTIONALITY!
            print(f"🔹 [OpenAITool._run] → List streaming (BATCH STREAMING)")
            return self._run_batch_stream(input_text, model_param, max_retries, backoff_time, num_workers)
            
        # Handle list input with async load balancing
        try:
            # Check if we're already in an event loop
            asyncio.get_running_loop()
            # If we're in an event loop, run in a separate thread with new loop
            import threading
            import concurrent.futures
            
            def run_in_thread():
                # Create a new event loop for this thread
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    return new_loop.run_until_complete(self._run_batch_async(input_text, model_param, max_retries, backoff_time, num_workers))
                finally:
                    new_loop.close()
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_in_thread)
                return future.result()
                
        except RuntimeError:
            # No event loop running, safe to use asyncio.run()
            return asyncio.run(self._run_batch_async(input_text, model_param, max_retries, backoff_time, num_workers))

    def _run_single(
        self,
        text: str,
        model_param: Optional[Dict[str, Any]] = {"max_tokens": 4096, "temperature": 0.7, "top_p": 0.95, "top_k": 40},
        max_retries: int = 3,
        backoff_time: float = 10.0,
    ) -> str:
        """Process a single text input with retries"""
        if not self.api_key:
            raise ValueError("Please set your OpenAI API key.")
        openai.api_key = self.api_key
        openai.api_base = self.endpoint
        # print("--------------------------------")
        # print(f"[DEBUG] model_param: {model_param}")
        # print("--------------------------------")
        if model_param:
            do_sample = model_param.get("do_sample", True) 
        else:
            do_sample = True
            model_param = {"max_tokens": 4096, "temperature": 0.7, "top_p": 0.95, "top_k": 40}
        # print(f"[DEBUG] do_sample: {do_sample}")
        params = {
            "model": self.model_name,
            "messages": [{"role": "user", "content": text}],
            "temperature": model_param.get("temperature", 0.0 if not do_sample else 0.7),
            "max_tokens": model_param.get("max_tokens", 4096),
            "top_p": model_param.get("top_p", 1.0),
            "frequency_penalty": model_param.get("frequency_penalty", 0),
            "presence_penalty": model_param.get("presence_penalty", 0),
        }

        attempts = 0
        while True:
            try:
                response = openai.chat.completions.create(**params)
                return response.choices[0].message.content.strip()
            except OpenAIError as e:
                attempts += 1
                if attempts >= max_retries:
                    raise
                time.sleep(backoff_time)
                backoff_time *= 2

    def _run_single_stream(
        self,
        text: str,
        model_param: Optional[Dict[str, Any]] = {"max_tokens": 4096, "temperature": 0.7, "top_p": 0.95, "top_k": 40},
        max_retries: int = 3,
        backoff_time: float = 10.0,
    ) -> Iterator[str]:
        """Process a single text input with streaming and retries"""
        if not self.api_key:
            yield "Error: Please set your OpenAI API key."
            return
        
        openai.api_key = self.api_key
        openai.api_base = self.endpoint
        
        if model_param:
            do_sample = model_param.get("do_sample", True) 
        else:
            do_sample = True
            
        params = {
            "model": self.model_name,
            "messages": [{"role": "user", "content": text}],
            "temperature": model_param.get("temperature", 0.0 if not do_sample else 0.7) if model_param else 0.7,
            "max_tokens": model_param.get("max_tokens", 4096) if model_param else 4096,
            "top_p": model_param.get("top_p", 1.0) if model_param else 1.0,
            "frequency_penalty": model_param.get("frequency_penalty", 0) if model_param else 0,
            "presence_penalty": model_param.get("presence_penalty", 0) if model_param else 0,
            "stream": True
        }

        attempts = 0
        while attempts < max_retries:
            try:
                stream = openai.chat.completions.create(**params)
                for chunk in stream:
                    if chunk.choices[0].delta.content is not None:
                        yield chunk.choices[0].delta.content
                return
            except OpenAIError as e:
                attempts += 1
                if attempts >= max_retries:
                    yield f"Error after {max_retries} attempts: {str(e)}"
                    return
                time.sleep(backoff_time)
                backoff_time *= 2
            except Exception as e:
                yield f"Exception: {e}"
                return

    def _run_batch_stream(
        self,
        input_texts: List[str],
        model_param: Optional[Dict[str, Any]] = {"max_tokens": 4096, "temperature": 0.7, "top_p": 0.95, "top_k": 40},
        max_retries: int = 3,
        backoff_time: float = 10.0,
        num_workers: int = 50,
    ) -> List[Iterator[str]]:
        """
        Process batch of texts with streaming for each input.
        
        Returns:
            List of iterators, one for each input text
        """
        n = len(input_texts)
        print(f"🏗️  [OpenAITool] Setting up batch streaming:")
        print(f"   • Creating {n} streaming queues...")
        print(f"   • Using {num_workers} workers")
        
        # Create a streaming queue for each input
        streaming_queues = [StreamingQueue() for _ in range(n)]
        print(f"✅ [OpenAITool] Created {len(streaming_queues)} StreamingQueue objects")
        
        def run_async_streaming():
            try:
                # Check if we're already in an event loop
                asyncio.get_running_loop()
                # If we're in an event loop, run in a separate thread with new loop
                import threading
                import concurrent.futures
                
                def run_in_thread():
                    # Create a new event loop for this thread
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        return new_loop.run_until_complete(
                            self._run_batch_stream_async(input_texts, streaming_queues, model_param, max_retries, backoff_time, num_workers)
                        )
                    finally:
                        new_loop.close()
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_in_thread)
                    future.result()
                    
            except RuntimeError:
                # No event loop running, safe to use asyncio.run()
                asyncio.run(self._run_batch_stream_async(input_texts, streaming_queues, model_param, max_retries, backoff_time, num_workers))
        
        # Start the async processing in a separate thread
        threading.Thread(target=run_async_streaming, daemon=True).start()
        
        # Return the iterators immediately
        return streaming_queues

    async def _run_batch_stream_async(
        self,
        input_texts: List[str],
        streaming_queues: List[StreamingQueue], 
        model_param: Optional[Dict[str, Any]] = {"max_tokens": 4096, "temperature": 0.7, "top_p": 0.95, "top_k": 40},
        max_retries: int = 3,
        backoff_time: float = 10.0,
        num_workers: int = 50
    ):
        """Async batch streaming implementation"""
        n = len(input_texts)
        print(f"🚀 [OpenAITool] Starting async batch streaming:")
        print(f"   • Processing {n} texts")
        print(f"   • Workers: {num_workers}")
        
        # Build queue of indices
        queue = asyncio.Queue()
        for i in range(n):
            queue.put_nowait(i)
        
        print(f"📋 [OpenAITool] Created work queue with {queue.qsize()} items")

        # Create progress bar
        pbar = tqdm(total=n, desc="Processing OpenAI streaming requests", unit="req")

        async with aiohttp.ClientSession() as session:
            # Worker function for streaming
            async def worker(worker_id: int):
                worker_processed = 0
                while True:
                    try:
                        # Use timeout to avoid hanging indefinitely
                        idx = await asyncio.wait_for(queue.get(), timeout=1.0)
                        await self._send_stream_request_async(
                            session, input_texts[idx], streaming_queues[idx], idx, model_param, max_retries, backoff_time
                        )
                        worker_processed += 1
                        pbar.update(1)
                        queue.task_done()
                    except asyncio.TimeoutError:
                        # No more items available, exit worker
                        break
                    except Exception as e:
                        print(f"[ERROR] Worker {worker_id} encountered error: {e}")
                        break
                print(f"⚡ [OpenAITool] Worker {worker_id} processed {worker_processed} requests")

            # Start workers
            print(f"👷 [OpenAITool] Starting {num_workers} async workers...")
            actual_workers = min(num_workers, n)
            tasks = [asyncio.create_task(worker(i)) for i in range(actual_workers)]
            
            start = time.time()
            await asyncio.gather(*tasks, return_exceptions=True)
            
            # Ensure all queue items are processed
            await queue.join()
            
            pbar.close()
            elapsed = time.time() - start
            print(f"✅ [OpenAITool] All async workers completed in {elapsed:.2f} seconds")
            print(f"   • Processed {n} requests")
            print(f"   • Average: {n/elapsed:.1f} requests/second")

    async def _send_stream_request_async(
        self, 
        session, 
        text: str, 
        stream_queue: StreamingQueue, 
        idx: int, 
        model_param: Optional[Dict[str, Any]] = {"max_tokens": 4096, "temperature": 0.7, "top_p": 0.95, "top_k": 40},
        max_retries: int = 3,
        backoff_time: float = 10.0
    ):
        """Send async streaming request to OpenAI endpoint"""
        if not self.api_key:
            stream_queue.error("API key not set")
            return
        
        if model_param:
            do_sample = model_param.get("do_sample", True) 
        else:
            do_sample = True
            
        params = {
            "model": self.model_name,
            "messages": [{"role": "user", "content": text}],
            "temperature": model_param.get("temperature", 0.0 if not do_sample else 0.7) if model_param else 0.7,
            "max_tokens": model_param.get("max_tokens", 4096) if model_param else 4096,
            "top_p": model_param.get("top_p", 1.0) if model_param else 1.0,
            "frequency_penalty": model_param.get("frequency_penalty", 0) if model_param else 0,
            "presence_penalty": model_param.get("presence_penalty", 0) if model_param else 0,
            "stream": True
        }

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        attempts = 0
        while attempts < max_retries:
            try:
                chat_url = f"{self.endpoint}/chat/completions"
                async with session.post(chat_url, json=params, headers=headers) as resp:
                    if resp.status == 200:
                        async for line in resp.content:
                            line_str = line.decode('utf-8').strip()
                            if line_str.startswith('data: '):
                                data_str = line_str[6:]  # Remove 'data: ' prefix
                                if data_str == '[DONE]':
                                    break
                                try:
                                    data = json.loads(data_str)
                                    if 'choices' in data and len(data['choices']) > 0:
                                        delta = data['choices'][0].get('delta', {})
                                        content = delta.get('content', '')
                                        if content:
                                            stream_queue.put(content)
                                except json.JSONDecodeError:
                                    continue
                        stream_queue.finish()
                        return
                    else:
                        attempts += 1
                        if attempts >= max_retries:
                            stream_queue.error(f"HTTP {resp.status}")
                            return
                        await asyncio.sleep(backoff_time)
                        backoff_time *= 2
            except Exception as e:
                attempts += 1
                if attempts >= max_retries:
                    stream_queue.error(str(e))
                    return
                await asyncio.sleep(backoff_time)
                backoff_time *= 2

    async def _send_request_async(
        self, 
        session, 
        text: str, 
        results: List, 
        idx: int, 
        model_param: Optional[Dict[str, Any]] = {"max_tokens": 4096, "temperature": 0.7, "top_p": 0.95, "top_k": 40},
        max_retries: int = 3,
        backoff_time: float = 10.0
    ):
        """Send async request to OpenAI endpoint"""
        if not self.api_key:
            results[idx] = ""
            return
        
        
        if model_param:
            do_sample = model_param.get("do_sample", True) 
        else:
            do_sample = True
            
        params = {
            "model": self.model_name,
            "messages": [{"role": "user", "content": text}],
            "temperature": model_param.get("temperature", 0.0 if not do_sample else 0.7) if model_param else 0.7,
            "max_tokens": model_param.get("max_tokens", 4096) if model_param else 4096,
            "top_p": model_param.get("top_p", 1.0) if model_param else 1.0,
            "frequency_penalty": model_param.get("frequency_penalty", 0) if model_param else 0,
            "presence_penalty": model_param.get("presence_penalty", 0) if model_param else 0,
        }
        # print(f"[DEBUG] params: {params}")

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        attempts = 0
        while attempts < max_retries:
            try:
                chat_url = f"{self.endpoint}/chat/completions"
                async with session.post(chat_url, json=params, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        results[idx] = data["choices"][0]["message"]["content"].strip()
                        # print(f"[DEBUG] Successfully processed request {idx}")
                        return
                    else:
                        attempts += 1
                        if attempts >= max_retries:
                            results[idx] = f""
                            print(f"[ERROR] Request {idx} failed with status {resp.status}")
                            return
                        await asyncio.sleep(backoff_time)
                        backoff_time *= 2
            except Exception as e:
                attempts += 1
                if attempts >= max_retries:
                    results[idx] = f""
                    print(f"[ERROR] Exception for request {idx}: {e}")
                    return
                await asyncio.sleep(backoff_time)
                backoff_time *= 2

    async def _run_batch_async(
        self, 
        input_texts: List[str], 
        model_param: Optional[Dict[str, Any]] = {"max_tokens": 4096, "temperature": 0.7, "top_p": 0.95, "top_k": 40},
        max_retries: int = 3,
        backoff_time: float = 10.0,
        num_workers: int = 50
    ) -> List[str]:
        """Process batch of texts with async load balancing"""
        n = len(input_texts)
        results = [None] * n
        # print("--------------------------------")
        # print(f"[DEBUG] model_param: {model_param}")
        # print("--------------------------------")
        
        # Build queue of indices
        queue = asyncio.Queue()
        for i in range(n):
            queue.put_nowait(i)

        # Create progress bar
        pbar = tqdm(total=n, desc="Processing OpenAI requests", unit="req")

        async with aiohttp.ClientSession() as session:
            # Worker function for load balancing
            async def worker(worker_id: int):
                while True:
                    try:
                        # Use timeout to avoid hanging indefinitely
                        idx = await asyncio.wait_for(queue.get(), timeout=1.0)
                        # print(f"[DEBUG] Worker {worker_id} processing request {idx}")
                        await self._send_request_async(session, input_texts[idx], results, idx, model_param, max_retries, backoff_time)
                        pbar.update(1)
                        queue.task_done()
                    except asyncio.TimeoutError:
                        # No more items available, exit worker
                        break
                    except Exception as e:
                        print(f"[ERROR] Worker {worker_id} encountered error: {e}")
                        break

            # Start workers (limit to reasonable number)
            actual_workers = min(num_workers, n)
            tasks = [asyncio.create_task(worker(i)) for i in range(actual_workers)]
            
            start = time.time()
            # Wait for all tasks to complete or timeout
            await asyncio.gather(*tasks, return_exceptions=True)
            
            # Ensure all queue items are processed
            await queue.join()
            
            pbar.close()
            print(f"[INFO] Batch processing completed in {time.time() - start:.2f} seconds.")

        return results

class VertexTool(BaseTool, BaseModel):
    """
    Tool wrapping Google's Gemini via google.generativeai.
    """
    name: str = "vertex_tool"
    description: str = "Send chat requests to Google Gemini via google.generativeai."
    
    # Don't set default values for these fields
    model_name: Optional[str] = None 
    api_key: Optional[str] = None
    
    # Parameters that will be set during init but not part of the model
    _initialized: bool = False

    def __init__(self, **kwargs):
        # First call BaseTool.__init__ to avoid recursion
        BaseTool.__init__(self)
        
        # Store the model_name locally before BaseModel init
        model_name_val = kwargs.get("model_name")
        
        # Initialize BaseModel with filtered arguments
        BaseModel.__init__(self, **kwargs)
        
        # Prevent multiple initializations
        if self._initialized:
            return
            
        # Set model_name from constructor if provided
        if model_name_val:
            self.model_name = model_name_val
        
        # Use default if not set
        if not self.model_name:
            self.model_name = "gemini-2.0-flash"
        
        # Get API key from env if not provided
        if not self.api_key:
            self.api_key = os.environ.get("gemini_key")
        
        if not self.api_key:
            raise ValueError("Please set your Vertex AI API key or pass it in the constructor")
            
        # Configure the client
        genai.configure(api_key=self.api_key)
        
        # Mark as initialized
        self._initialized = True
        
        print(f"VertexTool initialized with model: '{self.model_name}'")

    def _run(
        self,
        input_text: Union[str, List[str]],
        model_param: Optional[Dict[str, Any]] = {"max_tokens": 4096, "temperature": 0.7, "top_p": 0.95, "top_k": 40},
        max_retries: int = 3,
        backoff_time: float = 10.0,
        num_workers: int = 50,
        stream: bool = False,
    ) -> Union[str, List[str], Iterator[str], List[Iterator[str]]]:
        """
        Send prompt(s) to Vertex AI and return the generated text(s).
        
        Args:
            input_text: Single string or list of strings to process
            model_param: Model parameters
            max_retries: Maximum retry attempts
            backoff_time: Initial backoff time for retries
            num_workers: Number of concurrent workers for batch processing
            stream: Whether to stream the response
            
        Returns:
            - Single string if input was string and stream=False
            - Iterator[str] if input was string and stream=True  
            - List[str] if input was list and stream=False
            - List[Iterator[str]] if input was list and stream=True (batch streaming)
        """
        print(f"🎯 [VertexTool._run] Called with:")
        print(f"   • input_text type: {type(input_text).__name__}")
        print(f"   • input_text length: {len(input_text) if isinstance(input_text, list) else 'N/A (single string)'}")
        print(f"   • stream: {stream}")
        print(f"   • num_workers: {num_workers}")
        
        # Handle single string input
        if isinstance(input_text, str):
            print(f"🔸 [VertexTool._run] Taking SINGLE input path")
            if stream:
                print(f"🔸 [VertexTool._run] → Single streaming")
                return self._run_single_stream(input_text, model_param, max_retries, backoff_time)
            else:
                print(f"🔸 [VertexTool._run] → Single non-streaming")
                return self._run_single(input_text, model_param, max_retries, backoff_time)
        
        # Handle list input - both streaming and non-streaming
        print(f"🔹 [VertexTool._run] Taking LIST input path")
        if stream:
            # Batch streaming - NEW FUNCTIONALITY!
            print(f"🔹 [VertexTool._run] → List streaming (BATCH STREAMING)")
            return self._run_batch_stream(input_text, model_param, max_retries, backoff_time, num_workers)
            
        # Handle list input with async load balancing
        try:
            # Check if we're already in an event loop
            asyncio.get_running_loop()
            # If we're in an event loop, run in a separate thread with new loop
            import threading
            import concurrent.futures
            
            def run_in_thread():
                # Create a new event loop for this thread
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    return new_loop.run_until_complete(self._run_batch_async(input_text, model_param, max_retries, backoff_time, num_workers))
                finally:
                    new_loop.close()
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_in_thread)
                return future.result()
                
        except RuntimeError:
            # No event loop running, safe to use asyncio.run()
            return asyncio.run(self._run_batch_async(input_text, model_param, max_retries, backoff_time, num_workers))

    def _run_single(
        self,
        text: str,
        model_param: Optional[Dict[str, Any]] = {"max_tokens": 4096, "temperature": 0.7, "top_p": 0.95, "top_k": 40},
        max_retries: int = 3,
        backoff_time: float = 10.0,
    ) -> str:
        """Send a single prompt to Vertex AI and return the generated text."""
        gen_params = {
            "model_name": self.model_name,
            "generation_config": {
                "temperature": 0 if not model_param or not model_param.get("do_sample", True)
                    else model_param.get("temperature", 1),
                "top_p": model_param.get("top_p", 0.95) if model_param else 0.95,
                "top_k": model_param.get("top_k", 40) if model_param else 40,
                "max_output_tokens": model_param.get("max_output_tokens", 8192)
                    if model_param else 8192,
                "response_mime_type": "text/plain",
            },
        }

        attempts = 0
        while True:
            try:
                model = genai.GenerativeModel(**gen_params)
                resp = model.generate_content(text)
                return resp.text
            except Exception as e:
                attempts += 1
                if attempts >= max_retries:
                    raise Exception(f"Failed after {max_retries} attempts: {str(e)}")
                time.sleep(backoff_time)
                backoff_time *= 2

    def _run_single_stream(
        self,
        text: str,
        model_param: Optional[Dict[str, Any]] = {"max_tokens": 4096, "temperature": 0.7, "top_p": 0.95, "top_k": 40},
        max_retries: int = 3,
        backoff_time: float = 10.0,
    ) -> Iterator[str]:
        """Send a single prompt to Vertex AI and return the generated text as a stream."""
        gen_params = {
            "model_name": self.model_name,
            "generation_config": {
                "temperature": 0 if not model_param or not model_param.get("do_sample", True)
                    else model_param.get("temperature", 1),
                "top_p": model_param.get("top_p", 0.95) if model_param else 0.95,
                "top_k": model_param.get("top_k", 40) if model_param else 40,
                "max_output_tokens": model_param.get("max_output_tokens", 8192)
                    if model_param else 8192,
                "response_mime_type": "text/plain",
            },
        }

        attempts = 0
        while attempts < max_retries:
            try:
                model = genai.GenerativeModel(**gen_params)
                stream = model.generate_content(text, stream=True)
                for chunk in stream:
                    if chunk.text:
                        yield chunk.text
                return
            except Exception as e:
                attempts += 1
                if attempts >= max_retries:
                    yield f"Error after {max_retries} attempts: {str(e)}"
                    return
                time.sleep(backoff_time)
                backoff_time *= 2

    def _run_batch_stream(
        self,
        input_texts: List[str],
        model_param: Optional[Dict[str, Any]] = {"max_tokens": 4096, "temperature": 0.7, "top_p": 0.95, "top_k": 40},
        max_retries: int = 3,
        backoff_time: float = 10.0,
        num_workers: int = 50,
    ) -> List[Iterator[str]]:
        """
        Process batch of texts with streaming for each input.
        
        Returns:
            List of iterators, one for each input text
        """
        n = len(input_texts)
        print(f"🏗️  [VertexTool] Setting up batch streaming:")
        print(f"   • Creating {n} streaming queues...")
        print(f"   • Using {num_workers} workers")
        
        # Create a streaming queue for each input
        streaming_queues = [StreamingQueue() for _ in range(n)]
        print(f"✅ [VertexTool] Created {len(streaming_queues)} StreamingQueue objects")
        
        def run_async_streaming():
            try:
                # Check if we're already in an event loop
                asyncio.get_running_loop()
                # If we're in an event loop, run in a separate thread with new loop
                import threading
                import concurrent.futures
                
                def run_in_thread():
                    # Create a new event loop for this thread
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        return new_loop.run_until_complete(
                            self._run_batch_stream_async(input_texts, streaming_queues, model_param, max_retries, backoff_time, num_workers)
                        )
                    finally:
                        new_loop.close()
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_in_thread)
                    future.result()
                    
            except RuntimeError:
                # No event loop running, safe to use asyncio.run()
                asyncio.run(self._run_batch_stream_async(input_texts, streaming_queues, model_param, max_retries, backoff_time, num_workers))
        
        # Start the async processing in a separate thread
        threading.Thread(target=run_async_streaming, daemon=True).start()
        
        # Return the iterators immediately
        return streaming_queues

    async def _run_batch_stream_async(
        self,
        input_texts: List[str],
        streaming_queues: List[StreamingQueue], 
        model_param: Optional[Dict[str, Any]] = {"max_tokens": 4096, "temperature": 0.7, "top_p": 0.95, "top_k": 40},
        max_retries: int = 3,
        backoff_time: float = 10.0,
        num_workers: int = 50
    ):
        """Async batch streaming implementation"""
        n = len(input_texts)
        print(f"🚀 [VertexTool] Starting async batch streaming:")
        print(f"   • Processing {n} texts")
        print(f"   • Workers: {num_workers}")
        
        # Build queue of indices
        queue = asyncio.Queue()
        for i in range(n):
            queue.put_nowait(i)
        
        print(f"📋 [VertexTool] Created work queue with {queue.qsize()} items")

        # Create progress bar
        pbar = tqdm(total=n, desc="Processing Vertex streaming requests", unit="req")

        # Worker function for streaming
        async def worker(worker_id: int):
            worker_processed = 0
            while not queue.empty():
                try:
                    idx = await queue.get()
                    await self._send_stream_request_async(
                        input_texts[idx], streaming_queues[idx], idx, model_param, max_retries, backoff_time
                    )
                    worker_processed += 1
                    pbar.update(1)
                    queue.task_done()
                except asyncio.QueueEmpty:
                    break
                except Exception as e:
                    print(f"[ERROR] Worker {worker_id} encountered error: {e}")
                    break
            print(f"⚡ [VertexTool] Worker {worker_id} processed {worker_processed} requests")

        # Start workers
        print(f"👷 [VertexTool] Starting {num_workers} async workers...")
        actual_workers = min(num_workers, n)
        tasks = [asyncio.create_task(worker(i)) for i in range(actual_workers)]
        
        start = time.time()
        await asyncio.gather(*tasks, return_exceptions=True)
        
        pbar.close()
        elapsed = time.time() - start
        print(f"✅ [VertexTool] All async workers completed in {elapsed:.2f} seconds")
        print(f"   • Processed {n} requests")
        print(f"   • Average: {n/elapsed:.1f} requests/second")

    async def _send_stream_request_async(
        self, 
        text: str, 
        stream_queue: StreamingQueue, 
        idx: int, 
        model_param: Optional[Dict[str, Any]] = {"max_tokens": 4096, "temperature": 0.7, "top_p": 0.95, "top_k": 40},
        max_retries: int = 3,
        backoff_time: float = 10.0
    ):
        """Send async streaming request to Vertex AI"""
        gen_params = {
            "model_name": self.model_name,
            "generation_config": {
                "temperature": 0 if not model_param or not model_param.get("do_sample", True)
                    else model_param.get("temperature", 1),
                "top_p": model_param.get("top_p", 0.95) if model_param else 0.95,
                "top_k": model_param.get("top_k", 40) if model_param else 40,
                "max_output_tokens": model_param.get("max_output_tokens", 8192)
                    if model_param else 8192,
                "response_mime_type": "text/plain",
            },
        }

        attempts = 0
        while attempts < max_retries:
            try:
                # Run the synchronous genai streaming call in a thread pool
                def _sync_generate_stream():
                    model = genai.GenerativeModel(**gen_params)
                    stream = model.generate_content(text, stream=True)
                    chunks = []
                    for chunk in stream:
                        if chunk.text:
                            chunks.append(chunk.text)
                    return chunks
                
                # Use asyncio.to_thread to run the synchronous call in a thread pool
                chunks = await asyncio.to_thread(_sync_generate_stream)
                
                # Put all chunks into the queue
                for chunk in chunks:
                    stream_queue.put(chunk)
                
                stream_queue.finish()
                return
            except Exception as e:
                attempts += 1
                if attempts >= max_retries:
                    stream_queue.error(f"Error after {max_retries} attempts: {str(e)}")
                    return
                await asyncio.sleep(backoff_time)
                backoff_time *= 2

    async def _send_request_async(
        self, 
        text: str, 
        results: List, 
        idx: int, 
        model_param: Optional[Dict[str, Any]] = {"max_tokens": 4096, "temperature": 0.7, "top_p": 0.95, "top_k": 40},
        max_retries: int = 3,
        backoff_time: float = 10.0
    ):
        """Send async request to Vertex AI"""
        gen_params = {
            "model_name": self.model_name,
            "generation_config": {
                "temperature": 0 if not model_param or not model_param.get("do_sample", True)
                    else model_param.get("temperature", 1),
                "top_p": model_param.get("top_p", 0.95) if model_param else 0.95,
                "top_k": model_param.get("top_k", 40) if model_param else 40,
                "max_output_tokens": model_param.get("max_output_tokens", 8192)
                    if model_param else 8192,
                "response_mime_type": "text/plain",
            },
        }
        # print(f"[DEBUG] params: {gen_params}")

        attempts = 0
        while attempts < max_retries:
            try:
                # Run the synchronous genai call in a thread pool to avoid blocking the event loop
                def _sync_generate():
                    model = genai.GenerativeModel(**gen_params)
                    resp = model.generate_content(text)
                    return resp.text
                
                # Use asyncio.to_thread to run the synchronous call in a thread pool
                response_text = await asyncio.to_thread(_sync_generate)
                results[idx] = response_text
                # print(f"[DEBUG] Successfully processed request {idx}")
                return
            except Exception as e:
                attempts += 1
                if attempts >= max_retries:
                    results[idx] = f""
                    print(f"[ERROR] Exception for request {idx}: {e}")
                    return
                await asyncio.sleep(backoff_time)
                backoff_time *= 2
    async def _run_batch_async(
        self, 
        input_texts: List[str], 
        model_param: Optional[Dict[str, Any]] = {"max_tokens": 4096, "temperature": 0.7, "top_p": 0.95, "top_k": 40},
        max_retries: int = 3,
        backoff_time: float = 10.0,
        num_workers: int = 50
    ) -> List[str]:
        """Process batch of texts with async load balancing"""
        n = len(input_texts)
        results = [None] * n
        
        # Build queue of indices
        queue = asyncio.Queue()
        for i in range(n):
            queue.put_nowait(i)

        # Create progress bar
        pbar = tqdm(total=n, desc="Processing Vertex requests", unit="req")

        # Worker function for load balancing
        async def worker(worker_id: int):
            while not queue.empty():
                try:
                    idx = await queue.get()
                    # print(f"[DEBUG] Worker {worker_id} processing request {idx}")
                    await self._send_request_async(input_texts[idx], results, idx, model_param, max_retries, backoff_time)
                    pbar.update(1)
                    queue.task_done()
                except asyncio.QueueEmpty:
                    break

        # Start workers (limit to reasonable number)
        actual_workers = min(num_workers, n)
        tasks = [asyncio.create_task(worker(i)) for i in range(actual_workers)]
        
        start = time.time()
        await asyncio.gather(*tasks)
        pbar.close()
        print(f"[INFO] Batch processing completed in {time.time() - start:.2f} seconds.")

        return results

    async def _arun(
        self,
        input_text: Union[str, List[str]],
        model_param: Optional[Dict[str, Any]] = {"max_tokens": 4096, "temperature": 0.7, "top_p": 0.95, "top_k": 40},
        max_retries: int = 3,
        backoff_time: float = 10.0,
        num_workers: int = 50,
        stream: bool = False,
    ) -> Union[str, List[str], Iterator[str], List[Iterator[str]]]:
        """Async wrapper around `_run`."""
        return self._run(input_text, model_param, max_retries, backoff_time, num_workers, stream)

class LLMManager(BaseModel):
    """Manages LLM tools"""
    model_configs: Dict[str, Dict[str, Any]] = Field(default_factory=lambda: model_configs)
    models: Dict[str, BaseTool] = Field(default_factory=dict)

    def __init__(self, **data: Any):
        super().__init__(**data)
        
        for model_key, config in self.model_configs.items():
            try:
                # Get model type
                model_type = config.get("type")
                if not model_type:
                    print(f"Warning: No model type specified for {model_key}")
                    continue
                    
                # Make a clean copy of the config
                tool_params = {}
                # Always ensure model_name is set to the right value
                tool_params["model_name"] = config.get("model_name", model_key)
                
                # Add other params as needed
                if "api_key" in config:
                    tool_params["api_key"] = config["api_key"]
                if "endpoint" in config:
                    tool_params["endpoint"] = config["endpoint"]
                if "url" in config:
                    tool_params["url"] = config["url"]
                if "enable_thinking" in config:
                    tool_params["enable_thinking"] = config["enable_thinking"]
                
                # # Print config for debugging
                # print(f"\nCreating {model_type} tool for {model_key}:")
                # for k, v in tool_params.items():
                #     if k == "api_key" and v:
                #         print(f"  {k}: [API KEY HIDDEN]")
                #     else:
                #         print(f"  {k}: {v}")
                
                # Create the appropriate tool
                if model_type == "vertex":
                    self.models[model_key] = VertexTool(**tool_params)
                elif model_type == "openai":
                    self.models[model_key] = OpenAITool(**tool_params)
                elif model_type == "vllm":
                    self.models[model_key] = VLLMTool(**tool_params)
                elif model_type == "ollama":
                    self.models[model_key] = OllamaTool(**tool_params)
                elif model_type == "lmstudio":
                    self.models[model_key] = LMStudioTool(**tool_params)
                else:
                    print(f"Unknown model type: {model_type} for {model_key}")
                    continue
                    
            except Exception as e:
                print(f"Error creating tool for {model_key}: {e}")
                import traceback
                traceback.print_exc()
    
    def test_models(self):
        """Test all models by printing their attributes"""
        print("\nTesting all models:")
        for key, tool in self.models.items():
            print(f"\n{key}:")
            print(f"  Class: {tool.__class__.__name__}")
            print(f"  Model name: {tool.model_name}")

    def get_tool(self, model_name: str) -> Optional[BaseTool]:
        """
        Retrieve the BaseTool instance for a given model name,
        or None if not configured.
        """
        return self.models.get(model_name)

    def create_tool_node(self) -> ToolNode:
        """
        Wrap *all* configured tools in a single ToolNode (for LangGraph).
        """
        return ToolNode(tools=list(self.models.values()))



if __name__ == "__main__":
    llm_manager = LLMManager()
    # openai_tool = llm_manager.get_tool("gpt-4o-mini")
    openai_tool = llm_manager.get_tool("gpt-4o")
    # openai_tool = llm_manager.get_tool("gemini-1.5-pro")
    # openai_tool = llm_manager.get_tool("qwen3:8b_LM")
    # openai_tool = llm_manager.get_tool("qwen3:32b_LM")
    # print(f"llm_manager.models: {llm_manager.models}")
    # openai_tool = llm_manager.get_tool("gemini-2.5-pro")
    # openai_tool = llm_manager.get_tool("gemini-2.5-flash")
    # openai_tool = llm_manager.get_tool("gemini-2.5-flash-lite-preview-06-17")
    print("---------------------------")
    print(openai_tool)
    if openai_tool:
        # # Test non-streaming
        # print("=== Testing Non-Streaming ===")
        # result = openai_tool.invoke({
        #     "input_text": "What's the capital of India? Explain in 100 words.",
        #     "model_param": None,
        #     "num_workers": 50,
        #     "stream": False
        # })
        # print("OpenAI non-streaming says:", result)
        
        # # Test single streaming
        # print("\n=== Testing Single Streaming ===")
        # print("OpenAI streaming response: ", end="", flush=True)
        # stream_result = openai_tool.invoke({
        #     "input_text": "What's the capital of France? Explain in 100 words.",
        #     "model_param": None,
        #     "num_workers": 50,
        #     "stream": True
        # })
        # full_response = ""
        # for chunk in stream_result:
        #     print(chunk, end="", flush=True)
        #     full_response += chunk
        # print(f"\n✅ Single streaming complete. Total length: {len(full_response)} chars")
        
        # Test batch streaming
        print("\n=== Testing Batch Streaming ===")
        batch_prompts = [
            "What's physics? Brief answer.",
            "What's the AI? Brief answer.", 
            "What's the chemistry of water? Brief answer."
        ]
        batch_streams = openai_tool.invoke({
            "input_text": batch_prompts,
            "model_param": None,
            "num_workers": 50,
            "stream": True
        })
        
        print(f"Got {len(batch_streams)} iterators for batch streaming")
        for i, (prompt, iterator) in enumerate(zip(batch_prompts, batch_streams)):
            print(f"\nPrompt {i+1}: {prompt}")
            print(f"Response {i+1}: ", end="", flush=True)
            response = ""
            for chunk in iterator:
                print(chunk, end="", flush=True)
                response += chunk
            print(f" ✅ ({len(response)} chars)")
        
        print("\n✅ All streaming tests completed!")

    # python3 -m llm_models.models