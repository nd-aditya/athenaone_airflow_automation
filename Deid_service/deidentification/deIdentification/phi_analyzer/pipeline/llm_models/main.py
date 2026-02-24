from fastapi import FastAPI
from llm_models.llm_api import router

app = FastAPI(
    title="LLM Models Microservice",
    description="Unified microservice for LLM chat generation using OpenAI, Vertex, vLLM, and Ollama",
    version="1.0.0"
)

# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "llm_models"}

app.include_router(router, prefix="/api/llm", tags=["llm_models"])

# uvicorn llm_models.main:app --host 0.0.0.0 --port 8005
