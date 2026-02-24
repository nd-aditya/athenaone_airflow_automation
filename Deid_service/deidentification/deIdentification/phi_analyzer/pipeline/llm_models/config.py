import os
open_ai_key=os.getenv("open_ai_key")
gemini_key=os.getenv("gemini_key")
model_configs = {
    "google/gemma-2-9b-it": {"model_name":"google/gemma-2-9b-it","type": "vllm", "endpoint": "http://localhost:8000/v1"},
    "gpt-4o": {"model_name":"gpt-4o","type": "openai", "api_key": os.getenv("open_ai_key")},
    "gpt-4o-mini": {"model_name":"gpt-4o-mini","type": "openai", "api_key": os.getenv("open_ai_key")},
    "gpt-4.1-nano": {"model_name":"gpt-4.1-nano","type": "openai", "api_key": os.getenv("open_ai_key")},
    "gpt-4.1": {"model_name":"gpt-4.1","type": "openai", "api_key": os.getenv("open_ai_key")},
    "gemini-1.5-flash": {"model_name":"gemini-1.5-flash","type": "vertex", "api_key": os.getenv("gemini_key")},
    "gemini-2.0-flash": {"model_name":"gemini-2.0-flash","type": "vertex", "api_key": os.getenv("gemini_key")},
    "gemini-1.5-pro": {"model_name":"gemini-1.5-pro","type": "vertex", "api_key": os.getenv("gemini_key")},
    "gemini-2.5-pro": {"model_name":"gemini-2.5-pro","type": "vertex", "api_key": os.getenv("gemini_key")},
    "gemini-2.5-flash": {"model_name":"gemini-2.5-flash","type": "vertex", "api_key": os.getenv("gemini_key")},
    "gemini-2.5-flash-lite-preview-06-17": {"model_name":"gemini-2.5-flash-lite-preview-06-17","type": "vertex", "api_key": os.getenv("gemini_key")},
    # Add Ollama models
    "qwen3:32b": {"model_name":"qwen3:32b","type": "ollama", "endpoint": "http://localhost:11434", "enable_thinking": False},
    # Add LM studio models
    "qwen3:8b_LM": {'model_name':["qwen3-8b"] + [f"qwen3-8b:{i}" for i in range(2, 11)], "type": "lmstudio", "url": "http://localhost:1234/v1/chat/completions", "enable_thinking": False},
    "qwen3:32b_LM": {'model_name':["qwen3-32b"], "type": "lmstudio", "url": "http://localhost:1234/v1/chat/completions", "enable_thinking": False},
    "gpt_oss_20b_LM" : {'model_name':["openai/gpt-oss-20b"], "type": "lmstudio", "url": "http://localhost:1234/v1/chat/completions", "enable_thinking": False},
    "qwen3-32b-mlx" : {'model_name':["qwen3-32b-mlx","qwen3-32b-mlx:2"], "type": "lmstudio", "url": "http://localhost:1234/v1/chat/completions", "enable_thinking": False}
       
}