import json
import re
from typing import List, Dict, Any
import torch
import dateparser

from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline

MODEL_ID = "Qwen/Qwen3-32B-Instruct"   # or "Qwen/Qwen3-32B" if you insist
MAX_NEW_TOKENS = 1024

PROMPT_TEMPLATE = """You are a precise information extraction assistant.
Extract EVERY date/time expression from the text below.
Return ONLY a valid JSON array. Each element must be an object:
{{
  "match": "<the exact substring>",
  "start": <integer start index in the text>,
  "end": <integer end index (exclusive)>,
  "iso": "YYYY-MM-DD HH:MM:SS"  // normalized if you can, else null
}}

Text:
\"\"\"{text}\"\"\"

JSON:
"""

def build_pipeline(model_id: str):
    tokenizer = AutoTokenizer.from_pretrained(model_id, trust_remote_code=True)
    model = AutoModelForCausalLM.from_pretrained(
        model_id,
        trust_remote_code=True,
        torch_dtype=torch.bfloat16 if torch.cuda.is_available() else torch.float32,
        device_map="auto"
    )
    return pipeline(
        "text-generation",
        model=model,
        tokenizer=tokenizer,
        device_map="auto",
        torch_dtype=torch.bfloat16 if torch.cuda.is_available() else torch.float32
    )

def ensure_iso(dt_str: str) -> str:
    """Try to normalize a datetime string to ISO 'YYYY-MM-DD HH:MM:SS'."""
    if not dt_str:
        return None
    dt = dateparser.parse(dt_str)
    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else None

def extract_dates_with_qwen(text: str, model_id: str = MODEL_ID) -> List[Dict[str, Any]]:
    pipe = build_pipeline(model_id)
    prompt = PROMPT_TEMPLATE.format(text=text)

    out = pipe(
        prompt,
        max_new_tokens=MAX_NEW_TOKENS,
        do_sample=False,
        temperature=0.0,
        eos_token_id=pipe.tokenizer.eos_token_id,
    )[0]["generated_text"]

    # Keep only the JSON part (model might echo prompt)
    json_match = re.search(r"\[.*\]\s*$", out, re.S)
    if not json_match:
        raise ValueError("Model did not return a JSON array. Full output:\n" + out)

    data = json.loads(json_match.group(0))

    # (Optional) normalize iso fields on our side to be safe
    for item in data:
        if "iso" in item:
            item["iso"] = ensure_iso(item.get("iso"))
        else:
            item["iso"] = ensure_iso(item.get("match"))

    return data

if __name__ == "__main__":
    sample_text = """
    We met on 08/01/2022 and again on Oct 5th, 2023 at 14:30.
    The deadline is 20240718125959. Another note: 15-07-2024 09:30:00.
    Yesterday at 5pm we decided to move it to 2024-12-01.
    """
    results = extract_dates_with_qwen(sample_text)
    print(json.dumps(results, indent=2))
