# mapping/snomed_mapper.py
import requests

SNOMED_API = "https://snowstorm.ihtsdotools.org/snowstorm/snomed-ct/browser/MAIN/concepts"

def lookup(term: str):
    params = {"term": term, "activeFilter": "true", "limit": 1}
    resp = requests.get(SNOMED_API, params=params)
    if resp.status_code == 200:
        items = resp.json().get("items", [])
        if items:
            return {
                "code": items[0]["conceptId"],
                "display": items[0]["fsn"]["term"]
            }
    return None
