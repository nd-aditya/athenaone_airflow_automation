# mapping/loinc_mapper.py
import requests

LOINC_API = "https://clinicaltables.nlm.nih.gov/api/loinc_items/v3/search"

def lookup(term: str):
    """Search LOINC for a lab/measurement term."""
    params = {"sf": "LOINC_NUM,COMPONENT", "terms": term, "maxList": 1}
    resp = requests.get(LOINC_API, params=params)
    if resp.status_code == 200:
        data = resp.json()
        if data and len(data) > 3 and data[3]:
            code, desc = data[3][0]
            return {"code": code, "display": desc}
    return None
