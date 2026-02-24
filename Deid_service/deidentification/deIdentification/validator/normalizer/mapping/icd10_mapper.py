# mapping/icd10_mapper.py
import requests

ICD10_API = "https://clinicaltables.nlm.nih.gov/api/icd10cm/v3/search"

def lookup(term: str):
    """Search ICD-10 for a term and return the first match."""
    params = {"sf": "code,name", "terms": term, "maxList": 1}
    resp = requests.get(ICD10_API, params=params)
    if resp.status_code == 200:
        data = resp.json()
        if data and len(data) > 3 and data[3]:
            # data[3] contains matches in form [code, description]
            code, desc = data[3][0]
            return {"code": code, "display": desc}
    return None
