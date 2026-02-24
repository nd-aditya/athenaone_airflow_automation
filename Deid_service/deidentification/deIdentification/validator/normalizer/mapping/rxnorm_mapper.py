# mapping/rxnorm_mapper.py
import requests

RXNORM_API = "https://rxnav.nlm.nih.gov/REST/approximateTerm.json"

def lookup(term: str):
    """Search RxNorm for a medication term."""
    params = {"term": term, "maxEntries": 1}
    resp = requests.get(RXNORM_API, params=params)
    if resp.status_code == 200:
        data = resp.json()
        candidates = data.get("approximateGroup", {}).get("candidate", [])
        if candidates:
            rxcui = candidates[0].get("rxcui")
            if rxcui:
                name_resp = requests.get(f"https://rxnav.nlm.nih.gov/REST/rxcui/{rxcui}/properties.json")
                if name_resp.status_code == 200:
                    props = name_resp.json().get("properties", {})
                    return {"code": props.get("rxcui"), "display": props.get("name")}
    return None
