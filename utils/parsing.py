import json

def parse_list_str(val):
    """
    Transforma strings tipo '__list__["abc", "def"]' em listas reais.
    """
    if isinstance(val, str) and val.startswith("__list__"):
        try:
            return json.loads(val.replace("__list__", ""))
        except Exception:
            return [val]
    return val