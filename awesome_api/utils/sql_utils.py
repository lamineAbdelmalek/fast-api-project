from typing import Dict, Any, Sequence


def parametrized_in_clause(size: int, prefix: str = "") -> str:
    if size == 0:
        raise ValueError("Chunk can not be empty")
    elif size == 1:
        clause = f" = :{prefix}_0"
    else:
        values = ", ".join([f":{prefix}_{i}" for i in range(size)])
        clause = f" IN ({values})"
    return clause

def generate_param_dict(values: Sequence[str], prefix: str= "") -> Dict[str, Any]:
    return {f"{prefix}_{i}": value for i, value in enumerate(values)}