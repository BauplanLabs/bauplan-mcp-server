from typing import Any


def field_to_dict(field: Any) -> dict[str, Any]:
    return {
        "id": field.id,
        "name": field.name,
        "required": field.required,
        "type": field.type,
    }
