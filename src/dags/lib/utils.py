import json
from datetime import datetime, date
from typing import Any, Dict, Optional
from urllib.parse import urljoin

import requests
from bson.objectid import ObjectId
from pydantic import BaseModel


def json2str(obj: Any, *, default: Optional[Any] = None) -> str:
    return json.dumps(to_dict(obj), default=default, sort_keys=True, ensure_ascii=False)


def str2json(str: str) -> Dict:
    return json.loads(str)


def to_dict(obj, classkey=None):
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(obj, date):
        return obj.strftime("%Y-%m-%d")
    elif isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, dict):
        data = {}
        for (k, v) in obj.items():
            data[k] = to_dict(v, classkey)
        return data
    elif isinstance(obj, BaseModel):
        return to_dict(obj.dict())
    elif hasattr(obj, "_ast"):
        return to_dict(obj._ast())
    elif hasattr(obj, "__iter__") and not isinstance(obj, str):
        return [to_dict(v, classkey) for v in obj]
    elif hasattr(obj, "__dict__"):
        data = dict([(key, to_dict(value, classkey))
                     for key, value in obj.__dict__.items()
                     if not callable(value) and not key.startswith('_')])
        if classkey is not None and hasattr(obj, "__class__"):
            data[classkey] = obj.__class__.__name__
        return data
    else:
        return obj


class CustomSession(requests.Session):
    def __init__(self, base_url: str, headers: Dict):
        super().__init__()
        self.base_url = base_url
        self.headers.update(headers)

    def request(self, method: str, url: str, *args, **kwargs):
        joined_url = urljoin(self.base_url, url)
        return super().request(method, joined_url, *args, **kwargs)
