import collections
from dataclasses import is_dataclass, asdict
from datetime import datetime
from ujson import dumps as ujson_dumps

def cvt_nested_datetime_isoformat(o):
    if isinstance(o, dict):
        return {k: cvt_nested_datetime_isoformat(v) for k, v in o.items()}
    elif is_dataclass(o):
        return cvt_nested_datetime_isoformat(asdict(o))
    elif isinstance(o, list) or isinstance(o, collections.deque):
        return [cvt_nested_datetime_isoformat(e) for e in o]
    elif isinstance(o, datetime):
        return o.isoformat()
    else:
        return o


def dumps(obj): 

    """ Dumps json and formats datetime to isotime """
    return ujson_dumps(cvt_nested_datetime_isoformat(obj))