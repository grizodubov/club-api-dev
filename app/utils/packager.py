import re
import gzip
import orjson



COMPRESSION_LIMIT = 512

PACK_METHODS = {
    'tuple': lambda x: orjson.dumps(list(x)),
    'set': lambda x: orjson.dumps(list(x)),
    'list': lambda x: orjson.dumps(x),
    'dict': lambda x: orjson.dumps(x),
    'int': lambda x: str(x).encode(),
    'float': lambda x: str(x).encode(),
    'str': lambda x: x.encode(),
}

UNPACK_METHODS = {
    'tuple': lambda x: tuple(orjson.loads(x)),
    'set': lambda x: set(orjson.loads(x)),
    'list': lambda x: orjson.loads(x),
    'dict': lambda x: orjson.loads(x),
    'int': lambda x: int(x.decode()),
    'float': lambda x: float(x.decode()),
    'str': lambda x: x.decode(),
}



####################################################################
def pack(data, compression = False, limit = None):
    t = type(data).__name__
    bstr = PACK_METHODS[t](data) if t in PACK_METHODS else None
    prefix = t.encode()
    if compression and len(bstr) >= (limit or COMPRESSION_LIMIT):
        bstr = gzip.compress(bstr)
        prefix += b'|gzip'
    return prefix + b':' + bstr



####################################################################
def unpack(bstr):
    data = None
    if bstr is not None:
        res = re.match(rb'([^:\|]+)\|?([^:]*):', bstr)
        if res:
            t = res.group(1).decode()
            if t in UNPACK_METHODS:
                d = bstr[len(res.group(0)):]
                if res.group(2) == b'gzip':
                    d = gzip.decompress(d)
                data = UNPACK_METHODS[t](d)
    return data
