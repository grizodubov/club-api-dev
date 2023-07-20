import re
import pprint



################################################################
def validate(data, scheme, strict = False):
    if strict:
        for dkey in data:
            if dkey not in scheme:
                pprint.pprint('v error!', 'scheme', dkey)
                return None
    for skey, sval in scheme.items():
        if sval['type'] == 'dict':
            sval['strict'] = sval['strict'] if 'strict' in sval else strict
            if 'scheme' not in sval:
                pprint.pprint('v error!', 'dict', skey, sval)
                return None
            if 'choice' in sval:
                cs = 0
                for key in sval['scheme']:
                    if key in sval['choice']:
                        sval['scheme'][key]['required'] = False
                        cs += 1
                if not cs:
                    pprint.pprint('v error!', 'choice1', skey, sval)
                    return None
                if strict and cs > 1:
                    pprint.pprint('v error!', 'choice2', skey, sval)
                    return None
        if skey in data:
            if data[skey] is None or (type(data[skey]) == list and len(data[skey]) == 0):
                if 'null' in sval and sval['null']:
                    continue
                else:
                    pprint.pprint('v error!', 'null', skey, sval)
                    return None
            wrap = False
            if type(data[skey]) != list:
                wrap = True
                values = [ data[skey] ]
            else:
                if 'list' not in sval and not sval['list']:
                    pprint.pprint('v error!', 'list', skey, sval)
                    return None
                values = data[skey]
            for value in values:
                if not VALIDCHECK[sval['type']](value, sval):
                    pprint.pprint('v error!', 'valid', skey, sval)
                    return None
            if 'processing' in sval:
                if wrap:
                    data[skey] = sval['processing'](values[0])
                else:
                    data[skey] = [ sval['processing'](value) for value in values ]
        else:
            if 'required' in sval and sval['required']:
                pprint.pprint('v error!', 'required', skey, sval)
                return None
            if 'default' in sval:
                data[skey] = sval['default']
    return data



################################################################
def check_dict(value, scheme):
    if not isinstance(value, dict):
        return False
    return validate(value, scheme['scheme'], )



################################################################
def check_int(value, scheme):
    if isinstance(value, str):
        value = int(value)
    if not isinstance(value, int):
        return False
    if 'value_min' in scheme:
        if value < scheme['value_min']:
            return False
    if 'value_max' in scheme:
        if value > scheme['value_max']:
            return False
    return True



################################################################
def check_str(value, scheme):
    if not isinstance(value, str):
        return False
    if 'length_min' in scheme:
        if len(value) < scheme['length_min']:
            return False
    if 'length_max' in scheme:
        if len(value) > scheme['length_max']:
            return False
    if 'pattern' in scheme:
        if not re.search(scheme['pattern'], value):
            return False
    if 'value' in scheme:
        if value != scheme['value']:
            return False
    if 'values' in scheme:
        if value not in scheme['values']:
            return False
    if 'exceptions' in scheme:
        if value in scheme['exceptions']:
            return False
    return True



################################################################
def check_bool(value, scheme):
    if type(value) != bool:
        return False
    return True



################################################################
VALIDCHECK = {
    'dict': check_dict,
    'int': check_int,
    'str': check_str,
    'bool': check_bool,
}
