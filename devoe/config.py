import os
import json
import pepperoni

def make_config(obj=None):
    log = pepperoni.logger()
    if isinstance(obj, str) is True:
        log.debug(f'Parsing configurator <{obj}>...')
    elif isinstance(obj, dict) is True:
        log.debug(f'Parsing configurator {list(obj.keys())}...')
    try:
        if isinstance(obj, str) is True:
            path = os.path.abspath(obj)
            if os.path.exists(path) is True:
                with open(path, 'r') as fh:
                    config = json.load(fh)
            else:
                log.warning(f'File {path} not found')
        elif isinstance(obj, dict) is True:
            config = obj
        else:
            config = {}
    except:
        log.critical()
    else:
        return config

def save_json(data, path):
    if os.path.exists(path) is True:
        raise OSError(f'file {path} exists')
    else:
        with open(path, 'w') as fh:
            json.dump(data, fh, indent=2)
    pass
