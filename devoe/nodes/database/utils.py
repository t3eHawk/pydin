import configparser

def parse_config(path):
    config = configparser.ConfigParser(allow_no_value=True)
    config.read(path)
    return config
