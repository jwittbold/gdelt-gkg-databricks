import toml
from cloudpathlib import CloudPath


# load configuration variables from .toml file
with open(CloudPath('az://gdeltdata/config/config.toml'), 'r') as f:
    config = toml.load(f)
