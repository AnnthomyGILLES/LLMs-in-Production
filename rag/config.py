import yaml


def load_config(config_file_path='config.yaml'):
    with open(config_file_path, 'r') as config_file:
        config = yaml.safe_load(config_file)
    return config


# Load the configuration when the module is imported
config = load_config()
