import toml


PATH = 'app/config'
ALLOWED = { 'REDIS', 'PG' }


####################################################################
class Config:


    ################################################################
    def __init__(self):
        global PATH
        global ALLOWED
        with open(f'{PATH}/settings.toml', 'r', encoding = 'utf-8') as toml_file:
            setattr(self, 'settings', toml.load(toml_file))
        configs = [ key.lower() for key in self.settings['API'] if key.upper() in ALLOWED and self.settings['API'][key] ]
        for cfg in configs:
            with open(f'{PATH}/{cfg}.toml', 'r', encoding = 'utf-8') as toml_file:
                setattr(self, cfg, toml.load(toml_file))
