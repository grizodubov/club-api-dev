import orjson

from app.core.context import get_api_context



####################################################################
class Model:


    ################################################################
    def __init__(self):
        self.id = 0
        self.model = None
        self.time_create = None
        self.time_update = None
        self._storage = None


    ################################################################
    def reset(self):
        self.__class__.__init__(self)


    ################################################################
    def set_draft(self, attrs):
        Model.set(self, attrs)


    ################################################################
    def set(self, attrs):
        api = get_api_context()
        if attrs:
            self.__dict__ = { **self.__dict__, **attrs }
        model = self.clause()
        if self._ancestors:
            ancestors = orjson.loads(self._ancestors)
            for item in ancestors[0]:
                alias = list(item.keys())[0]
                self._path[api.models['alias_to_node'][alias]] = item[alias]


    ################################################################
    def check(self, data):
        result = {}
        for k, v in data.items():
            if getattr(self, k) != v:
                result[k] = v
        return result


    ################################################################
    def clause(self):
        return self.__class__.__name__.lower()


    ################################################################
    def to_dict(self):
        return {
            k: v for k, v in self.__dict__.items() if k[0] != '_'
        }
