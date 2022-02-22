import collections


class NamespaceDict(collections.OrderedDict):

    def __getitem__(self, key):
        try:
            return super(NamespaceDict, self).__getitem__(key)
        except KeyError:
            if '_factory' in self.__dict__:
                try:
                    value = self._factory(key)
                    self[key] = value
                    return value
                except:
                    pass
            raise

    def __setitem__(self, key, value):
        if value.__class__ in [ dict, collections.OrderedDict ]:
            value = NamespaceDict(value)
        super(NamespaceDict, self).__setitem__(key, value)

    def __getattr__(self, name):
        name = name.replace('_', ' ')
        try:
            return self[name]
        except:
            raise AttributeError()

    def __setattr__(self, name, value):
        if name in self.__dict__ or name[:1] == '_':
            self.__dict__[name] = value
        else:
            name = name.replace('_', ' ')
            self[name] = value

    def __delattr__(self, name):
        if name in self:
            del self[name]
        elif name in self.__dict__:
            del self.__dict__[name]

