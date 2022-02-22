__all__ = [ ]

class Context(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
    def defaults(self, **kwargs):
        result = Context(**self.__dict__)
        for key, value in kwargs.items():
            if key not in result.__dict__:
                result.__dict__[key] = value
        return result
    def apply(self, **kwargs):
        result = Context(**self.__dict__)
        result.__dict__.update(kwargs)
        return result

class WithContext(object):
    @classmethod
    def create_class(cls, context):
        class Target(cls):
            def __init__(self, *args, **kwargs):
                self.context = context
                super(Target, self).__init__(*args, **kwargs)
        Target.__name__ = cls.__name__
        for tag, regex in getattr(context, 'resolvers', { }).items():
            Target.add_implicit_resolver(tag, regex, None)
        return Target
