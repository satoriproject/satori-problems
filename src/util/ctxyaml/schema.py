import collections
import copy
import heapq
import re
import six
import types

from util import ctxyaml
from util.nsdict import NamespaceDict


class SchemaError(ValueError):

    def __init__(self, message, details = None):
        super(SchemaError, self).__init__(message)
        self.details = details or [ ]

    def __str__(self):
        return super(SchemaError, self).__str__() + '\n' + str(self.details)


class SchemaMeta(type):

    activators = { }

    def __new__(cls, name, bases, attrs):
        activator = attrs.pop('_activate', None)
        klass = super(SchemaMeta, cls).__new__(cls, name, bases, attrs)
        if activator:
            SchemaMeta.activators[klass] = activator
        return klass

    def __call__(cls, spec=None, **kwargs):
        # normalize arguments
        if not spec:
            spec = { }
        elif isinstance(spec, six.string_types):
            spec = ctxyaml.load(spec)
            if isinstance(spec, six.string_types):
                spec = { 'type' : spec }
        elif isinstance(spec, ( list, tuple )):
            spec = { 'options' : list(spec) }
        spec.update(kwargs)
        # gather schema classes to be activated
        degrees = { cls : 0 }
        for klass, activate in SchemaMeta.activators.items():
            if activate(spec):
                degrees[klass] = 0
        # compute consistent ordering
        ordered = [ ]
        for klass in list(degrees.keys()):
            for subklass in klass.__mro__[1:]:
                if subklass in degrees:
                    degrees[subklass] += 1
        bottoms = [ klass for klass, degree in degrees.items() if not degree ]
        heapq.heapify(bottoms)
        while bottoms:
            klass = heapq.heappop(bottoms)
            ordered.append(klass)
            for subklass in klass.__mro__[1:]:
                if subklass in degrees:
                    degrees[subklass] -= 1
                    if not degrees[subklass]:
                        heapq.heappush(bottoms, subklass)
        for degree in degrees.values():
            if degree:
                raise SchemaError("Inconsistent schema options")
        # create and instantiate a composite schema class
        klass = type(cls.__name__, tuple(ordered), { })
        self = klass.__new__(klass)
        self.__init__(spec)
        errors = [ ]
        self._validate_schema(self, errors, '')
        if errors:
            raise SchemaError("Incorrect schema", errors)
        return self


class ValidationError(ValueError):

    def __init__(self, message, path):
        super(ValidationError, self).__init__()
        self.message = message
        self.path = path

    def __str__(self):
        return "%s: %s" % ( self.path, self.message )

    def __repr__(self):
        return str(self)


@six.add_metaclass(SchemaMeta)
class Schema(object):

    def __init__(self, spec):
        self.required = spec.get('required', False)
        self.default_value = spec.get('default', None)
        if self.required and self.default_value is not None:
            raise SchemaError("cannot provide a default value for a required element")

    def _validate(self, data, errors, path):
        if self.required and data is None:
            errors.append(ValidationError("Value required but not provided", path))

    def _validate_schema(self, schema, errors, path):
        if self.required and not schema.required:
            self._validate(schema.default_value, errors, path + ' (default)')
        if isinstance(schema, OptionsValidator):
            for option in schema.allowed_values:
                self._validate(option, errors, path + ' (option)')

    def _augment_spec(self, spec):
        if self.required:
            spec.setdefault('required', True)
        if self.default_value is not None:
            spec.setdefault('default', self.default_value)

    def validate(self, data):
        errors = [ ]
        self._validate(data, errors, '')
        if errors:
            raise ValueError(str(errors))

    def validate_schema(self, schema):
        errors = [ ]
        self._validate_schema(schema, errors, '')
        if errors:
            raise SchemaError("schema too weak", errors)

    def normalize(self, data):
        if data is None:
            return self.default_value
        return data

    def simplify(self, data):
        if data == self.default_value:
            return None
        return data

    def blueprint(self):
        raise ValueError("No blueprint available for this schema")

    def specification(self):
        spec = collections.OrderedDict()
        self._augment_spec(spec)
        return spec or None

    def __str__(self):
        return ctxyaml.dump(self.specification())

    def __add__(self, other):
        if not isinstance(other, Schema):
            other = Schema(other)
        spec = collections.OrderedDict()
        self._augment_spec(spec)
        other._augment_spec(spec)
        return Schema(spec)


class MissingValueError(ValidationError):

    def __init__(self, path):
        super(MissingValueError, self).__init__("value required but missing", path)


class TypeValidator(Schema):

    type_map = {
        'boolean':      [ bool ],
        'string':       list(six.string_types),
        'integer':      list(six.integer_types),
        'real':         [ float, int ],
        'sequence':     [ list, tuple ],
        'list':         [ list, tuple ],
        'tuple':        [ list, tuple ],
        'map':          [ dict, collections.OrderedDict ],
        'record':       [ dict, collections.OrderedDict ],
    }

    def _activate(spec):
        return 'type' in spec

    def __init__(self, spec):
        super(TypeValidator, self).__init__(spec)
        self.allowed_types = spec['type']
        if isinstance(self.allowed_types, six.string_types):
            self.allowed_types = [ self.allowed_types ]
        if not self.allowed_types:
            raise SchemaError("The set of allowed types is empty")

    def _validate(self, data, errors, path):
        super(TypeValidator, self)._validate(data, errors, path)
        types = tuple(t for n in self.allowed_types for t in TypeValidator.type_map[n]) + ( type(None), )
        if not isinstance(data, types):
            errors.append(ValidationError("value '%s' has type '%s', none of the allowed '%s'" % ( data, type(data), types ), path))

    def _validate_schema(self, schema, errors, path):
        super(TypeValidator, self)._validate_schema(schema, errors, path)
        if isinstance(schema, TypeValidator):
            for type in schema.allowed_types:
                if type not in self.allowed_types:
                    errors.append(ValidationError("additional allowed type '%s'" % ( type ), path))
        elif not isinstance(schema, OptionsValidator):
            errors.append(ValidationError("allowed types not specified", path))

    def _augment_spec(self, spec):
        super(TypeValidator, self)._augment_spec(spec)
        if 'type' in spec:
            types = spec['type']
            if isinstance(types, six.string_types):
                types = [ types ]
            types = [ t for t in types if t in self.allowed_types ]
        else:
            types = self.allowed_types
        if len(types) == 1:
            types = types[0]
        spec['type'] = types

    def blueprint(self):
        if len(self.allowed_types) == 1:
            type = self.allowed_types[0]
            if type in [ 'map', 'record' ]:
                return NamespaceDict()
        return super(TypeValidator, self).blueprint()


class OptionsValidator(Schema):

    def _activate(spec):
        return 'options' in spec

    def __init__(self, spec):
        super(OptionsValidator, self).__init__(spec)
        self.allowed_values = spec['options']

    def _validate(self, data, errors, path):
        super(OptionsValidator, self)._validate(data, errors, path)
        if data not in self.allowed_values:
            errors.append(ValidationError("value '%s' not among the allowed '%s'" % ( data, self.allowed_values ), path))

    def _validate_schema(self, schema, errors, path):
        super(OptionsValidator, self)._validate_schema(schema, errors, path)
        if not isinstance(schema, OptionsValidator):
            errors.append(ValidationError("allowed values are not specified", path))

    def _augment_spec(self, spec):
        super(OptionsValidator, self)._augment_spec(spec)
        if 'options' in spec:
            spec['options'] = [ option for option in spec['options'] if option in self.allowed_values ]
        else:
            spec['options'] = self.allowed_values


class MinValueValidator(Schema):

    def _activate(spec):
        return 'min' in spec

    def __init__(self, spec):
        super(MinValueValidator, self).__init__(spec)
        self.min_value = spec['min']

    def _validate(self, data, errors, path):
        super(MinValueValidator, self)._validate(data, errors, path)
        if not self.min_value <= data:
            errors.append(ValidationError("value '%s' not above the required minimum '%s'" % ( data, self.min_value ), path))

    def _validate_schema(self, schema, errors, path):
        super(MinValueValidator, self)._validate_schema(schema, errors, path)
        if hasattr(schema, 'min_value'):
            if schema.min_value < self.min_value:
                errors.append(ValidationError("allowed minimum '%s' is lower than '%s'" % ( schema.min_value, self.min_value ), path))
        elif not isinstance(schema, OptionsValidator):
            errors.append(ValidationError("minimum allowed value is not specified", path))

    def _augment_spec(self, spec):
        super(MinValueValidator, self)._augment_spec(spec)
        spec['min'] = min(self.min_value, spec.get('min', self.min_value))


class MaxValueValidator(Schema):

    def _activate(spec):
        return 'max' in spec

    def __init__(self, spec):
        super(MaxValueValidator, self).__init__(spec)
        self.max_value = spec['max']

    def _validate(self, data, errors, path):
        super(MaxValueValidator, self)._validate(data, errors, path)
        if not data <= self.max_value:
            errors.append(ValidationError("value '%s' not below the required maximum '%s'" % ( data, self.max_value ), path))

    def _validate_schema(self, schema, errors, path):
        super(MaxValueValidator, self)._validate_schema(schema, errors, path)
        if hasattr(schema, 'max_value'):
            if schema.max_value > self.max_value:
                errors.append(ValidationError("allowed maximum '%s' is greter than '%s'" % ( schema.max_value, self.max_value ), path))
        elif not isinstance(schema, OptionsValidator):
            errors.append(ValidationError("maximum allowed value is not specified", path))

    def _augment_spec(self, spec):
        super(MaxValueValidator, self)._augment_spec(spec)
        spec['max'] = min(self.max_value, spec.get('max', self.max_value))


class RegexValidator(TypeValidator):

    def _activate(spec):
        return 'regex' in spec

    def __init__(self, spec):
        spec.setdefault('type', 'string')
        super(RegexValidator, self).__init__(spec)
        self.regex = re.compile(spec['regex'] + '$')

    def _validate(self, data, errors, path):
        super(RegexValidator, self)._validate(data, errors, path)
        if not self.regex.match(data):
            errors.append(ValidationError("value '%s' does not match the required regex '%s'" % ( data, self.regex ), path))

    def _validate_schema(self, schema, errors, path):
        super(RegexValidator, self)._validate_schema(schema, errors, path)
        if isinstance(schema, RegexValidator):
            if schema.regex.pattern != self.regex.pattern:
                errors.append(ValidationError("required regex '%s' is different than '%s' (and subsumption is not implemented)" % ( schema.regex.pattern[:-1], self.regex.pattern[:-1] ), path))
        elif not isinstance(schema, OptionsValidator):
            errors.append(ValidationError("allowed values are not constrained by a regex", path))

    def _augment_spec(self, spec):
        super(RegexValidator, self)._augment_spec(spec)
        if 'regex' in spec:
            if spec['regex'] != self.regex.pattern[:-1]:
                raise NotImplementedError("cannot merge schemas with different regex validators")
        else:
            spec['regex'] = self.regex.pattern[:-1]


class RecordSchema(TypeValidator):

    def _activate(spec):
        if 'fields' in spec or 'entries' in spec:
            return True
        types = spec.get('type', [ ])
        if isinstance(types, six.string_types):
            types = [ types ]
        if len(types) == 1 and types[0] == 'record':
            return True
        return False

    def __init__(self, spec):
        self.fields = collections.OrderedDict()
        defaults = collections.OrderedDict()
        for field, subspec in (spec.get('fields') or spec.get('entries') or { }).items():
            subschema = Schema(subspec)
            self.fields[field] = subschema
            if subschema.default_value is not None:
                defaults[field] = subschema.default_value
        spec.setdefault('type', [ 'record' ])
        spec.setdefault('default', defaults)
        super(RecordSchema, self).__init__(spec)
        
    def _validate(self, data, errors, path):
        super(RecordSchema, self)._validate(data, errors, path)
        for field, schema in self.fields.items():
            schema._validate(data.get(field), errors, path + '.' + field)

    def _validate_schema(self, schema, errors, path):
        super(RecordSchema, self)._validate_schema(schema, errors, path)
        if isinstance(schema, RecordSchema):
            for field, subschema in self.fields.items():
                if field in schema.fields:
                    subschema._validate_schema(schema.fields[field], errors, path + '.' + field)
                else:
                    errors.append(ValidationError("no specification for field '%s'" % field, path))
        elif not isinstance(schema, OptionsValidator):
            errors.append(ValidationError("no record specification", path))

    def _augment_spec(self, spec):
        super(RecordSchema, self)._augment_spec(spec)
        fields = spec.setdefault('fields', collections.OrderedDict())
        for field, schema in self.fields.items():
            schema._augment_spec(fields.setdefault(field, { }))

    def normalize(self, data):
        data = super(RecordSchema, self).normalize(data)
        data = copy.copy(data)
        for field, schema in self.fields.items():
            data[field] = schema.normalize(data.get(field))
        return data

    def simplify(self, data):
        data = copy.copy(data)
        for field, schema in self.fields.items():
            data[field] = schema.simplify(data[field])
            if data[field] is None:
                del data[field]
        return super(RecordSchema, self).simplify(data)

    def blueprint(self):
        blue = super(RecordSchema, self).blueprint()
        blue._factory = lambda key : self.fields[key].blueprint()
        return blue


class MapSchema(TypeValidator):

    def _activate(spec):
        if 'key' in spec or 'value' in spec:
            return True
        types = spec.get('type', [ ])
        if isinstance(types, six.string_types):
            types = [ types ]
        if len(types) == 1 and types[0] == 'map':
            return True
        return False

    def __init__(self, spec):
        spec.setdefault('type', [ 'map' ])
        super(MapSchema, self).__init__(spec)
        self.key_schema = Schema(spec.get('key'))
        if self.key_schema.default_value is not None:
            raise SchemaError("cannot specify default value for mapping keys")
        self.value_schema = Schema(spec.get('value'))

    def _validate(self, data, errors, path):
        super(MapSchema, self)._validate(data, errors, path)
        for key, value in data.items():
            self.key_schema._validate(key, errors, path + '[keys]')
            self.value_schema._validate(value, errors, path + '[values]')

    def _validate_schema(self, schema, errors, path):
        super(MapSchema, self)._validate_schema(schema, errors, path)
        if isinstance(schema, MapSchema):
            self.key_schema._validate_schema(schema.key_schema, errors, path + '[keys]')
            self.value_schema._validate_schema(schema.value_schema, errors, path + '[values]')
        elif not isinstance(schema, OptionsValidator):
            errors.append(ValidationError("no map specification", path))

    def _augment_spec(self, spec):
        super(MapSchema, self)._augment_spec(spec)
        key = spec.setdefault('key', collections.OrderedDict())
        self.key_schema._augment_spec(key)
        value = spec.setdefault('value', collections.OrderedDict())
        self.value_schema._augment_spec(value)

    def normalize(self, data):
        data = super(MapSchema, self).normalize(data)
        data = copy.copy(data)
        for key, value in data.items():
            data[key] = self.value_schema.normalize(value)
        return data

    def simplify(self, data):
        data = copy.copy(data)
        for key, value in data.items():
            data[key] = self.value_schema.simplify(value)
        return super(MapSchema, self).simplify(data)

    def blueprint(self):
        blue = super(ValueValidator, self).blueprint()
        blue._factory = lambda key : self.value_schema.blueprint()
        return blue


class ListSchema(TypeValidator):

    def _activate(spec):
        if 'item' in spec or 'element' in spec:
            return True
        types = spec.get('type', [ ])
        if isinstance(types, six.string_types):
            types = [ types ]
        if len(types) == 1 and types[0] in [ 'list', 'sequence' ]:
            return True
        return False

    def __init__(self, spec):
        spec.setdefault('type', [ 'list' ])
        super(ListSchema, self).__init__(spec)
        self.item_schema = Schema(spec.get('item') or spec.get('element') or None)
        if self.item_schemai.default_value is not None:
            raise SchemaError("cannot specify a default value for list items")

    def _validate(self, data, errors, path):
        super(ListSchema, self)._validate(data, errors, path)
        for index, item in enumerate(data):
            self.item_schema._validate(item, errors, path + '[' + str(index) + ']')

    def _validate_schema(self, schema, errors, path):
        super(ListSchema, self)._validate_schema(schema, errors, path)
        if hasattr(schema, 'item_schema'):
            self.item_schema._validate_schema(schema.item_schema, errors, path + '[items]')
        elif not isinstance(schema, OptionsValidator):
            errors.append(ValidationError("no list specification", path))

    def _augment_spec(self, spec):
        super(ListSchema, self)._augment_spec(spec)
        item = spec.setdefault('item', collections.OrderedDict())
        self.item_schema._augment_spec(item)

    def normalize(self, data):
        data = super(ListSchema, self).normalize(data)
        data = copy.copy(data)
        for index in range(len(data)):
            data[index] = self.item_schema.normalize(data[index])
        return data

    def simplify(self, data):
        result = copy.copy(data)
        for index in range(len(data)):
            data[index] = self.item_schema.simplify(data[index])
        return super(ListSchema, self).simplify(data)

