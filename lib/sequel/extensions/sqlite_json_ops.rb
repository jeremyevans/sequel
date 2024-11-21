# frozen-string-literal: true
#
# The sqlite_json_ops extension adds support to Sequel's DSL to make
# it easier to call SQLite JSON functions and operators (added
# first in SQLite 3.38.0).  It also supports the SQLite JSONB functions
# added in SQLite 3.45.0.
#
# To load the extension:
#
#   Sequel.extension :sqlite_json_ops
#
# This extension works by calling methods on Sequel::SQLite::JSONOp and
# Sequel::SQLite::JSONBOp objects, which you can create using
# Sequel.sqlite_json_op and Sequel.sqlite_jsonb_op:
#
#   j = Sequel.sqlite_json_op(:json_column)
#   jb = Sequel.sqlite_jsonb_op(:jsonb_column)
#
# Also, on most Sequel expression objects, you can call the sqlite_json_op or
# sqlite_jsonb_op method to create a Sequel::SQLite::JSONOp or 
# Sequel::SQLite::JSONBOp object:
#
#   j = Sequel[:json_column].sqlite_json_op
#   jb = Sequel[:jsonb_column].sqlite_jsonb_op
#
# If you have loaded the {core_extensions extension}[rdoc-ref:doc/core_extensions.rdoc],
# or you have loaded the core_refinements extension
# and have activated refinements for the file, you can also use Symbol#sqlite_json_op:
# or Symbol#sqlite_jsonb_op:
#
#   j = :json_column.sqlite_json_op
#   jb = :json_column.sqlite_jsonb_op
#
# The following methods are available for Sequel::SQLite::JSONOp instances:
#
#   j[1]                     # (json_column ->> 1)
#   j.get(1)                 # (json_column ->> 1)
#   j.get_json(1)            # (json_column -> 1)
#   j.extract('$.a')         # json_extract(json_column, '$.a')
#   jb.extract('$.a')        # jsonb_extract(jsonb_column, '$.a')
#
#   j.array_length           # json_array_length(json_column)
#   j.type                   # json_type(json_column)
#   j.valid                  # json_valid(json_column)
#   jb.json                  # json(jsonb_column)
#   j.jsonb                  # jsonb(json_column)
#
#   j.insert('$.a', 1)       # json_insert(json_column, '$.a', 1)
#   j.set('$.a', 1)          # json_set(json_column, '$.a', 1)
#   j.replace('$.a', 1)      # json_replace(json_column, '$.a', 1)
#   j.remove('$.a')          # json_remove(json_column, '$.a')
#   j.patch('{"a":2}')       # json_patch(json_column, '{"a":2}')
#
#   jb.insert('$.a', 1)      # jsonb_insert(jsonb_column, '$.a', 1)
#   jb.set('$.a', 1)         # jsonb_set(jsonb_column, '$.a', 1)
#   jb.replace('$.a', 1)     # jsonb_replace(jsonb_column, '$.a', 1)
#   jb.remove('$.a')         # jsonb_remove(jsonb_column, '$.a')
#   jb.patch('{"a":2}')      # jsonb_patch(jsonb_column, '{"a":2}')
#
#   j.each                   # json_each(json_column)
#   j.tree                   # json_tree(json_column)
#
# Related modules: Sequel::SQLite::JSONBaseOp, Sequel::SQLite::JSONOp,
# Sequel::SQLite::JSONBOp

#
module Sequel
  module SQLite
    # JSONBaseOp is an abstract base wrapper class for a object that
    # defines methods that return Sequel expression objects representing
    # SQLite json operators and functions. It is subclassed by both
    # JSONOp and JSONBOp for json and jsonb specific behavior.
    #
    # In the method documentation examples, assume that:
    #
    #   json_op = Sequel.sqlite_json_op(:json)
    class JSONBaseOp < Sequel::SQL::Wrapper
      GET = ["(".freeze, " ->> ".freeze, ")".freeze].freeze
      private_constant :GET

      GET_JSON = ["(".freeze, " -> ".freeze, ")".freeze].freeze
      private_constant :GET_JSON

      # Returns an expression for getting the JSON array element or object field
      # at the specified path as a SQLite value.
      #
      #   json_op[1]         # (json ->> 1)
      #   json_op['a']       # (json ->> 'a')
      #   json_op['$.a.b']   # (json ->> '$.a.b')
      #   json_op['$[1][2]'] # (json ->> '$[1][2]')
      def [](key)
        json_op(GET, key)
      end
      alias get []

      # Returns an expression for the length of the JSON array, or the JSON array at
      # the given path.
      #
      #   json_op.array_length         # json_array_length(json)
      #   json_op.array_length('$[1]') # json_array_length(json, '$[1]')
      def array_length(*args)
        Sequel::SQL::NumericExpression.new(:NOOP, SQL::Function.new(:json_array_length, self, *args))
      end

      # Returns an expression for a set of information extracted from the top-level
      # members of the JSON array or object, or the top-level members of the JSON array
      # or object at the given path.
      #
      #   json_op.each        # json_each(json)
      #   json_op.each('$.a') # json_each(json, '$.a')
      def each(*args)
        SQL::Function.new(:json_each, self, *args)
      end

      # Returns an expression for the JSON array element or object field at the specified
      # path as a SQLite value, but only accept paths as arguments, and allow the use of
      # multiple paths.
      #
      #   json_op.extract('$.a')        # json_extract(json, '$.a')
      #   json_op.extract('$.a', '$.b') # json_extract(json, '$.a', '$.b')
      def extract(*a)
        function(:extract, *a)
      end

      # Returns an expression for getting the JSON array element or object field at the
      # specified path as a JSON value.
      #
      #   json_op.get_json(1)         # (json -> 1)
      #   json_op.get_json('a')       # (json -> 'a')
      #   json_op.get_json('$.a.b')   # (json -> '$.a.b')
      #   json_op.get_json('$[1][2]') # (json -> '$[1][2]')
      def get_json(key)
        self.class.new(json_op(GET_JSON, key))
      end

      # Returns an expression for creating new entries at the given paths in the JSON array
      # or object, but not overwriting existing entries.
      #
      #   json_op.insert('$.a', 1)           # json_insert(json, '$.a', 1)
      #   json_op.insert('$.a', 1, '$.b', 2) # json_insert(json, '$.a', 1, '$.b', 2)
      def insert(path, value, *args)
        wrapped_function(:insert, path, value, *args)
      end

      # Returns an expression for a minified version of the JSON.
      #
      #   json_op.json   # json(json)
      def json
        JSONOp.new(SQL::Function.new(:json, self))
      end
      alias minify json

      # Returns the JSONB format of the JSON.
      #
      #   json_op.jsonb   # jsonb(json)
      def jsonb
        JSONBOp.new(SQL::Function.new(:jsonb, self))
      end

      # Returns an expression for updating the JSON object using the RFC 7396 MergePatch algorithm
      #
      #   json_op.patch('{"a": 1, "b": null}') # json_patch(json, '{"a": 1, "b": null}')
      def patch(json_patch)
        wrapped_function(:patch, json_patch)
      end

      # Returns an expression for removing entries at the given paths from the JSON array or object.
      #
      #   json_op.remove('$.a')        # json_remove(json, '$.a')
      #   json_op.remove('$.a', '$.b') # json_remove(json, '$.a', '$.b')
      def remove(path, *paths)
        wrapped_function(:remove, path, *paths)
      end

      # Returns an expression for replacing entries at the given paths in the JSON array or object,
      # but not creating new entries.
      #
      #   json_op.replace('$.a', 1)           # json_replace(json, '$.a', 1)
      #   json_op.replace('$.a', 1, '$.b', 2) # json_replace(json, '$.a', 1, '$.b', 2)
      def replace(path, value, *args)
        wrapped_function(:replace, path, value, *args)
      end

      # Returns an expression for creating or replacing entries at the given paths in the
      # JSON array or object.
      #
      #   json_op.set('$.a', 1)           # json_set(json, '$.a', 1)
      #   json_op.set('$.a', 1, '$.b', 2) # json_set(json, '$.a', 1, '$.b', 2)
      def set(path, value, *args)
        wrapped_function(:set, path, value, *args)
      end

      # Returns an expression for a set of information extracted from the JSON array or object, or
      # the JSON array or object at the given path.
      #
      #   json_op.tree        # json_tree(json)
      #   json_op.tree('$.a') # json_tree(json, '$.a')
      def tree(*args)
        SQL::Function.new(:json_tree, self, *args)
      end

      # Returns an expression for the type of the JSON value or the JSON value at the given path.
      #
      #   json_op.type         # json_type(json)
      #   json_op.type('$[1]') # json_type(json, '$[1]')
      def type(*args)
        Sequel::SQL::StringExpression.new(:NOOP, SQL::Function.new(:json_type, self, *args))
      end
      alias typeof type

      # Returns a boolean expression for whether the JSON is valid or not.
      def valid
        Sequel::SQL::BooleanExpression.new(:NOOP, SQL::Function.new(:json_valid, self))
      end

      private

      # Internals of the [], get, get_json methods, using a placeholder literal string.
      def json_op(str, args)
        self.class.new(Sequel::SQL::PlaceholderLiteralString.new(str, [self, args]))
      end

      # Internals of the methods that return functions prefixed with +json_+.
      def function(name, *args)
        SQL::Function.new("#{function_prefix}_#{name}", self, *args)
      end

      # Internals of the methods that return functions prefixed with +json_+, that
      # return JSON values.
      def wrapped_function(*args)
        self.class.new(function(*args))
      end
    end

    # JSONOp is used for SQLite json-specific functions and operators.
    class JSONOp < JSONBaseOp
      private

      def function_prefix
        "json"
      end
    end

    # JSONOp is used for SQLite jsonb-specific functions and operators.
    class JSONBOp < JSONBaseOp
      private

      def function_prefix
        "jsonb"
      end
    end

    module JSONOpMethods
      # Wrap the receiver in an JSONOp so you can easily use the SQLite
      # json functions and operators with it.
      def sqlite_json_op
        JSONOp.new(self)
      end

      # Wrap the receiver in an JSONBOp so you can easily use the SQLite
      # jsonb functions and operators with it.
      def sqlite_jsonb_op
        JSONBOp.new(self)
      end
    end
  end

  module SQL::Builders
    # Return the object wrapped in an SQLite::JSONOp.
    def sqlite_json_op(v)
      case v
      when SQLite::JSONOp
        v
      else
        SQLite::JSONOp.new(v)
      end
    end

    # Return the object wrapped in an SQLite::JSONBOp.
    def sqlite_jsonb_op(v)
      case v
      when SQLite::JSONBOp
        v
      else
        SQLite::JSONBOp.new(v)
      end
    end
  end

  class SQL::GenericExpression
    include Sequel::SQLite::JSONOpMethods
  end

  class LiteralString
    include Sequel::SQLite::JSONOpMethods
  end
end

# :nocov:
if Sequel.core_extensions?
  class Symbol
    include Sequel::SQLite::JSONOpMethods
  end
end

if defined?(Sequel::CoreRefinements)
  module Sequel::CoreRefinements
    refine Symbol do
      send INCLUDE_METH, Sequel::SQLite::JSONOpMethods
    end
  end
end
# :nocov:
