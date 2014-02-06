# The pg_json_ops extension adds support to Sequel's DSL to make
# it easier to call PostgreSQL JSON functions and operators (added
# first in PostgreSQL 9.3).
#
# To load the extension:
#
#   Sequel.extension :pg_json_ops
#
# The most common usage is passing an expression to Sequel.pg_json_op:
#
#   j = Sequel.pg_json_op(:json_column)
#
# If you have also loaded the pg_json extension, you can use
# Sequel.pg_json as well:
#
#  j = Sequel.pg_json(:json_column)
#
# Also, on most Sequel expression objects, you can call the pg_json
# method:
#
#   j = Sequel.expr(:json_column).pg_json
#
# If you have loaded the {core_extensions extension}[rdoc-ref:doc/core_extensions.rdoc],
# or you have loaded the core_refinements extension
# and have activated refinements for the file, you can also use Symbol#pg_json:
#
#   j = :json_column.pg_json
#
# This creates a Sequel::Postgres::JSONOp object that can be used
# for easier querying:
#
#   j[1]                     # (json_column -> 1)
#   j[%w'a b']               # (json_column #> ARRAY['a','b'])
#   j.get_text(1)            # (json_column ->> 1)
#   j.get_text(%w'a b')      # (json_column #>> ARRAY['a','b'])
#   j.extract('a', 'b')      # json_extract_path(json_column, 'a', 'b')
#   j.extract_text('a', 'b') # json_extract_path_text(json_column, 'a', 'b')
#
#   j.array_length           # json_array_length(json_column)
#   j.array_elements         # json_array_elements(json_column)
#   j.each                   # json_each(json_column)
#   j.each_text              # json_each_text(json_column)
#   j.keys                   # json_object_keys(json_column)
#
#   j.populate(:a)           # json_populate_record(:a, json_column)
#   j.populate_set(:a)       # json_populate_recordset(:a, json_column)
#
# If you are also using the pg_json extension, you should load it before
# loading this extension.  Doing so will allow you to use JSONHash#op and
# JSONArray#op to get a JSONOp, allowing you to perform json operations
# on json literals.
#
# In order to get the automatic conversion from a ruby array to a PostgreSQL array
# (as shown in the #[] and #get_text examples above), you need to load the pg_array
# extension.

module Sequel
  module Postgres
    # The JSONOp class is a simple container for a single object that
    # defines methods that yield Sequel expression objects representing
    # PostgreSQL json operators and functions.
    #
    # In the method documentation examples, assume that:
    #
    #   json_op = Sequel.pg_json(:json)
    class JSONOp < Sequel::SQL::Wrapper
      GET = ["(".freeze, " -> ".freeze, ")".freeze].freeze
      GET_TEXT = ["(".freeze, " ->> ".freeze, ")".freeze].freeze
      GET_PATH = ["(".freeze, " #> ".freeze, ")".freeze].freeze
      GET_PATH_TEXT = ["(".freeze, " #>> ".freeze, ")".freeze].freeze

      # Get JSON array element or object field as json.  If an array is given,
      # gets the object at the specified path.
      #
      #   json_op[1] # (json -> 1)
      #   json_op['a'] # (json -> 'a')
      #   json_op[%w'a b'] # (json #> ARRAY['a', 'b'])
      def [](key)
        if is_array?(key)
          json_op(GET_PATH, wrap_array(key))
        else
          json_op(GET, key)
        end
      end
      alias get []

      # Returns a set of json values for the elements in the json array.
      #
      #   json_op.array_elements # json_oarray_elements(json)
      def array_elements
        function(:json_array_elements)
      end

      # Get the length of the outermost json array.
      #
      #   json_op.array_length # json_array_length(json)
      def array_length
        Sequel::SQL::NumericExpression.new(:NOOP, function(:json_array_length))
      end

      # Returns a set of key and value pairs, where the keys
      # are text and the values are JSON.
      #
      #   json_op.each # json_each(json)
      def each
        function(:json_each)
      end

      # Returns a set of key and value pairs, where the keys
      # and values are both text.
      #
      #   json_op.each_text # json_each_text(json)
      def each_text
        function(:json_each_text)
      end

      # Returns a json value for the object at the given path.
      #
      #   json_op.extract('a') # json_extract_path(json, 'a')
      #   json_op.extract('a', 'b') # json_extract_path(json, 'a', 'b')
      def extract(*a)
        JSONOp.new(function(:json_extract_path, *a))
      end

      # Returns a text value for the object at the given path.
      #
      #   json_op.extract_text('a') # json_extract_path_text(json, 'a')
      #   json_op.extract_text('a', 'b') # json_extract_path_text(json, 'a', 'b')
      def extract_text(*a)
        Sequel::SQL::StringExpression.new(:NOOP, function(:json_extract_path_text, *a))
      end

      # Get JSON array element or object field as text.  If an array is given,
      # gets the object at the specified path.
      #
      #   json_op.get_text(1) # (json ->> 1)
      #   json_op.get_text('a') # (json ->> 'a')
      #   json_op.get_text(%w'a b') # (json #>> ARRAY['a', 'b'])
      def get_text(key)
        if is_array?(key)
          json_op(GET_PATH_TEXT, wrap_array(key))
        else
          json_op(GET_TEXT, key)
        end
      end

      # Returns a set of keys AS text in the json object.
      #
      #   json_op.keys # json_object_keys(json)
      def keys
        function(:json_object_keys)
      end

      # Return the receiver, since it is already a JSONOp.
      def pg_json
        self
      end

      # Expands the given argument using the columns in the json.
      #
      #   json_op.populate(arg) # json_populate_record(arg, json)
      def populate(arg)
        SQL::Function.new(:json_populate_record, arg, self)
      end

      # Expands the given argument using the columns in the json.
      #
      #   json_op.populate_set(arg) # json_populate_recordset(arg, json)
      def populate_set(arg)
        SQL::Function.new(:json_populate_recordset, arg, self)
      end

      private

      # Return a placeholder literal with the given str and args, wrapped
      # in an JSONOp, used by operators that return json.
      def json_op(str, args)
        JSONOp.new(Sequel::SQL::PlaceholderLiteralString.new(str, [self, args]))
      end

      # Return a function with the given name, and the receiver as the first
      # argument, with any additional arguments given.
      def function(name, *args)
        SQL::Function.new(name, self, *args)
      end

      # Whether the given object represents an array in PostgreSQL.
      def is_array?(a)
        a.is_a?(Array) || (defined?(PGArray) && a.is_a?(PGArray)) || (defined?(ArrayOp) && a.is_a?(ArrayOp))
      end

      # Automatically wrap argument in a PGArray if it is a plain Array.
      # Requires that the pg_array extension has been loaded to work.
      def wrap_array(arg)
        if arg.instance_of?(Array) && Sequel.respond_to?(:pg_array)
          Sequel.pg_array(arg)
        else
          arg
        end
      end
    end

    module JSONOpMethods
      # Wrap the receiver in an JSONOp so you can easily use the PostgreSQL
      # json functions and operators with it.
      def pg_json
        JSONOp.new(self)
      end
    end

    if defined?(JSONArray)
      class JSONArray
        # Wrap the JSONHash instance in an JSONOp, allowing you to easily use
        # the PostgreSQL json functions and operators with literal jsons.
        def op
          JSONOp.new(self)
        end
      end

      class JSONHash
        # Wrap the JSONHash instance in an JSONOp, allowing you to easily use
        # the PostgreSQL json functions and operators with literal jsons.
        def op
          JSONOp.new(self)
        end
      end
    end
  end

  module SQL::Builders
    # Return the object wrapped in an Postgres::JSONOp.
    def pg_json_op(v)
      case v
      when Postgres::JSONOp
        v
      else
        Postgres::JSONOp.new(v)
      end
    end
  end

  class SQL::GenericExpression
    include Sequel::Postgres::JSONOpMethods
  end

  class LiteralString
    include Sequel::Postgres::JSONOpMethods
  end
end

# :nocov:
if Sequel.core_extensions?
  class Symbol
    include Sequel::Postgres::JSONOpMethods
  end
end

if defined?(Sequel::CoreRefinements)
  module Sequel::CoreRefinements
    refine Symbol do
      include Sequel::Postgres::JSONOpMethods
    end
  end
end
# :nocov:
