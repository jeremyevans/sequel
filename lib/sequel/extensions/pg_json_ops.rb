# The pg_json_ops extension adds support to Sequel's DSL to make
# it easier to call PostgreSQL JSON functions and operators (added
# first in PostgreSQL 9.3).  It also supports the JSONB functions
# and operators added in PostgreSQL 9.4).
#
# To load the extension:
#
#   Sequel.extension :pg_json_ops
#
# The most common usage is passing an expression to Sequel.pg_json_op
# or Sequel.pg_jsonb_op:
#
#   j = Sequel.pg_json_op(:json_column)
#   jb = Sequel.pg_jsonb_op(:jsonb_column)
#
# If you have also loaded the pg_json extension, you can use
# Sequel.pg_json or Sequel.pg_jsonb as well:
#
#  j = Sequel.pg_json(:json_column)
#  jb = Sequel.pg_jsonb(:jsonb_column)
#
# Also, on most Sequel expression objects, you can call the pg_json
# or pg_jsonb # method:
#
#   j = Sequel.expr(:json_column).pg_json
#   jb = Sequel.expr(:jsonb_column).pg_jsonb
#
# If you have loaded the {core_extensions extension}[rdoc-ref:doc/core_extensions.rdoc],
# or you have loaded the core_refinements extension
# and have activated refinements for the file, you can also use Symbol#pg_json or
# Symbol#pg_jsonb:
#
#   j = :json_column.pg_json
#   jb = :jsonb_column.pg_jsonb
#
# This creates a Sequel::Postgres::JSONOp or Sequel::Postgres::JSONBOp object that can be used
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
#   j.array_elements_text    # json_array_elements_text(json_column)
#   j.each                   # json_each(json_column)
#   j.each_text              # json_each_text(json_column)
#   j.keys                   # json_object_keys(json_column)
#   j.typeof                 # json_typeof(json_column)
#
#   j.populate(:a)           # json_populate_record(:a, json_column)
#   j.populate_set(:a)       # json_populate_recordset(:a, json_column)
#   j.to_record              # json_to_record(json_column)
#   j.to_recordset           # json_to_recordset(json_column)
#
# If you are also using the pg_json extension, you should load it before
# loading this extension.  Doing so will allow you to use the #op method on
# JSONHash, JSONHarray, JSONBHash, and JSONBArray, allowing you to perform json/jsonb operations
# on json/jsonb literals.
#
# In order to get the automatic conversion from a ruby array to a PostgreSQL array
# (as shown in the #[] and #get_text examples above), you need to load the pg_array
# extension.

#
module Sequel
  module Postgres
    # The JSONBaseOp class is a simple container for a single object that
    # defines methods that yield Sequel expression objects representing
    # PostgreSQL json operators and functions.
    #
    # In the method documentation examples, assume that:
    #
    #   json_op = Sequel.pg_json(:json)
    class JSONBaseOp < Sequel::SQL::Wrapper
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
      #   json_op.array_elements # json_array_elements(json)
      def array_elements
        function(:array_elements)
      end

      # Returns a set of text values for the elements in the json array.
      #
      #   json_op.array_elements_text # json_array_elements_text(json)
      def array_elements_text
        function(:array_elements_text)
      end

      # Get the length of the outermost json array.
      #
      #   json_op.array_length # json_array_length(json)
      def array_length
        Sequel::SQL::NumericExpression.new(:NOOP, function(:array_length))
      end

      # Returns a set of key and value pairs, where the keys
      # are text and the values are JSON.
      #
      #   json_op.each # json_each(json)
      def each
        function(:each)
      end

      # Returns a set of key and value pairs, where the keys
      # and values are both text.
      #
      #   json_op.each_text # json_each_text(json)
      def each_text
        function(:each_text)
      end

      # Returns a json value for the object at the given path.
      #
      #   json_op.extract('a') # json_extract_path(json, 'a')
      #   json_op.extract('a', 'b') # json_extract_path(json, 'a', 'b')
      def extract(*a)
        self.class.new(function(:extract_path, *a))
      end

      # Returns a text value for the object at the given path.
      #
      #   json_op.extract_text('a') # json_extract_path_text(json, 'a')
      #   json_op.extract_text('a', 'b') # json_extract_path_text(json, 'a', 'b')
      def extract_text(*a)
        Sequel::SQL::StringExpression.new(:NOOP, function(:extract_path_text, *a))
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
        function(:object_keys)
      end

      # Expands the given argument using the columns in the json.
      #
      #   json_op.populate(arg) # json_populate_record(arg, json)
      def populate(arg)
        SQL::Function.new(function_name(:populate_record), arg, self)
      end

      # Expands the given argument using the columns in the json.
      #
      #   json_op.populate_set(arg) # json_populate_recordset(arg, json)
      def populate_set(arg)
        SQL::Function.new(function_name(:populate_recordset), arg, self)
      end

      # Builds arbitrary record from json object.  You need to define the
      # structure of the record using #as on the resulting object:
      #
      #   json_op.to_record.as(:x, [Sequel.lit('a integer'), Sequel.lit('b text')]) # json_to_record(json) AS x(a integer, b text)
      def to_record
        function(:to_record)
      end

      # Builds arbitrary set of records from json array of objects.  You need to define the
      # structure of the records using #as on the resulting object:
      #
      #   json_op.to_recordset.as(:x, [Sequel.lit('a integer'), Sequel.lit('b text')]) # json_to_recordset(json) AS x(a integer, b text)
      def to_recordset
        function(:to_recordset)
      end

      # Returns the type of the outermost json value as text.
      #
      #   json_op.typeof # json_typeof(json)
      def typeof
        function(:typeof)
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
        SQL::Function.new(function_name(name), self, *args)
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

    # JSONBaseOp subclass for the json type
    class JSONOp < JSONBaseOp
      # Return the receiver, since it is already a JSONOp.
      def pg_json
        self
      end

      private

      # The json type functions are prefixed with json_
      def function_name(name)
        "json_#{name}"
      end
    end

    # JSONBaseOp subclass for the jsonb type.
    #
    # In the method documentation examples, assume that:
    #
    #   jsonb_op = Sequel.pg_jsonb(:jsonb)
    class JSONBOp < JSONBaseOp
      CONTAIN_ALL = ["(".freeze, " ?& ".freeze, ")".freeze].freeze
      CONTAIN_ANY = ["(".freeze, " ?| ".freeze, ")".freeze].freeze
      CONTAINS = ["(".freeze, " @> ".freeze, ")".freeze].freeze
      CONTAINED_BY = ["(".freeze, " <@ ".freeze, ")".freeze].freeze
      HAS_KEY = ["(".freeze, " ? ".freeze, ")".freeze].freeze

      # Check if the receiver contains all of the keys in the given array:
      #
      #   jsonb_op.contain_all(:a) # (jsonb ?& a)
      def contain_all(other)
        bool_op(CONTAIN_ALL, wrap_input_array(other))
      end

      # Check if the receiver contains any of the keys in the given array:
      #
      #   jsonb_op.contain_any(:a) # (jsonb ?| a)
      def contain_any(other)
        bool_op(CONTAIN_ANY, wrap_input_array(other))
      end

      # Check if the receiver contains all entries in the other jsonb:
      #
      #   jsonb_op.contains(:h) # (jsonb @> h)
      def contains(other)
        bool_op(CONTAINS, wrap_input_jsonb(other))
      end

      # Check if the other jsonb contains all entries in the receiver:
      #
      #   jsonb_op.contained_by(:h) # (jsonb <@ h)
      def contained_by(other)
        bool_op(CONTAINED_BY, wrap_input_jsonb(other))
      end

      # Check if the receiver contains the given key:
      #
      #   jsonb_op.has_key?('a') # (jsonb ? 'a')
      def has_key?(key)
        bool_op(HAS_KEY, key)
      end
      alias include? has_key?

      # Return the receiver, since it is already a JSONBOp.
      def pg_jsonb
        self
      end

      private

      # Return a placeholder literal with the given str and args, wrapped
      # in a boolean expression, used by operators that return booleans.
      def bool_op(str, other)
        Sequel::SQL::BooleanExpression.new(:NOOP, Sequel::SQL::PlaceholderLiteralString.new(str, [value, other]))
      end

      # Wrap argument in a PGArray if it is an array
      def wrap_input_array(obj)
        if obj.is_a?(Array) && Sequel.respond_to?(:pg_array) 
          Sequel.pg_array(obj)
        else
          obj
        end
      end

      # Wrap argument in a JSONBArray or JSONBHash if it is an array or hash.
      def wrap_input_jsonb(obj)
        if Sequel.respond_to?(:pg_jsonb) && (obj.is_a?(Array) || obj.is_a?(Hash))
          Sequel.pg_jsonb(obj)
        else
          obj
        end
      end

      # The jsonb type functions are prefixed with jsonb_
      def function_name(name)
        "jsonb_#{name}"
      end
    end

    module JSONOpMethods
      # Wrap the receiver in an JSONOp so you can easily use the PostgreSQL
      # json functions and operators with it.
      def pg_json
        JSONOp.new(self)
      end
      #
      # Wrap the receiver in an JSONBOp so you can easily use the PostgreSQL
      # jsonb functions and operators with it.
      def pg_jsonb
        JSONBOp.new(self)
      end
    end

    if defined?(JSONArray)
      class JSONArray
        # Wrap the JSONArray instance in an JSONOp, allowing you to easily use
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

      class JSONBArray
        # Wrap the JSONBArray instance in an JSONBOp, allowing you to easily use
        # the PostgreSQL jsonb functions and operators with literal jsonbs.
        def op
          JSONBOp.new(self)
        end
      end

      class JSONBHash
        # Wrap the JSONBHash instance in an JSONBOp, allowing you to easily use
        # the PostgreSQL jsonb functions and operators with literal jsonbs.
        def op
          JSONBOp.new(self)
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

    # Return the object wrapped in an Postgres::JSONBOp.
    def pg_jsonb_op(v)
      case v
      when Postgres::JSONBOp
        v
      else
        Postgres::JSONBOp.new(v)
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
