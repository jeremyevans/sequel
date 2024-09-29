# frozen-string-literal: true
#
# The pg_json_ops extension adds support to Sequel's DSL to make
# it easier to call PostgreSQL JSON functions and operators (added
# first in PostgreSQL 9.3).  It also supports the JSONB functions
# and operators added in PostgreSQL 9.4, as well as additional
# functions and operators added in later versions.
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
# or pg_jsonb method:
#
#   j = Sequel[:json_column].pg_json
#   jb = Sequel[:jsonb_column].pg_jsonb
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
# for easier querying.  The following methods are available for both JSONOp and JSONBOp instances:
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
#   j.strip_nulls            # json_strip_nulls(json_column)
#
#   j.populate(:a)           # json_populate_record(:a, json_column)
#   j.populate_set(:a)       # json_populate_recordset(:a, json_column)
#   j.to_record              # json_to_record(json_column)
#   j.to_recordset           # json_to_recordset(json_column)
#
# There are additional methods are are only supported on JSONBOp instances:
#
#   j - 1                     # (jsonb_column - 1)
#   j.concat(:h)              # (jsonb_column || h)
#   j.contain_all(:a)         # (jsonb_column ?& a)
#   j.contain_any(:a)         # (jsonb_column ?| a)
#   j.contains(:h)            # (jsonb_column @> h)
#   j.contained_by(:h)        # (jsonb_column <@ h)
#   j.delete_path(%w'0 a')    # (jsonb_column #- ARRAY['0','a'])
#   j.has_key?('a')           # (jsonb_column ? 'a')
#   j.insert(%w'0 a', 'a'=>1) # jsonb_insert(jsonb_column, ARRAY[0, 'a'], '{"a":1}'::jsonb, false)
#   j.pretty                  # jsonb_pretty(jsonb_column)
#   j.set(%w'0 a', :h)        # jsonb_set(jsonb_column, ARRAY['0','a'], h, true)
#
#   j.set_lax(%w'0 a', :h, false, 'raise_exception')
#   # jsonb_set_lax(jsonb_column, ARRAY['0','a'], h, false, 'raise_exception')
#
# On PostgreSQL 12+ SQL/JSON path functions and operators are supported:
#
#   j.path_exists('$.foo')      # (jsonb_column @? '$.foo')
#   j.path_match('$.foo')       # (jsonb_column @@ '$.foo')
#
#   j.path_exists!('$.foo')     # jsonb_path_exists(jsonb_column, '$.foo')
#   j.path_match!('$.foo')      # jsonb_path_match(jsonb_column, '$.foo')
#   j.path_query('$.foo')       # jsonb_path_query(jsonb_column, '$.foo')
#   j.path_query_array('$.foo') # jsonb_path_query_array(jsonb_column, '$.foo')
#   j.path_query_first('$.foo') # jsonb_path_query_first(jsonb_column, '$.foo')
#
# For the PostgreSQL 12+ SQL/JSON path functions, one argument is required (+path+) and
# two more arguments are optional (+vars+ and +silent+).  +path+ specifies the JSON path.
# +vars+ specifies a hash or a string in JSON format of named variables to be
# substituted in +path+. +silent+ specifies whether errors are suppressed. By default,
# errors are not suppressed.
#
# On PostgreSQL 13+ timezone-aware SQL/JSON path functions and operators are supported:
#
#   j.path_exists_tz!('$.foo')     # jsonb_path_exists_tz(jsonb_column, '$.foo')
#   j.path_match_tz!('$.foo')      # jsonb_path_match_tz(jsonb_column, '$.foo')
#   j.path_query_tz('$.foo')       # jsonb_path_query_tz(jsonb_column, '$.foo')
#   j.path_query_array_tz('$.foo') # jsonb_path_query_array_tz(jsonb_column, '$.foo')
#   j.path_query_first_tz('$.foo') # jsonb_path_query_first_tz(jsonb_column, '$.foo')
#
# On PostgreSQL 14+, The JSONB <tt>[]</tt> method will use subscripts instead of being
# the same as +get+, if the value being wrapped is an identifer:
#
#   Sequel.pg_jsonb_op(:jsonb_column)[1]       # jsonb_column[1]
#   Sequel.pg_jsonb_op(:jsonb_column)[1][2]    # jsonb_column[1][2]
#   Sequel.pg_jsonb_op(Sequel[:j][:b])[1]      # j.b[1]
#
# This support allows you to use JSONB subscripts in UPDATE statements to update only
# part of a column:
#
#   c = Sequel.pg_jsonb_op(:c)
#   DB[:t].update(c['key1'] => '1', c['key2'] => '"a"')
#   #  UPDATE "t" SET "c"['key1'] = '1', "c"['key2'] = '"a"'
#
# Note that you have to provide the value of a JSONB subscript as a JSONB value, so this
# will update +key1+ to use the number <tt>1</tt>, and +key2+ to use the string <tt>a</tt>.
# For this reason it may be simpler to use +to_json+:
#
#   c = Sequel.pg_jsonb_op(:c)
#   DB[:t].update(c['key1'] => 1.to_json, c['key2'] => "a".to_json)
#
# On PostgreSQL 16+, the <tt>IS [NOT] JSON</tt> operator is supported:
#
#   j.is_json                              # j IS JSON
#   j.is_json(type: :object)               # j IS JSON OBJECT
#   j.is_json(type: :object, unique: true) # j IS JSON OBJECT WITH UNIQUE
#   j.is_not_json                          # j IS NOT JSON
#   j.is_not_json(type: :array)            # j IS NOT JSON ARRAY
#   j.is_not_json(unique: true)            # j IS NOT JSON WITH UNIQUE
#
# On PostgreSQL 17+, the additional JSON functions are supported (see method documentation
# for additional options):
#
#   j.exists('$.foo')     # json_exists(jsonb_column, '$.foo')
#   j.value('$.foo')      # json_value(jsonb_column, '$.foo')
#   j.query('$.foo')      # json_query(jsonb_column, '$.foo')
#
#   j.exists('$.foo', passing: {a: 1}) # json_exists(jsonb_column, '$.foo' PASSING 1 AS a)
#   j.value('$.foo', returning: Time)  # json_value(jsonb_column, '$.foo' RETURNING timestamp)
#   j.query('$.foo', wrapper: true)    # json_query(jsonb_column, '$.foo' WITH WRAPPER)
#
#   j.table('$.foo') do
#      String :bar
#      Integer :baz
#   end
#   # json_table("jsonb_column", '$.foo' COLUMNS("bar" text, "baz" integer))
#
#   j.table('$.foo', passing: {a: 1}) do
#      ordinality :id
#      String :bar, format: :json, on_error: :empty_object
#      nested '$.baz' do
#        Integer :q, path: '$.quux', on_empty: :error
#      end
#      exists :x, Date, on_error: false
#   end
#   # json_table(jsonb_column, '$.foo' PASSING 1 AS a COLUMNS(
#   #   "id" FOR ORDINALITY,
#   #   "bar" text FORMAT JSON EMPTY OBJECT ON ERROR,
#   #   NESTED '$.baz' COLUMNS(
#   #     "q" integer PATH '$.quux' ERROR ON EMPTY
#   #   ),
#   #   "d" date EXISTS FALSE ON ERROR
#   # ))
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
# Related modules: Sequel::Postgres::JSONBaseOp,  Sequel::Postgres::JSONOp,
# Sequel::Postgres::JSONBOp

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

      IS_JSON = ["(".freeze, " IS JSON".freeze, "".freeze, ")".freeze].freeze
      IS_NOT_JSON = ["(".freeze, " IS NOT JSON".freeze, "".freeze, ")".freeze].freeze
      EMPTY_STRING = Sequel::LiteralString.new('').freeze
      WITH_UNIQUE = Sequel::LiteralString.new(' WITH UNIQUE').freeze
      IS_JSON_MAP = {
        nil => EMPTY_STRING,
        :value => Sequel::LiteralString.new(' VALUE').freeze,
        :scalar => Sequel::LiteralString.new(' SCALAR').freeze,
        :object => Sequel::LiteralString.new(' OBJECT').freeze,
        :array => Sequel::LiteralString.new(' ARRAY').freeze
      }.freeze

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

      # Return whether the given JSON path yields any items in the receiver.
      # Options:
      #
      # :on_error :: How to handle errors when evaluating the JSON path expression.
      #              true :: Return true
      #              false :: Return false (default behavior)
      #              :null :: Return nil
      #              :error :: raise a DatabaseError
      # :passing :: Variables to pass to the JSON path expression.  Keys are variable
      #             names, values are the values of the variable.
      # 
      #   json_op.exists("$.a") # json_exists(json, '$.a')
      #   json_op.exists("$.a", passing: {a: 1}) # json_exists(json, '$.a' PASSING 1 AS a)
      #   json_op.exists("$.a", on_error: :error) # json_exists(json, '$.a' ERROR ON ERROR)
      def exists(path, opts=OPTS)
        Sequel::SQL::BooleanExpression.new(:NOOP, JSONExistsOp.new(self, path, opts))
      end

      # Returns a JSON value for the object at the given path.
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

      # Return whether the json object can be parsed as JSON.
      #
      # Options:
      # :type :: Check whether the json object can be parsed as a specific type
      #          of JSON (:value, :scalar, :object, :array).
      # :unique :: Check JSON objects for unique keys.
      #
      #   json_op.is_json                 # json IS JSON
      #   json_op.is_json(type: :object)  # json IS JSON OBJECT
      #   json_op.is_json(unique: true)   # json IS JSON WITH UNIQUE
      def is_json(opts=OPTS)
        _is_json(IS_JSON, opts)
      end

      # Return whether the json object cannot be parsed as JSON. The opposite
      # of #is_json. See #is_json for options.
      #
      #   json_op.is_not_json                 # json IS NOT JSON
      #   json_op.is_not_json(type: :object)  # json IS NOT JSON OBJECT
      #   json_op.is_not_json(unique: true)   # json IS NOT JSON WITH UNIQUE
      def is_not_json(opts=OPTS)
        _is_json(IS_NOT_JSON, opts)
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

      # Return the result of applying the JSON path expression to the receiver, by default
      # returning results as jsonb.  Options:
      #
      # :on_empty :: How to handle case where path expression yields an empty set.
      #              Uses same values as :on_error option.
      # :on_error :: How to handle errors when evaluating the JSON path expression:
      #              :null :: Return nil (default)
      #              :empty_array :: Return an empty array
      #              :empty_object :: Return an empty object
      #              :error :: raise a DatabaseError
      #              any other value :: used as default value
      # :passing :: Variables to pass to the JSON path expression.  Keys are variable
      #             names, values are the values of the variable.
      # :returning :: The data type to return (jsonb by default)
      # :wrapper :: How to wrap returned values:
      #             true, :unconditional :: Always wrap returning values in an array
      #             :conditional :: Only wrap multiple return values in an array
      #             :omit_quotes :: Do not wrap scalar strings in quotes
      # 
      #   json_op.query("$.a") # json_query(json, '$.a')
      #   json_op.query("$.a", passing: {a: 1}) # json_query(json, '$.a' PASSING 1 AS a)
      #   json_op.query("$.a", on_error: :empty_array) # json_query(json, '$.a' EMPTY ARRAY ON ERROR)
      #   json_op.query("$.a", returning: Time) # json_query(json, '$.a' RETURNING timestamp)
      #   json_op.query("$.a", on_empty: 2) # json_query(json, '$.a' DEFAULT 2 ON EMPTY)
      #   json_op.query("$.a", wrapper: true) # json_query(json, '$.a' WITH WRAPPER)
      def query(path, opts=OPTS)
        self.class.new(JSONQueryOp.new(self, path, opts))
      end

      # Returns a json value stripped of all internal null values.
      #
      #   json_op.strip_nulls # json_strip_nulls(json)
      def strip_nulls
        self.class.new(function(:strip_nulls))
      end

      # Returns json_table SQL function expression, querying JSON data and returning
      # the results as a relational view, which can be accessed similarly to a regular
      # SQL table. This accepts a block that is handled in a similar manner to
      # Database#create_table, though it operates differently.
      #
      # Table level options:
      #
      # :on_error :: How to handle errors when evaluating the JSON path expression.
      #              :empty_array :: Return an empty array/result set
      #              :error :: raise a DatabaseError
      # :passing :: Variables to pass to the JSON path expression.  Keys are variable
      #             names, values are the values of the variable.
      #
      # Inside the block, the following methods can be used:
      #
      # ordinality(name) :: Include a FOR ORDINALITY column, which operates similar to an
      #                     autoincrementing primary key.
      # column(name, type, opts={}) :: Return a normal column that uses the given type.
      # exists(name, type, opts={}) :: Return a boolean column for whether the JSON path yields any values.
      # nested(path, &block) :: Extract nested data from the result set at the given path.
      #                         This block is treated the same as a json_table block, and
      #                         arbitrary levels of nesting are supported.
      # 
      # The +column+ method supports the following options:
      # 
      # :path :: JSON path to the object (the default is <tt>$.NAME</tt>, where +NAME+ is the
      #          name of the column).
      # :format :: Set to +:json+ to use FORMAT JSON, when you expect the value to be a
      #            valid JSON object.
      # :on_empty, :on_error :: How to handle case where JSON path evaluation is empty or
      #                         results in an error. Values supported are:
      #                         :empty_array :: Return empty array (requires <tt>format: :json</tt>)
      #                         :empty_object :: Return empty object (requires <tt>format: :json</tt>)
      #                         :error :: Raise a DatabaseError
      #                         :null :: Return nil (NULL)
      # :wrapper :: How to wrap returned values:
      #             true, :unconditional :: Always wrap returning values in an array
      #             :conditional :: Only wrap multiple return values in an array
      #             :keep_quotes :: Wrap scalar strings in quotes
      #             :omit_quotes :: Do not wrap scalar strings in quotes
      #
      # The +exists+ method supports the following options:
      #
      # :path :: JSON path to the object (same as +column+ option)
      # :on_error :: How to handle case where JSON path evaluation results in an error.
      #              Values supported are:
      #              :error :: Raise a DatabaseError
      #              true :: Return true
      #              false :: Return false
      #              :null :: Return nil (NULL)
      #
      # Inside the block, methods for Ruby class names are also supported, allowing you
      # to use syntax such as:
      #
      #   json_op.table('$.a') do
      #     String :b
      #     Integer :c, path: '$.d'
      #   end
      #
      # One difference between this method and Database#create_table is that method_missing
      # is not supported inside the block.  Use the +column+ method for PostgreSQL types
      # that are not mapped to Ruby classes.
      def table(path, opts=OPTS, &block)
        JSONTableOp.new(self, path, opts, &block)
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

      # If called without arguments, operates as SQL::Wrapper#value.  Otherwise, 
      # return the result of applying the JSON path expression to the receiver, by default
      # returning results as text.  Options:
      #
      # :on_empty :: How to handle case where path expression yields an empty set.
      #              Uses same values as :on_error option.
      # :on_error :: How to handle errors when evaluating the JSON path expression.
      #              :null :: Return nil (default)
      #              :error :: raise a DatabaseError
      #              any other value :: used as default value
      # :passing :: Variables to pass to the JSON path expression.  Keys are variable
      #             names, values are the values of the variable.
      # :returning :: The data type to return (text by default)
      # 
      #   json_op.value("$.a") # json_value(json, '$.a')
      #   json_op.value("$.a", passing: {a: 1}) # json_value(json, '$.a' PASSING 1 AS a)
      #   json_op.value("$.a", on_error: :error) # json_value(json, '$.a' ERROR ON ERROR)
      #   json_op.value("$.a", returning: Time) # json_value(json, '$.a' RETURNING timestamp)
      #   json_op.value("$.a", on_empty: 2) # json_value(json, '$.a' DEFAULT 2 ON EMPTY)
      def value(path=(no_args_given = true), opts=OPTS)
        if no_args_given
          # Act as SQL::Wrapper#value
          super()
        else
          Sequel::SQL::StringExpression.new(:NOOP, JSONValueOp.new(self, path, opts))
        end
      end

      private

      # Internals of IS [NOT] JSON support
      def _is_json(lit_array, opts)
        raise Error, "invalid is_json :type option: #{opts[:type].inspect}" unless type = IS_JSON_MAP[opts[:type]]
        unique = opts[:unique] ? WITH_UNIQUE : EMPTY_STRING
        Sequel::SQL::BooleanExpression.new(:NOOP, Sequel::SQL::PlaceholderLiteralString.new(lit_array, [self, type, unique]))
      end

      # Return a placeholder literal with the given str and args, wrapped
      # in an JSONOp or JSONBOp, used by operators that return json or jsonb.
      def json_op(str, args)
        self.class.new(Sequel::SQL::PlaceholderLiteralString.new(str, [self, args]))
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
      CONCAT = ["(".freeze, " || ".freeze, ")".freeze].freeze
      CONTAIN_ALL = ["(".freeze, " ?& ".freeze, ")".freeze].freeze
      CONTAIN_ANY = ["(".freeze, " ?| ".freeze, ")".freeze].freeze
      CONTAINS = ["(".freeze, " @> ".freeze, ")".freeze].freeze
      CONTAINED_BY = ["(".freeze, " <@ ".freeze, ")".freeze].freeze
      DELETE_PATH = ["(".freeze, " #- ".freeze, ")".freeze].freeze
      HAS_KEY = ["(".freeze, " ? ".freeze, ")".freeze].freeze
      PATH_EXISTS = ["(".freeze, " @? ".freeze, ")".freeze].freeze
      PATH_MATCH = ["(".freeze, " @@ ".freeze, ")".freeze].freeze

      # Support subscript syntax for JSONB.
      def [](key)
        if is_array?(key)
          super
        else
          case @value
          when Symbol, SQL::Identifier, SQL::QualifiedIdentifier, JSONBSubscriptOp
            # Only use subscripts for identifiers.  In other cases, switching from
            # the -> operator to [] for subscripts causes SQL syntax issues.  You
            # only need the [] for subscripting when doing assignment, and
            # assignment is generally done on identifiers.
            self.class.new(JSONBSubscriptOp.new(self, key))
          else
            super
          end
        end
      end

      # jsonb expression for deletion of the given argument from the
      # current jsonb.
      #
      #   jsonb_op - "a" # (jsonb - 'a')
      def -(other)
        self.class.new(super)
      end

      # jsonb expression for concatenation of the given jsonb into
      # the current jsonb.
      #
      #   jsonb_op.concat(:h) # (jsonb || h)
      def concat(other)
        json_op(CONCAT, wrap_input_jsonb(other))
      end

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

      # Removes the given path from the receiver.
      #
      #   jsonb_op.delete_path(:h) # (jsonb #- h)
      def delete_path(other)
        json_op(DELETE_PATH, wrap_input_array(other))
      end

      # Check if the receiver contains the given key:
      #
      #   jsonb_op.has_key?('a') # (jsonb ? 'a')
      def has_key?(key)
        bool_op(HAS_KEY, key)
      end
      alias include? has_key?

      # Inserts the given jsonb value at the given path in the receiver.
      # The default is to insert the value before the given path, but
      # insert_after can be set to true to insert it after the given path.
      #
      #   jsonb_op.insert(['a', 'b'], h) # jsonb_insert(jsonb, ARRAY['a', 'b'], h, false)
      #   jsonb_op.insert(['a', 'b'], h, true) # jsonb_insert(jsonb, ARRAY['a', 'b'], h, true)
      def insert(path, other, insert_after=false)
        self.class.new(function(:insert, wrap_input_array(path), wrap_input_jsonb(other), insert_after))
      end

      # Returns whether the JSON path returns any item for the json object.
      #
      #   json_op.path_exists("$.foo") # (json @? '$.foo')
      def path_exists(path)
        bool_op(PATH_EXISTS, path)
      end

      # Returns whether the JSON path returns any item for the json object.
      #
      #   json_op.path_exists!("$.foo")
      #   # jsonb_path_exists(json, '$.foo')
      #
      #   json_op.path_exists!("$.foo ? ($ > $x)", x: 2)
      #   # jsonb_path_exists(json, '$.foo ? ($ > $x)', '{"x":2}')
      #
      #   json_op.path_exists!("$.foo ? ($ > $x)", {x: 2}, true)
      #   # jsonb_path_exists(json, '$.foo ? ($ > $x)', '{"x":2}', true)
      def path_exists!(path, vars=nil, silent=nil)
        Sequel::SQL::BooleanExpression.new(:NOOP, _path_function(:jsonb_path_exists, path, vars, silent))
      end

      # The same as #path_exists!, except that timezone-aware conversions are used for date/time values.
      def path_exists_tz!(path, vars=nil, silent=nil)
        Sequel::SQL::BooleanExpression.new(:NOOP, _path_function(:jsonb_path_exists_tz, path, vars, silent))
      end

      # Returns the first item of the result of JSON path predicate check for the json object.
      # Returns nil if the first item is not true or false.
      #
      #   json_op.path_match("$.foo") # (json @@ '$.foo')
      def path_match(path)
        bool_op(PATH_MATCH, path)
      end

      # Returns the first item of the result of JSON path predicate check for the json object.
      # Returns nil if the first item is not true or false and silent is true.
      #
      #   json_op.path_match!("$.foo")
      #   # jsonb_path_match(json, '$.foo')
      #
      #   json_op.path_match!("$.foo ? ($ > $x)", x: 2)
      #   # jsonb_path_match(json, '$.foo ? ($ > $x)', '{"x":2}')
      #
      #   json_op.path_match!("$.foo ? ($ > $x)", {x: 2}, true)
      #   # jsonb_path_match(json, '$.foo ? ($ > $x)', '{"x":2}', true)
      def path_match!(path, vars=nil, silent=nil)
        Sequel::SQL::BooleanExpression.new(:NOOP, _path_function(:jsonb_path_match, path, vars, silent))
      end

      # The same as #path_match!, except that timezone-aware conversions are used for date/time values.
      def path_match_tz!(path, vars=nil, silent=nil)
        Sequel::SQL::BooleanExpression.new(:NOOP, _path_function(:jsonb_path_match_tz, path, vars, silent))
      end

      # Returns a set of all jsonb values specified by the JSON path
      # for the json object.
      #
      #   json_op.path_query("$.foo")
      #   # jsonb_path_query(json, '$.foo')
      #
      #   json_op.path_query("$.foo ? ($ > $x)", x: 2)
      #   # jsonb_path_query(json, '$.foo ? ($ > $x)', '{"x":2}')
      #
      #   json_op.path_query("$.foo ? ($ > $x)", {x: 2}, true)
      #   # jsonb_path_query(json, '$.foo ? ($ > $x)', '{"x":2}', true)
      def path_query(path, vars=nil, silent=nil)
        _path_function(:jsonb_path_query, path, vars, silent)
      end

      # The same as #path_query, except that timezone-aware conversions are used for date/time values.
      def path_query_tz(path, vars=nil, silent=nil)
        _path_function(:jsonb_path_query_tz, path, vars, silent)
      end

      # Returns a jsonb array of all values specified by the JSON path
      # for the json object.
      #
      #   json_op.path_query_array("$.foo")
      #   # jsonb_path_query_array(json, '$.foo')
      #
      #   json_op.path_query_array("$.foo ? ($ > $x)", x: 2)
      #   # jsonb_path_query_array(json, '$.foo ? ($ > $x)', '{"x":2}')
      #
      #   json_op.path_query_array("$.foo ? ($ > $x)", {x: 2}, true)
      #   # jsonb_path_query_array(json, '$.foo ? ($ > $x)', '{"x":2}', true)
      def path_query_array(path, vars=nil, silent=nil)
        JSONBOp.new(_path_function(:jsonb_path_query_array, path, vars, silent))
      end

      # The same as #path_query_array, except that timezone-aware conversions are used for date/time values.
      def path_query_array_tz(path, vars=nil, silent=nil)
        JSONBOp.new(_path_function(:jsonb_path_query_array_tz, path, vars, silent))
      end

      # Returns the first item of the result specified by the JSON path
      # for the json object.
      #
      #   json_op.path_query_first("$.foo")
      #   # jsonb_path_query_first(json, '$.foo')
      #
      #   json_op.path_query_first("$.foo ? ($ > $x)", x: 2)
      #   # jsonb_path_query_first(json, '$.foo ? ($ > $x)', '{"x":2}')
      #
      #   json_op.path_query_first("$.foo ? ($ > $x)", {x: 2}, true)
      #   # jsonb_path_query_first(json, '$.foo ? ($ > $x)', '{"x":2}', true)
      def path_query_first(path, vars=nil, silent=nil)
        JSONBOp.new(_path_function(:jsonb_path_query_first, path, vars, silent))
      end

      # The same as #path_query_first, except that timezone-aware conversions are used for date/time values.
      def path_query_first_tz(path, vars=nil, silent=nil)
        JSONBOp.new(_path_function(:jsonb_path_query_first_tz, path, vars, silent))
      end

      # Return the receiver, since it is already a JSONBOp.
      def pg_jsonb
        self
      end

      # Return a pretty printed version of the receiver as a string expression.
      #
      #   jsonb_op.pretty # jsonb_pretty(jsonb)
      def pretty
        Sequel::SQL::StringExpression.new(:NOOP, function(:pretty))
      end

      # Set the given jsonb value at the given path in the receiver.
      # By default, this will create the value if it does not exist, but
      # create_missing can be set to false to not create a new value.
      #
      #   jsonb_op.set(['a', 'b'], h) # jsonb_set(jsonb, ARRAY['a', 'b'], h, true)
      #   jsonb_op.set(['a', 'b'], h, false) # jsonb_set(jsonb, ARRAY['a', 'b'], h, false)
      def set(path, other, create_missing=true)
        self.class.new(function(:set, wrap_input_array(path), wrap_input_jsonb(other), create_missing))
      end

      # The same as #set, except if +other+ is +nil+, then behaves according to +null_value_treatment+,
      # which can be one of 'raise_exception', 'use_json_null' (default), 'delete_key', or 'return_target'.
      def set_lax(path, other, create_missing=true, null_value_treatment='use_json_null')
        self.class.new(function(:set_lax, wrap_input_array(path), wrap_input_jsonb(other), create_missing, null_value_treatment))
      end

      private

      # Internals of the jsonb SQL/JSON path functions.
      def _path_function(func, path, vars, silent)
        args = []
        if vars
          if vars.is_a?(Hash)
            vars = vars.to_json
          end
          args << vars

          unless silent.nil?
            args << silent
          end
        end
        SQL::Function.new(func, self, path, *args)
      end

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

    # Represents JSONB subscripts. This is abstracted because the
    # subscript support depends on the database version.
    class JSONBSubscriptOp < SQL::Expression
      SUBSCRIPT = ["".freeze, "[".freeze, "]".freeze].freeze

      # The expression being subscripted
      attr_reader :expression

      # The subscript to use
      attr_reader :sub

      # Set the expression and subscript to the given arguments
      def initialize(expression, sub)
        @expression = expression
        @sub = sub
        freeze
      end

      # Use subscripts instead of -> operator on PostgreSQL 14+
      def to_s_append(ds, sql)
        server_version = ds.db.server_version
        frag = server_version && server_version >= 140000 ? SUBSCRIPT : JSONOp::GET
        ds.literal_append(sql, Sequel::SQL::PlaceholderLiteralString.new(frag, [@expression, @sub]))
      end

      # Support transforming of jsonb subscripts
      def sequel_ast_transform(transformer)
        self.class.new(transformer.call(@expression), transformer.call(@sub))
      end
    end

    # Object representing json_exists calls
    class JSONExistsOp < SQL::Expression
      ON_ERROR_SQL = {
        true => 'TRUE',
        false => 'FALSE',
        :null =>  'UNKNOWN',
        :error => 'ERROR',
      }.freeze
      private_constant :ON_ERROR_SQL

      # Expression (context_item in PostgreSQL terms), usually JSONBaseOp instance
      attr_reader :expr

      # JSON path expression to apply against the expression
      attr_reader :path

      # Variables to set in the JSON path expression
      attr_reader :passing

      # How to handle errors when evaluating the JSON path expression
      attr_reader :on_error

      # See JSONBaseOp#exists for documentation on the options.
      def initialize(expr, path, opts=OPTS)
        @expr = expr
        @path = path
        @passing = opts[:passing]
        @on_error = opts[:on_error]
        freeze
      end

      # Append the SQL function call expression to the SQL
      def to_s_append(ds, sql)
        to_s_append_function_name(ds, sql)
        to_s_append_args_passing(ds, sql)
        to_s_append_on_error(ds, sql)
        sql << ')'
      end

      # Support transforming of function call expression
      def sequel_ast_transform(transformer)
        opts = {}
        transform_opts(transformer, opts)
        self.class.new(transformer.call(@expr), @path, opts)
      end

      private

      # Set the :passing and :on_error options when doing an
      # AST transform.
      def transform_opts(transformer, opts)
        if @passing
          passing = opts[:passing] = {}
          @passing.each do |k, v|
            passing[k] = transformer.call(v)
          end
        end

        opts[:on_error] = @on_error
      end
      
      def to_s_append_function_name(ds, sql)
        sql << 'json_exists('
      end

      # Append the expression, path, and optional PASSING fragments
      def to_s_append_args_passing(ds, sql)
        ds.literal_append(sql, @expr)
        sql << ', '
        ds.literal_append(sql, @path)

        if (passing = @passing) && !passing.empty?
          sql << ' PASSING '
          comma = false
          passing.each do |k, v|
            if comma
              sql << ', '
            else
              comma = true
            end
            ds.literal_append(sql, v)
            sql << " AS " << k.to_s
          end
        end
      end

      # Append the optional ON ERROR fragments
      def to_s_append_on_error(ds, sql)
        unless @on_error.nil?
          sql << " "
          to_s_append_on_value(ds, sql, @on_error)
          sql << " ON ERROR"
        end
      end

      # Append the value to use for ON ERROR
      def to_s_append_on_value(ds, sql, value)
        sql << ON_ERROR_SQL.fetch(value)
      end
    end

    # Object representing json_value calls
    class JSONValueOp < JSONExistsOp
      ON_SQL = {
        :null =>  'NULL',
        :error => 'ERROR',
      }.freeze
      private_constant :ON_SQL

      # The database type to cast returned values to
      attr_reader :returning

      # How to handle cases where the JSON path expression evaluation yields
      # an empty set.
      attr_reader :on_empty

      # See JSONBaseOp#value for documentation of the options.
      def initialize(expr, path, opts=OPTS)
        @returning = opts[:returning]
        @on_empty = opts[:on_empty]
        super
      end

      private

      # Also handle transforming the returning and on_empty options.
      def transform_opts(transformer, opts)
        super
        opts[:returning] = @returning
        on_error = @on_error
        on_error = transformer.call(on_error) unless on_sql_value(on_error)
        opts[:on_error] = on_error
        on_empty = @on_empty
        on_empty = transformer.call(on_empty) unless on_sql_value(on_empty)
        opts[:on_empty] = on_empty
      end
      
      def to_s_append_function_name(ds, sql)
        sql << 'json_value('
      end

      # Also append the optional RETURNING fragment
      def to_s_append_args_passing(ds, sql)
        super

        if @returning
          sql << ' RETURNING ' << ds.db.cast_type_literal(@returning).to_s
        end
      end

      # Also append the optional ON EMPTY fragment
      def to_s_append_on_error(ds, sql)
        unless @on_empty.nil?
          sql << " "
          to_s_append_on_value(ds, sql, @on_empty)
          sql << " ON EMPTY"
        end

        super
      end

      # Handle DEFAULT values in ON EMPTY/ON ERROR fragments
      def to_s_append_on_value(ds, sql, value)
        if v = on_sql_value(value)
          sql << v
        else
          sql << 'DEFAULT '
          default_literal_append(ds, sql, value)
        end
      end

      # Do not auto paramterize default value, as PostgreSQL doesn't allow it.
      def default_literal_append(ds, sql, v)
        if sql.respond_to?(:skip_auto_param)
          sql.skip_auto_param do
            ds.literal_append(sql, v)
          end
        else
          ds.literal_append(sql, v)
        end
      end

      def on_sql_value(value)
        ON_SQL[value]
      end
    end

    # Object representing json_query calls
    class JSONQueryOp < JSONValueOp
      ON_SQL = {
        :null =>  'NULL',
        :error => 'ERROR',
        :empty_array => 'EMPTY ARRAY',
        :empty_object => 'EMPTY OBJECT',
      }.freeze
      private_constant :ON_SQL

      WRAPPER = {
        :conditional => ' WITH CONDITIONAL WRAPPER',
        :unconditional => ' WITH WRAPPER',
        :omit_quotes => ' OMIT QUOTES'
      }
      WRAPPER[true] = WRAPPER[:unconditional]
      WRAPPER.freeze
      private_constant :WRAPPER

      # How to handle wrapping of results
      attr_reader :wrapper

      # See JSONBaseOp#query for documentation of the options.
      def initialize(expr, path, opts=OPTS)
        @wrapper = opts[:wrapper]
        super
      end

      private
      
      # Also handle transforming the wrapper option
      def transform_opts(transformer, opts)
        super
        opts[:wrapper] = @wrapper
      end
      
      def to_s_append_function_name(ds, sql)
        sql << 'json_query('
      end

      # Also append the optional WRAPPER/OMIT QUOTES fragment
      def to_s_append_args_passing(ds, sql)
        super

        if @wrapper
          sql << WRAPPER.fetch(@wrapper)
        end
      end

      def on_sql_value(value)
        ON_SQL[value]
      end
    end

    # Object representing json_table calls
    class JSONTableOp < SQL::Expression
      TABLE_ON_ERROR_SQL = {
        :error => ' ERROR ON ERROR',
        :empty_array => ' EMPTY ARRAY ON ERROR',
      }.freeze
      private_constant :TABLE_ON_ERROR_SQL

      COLUMN_ON_SQL = {
        :null =>  ' NULL',
        :error => ' ERROR',
        :empty_array => ' EMPTY ARRAY',
        :empty_object => ' EMPTY OBJECT',
      }.freeze
      private_constant :COLUMN_ON_SQL

      EXISTS_ON_ERROR_SQL = {
        :error => ' ERROR',
        true => ' TRUE',
        false => ' FALSE',
        :null => ' UNKNOWN',
      }.freeze
      private_constant :EXISTS_ON_ERROR_SQL

      WRAPPER = {
        :conditional => ' WITH CONDITIONAL WRAPPER',
        :unconditional => ' WITH WRAPPER',
        :omit_quotes => ' OMIT QUOTES',
        :keep_quotes => ' KEEP QUOTES',
      }
      WRAPPER[true] = WRAPPER[:unconditional]
      WRAPPER.freeze
      private_constant :WRAPPER

      # Class used to evaluate json_table blocks and nested blocks
      class ColumnDSL
        # Return array of column information recorded for the instance
        attr_reader :columns

        def self.columns(&block)
          new(&block).columns.freeze
        end

        def initialize(&block)
          @columns = []
          instance_exec(&block)
        end

        # Include a FOR ORDINALITY column
        def ordinality(name)
          @columns << [:ordinality, name].freeze
        end

        # Include a regular column with the given type
        def column(name, type, opts=OPTS)
          @columns << [:column, name, type, opts].freeze
        end

        # Include an EXISTS column with the given type
        def exists(name, type, opts=OPTS)
          @columns << [:exists, name, type, opts].freeze
        end

        # Include a nested set of columns at the given path.
        def nested(path, &block)
          @columns << [:nested, path, ColumnDSL.columns(&block)].freeze
        end

        # Include a bigint column
        def Bignum(name, opts=OPTS)
          @columns << [:column, name, :Bignum, opts].freeze
        end

        # Define methods for handling other generic types
        %w'String Integer Float Numeric BigDecimal Date DateTime Time File TrueClass FalseClass'.each do |meth|
          klass = Object.const_get(meth)
          define_method(meth) do |name, opts=OPTS|
            @columns << [:column, name, klass, opts].freeze
          end
        end
      end
      private_constant :ColumnDSL

      # See JSONBaseOp#table for documentation on the options.
      def initialize(expr, path, opts=OPTS, &block)
        @expr = expr
        @path = path
        @passing = opts[:passing]
        @on_error = opts[:on_error]
        @columns = opts[:_columns] || ColumnDSL.columns(&block)
        freeze
      end

      # Append the json_table function call expression to the SQL
      def to_s_append(ds, sql)
        sql << 'json_table('
        ds.literal_append(sql, @expr)
        sql << ', '
        default_literal_append(ds, sql, @path)

        if (passing = @passing) && !passing.empty?
          sql << ' PASSING '
          comma = false
          passing.each do |k, v|
            if comma
              sql << ', '
            else
              comma = true
            end
            ds.literal_append(sql, v)
            sql << " AS " << k.to_s
          end
        end

        to_s_append_columns(ds, sql, @columns)
        sql << TABLE_ON_ERROR_SQL.fetch(@on_error) if @on_error
        sql << ')'
      end

      # Support transforming of json_table expression
      def sequel_ast_transform(transformer)
        opts = {:on_error=>@on_error, :_columns=>@columns}

        if @passing
          passing = opts[:passing] = {}
          @passing.each do |k, v|
            passing[k] = transformer.call(v)
          end
        end

        self.class.new(transformer.call(@expr), @path, opts)
      end

      private

      # Append the set of column information to the SQL.  Separated to handle
      # nested sets of columns.
      def to_s_append_columns(ds, sql, columns)
        sql << ' COLUMNS('
        comma = nil
        columns.each do |column|
          if comma
            sql << comma
          else
            comma = ', '
          end
          to_s_append_column(ds, sql, column)
        end
        sql << ')'
      end

      # Append the column information to the SQL.  Handles the various
      # types of json_table columns.
      def to_s_append_column(ds, sql, column)
        case column[0]
        when :column
          _, name, type, opts = column
          ds.literal_append(sql, name)
          sql << ' ' << ds.db.send(:type_literal, opts.merge(:type=>type)).to_s
          sql << ' FORMAT JSON' if opts[:format] == :json
          to_s_append_path(ds, sql, opts[:path])
          sql << WRAPPER.fetch(opts[:wrapper]) if opts[:wrapper]
          to_s_append_on_value(ds, sql, opts[:on_empty], " ON EMPTY")
          to_s_append_on_value(ds, sql, opts[:on_error], " ON ERROR")
        when :ordinality
          ds.literal_append(sql, column[1])
          sql << ' FOR ORDINALITY'
        when :exists
          _, name, type, opts = column
          ds.literal_append(sql, name)
          sql << ' ' << ds.db.send(:type_literal, opts.merge(:type=>type)).to_s
          sql << ' EXISTS'
          to_s_append_path(ds, sql, opts[:path])
          unless (on_error = opts[:on_error]).nil?
            sql << EXISTS_ON_ERROR_SQL.fetch(on_error) << " ON ERROR"
          end
        else # when :nested
          _, path, columns = column
          sql << 'NESTED '
          default_literal_append(ds, sql, path)
          to_s_append_columns(ds, sql, columns)
        end
      end

      # Handle DEFAULT values in ON EMPTY/ON ERROR fragments
      def to_s_append_on_value(ds, sql, value, cond)
        if value
          if v = COLUMN_ON_SQL[value]
            sql << v
          else
            sql << ' DEFAULT '
            default_literal_append(ds, sql, value)
          end
          sql << cond
        end
      end

      # Append path caluse to the SQL
      def to_s_append_path(ds, sql, path)
        if path
          sql << ' PATH '
          default_literal_append(ds, sql, path)
        end
      end

      # Do not auto paramterize default value or path value, as PostgreSQL doesn't allow it.
      def default_literal_append(ds, sql, v)
        if sql.respond_to?(:skip_auto_param)
          sql.skip_auto_param do
            ds.literal_append(sql, v)
          end
        else
          ds.literal_append(sql, v)
        end
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

    # :nocov:
    if defined?(JSONArray)
    # :nocov:
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
      send INCLUDE_METH, Sequel::Postgres::JSONOpMethods
    end
  end
end
# :nocov:
