# The pg_array extension adds support for Sequel to handle
# PostgreSQL's array types.
#
# This extension integrates with Sequel's native postgres adapter and
# the jdbc/postgresql adapter, so that when array fields are retrieved,
# they are parsed and returned as instances of Sequel::Postgres::PGArray.
# PGArray is a DelegateClass of Array, so it mostly acts like an array, but not
# completely (is_a?(Array) is false).  If you want the actual array,
# you can call PGArray#to_a.  This is done so that Sequel does not
# treat a PGArray like an Array by default, which would cause issues.
#
# In addition to the parsers, this extension comes with literalizers
# for PGArray using the standard Sequel literalization callbacks, so
# they work with on all adapters.
#
# To turn an existing Array into a PGArray:
#
#   Sequel.pg_array(array)
#
# If you have loaded the {core_extensions extension}[rdoc-ref:doc/core_extensions.rdoc],
# or you have loaded the core_refinements extension
# and have activated refinements for the file, you can also use Array#pg_array:
#
#   array.pg_array
#
# You can also provide a type, though in many cases it isn't necessary:
#
#   Sequel.pg_array(array, :varchar) # or :integer, :"double precision", etc.
#   array.pg_array(:varchar) # or :integer, :"double precision", etc.
#
# So if you want to insert an array into an integer[] database column:
#
#   DB[:table].insert(:column=>Sequel.pg_array([1, 2, 3]))
#
# To use this extension, first load it into your Sequel::Database instance:
#
#   DB.extension :pg_array
#
# See the {schema modification guide}[rdoc-ref:doc/schema_modification.rdoc]
# for details on using postgres array columns in CREATE/ALTER TABLE statements.
#
# If you are not using the native postgres or jdbc/postgresql adapter and are using array
# types as model column values you probably should use the the pg_typecast_on_load plugin
# if the column values are returned as a string.
#
# This extension by default includes handlers for array types for
# all scalar types that the native postgres adapter handles. It
# also makes it easy to add support for other array types.  In
# general, you just need to make sure that the scalar type is
# handled and has the appropriate converter installed in
# Sequel::Postgres::PG_TYPES or the Database instance's
# conversion_procs usingthe appropriate type OID.  For user defined
# types, you can do this via:
#
#   DB.conversion_procs[scalar_type_oid] = lambda{|string| }
#
# Then you can call
# Sequel::Postgres::PGArray::DatabaseMethods#register_array_type
# to automatically set up a handler for the array type.  So if you
# want to support the foo[] type (assuming the foo type is already
# supported):
#
#   DB.register_array_type('foo')
#
# You can also register array types on a global basis using
# Sequel::Postgres::PGArray.register.  In this case, you'll have
# to specify the type oids:
#
#   Sequel::Postgres::PG_TYPES[1234] = lambda{|string| }
#   Sequel::Postgres::PGArray.register('foo', :oid=>4321, :scalar_oid=>1234)
#
# Both Sequel::Postgres::PGArray::DatabaseMethods#register_array_type
# and Sequel::Postgres::PGArray.register support many options to
# customize the array type handling.  See the Sequel::Postgres::PGArray.register
# method documentation.
#
# If you want an easy way to call PostgreSQL array functions and
# operators, look into the pg_array_ops extension.
#
# This extension requires the json, strscan, and delegate libraries.

require 'delegate'
require 'strscan'
require 'json'
Sequel.require 'adapters/utils/pg_types'

module Sequel
  module Postgres
    # Represents a PostgreSQL array column value.
    class PGArray < DelegateClass(Array)
      include Sequel::SQL::AliasMethods

      ARRAY = "ARRAY".freeze
      DOUBLE_COLON = '::'.freeze
      EMPTY_ARRAY = "'{}'".freeze
      EMPTY_BRACKET = '[]'.freeze
      OPEN_BRACKET = '['.freeze
      CLOSE_BRACKET = ']'.freeze
      COMMA = ','.freeze
      BACKSLASH = '\\'.freeze
      EMPTY_STRING = ''.freeze
      OPEN_BRACE = '{'.freeze
      CLOSE_BRACE = '}'.freeze
      NULL = 'NULL'.freeze
      QUOTE = '"'.freeze

      # Global hash of database array type name strings to symbols (e.g. 'double precision' => :float),
      # used by the schema parsing for array types registered globally.
      ARRAY_TYPES = {}

      # Registers an array type that the extension should handle.  Makes a Database instance that
      # has been extended with DatabaseMethods recognize the array type given and set up the
      # appropriate typecasting.  Also sets up automatic typecasting for the native postgres
      # adapter, so that on retrieval, the values are automatically converted to PGArray instances.
      # The db_type argument should be the exact database type used (as returned by the PostgreSQL
      # format_type database function).  Accepts the following options:
      #
      # :array_type :: The type to automatically cast the array to when literalizing the array.
      #                Usually the same as db_type.
      # :converter :: A callable object (e.g. Proc), that is called with each element of the array
      #               (usually a string), and should return the appropriate typecasted object.
      # :oid :: The PostgreSQL OID for the array type.  This is used by the Sequel postgres adapter
      #         to set up automatic type conversion on retrieval from the database.
      # :parser :: Can be set to :json to use the faster JSON-based parser.  Note that the JSON-based
      #            parser can only correctly handle integers values correctly.  It doesn't handle
      #            full precision for numeric types, and doesn't handle NaN/Infinity values for
      #            floating point types.
      # :scalar_oid :: Should be the PostgreSQL OID for the scalar version of this array type. If given,
      #                automatically sets the :converter option by looking for scalar conversion
      #                proc.
      # :scalar_typecast :: Should be a symbol indicating the typecast method that should be called on
      #                     each element of the array, when a plain array is passed into a database
      #                     typecast method.  For example, for an array of integers, this could be set to
      #                     :integer, so that the typecast_value_integer method is called on all of the
      #                     array elements.  Defaults to :type_symbol option.
      # :type_procs :: A hash mapping oids to conversion procs, used for looking up the :scalar_oid and
      #                value and setting the :oid value.  Defaults to the global Sequel::Postgres::PG_TYPES.
      # :type_symbol :: The base of the schema type symbol for this type.  For example, if you provide
      #                 :integer, Sequel will recognize this type as :integer_array during schema parsing.
      #                 Defaults to the db_type argument.
      # :typecast_method_map :: The map in which to place the database type string to type symbol mapping.
      #                         Defaults to ARRAY_TYPES.
      # :typecast_methods_module :: If given, a module object to add the typecasting method to.  Defaults
      #                             to DatabaseMethods.
      #
      # If a block is given, it is treated as the :converter option.
      def self.register(db_type, opts=OPTS, &block)
        db_type = db_type.to_s
        type = (opts[:type_symbol] || db_type).to_sym
        type_procs = opts[:type_procs] || PG_TYPES
        mod = opts[:typecast_methods_module] || DatabaseMethods
        typecast_method_map = opts[:typecast_method_map] || ARRAY_TYPES

        if converter = opts[:converter]
          raise Error, "can't provide both a block and :converter option to register" if block
        else
          converter = block
        end

        if soid = opts[:scalar_oid]
          raise Error, "can't provide both a converter and :scalar_oid option to register" if converter 
          converter = type_procs[soid]
        end

        array_type = (opts[:array_type] || db_type).to_s.dup.freeze
        creator = (opts[:parser] == :json ? JSONCreator : Creator).new(array_type, converter)

        typecast_method_map[db_type] = :"#{type}_array"

        define_array_typecast_method(mod, type, creator, opts.fetch(:scalar_typecast, type))

        if oid = opts[:oid]
          type_procs[oid] = creator
        end

        nil
      end

      # Define a private array typecasting method in the given module for the given type that uses
      # the creator argument to do the type conversion.
      def self.define_array_typecast_method(mod, type, creator, scalar_typecast)
        mod.class_eval do
          meth = :"typecast_value_#{type}_array"
          scalar_typecast_method = :"typecast_value_#{scalar_typecast}"
          define_method(meth){|v| typecast_value_pg_array(v, creator, scalar_typecast_method)}
          private meth
        end
      end
      private_class_method :define_array_typecast_method

      module DatabaseMethods
        APOS = "'".freeze
        DOUBLE_APOS = "''".freeze
        ESCAPE_RE = /("|\\)/.freeze
        ESCAPE_REPLACEMENT = '\\\\\1'.freeze
        BLOB_RANGE = 1...-1

        # Create the local hash of database type strings to schema type symbols,
        # used for array types local to this database.
        def self.extended(db)
          db.instance_eval do
            @pg_array_schema_types ||= {}
            procs = conversion_procs
            procs[1115] = Creator.new("timestamp without time zone", procs[1114])
            procs[1185] = Creator.new("timestamp with time zone", procs[1184])
            copy_conversion_procs([143, 791, 1000, 1001, 1003, 1005, 1006, 1007, 1009, 1010, 1011, 1012, 1013, 1014, 1015, 1016, 1021, 1022, 1028, 1182, 1183, 1231, 1270, 1561, 1563, 2951])
            [:string_array, :integer_array, :decimal_array, :float_array, :boolean_array, :blob_array, :date_array, :time_array, :datetime_array].each do |v|
              @schema_type_classes[v] = PGArray
            end
          end
        end

        # Handle arrays in bound variables
        def bound_variable_arg(arg, conn)
          case arg
          when PGArray
            bound_variable_array(arg.to_a)
          when Array
            bound_variable_array(arg)
          else
            super
          end
        end

        # Register a database specific array type.  This can be used to support
        # different array types per Database.  Use of this method does not
        # affect global state, unlike PGArray.register.  See PGArray.register for
        # possible options.
        def register_array_type(db_type, opts=OPTS, &block)
          opts = {:type_procs=>conversion_procs, :typecast_method_map=>@pg_array_schema_types, :typecast_methods_module=>(class << self; self; end)}.merge(opts)
          unless (opts.has_key?(:scalar_oid) || block) && opts.has_key?(:oid)
            array_oid, scalar_oid = from(:pg_type).where(:typname=>db_type.to_s).get([:typarray, :oid])
            opts[:scalar_oid] = scalar_oid unless opts.has_key?(:scalar_oid) || block
            opts[:oid] = array_oid unless opts.has_key?(:oid)
          end
          PGArray.register(db_type, opts, &block)
          @schema_type_classes[:"#{opts[:type_symbol] || db_type}_array"] = PGArray
          conversion_procs_updated
        end

        # Return PGArray if this type matches any supported array type.
        def schema_type_class(type)
          super || (ARRAY_TYPES.each_value{|v| return PGArray if type == v}; nil)
        end

        private

        # Format arrays used in bound variables.
        def bound_variable_array(a)
          case a
          when Array
            "{#{a.map{|i| bound_variable_array(i)}.join(COMMA)}}"
          when Sequel::SQL::Blob
            "\"#{literal(a)[BLOB_RANGE].gsub(DOUBLE_APOS, APOS).gsub(ESCAPE_RE, ESCAPE_REPLACEMENT)}\""
          when Sequel::LiteralString
            a
          when String
            "\"#{a.gsub(ESCAPE_RE, ESCAPE_REPLACEMENT)}\""
          else
            literal(a)
          end
        end

        # Automatically handle array types for the given named types. 
        def convert_named_procs_to_procs(named_procs)
          h = super
          unless h.empty?
            from(:pg_type).where(:oid=>h.keys).select_map([:typname, :oid, :typarray]).each do |name, scalar_oid, array_oid|
              register_array_type(name, :type_procs=>h, :oid=>array_oid.to_i, :scalar_oid=>scalar_oid.to_i)
            end
          end
          h
        end

        # Manually override the typecasting for timestamp array types so that
        # they use the database's timezone instead of the global Sequel
        # timezone.
        def get_conversion_procs
          procs = super

          procs[1115] = Creator.new("timestamp without time zone", procs[1114])
          procs[1185] = Creator.new("timestamp with time zone", procs[1184])

          procs
        end

        # Look into both the current database's array schema types and the global
        # array schema types to get the type symbol for the given database type
        # string.
        def pg_array_schema_type(type)
          @pg_array_schema_types[type] || ARRAY_TYPES[type]
        end

        # Make the column type detection handle registered array types.
        def schema_column_type(db_type)
          if (db_type =~ /\A([^(]+)(?:\([^(]+\))?\[\]\z/io) && (type = pg_array_schema_type($1))
            type
          else
            super
          end
        end

        # Given a value to typecast and the type of PGArray subclass:
        # * If given a PGArray with a matching array_type, use it directly.
        # * If given a PGArray with a different array_type, return a PGArray
        #   with the creator's type.
        # * If given an Array, create a new PGArray instance for it.  This does not
        #   typecast all members of the array in ruby for performance reasons, but
        #   it will cast the array the appropriate database type when the array is
        #   literalized.
        def typecast_value_pg_array(value, creator, scalar_typecast_method=nil)
          case value
          when PGArray
            if value.array_type != creator.type
              PGArray.new(value.to_a, creator.type)
            else
              value
            end
          when Array
            if scalar_typecast_method && respond_to?(scalar_typecast_method, true)
              value = Sequel.recursive_map(value, method(scalar_typecast_method))
            end
            PGArray.new(value, creator.type)
          else
            raise Sequel::InvalidValue, "invalid value for array type: #{value.inspect}"
          end
        end
      end

      # PostgreSQL array parser that handles PostgreSQL array output format.
      # Note that does not handle all forms out input that PostgreSQL will
      # accept, and it will not raise an error for all forms of invalid input.
      class Parser < StringScanner
        UNQUOTED_RE = /[{}",]|[^{}",]+/
        QUOTED_RE = /["\\]|[^"\\]+/
        NULL_RE = /NULL",/
        OPEN_RE = /\{/

        # Set the source for the input, and any converter callable
        # to call with objects to be created.  For nested parsers
        # the source may contain text after the end current parse,
        # which will be ignored.
        def initialize(source, converter=nil)
          super(source)
          @converter = converter 
          @stack = [[]]
          @recorded = ""
        end

        # Take the buffer of recorded characters and add it to the array
        # of entries, and use a new buffer for recorded characters.
        def new_entry(include_empty=false)
          if !@recorded.empty? || include_empty
            entry = @recorded
            if entry == NULL && !include_empty
              entry = nil
            elsif @converter
              entry = @converter.call(entry)
            end
            @stack.last.push(entry)
            @recorded = ""
          end
        end

        # Parse the input character by character, returning an array
        # of parsed (and potentially converted) objects.
        def parse
          raise Sequel::Error, "invalid array, empty string" if eos?
          raise Sequel::Error, "invalid array, doesn't start with {" unless scan(OPEN_RE)

          while !eos?
            char = scan(UNQUOTED_RE)
            if char == COMMA
              # Comma outside quoted string indicates end of current entry
              new_entry
            elsif char == QUOTE
              raise Sequel::Error, "invalid array, opening quote with existing recorded data" unless @recorded.empty?
              while true
                char = scan(QUOTED_RE)
                if char == BACKSLASH
                  @recorded << getch
                elsif char == QUOTE
                  n = peek(1)
                  raise Sequel::Error, "invalid array, closing quote not followed by comma or closing brace" unless n == COMMA || n == CLOSE_BRACE
                  break
                else
                  @recorded << char
                end
              end
              new_entry(true)
            elsif char == OPEN_BRACE
              raise Sequel::Error, "invalid array, opening brace with existing recorded data" unless @recorded.empty?

              # Start of new array, add it to the stack
              new = []
              @stack.last << new
              @stack << new
            elsif char == CLOSE_BRACE
              # End of current array, add current entry to the current array
              new_entry

              if @stack.length == 1
                raise Sequel::Error, "array parsing finished without parsing entire string" unless eos?

                # Top level of array, parsing should be over.
                # Pop current array off stack and return it as result
                return @stack.pop
              else
                # Nested array, pop current array off stack
                @stack.pop
              end
            else
              # Add the character to the recorded character buffer.
              @recorded << char
            end
          end

          raise Sequel::Error, "array parsing finished with array unclosed"
        end
      end unless Sequel::Postgres.respond_to?(:parse_pg_array)

      # Callable object that takes the input string and parses it using Parser.
      class Creator
        # The converter callable that is called on each member of the array
        # to convert it to the correct type.
        attr_reader :converter

        # The database type to set on the PGArray instances returned.
        attr_reader :type

        # Set the type and optional converter callable that will be used.
        def initialize(type, converter=nil)
          @type = type
          @converter = converter
        end

        if Sequel::Postgres.respond_to?(:parse_pg_array)
        # :nocov:
          # Use sequel_pg's C-based parser if it has already been defined.
          def call(string)
            PGArray.new(Sequel::Postgres.parse_pg_array(string, @converter), @type)
          end
        # :nocov:
        else
          # Parse the string using Parser with the appropriate
          # converter, and return a PGArray with the appropriate database
          # type.
          def call(string)
            PGArray.new(Parser.new(string, @converter).parse, @type)
          end
        end
      end

      # Callable object that takes the input string and parses it using.
      # a JSON parser.  This should be faster than the standard Creator,
      # but only handles integer types correctly.
      class JSONCreator < Creator
        # Character conversion map mapping input strings to JSON replacements
        SUBST = {'{'.freeze=>'['.freeze, '}'.freeze=>']'.freeze, 'NULL'.freeze=>'null'.freeze}

        # Regular expression matching input strings to convert
        SUBST_RE = %r[\{|\}|NULL].freeze

        # Parse the input string by using a gsub to convert non-JSON characters to
        # JSON, running it through a regular JSON parser. If a converter is used, a
        # recursive map of the output is done to make sure that the entires in the
        # correct type.
        def call(string)
          array = Sequel.parse_json(string.gsub(SUBST_RE){|m| SUBST[m]})
          array = Sequel.recursive_map(array, @converter) if @converter
          PGArray.new(array, @type)
        end
      end

      # The type of this array.  May be nil if no type was given. If a type
      # is provided, the array is automatically casted to this type when
      # literalizing.  This type is the underlying type, not the array type
      # itself, so for an int4[] database type, it should be :int4 or 'int4'
      attr_accessor :array_type

      # Set the array to delegate to, and a database type.
      def initialize(array, type=nil)
        super(array)
        @array_type = type
      end

      # Append the array SQL to the given sql string. 
      # If the receiver has a type, add a cast to the
      # database array type.
      def sql_literal_append(ds, sql)
        at = array_type
        if empty? && at
          sql << EMPTY_ARRAY
        else
          sql << ARRAY
          _literal_append(sql, ds, to_a)
        end
        if at
          sql << DOUBLE_COLON << at.to_s << EMPTY_BRACKET
        end
      end

      private

      # Recursive method that handles multi-dimensional
      # arrays, surrounding each with [] and interspersing
      # entries with ,.
      def _literal_append(sql, ds, array)
        sql << OPEN_BRACKET
        comma = false
        commas = COMMA
        array.each do |i|
          sql << commas if comma
          if i.is_a?(Array)
            _literal_append(sql, ds, i)
          else
            ds.literal_append(sql, i)
          end
          comma = true
        end
        sql << CLOSE_BRACKET
      end

      # Register all array types that this extension handles by default.

      register('text', :oid=>1009, :type_symbol=>:string)
      register('integer', :oid=>1007, :parser=>:json)
      register('bigint', :oid=>1016, :parser=>:json, :scalar_typecast=>:integer)
      register('numeric', :oid=>1231, :scalar_oid=>1700, :type_symbol=>:decimal)
      register('double precision', :oid=>1022, :scalar_oid=>701, :type_symbol=>:float)

      register('boolean', :oid=>1000, :scalar_oid=>16)
      register('bytea', :oid=>1001, :scalar_oid=>17, :type_symbol=>:blob)
      register('date', :oid=>1182, :scalar_oid=>1082)
      register('time without time zone', :oid=>1183, :scalar_oid=>1083, :type_symbol=>:time)
      register('timestamp without time zone', :oid=>1115, :scalar_oid=>1114, :type_symbol=>:datetime)
      register('time with time zone', :oid=>1270, :scalar_oid=>1083, :type_symbol=>:time_timezone, :scalar_typecast=>:time)
      register('timestamp with time zone', :oid=>1185, :scalar_oid=>1184, :type_symbol=>:datetime_timezone, :scalar_typecast=>:datetime)

      register('smallint', :oid=>1005, :parser=>:json, :scalar_typecast=>:integer)
      register('oid', :oid=>1028, :parser=>:json, :scalar_typecast=>:integer)
      register('real', :oid=>1021, :scalar_oid=>700, :scalar_typecast=>:float)
      register('character', :oid=>1014, :array_type=>:text, :scalar_typecast=>:string)
      register('character varying', :oid=>1015, :scalar_typecast=>:string, :type_symbol=>:varchar)

      register('xml', :oid=>143, :scalar_oid=>142)
      register('money', :oid=>791, :scalar_oid=>790)
      register('bit', :oid=>1561, :scalar_oid=>1560)
      register('bit varying', :oid=>1563, :scalar_oid=>1562, :type_symbol=>:varbit)
      register('uuid', :oid=>2951, :scalar_oid=>2950)

      register('xid', :oid=>1011, :scalar_oid=>28)
      register('cid', :oid=>1012, :scalar_oid=>29)

      register('name', :oid=>1003, :scalar_oid=>19)
      register('tid', :oid=>1010, :scalar_oid=>27)
      register('int2vector', :oid=>1006, :scalar_oid=>22)
      register('oidvector', :oid=>1013, :scalar_oid=>30)
    end
  end

  module SQL::Builders
    # Return a Postgres::PGArray proxy for the given array and database array type.
    def pg_array(v, array_type=nil)
      case v
      when Postgres::PGArray
        if array_type.nil? || v.array_type == array_type
          v
        else
          Postgres::PGArray.new(v.to_a, array_type)
        end
      when Array
        Postgres::PGArray.new(v, array_type)
      else
        # May not be defined unless the pg_array_ops extension is used
        pg_array_op(v)
      end
    end
  end

  Database.register_extension(:pg_array, Postgres::PGArray::DatabaseMethods)
end

# :nocov:
if Sequel.core_extensions?
  class Array
    # Return a PGArray proxy to the receiver, using a
    # specific database type if given.  This is mostly useful
    # as a short cut for creating PGArray objects that didn't
    # come from the database.
    def pg_array(type=nil)
      Sequel::Postgres::PGArray.new(self, type)
    end
  end
end

if defined?(Sequel::CoreRefinements)
  module Sequel::CoreRefinements
    refine Array do
      def pg_array(type=nil)
        Sequel::Postgres::PGArray.new(self, type)
      end
    end
  end
end
# :nocov:
