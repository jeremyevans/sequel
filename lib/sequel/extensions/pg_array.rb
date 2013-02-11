# The pg_array extension adds support for Sequel to handle
# PostgreSQL's array types.
#
# This extension integrates with Sequel's native postgres adapter, so
# that when array fields are retrieved, they are parsed and returned
# as instances of Sequel::Postgres::PGArray.  PGArray is
# a DelegateClass of Array, so it mostly acts like an array, but not
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
# If you have loaded the {core_extensions extension}[link:files/doc/core_extensions_rdoc.html]),
# or you have loaded the {core_refinements extension}[link:files/doc/core_refinements_rdoc.html])
# and have activated refinements for the file, you can also use Array#pg_array:
#
#   array.pg_array
#
# You can also provide a type, though it many cases it isn't necessary:
#
#   Sequel.pg_array(array, :varchar) # or :integer, :"double precision", etc.
#   array.pg_array(:varchar) # or :integer, :"double precision", etc.
#
# So if you want to insert an array into an integer[] database column:
#
#   DB[:table].insert(:column=>Sequel.pg_array([1, 2, 3]))
#
# If you would like to use PostgreSQL arrays in your model objects, you
# probably want to modify the schema parsing/typecasting so that it
# recognizes and correctly handles the arrays, which you can do by:
#
#   DB.extension :pg_array
#
# If you are not using the native postgres adapter, you probably
# also want to use the typecast_on_load plugin in the model, and
# set it to typecast the array column(s) on load.
#
# This extension by default includes handlers for array types for
# all scalar types that the native postgres adapter handles. It
# also makes it easy to add support for other array types.  In
# general, you just need to make sure that the scalar type is
# handled and has the appropriate converter installed in
# Sequel::Postgres::PG_TYPES under the appropriate type OID.
# Then you can call Sequel::Postgres::PGArray.register with
# the appropriate arguments to automatically set up a handler
# for the array type.
#
# For example, if you add support for a scalar custom type named
# foo which uses OID 1234, and you want to add support for the
# foo[] type, which uses type OID 4321, you need to do:
#
#   Sequel::Postgres::PGArray.register('foo', :oid=>4321, :scalar_oid=>1234)
#
# Sequel::Postgres::PGArray.register has many additional options
# and should be able to handle most PostgreSQL array types.
#
# If you want an easy way to call PostgreSQL array functions and
# operators, look into the pg_array_ops extension.
#
# This extension requires both the json and delegate libraries.
#
# == Additional License
#
# PGArray::Parser code was translated from Javascript code in the
# node-postgres project and has the following additional license:
# 
# Copyright (c) 2010 Brian Carlson (brian.m.carlson@gmail.com)
# 
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject
# to the following conditions:
# 
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY
# KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE
# WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

require 'delegate'
require 'json'
Sequel.require 'adapters/utils/pg_types'

module Sequel
  module Postgres
    # Represents a PostgreSQL array column value.
    class PGArray < DelegateClass(Array)
      include Sequel::SQL::AliasMethods

      ARRAY = "ARRAY".freeze
      DOUBLE_COLON = '::'.freeze
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

      # Hash of database array type name strings to symbols (e.g. 'double precision' => :float),
      # used by the schema parsing.
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
      # :typecast_method :: If given, specifies the :type_symbol option, but additionally causes no
      #                     typecasting method to be created in the database.  This should only be used
      #                     to alias existing array types.  For example, if there is an array type that can be
      #                     treated just like an integer array, you can do :typecast_method=>:integer.
      # :typecast_methods_module :: If given, a module object to add the typecasting method to.  Defaults
      #                             to DatabaseMethods.
      #
      # If a block is given, it is treated as the :converter option.
      def self.register(db_type, opts={}, &block)
        db_type = db_type.to_s
        typecast_method = opts[:typecast_method]
        type = (typecast_method || opts[:type_symbol] || db_type).to_sym
        type_procs = opts[:type_procs] || PG_TYPES
        mod = opts[:typecast_methods_module] || DatabaseMethods

        if converter = opts[:converter]
          raise Error, "can't provide both a block and :converter option to register" if block
        else
          converter = block
        end

        if soid = opts[:scalar_oid]
          raise Error, "can't provide both a converter and :scalar_oid option to register" if converter 
          raise Error, "no conversion proc for :scalar_oid=>#{soid.inspect}" unless converter = type_procs[soid]
        end

        array_type = (opts[:array_type] || db_type).to_s.dup.freeze
        creator = (opts[:parser] == :json ? JSONCreator : Creator).new(array_type, converter)

        ARRAY_TYPES[db_type] = :"#{type}_array"

        define_array_typecast_method(mod, type, creator, opts.fetch(:scalar_typecast, type)) unless typecast_method

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

        # Make the column type detection handle registered array types.
        def schema_column_type(db_type)
          if (db_type =~ /\A([^(]+)(?:\([^(]+\))?\[\]\z/io) && (type = ARRAY_TYPES[$1])
            type
          else
            super
          end
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

        # Manually override the typecasting for timestamp array types so that
        # they use the database's timezone instead of the global Sequel
        # timezone.
        def get_conversion_procs
          procs = super

          procs[1115] = Creator.new("timestamp without time zone", procs[1114])
          procs[1185] = Creator.new("timestamp with time zone", procs[1184])

          procs
        end

        # Given a value to typecast and the type of PGArray subclass:
        # * If given a PGArray with a matching array_type, use it directly.
        # * If given a PGArray with a different array_type, return a PGArray
        #   with the creator's type.
        # * If given an Array, create a new PGArray instance for it.  This does not
        #   typecast all members of the array in ruby for performance reasons, but
        #   it will cast the array the appropriate database type when the array is
        #   literalized.
        # * If given a String, call the parser for the subclass with it.
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

      # PostgreSQL array parser that handles all types of input.
      #
      # This parser is very simple and unoptimized, but should still
      # be O(n) where n is the length of the input string.
      class Parser
        # Current position in the input string.
        attr_reader :pos

        # Set the source for the input, and any converter callable
        # to call with objects to be created.  For nested parsers
        # the source may contain text after the end current parse,
        # which will be ignored.
        def initialize(source, converter=nil)
          @source = source
          @source_length = source.length
          @converter = converter 
          @pos = -1
          @entries = []
          @recorded = ""
          @dimension = 0
        end

        # Return 2 objects, whether the next character in the input
        # was escaped with a backslash, and what the next character is.
        def next_char
          @pos += 1
          if (c = @source[@pos..@pos]) == BACKSLASH
            @pos += 1
            [true, @source[@pos..@pos]]
          else
            [false, c]
          end
        end

        # Add a new character to the buffer of recorded characters.
        def record(c)
          @recorded << c
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
            @entries.push(entry)
            @recorded = ""
          end
        end

        # Parse the input character by character, returning an array
        # of parsed (and potentially converted) objects.
        def parse(nested=false)
          # quote sets whether we are inside of a quoted string.
          quote = false
          until @pos >= @source_length
            escaped, char = next_char
            if char == OPEN_BRACE && !quote
              @dimension += 1
              if (@dimension > 1)
                # Multi-dimensional array encounter, use a subparser
                # to parse the next level down.
                subparser = self.class.new(@source[@pos..-1], @converter)
                @entries.push(subparser.parse(true))
                @pos += subparser.pos - 1
              end
            elsif char == CLOSE_BRACE && !quote
              @dimension -= 1
              if (@dimension == 0)
                new_entry
                # Exit early if inside a subparser, since the
                # text after parsing the current level should be
                # ignored as it is handled by the parent parser.
                return @entries if nested
              end
            elsif char == QUOTE && !escaped
              # If already inside the quoted string, this is the
              # ending quote, so add the entry.  Otherwise, this
              # is the opening quote, so set the quote flag.
              new_entry(true) if quote
              quote = !quote
            elsif char == COMMA && !quote
              # If not inside a string and a comma occurs, it indicates
              # the end of the entry, so add the entry.
              new_entry
            else
              # Add the character to the recorded character buffer.
              record(char)
            end
          end
          raise Sequel::Error, "array dimensions not balanced" unless @dimension == 0
          @entries
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
          # Use sequel_pg's C-based parser if it has already been defined.
          def call(string)
            PGArray.new(Sequel::Postgres.parse_pg_array(string, @converter), @type)
          end
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
        sql << ARRAY
        _literal_append(sql, ds, to_a)
        if at = array_type
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

      register('smallint', :oid=>1005, :parser=>:json, :typecast_method=>:integer)
      register('oid', :oid=>1028, :parser=>:json, :typecast_method=>:integer)
      register('real', :oid=>1021, :scalar_oid=>701, :typecast_method=>:float)
      register('character', :oid=>1014, :array_type=>:text, :typecast_method=>:string)
      register('character varying', :oid=>1015, :typecast_method=>:string)
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
