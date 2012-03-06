# The pg_array extension allows Sequel's postgres adapter to handle
# PostgreSQL's string and numeric array types.  It supports both
# single dimensional and multi-dimensional arrays.  For integer and
# float arrays, it uses a JSON-based parser which is written in C
# and should be fairly fast.  For string and decimal arrays, it uses
# a hand coded parser written in ruby that is unoptimized and probably
# slow.
#
# This extension integrates with Sequel's native postgres adapter, so
# that when array fields are retrieved, they are parsed and returned
# as instances of Sequel::Postgres::PGArray subclasses.  PGArray is
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
#   array.pg_array
#
# You can also provide a type, though it many cases it isn't necessary:
#
#   array.pg_array(:varchar) # or :int4, :"double precision", etc.
#
# So if you want to insert an array into an int4[] database column:
#
#   DB[:table].insert(:column=>[1, 2, 3].pg_array)
#
# This extension requires both the json and delegate libraries.
#
# ## Additional License
#
# PGArray::Parser code was translated from Javascript code in the
# node-postgres project and has the following license:
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

module Sequel
  module Postgres
    # Base class for the PostgreSQL array types.  Subclasses generally
    # just deal with parsing, so instances manually created from arrays
    # can use this class correctly.
    class PGArray < DelegateClass(Array)
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

      # PostgreSQL array parser that handles both text and numeric
      # input.  Because PostgreSQL arrays can contain objects that
      # can be literalized in any number of ways, it is not possible
      # to make a fully generic parser.
      #
      # This parser is very simple and unoptimized, but should still
      # be O(n) where n is the length of the input string.
      class Parser
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
      end

      # Parse the string using the generalized parser, setting the type
      # if given.
      def self.parse(string, type=nil)
        new(Parser.new(string, method(:convert_item)).parse, type)
      end

      # Return the item as-is by default, making conversion a no-op.
      def self.convert_item(s)
        s
      end
      private_class_method :convert_item

      # The type of this array.  May be nil if no type was given. If a type
      # is provided, the array is automatically casted to this type when
      # literalizing.  This type is the underlying type, not the array type
      # itself, so for an int4[] database type, it should be :int4 or 'int4'
      attr_accessor :array_type

      # Set the array to delegate to, and a database type.
      def initialize(array, type=nil)
        super(array)
        self.array_type = type
      end

      # The delegated object is always an array.
      alias to_a __getobj__

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
    end

    # PGArray subclass handling integer and float types, using a fast JSON
    # parser.  Does not handle numeric/decimal types, since JSON does deal
    # with arbitrary precision (see PGDecimalArray for that).
    class PGNumericArray < PGArray
      # Character conversion map mapping input strings to JSON replacements
      SUBST = {'{'.freeze=>'['.freeze, '}'.freeze=>']'.freeze, 'NULL'.freeze=>'null'.freeze}

      # Regular expression matching input strings to convert
      SUBST_RE = %r[\{|\}|NULL].freeze

      # Parse the input string by using a gsub to convert non-JSON characters to
      # JSON, running it through a regular JSON parser, and the doing a recursive
      # map over the output to make sure the entries are in the correct type (mostly,
      # to make sure real/double precision database types are returned as float and
      # not integer).
      def self.parse(string, type=nil)
        new(recursive_map(JSON.parse(string.gsub(SUBST_RE){|m| SUBST[m]})), type)
      end

      # Convert each item in the array to the correct type, handling multi-dimensional
      # arrays.
      def self.recursive_map(array)
        array.map do |i|
          if i.is_a?(Array)
            recursive_map(i)
          elsif i
            convert_item(i)
          end
        end
      end
      private_class_method :recursive_map
    end

    # PGArray subclass for decimal/numeric types.  Uses the general
    # parser as the JSON parser cannot handle arbitrary precision numbers.
    class PGDecimalArray < PGArray
      # Convert the item to a BigDecimal.
      def self.convert_item(s)
        BigDecimal.new(s.to_s)
      end
      private_class_method :convert_item

      ARRAY_TYPE = 'decimal'.freeze

      # Use the decimal type by default.
      def array_type
        super || ARRAY_TYPE
      end
    end

    # PGArray subclass for handling real/double precision arrays.
    class PGFloatArray < PGNumericArray
      # Convert the item to a float.
      def self.convert_item(s)
        s.to_f
      end
      private_class_method :convert_item

      ARRAY_TYPE = 'double precision'.freeze

      # Use the double precision type by default.
      def array_type
        super || ARRAY_TYPE
      end
    end

    # PGArray subclass for handling int2/int4/int8 arrays.
    class PGIntegerArray < PGNumericArray
      ARRAY_TYPE = 'int4'.freeze

      # Use the int4 type by default.
      def array_type
        super || ARRAY_TYPE
      end
    end

    # PGArray subclass for handling char/varchar/text arrays.
    class PGStringArray < PGArray
      CHAR = 'char'.freeze
      VARCHAR = 'varchar'.freeze
      TEXT = 'text'.freeze

      # By default, use a text array.  If char is given without
      # a size, use varchar instead, as otherwise Postgres assumes
      # length of 1, which is likely to cause data loss.
      def array_type
        case (c = super)
        when nil 
          TEXT
        when CHAR, :char
          VARCHAR
        else
          c
        end
      end
    end

    PG_TYPES = {} unless defined?(PG_TYPES)

    # Automatically convert the built-in numeric and text array
    # types. to PGArray instances on retrieval if the native
    # postgres adapter is used.
    [ [1005, PGIntegerArray, 'int2'.freeze],
      [1007, PGIntegerArray, 'int4'.freeze],
      [1016, PGIntegerArray, 'int8'.freeze],
      [1021, PGFloatArray, 'real'.freeze],
      [1022, PGFloatArray, 'double precision'.freeze],
      [1231, PGDecimalArray, 'numeric'.freeze],
      [1009, PGStringArray, 'text'.freeze],
      [1014, PGStringArray, 'char'.freeze],
      [1015, PGStringArray, 'varchar'.freeze]
    ].each do |ftype, klass, type|
      meth = klass.method(:parse)
      PG_TYPES[ftype] = lambda{|s| meth.call(s, type)}
    end
  end
end

class Array
  # Return a PGArray proxy to the receiver, using a
  # specific database type if given.  This is mostly useful
  # as a short cut for creating PGArray objects that didn't
  # come from the database.
  def pg_array(type=nil)
    Sequel::Postgres::PGArray.new(self, type)
  end
end
