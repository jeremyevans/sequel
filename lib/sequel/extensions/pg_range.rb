# The pg_range extension adds support for the PostgreSQL 9.2+ range
# types to Sequel.  PostgreSQL range types are similar to ruby's
# Range class, representating an array of values.  However, they
# are more flexible than ruby's ranges, allowing exclusive beginnings
# and endings (ruby's range only allows exclusive endings), and
# unbounded beginnings and endings (which ruby's range does not
# support).
#
# This extension integrates with Sequel's native postgres and jdbc/postgresql adapters, so
# that when range type values are retrieved, they are parsed and returned
# as instances of Sequel::Postgres::PGRange.  PGRange mostly acts
# like a Range, but it's not a Range as not all PostgreSQL range
# type values would be valid ruby ranges.  If the range type value
# you are using is a valid ruby range, you can call PGRange#to_range
# to get a Range.  However, if you call PGRange#to_range on a range
# type value uses features that ruby's Range does not support, an
# exception will be raised.
#
# In addition to the parser, this extension comes with literalizers
# for both PGRange and Range that use the standard Sequel literalization
# callbacks, so they work on all adapters.
#
# To turn an existing Range into a PGRange, use Sequel.pg_range:
#
#   Sequel.pg_range(range)
#
# If you have loaded the {core_extensions extension}[rdoc-ref:doc/core_extensions.rdoc],
# or you have loaded the core_refinements extension
# and have activated refinements for the file, you can also use Range#pg_range:
#
#   range.pg_range 
#
# You may want to specify a specific range type:
#
#   Sequel.pg_range(range, :daterange)
#   range.pg_range(:daterange)
#
# If you specify the range database type, Sequel will automatically cast
# the value to that type when literalizing.
#
# If you would like to use range columns in your model objects, you
# probably want to modify the schema parsing/typecasting so that it
# recognizes and correctly handles the range type columns, which you can
# do by:
#
#   DB.extension :pg_range
#
# If you are not using the native postgres or jdbc/postgresql adapters and are using range
# types as model column values you probably should use the
# pg_typecast_on_load plugin if the column values are returned as a string.
#
# See the {schema modification guide}[rdoc-ref:doc/schema_modification.rdoc]
# for details on using range type columns in CREATE/ALTER TABLE statements.
#
# This extension integrates with the pg_array extension.  If you plan
# to use arrays of range types, load the pg_array extension before the
# pg_range extension:
#
#   DB.extension :pg_array, :pg_range

Sequel.require 'adapters/utils/pg_types'

module Sequel
  module Postgres
    class PGRange
      include Sequel::SQL::AliasMethods

      # Map of string database type names to type symbols (e.g. 'int4range' => :int4range),
      # used in the schema parsing.
      RANGE_TYPES = {}

      EMPTY = 'empty'.freeze
      EMPTY_STRING = ''.freeze
      QUOTED_EMPTY_STRING = '""'.freeze
      OPEN_PAREN = "(".freeze
      CLOSE_PAREN = ")".freeze
      OPEN_BRACKET = "[".freeze
      CLOSE_BRACKET = "]".freeze
      ESCAPE_RE = /("|,|\\|\[|\]|\(|\))/.freeze
      ESCAPE_REPLACE = '\\\\\1'.freeze
      CAST = '::'.freeze

      # Registers a range type that the extension should handle.  Makes a Database instance that
      # has been extended with DatabaseMethods recognize the range type given and set up the
      # appropriate typecasting.  Also sets up automatic typecasting for the native postgres
      # adapter, so that on retrieval, the values are automatically converted to PGRange instances.
      # The db_type argument should be the name of the range type. Accepts the following options:
      #
      # :converter :: A callable object (e.g. Proc), that is called with the start or end of the range
      #               (usually a string), and should return the appropriate typecasted object.
      # :oid :: The PostgreSQL OID for the range type.  This is used by the Sequel postgres adapter
      #         to set up automatic type conversion on retrieval from the database.
      # :subtype_oid :: Should be the PostgreSQL OID for the range's subtype. If given,
      #                automatically sets the :converter option by looking for scalar conversion
      #                proc.
      #
      # If a block is given, it is treated as the :converter option.
      def self.register(db_type, opts=OPTS, &block)
        db_type = db_type.to_s.dup.freeze

        if converter = opts[:converter]
          raise Error, "can't provide both a block and :converter option to register" if block
        else
          converter = block
        end

        if soid = opts[:subtype_oid]
          raise Error, "can't provide both a converter and :scalar_oid option to register" if converter 
          raise Error, "no conversion proc for :scalar_oid=>#{soid.inspect} in PG_TYPES" unless converter = PG_TYPES[soid]
        end

        parser = Parser.new(db_type, converter)

        RANGE_TYPES[db_type] = db_type.to_sym

        DatabaseMethods.define_range_typecast_method(db_type, parser)

        if oid = opts[:oid]
          Sequel::Postgres::PG_TYPES[oid] = parser
        end

        nil
      end

      # Creates callable objects that convert strings into PGRange instances.
      class Parser
        # Regexp that parses the full range of PostgreSQL range type output,
        # except for empty ranges.
        PARSER = /\A(\[|\()("((?:\\"|[^"])*)"|[^"]*),("((?:\\"|[^"])*)"|[^"]*)(\]|\))\z/o

        REPLACE_RE = /\\(.)/.freeze
        REPLACE_WITH = '\1'.freeze

        # The database range type for this parser (e.g. 'int4range'),
        # automatically setting the db_type for the returned PGRange instances.
        attr_reader :db_type

        # A callable object to convert the beginning and ending of the range into
        # the appropriate ruby type.
        attr_reader :converter

        # Set the db_type and converter on initialization.
        def initialize(db_type, converter=nil)
          @db_type = db_type.to_s.dup.freeze if db_type
          @converter = converter
        end

        # Parse the range type input string into a PGRange value.
        def call(string)
          if string == EMPTY
            return PGRange.empty(db_type)
          end

          raise(InvalidValue, "invalid or unhandled range format: #{string.inspect}") unless matches = PARSER.match(string)

          exclude_begin = matches[1] == '('
          exclude_end = matches[6] == ')'

          # If the input is quoted, it needs to be unescaped.  Also, quoted input isn't
          # checked for emptiness, since the empty quoted string is considered an 
          # element that happens to be the empty string, while an unquoted empty string
          # is considered unbounded.
          #
          # While PostgreSQL allows pure escaping for input (without quoting), it appears
          # to always use the quoted output form when characters need to be escaped, so
          # there isn't a need to unescape unquoted output.
          if beg = matches[3]
            beg.gsub!(REPLACE_RE, REPLACE_WITH)
          else
            beg = matches[2] unless matches[2].empty?
          end
          if en = matches[5]
            en.gsub!(REPLACE_RE, REPLACE_WITH)
          else
            en = matches[4] unless matches[4].empty?
          end

          if c = converter
            beg = c.call(beg) if beg
            en = c.call(en) if en
          end

          PGRange.new(beg, en, :exclude_begin=>exclude_begin, :exclude_end=>exclude_end, :db_type=>db_type)
        end
      end

      module DatabaseMethods
        # Reset the conversion procs if using the native postgres adapter,
        # and extend the datasets to correctly literalize ruby Range values.
        def self.extended(db)
          db.instance_eval do
            extend_datasets(DatasetMethods)
            copy_conversion_procs([3904, 3906, 3912, 3926, 3905, 3907, 3913, 3927])
            [:int4range, :numrange, :tsrange, :tstzrange, :daterange, :int8range].each do |v|
              @schema_type_classes[v] = PGRange
            end
          end

          procs = db.conversion_procs
          procs[3908] = Parser.new("tsrange", procs[1114])
          procs[3910] = Parser.new("tstzrange", procs[1184])
          if defined?(PGArray::Creator)
            procs[3909] = PGArray::Creator.new("tsrange", procs[3908])
            procs[3911] = PGArray::Creator.new("tstzrange", procs[3910])
          end

        end

        # Define a private range typecasting method for the given type that uses
        # the parser argument to do the type conversion.
        def self.define_range_typecast_method(type, parser)
          meth = :"typecast_value_#{type}"
          define_method(meth){|v| typecast_value_pg_range(v, parser)}
          private meth
        end

        # Handle Range and PGRange values in bound variables
        def bound_variable_arg(arg, conn)
          case arg
          when PGRange 
            arg.unquoted_literal(schema_utility_dataset)
          when Range
            PGRange.from_range(arg).unquoted_literal(schema_utility_dataset)
          else
            super
          end
        end

        private

        # Handle arrays of range types in bound variables.
        def bound_variable_array(a)
          case a
          when PGRange, Range
            "\"#{bound_variable_arg(a, nil)}\""
          else
            super
          end
        end

        # Manually override the typecasting for tsrange and tstzrange types so that
        # they use the database's timezone instead of the global Sequel
        # timezone.
        def get_conversion_procs
          procs = super

          procs[3908] = Parser.new("tsrange", procs[1114])
          procs[3910] = Parser.new("tstzrange", procs[1184])
          if defined?(PGArray::Creator)
            procs[3909] = PGArray::Creator.new("tsrange", procs[3908])
            procs[3911] = PGArray::Creator.new("tstzrange", procs[3910])
          end

          procs
        end

        # Recognize the registered database range types.
        def schema_column_type(db_type)
          if type = RANGE_TYPES[db_type]
            type
          else
            super
          end
        end

        # Typecast value correctly to a PGRange.  If already an
        # PGRange instance with the same db_type, return as is.
        # If a PGRange with a different subtype, return a new
        # PGRange with the same values and the expected subtype.
        # If a Range object, create a PGRange with the given
        # db_type.  If a string, assume it is in PostgreSQL
        # output format and parse it using the parser.
        def typecast_value_pg_range(value, parser)
          case value
          when PGRange
            if value.db_type.to_s == parser.db_type
              value
            elsif value.empty?
              PGRange.empty(parser.db_type)
            else
              PGRange.new(value.begin, value.end, :exclude_begin=>value.exclude_begin?, :exclude_end=>value.exclude_end?, :db_type=>parser.db_type)
            end
          when Range
            PGRange.from_range(value, parser.db_type)
          when String
            parser.call(value)
          else
            raise Sequel::InvalidValue, "invalid value for range type: #{value.inspect}"
          end
        end
      end

      module DatasetMethods
        # Handle literalization of ruby Range objects, treating them as
        # PostgreSQL ranges.
        def literal_other_append(sql, v)
          case v
          when Range
            super(sql, Sequel::Postgres::PGRange.from_range(v))
          else
            super
          end
        end
      end

      include Enumerable

      # The beginning of the range.  If nil, the range has an unbounded beginning.
      attr_reader :begin

      # The end of the range.  If nil, the range has an unbounded ending.
      attr_reader :end

      # The PostgreSQL database type for the range (e.g. 'int4range').
      attr_reader :db_type

      # Create a new PGRange instance using the beginning and ending of the ruby Range,
      # with the given db_type.
      def self.from_range(range, db_type=nil)
        new(range.begin, range.end, :exclude_end=>range.exclude_end?, :db_type=>db_type)
      end

      # Create an empty PGRange with the given database type.
      def self.empty(db_type=nil)
        new(nil, nil, :empty=>true, :db_type=>db_type)
      end

      # Initialize a new PGRange instance.  Accepts the following options:
      #
      # :db_type :: The PostgreSQL database type for the range.
      # :empty :: Whether the range is empty (has no points)
      # :exclude_begin :: Whether the beginning element is excluded from the range.
      # :exclude_end :: Whether the ending element is excluded from the range.
      def initialize(beg, en, opts=OPTS)
        @begin = beg
        @end = en
        @empty = !!opts[:empty]
        @exclude_begin = !!opts[:exclude_begin]
        @exclude_end = !!opts[:exclude_end]
        @db_type = opts[:db_type]
        if @empty
          raise(Error, 'cannot have an empty range with either a beginning or ending') unless @begin.nil? && @end.nil? && opts[:exclude_begin].nil? && opts[:exclude_end].nil?
        end
      end

      # Delegate to the ruby range object so that the object mostly acts like a range.
      range_methods = %w'each last first step'
      range_methods << 'cover?' if RUBY_VERSION >= '1.9'
      range_methods.each do |m|
        class_eval("def #{m}(*a, &block) to_range.#{m}(*a, &block) end", __FILE__, __LINE__)
      end

      # Consider the receiver equal to other PGRange instances with the
      # same beginning, ending, exclusions, and database type.  Also consider
      # it equal to Range instances if this PGRange can be converted to a
      # a Range and those ranges are equal.
      def eql?(other)
        case other
        when PGRange
          if db_type == other.db_type
            if empty?
              other.empty?
            elsif other.empty?
              false
            else
              [:@begin, :@end, :@exclude_begin, :@exclude_end].all?{|v| instance_variable_get(v) == other.instance_variable_get(v)}
            end
          else
            false
          end
        when Range
          if valid_ruby_range?
            to_range.eql?(other)
          else
            false
          end
        else
          false
        end
      end
      alias == eql?

      # Allow PGRange values in case statements, where they return true if they
      # are equal to each other using eql?, or if this PGRange can be converted
      # to a Range, delegating to that range.
      def ===(other)
        if eql?(other)
          true
        else
          if valid_ruby_range?
            to_range === other 
          else
            false
          end
        end
      end

      # Whether this range is empty (has no points).  Note that for manually created ranges
      # (ones not retrieved from the database), this will only be true if the range
      # was created using the :empty option.
      def empty?
        @empty
      end

      # Whether the beginning element is excluded from the range.
      def exclude_begin?
        @exclude_begin
      end

      # Whether the ending element is excluded from the range.
      def exclude_end?
        @exclude_end
      end

      # Append a literalize version of the receiver to the sql.
      def sql_literal_append(ds, sql)
        ds.literal_append(sql, unquoted_literal(ds))
        if s = @db_type
          sql << CAST << s.to_s
        end
      end

      # Return a ruby Range object for this instance, if one can be created.
      def to_range
        return @range if @range
        raise(Error, "cannot create ruby range for an empty PostgreSQL range") if empty?
        raise(Error, "cannot create ruby range when PostgreSQL range excludes beginning element") if exclude_begin?
        raise(Error, "cannot create ruby range when PostgreSQL range has unbounded beginning") unless self.begin
        raise(Error, "cannot create ruby range when PostgreSQL range has unbounded ending") unless self.end
        @range = Range.new(self.begin, self.end, exclude_end?)
      end

      # Whether or not this PGRange is a valid ruby range.  In order to be a valid ruby range,
      # it must have a beginning and an ending (no unbounded ranges), and it cannot exclude
      # the beginning element.
      def valid_ruby_range?
        !(empty? || exclude_begin? || !self.begin || !self.end)
      end

      # Whether the beginning of the range is unbounded.
      def unbounded_begin?
        self.begin.nil? && !empty?
      end

      # Whether the end of the range is unbounded.
      def unbounded_end?
        self.end.nil? && !empty?
      end

      # Return a string containing the unescaped version of the range.
      # Separated out for use by the bound argument code.
      def unquoted_literal(ds)
        if empty?
          EMPTY
        else
          "#{exclude_begin? ? OPEN_PAREN : OPEN_BRACKET}#{escape_value(self.begin, ds)},#{escape_value(self.end, ds)}#{exclude_end? ? CLOSE_PAREN : CLOSE_BRACKET}"
        end
      end

      private

      # Escape common range types.  Instead of quoting, just backslash escape all
      # special characters.
      def escape_value(k, ds)
        case k
        when nil
          EMPTY_STRING
        when Date, Time
          ds.literal(k)[1...-1]
        when Integer, Float
          k.to_s
        when BigDecimal
          k.to_s('F')
        when LiteralString
          k
        when String
          if k.empty?
            QUOTED_EMPTY_STRING
          else
            k.gsub(ESCAPE_RE, ESCAPE_REPLACE)
          end
        else
          ds.literal(k).gsub(ESCAPE_RE, ESCAPE_REPLACE)
        end
      end
    end

    PGRange.register('int4range', :oid=>3904, :subtype_oid=>23)
    PGRange.register('numrange', :oid=>3906, :subtype_oid=>1700)
    PGRange.register('tsrange', :oid=>3908, :subtype_oid=>1114)
    PGRange.register('tstzrange', :oid=>3910, :subtype_oid=>1184)
    PGRange.register('daterange', :oid=>3912, :subtype_oid=>1082)
    PGRange.register('int8range', :oid=>3926, :subtype_oid=>20)
    if defined?(PGArray) && PGArray.respond_to?(:register)
      PGArray.register('int4range', :oid=>3905, :scalar_oid=>3904, :scalar_typecast=>:int4range)
      PGArray.register('numrange', :oid=>3907, :scalar_oid=>3906, :scalar_typecast=>:numrange)
      PGArray.register('tsrange', :oid=>3909, :scalar_oid=>3908, :scalar_typecast=>:tsrange)
      PGArray.register('tstzrange', :oid=>3911, :scalar_oid=>3910, :scalar_typecast=>:tstzrange)
      PGArray.register('daterange', :oid=>3913, :scalar_oid=>3912, :scalar_typecast=>:daterange)
      PGArray.register('int8range', :oid=>3927, :scalar_oid=>3926, :scalar_typecast=>:int8range)
    end
  end

  module SQL::Builders
    # Convert the object to a Postgres::PGRange.
    def pg_range(v, db_type=nil)
      case v
      when Postgres::PGRange
        if db_type.nil? || v.db_type == db_type
          v
        else
          Postgres::PGRange.new(v.begin, v.end, :exclude_begin=>v.exclude_begin?, :exclude_end=>v.exclude_end?, :db_type=>db_type)
        end
      when Range
        Postgres::PGRange.from_range(v, db_type)
      else
        # May not be defined unless the pg_range_ops extension is used
        pg_range_op(v)
      end
    end
  end

  Database.register_extension(:pg_range, Postgres::PGRange::DatabaseMethods)
end

# :nocov:
if Sequel.core_extensions?
  class Range 
    # Create a new PGRange using the receiver as the input range,
    # with the given database type.
    def pg_range(db_type=nil)
      Sequel::Postgres::PGRange.from_range(self, db_type)
    end
  end
end

if defined?(Sequel::CoreRefinements)
  module Sequel::CoreRefinements
    refine Range do
      def pg_range(db_type=nil)
        Sequel::Postgres::PGRange.from_range(self, db_type)
      end
    end
  end
end
# :nocov:
