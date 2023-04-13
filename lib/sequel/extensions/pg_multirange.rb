# frozen-string-literal: true
#
# The pg_multirange extension adds support for the PostgreSQL 14+ multirange
# types to Sequel.  PostgreSQL multirange types are similar to an array of
# ranges, where a match against the multirange is a match against any of the
# ranges in the multirange.
#
# When PostgreSQL multirange values are retrieved, they are parsed and returned
# as instances of Sequel::Postgres::PGMultiRange.  PGMultiRange mostly acts
# like an array of Sequel::Postgres::PGRange (see the pg_range extension).
#
# In addition to the parser, this extension comes with literalizers
# for PGMultiRanges, so they can be used in queries and as bound variables.
#
# To turn an existing array of Ranges into a PGMultiRange, use Sequel.pg_multirange.
# You must provide the type of multirange when creating the multirange:
#
#   Sequel.pg_multirange(array_of_date_ranges, :datemultirange)
#
# To use this extension, load it into the Database instance:
#
#   DB.extension :pg_multirange
#
# See the {schema modification guide}[rdoc-ref:doc/schema_modification.rdoc]
# for details on using multirange type columns in CREATE/ALTER TABLE statements.
#
# This extension makes it easy to add support for other multirange types.  In
# general, you just need to make sure that the subtype is handled and has the
# appropriate converter installed.  For user defined
# types, you can do this via:
#
#   DB.add_conversion_proc(subtype_oid){|string| }
#
# Then you can call
# Sequel::Postgres::PGMultiRange::DatabaseMethods#register_multirange_type
# to automatically set up a handler for the range type.  So if you
# want to support the timemultirange type (assuming the time type is already
# supported):
#
#   DB.register_multirange_type('timerange')
#
# This extension integrates with the pg_array extension.  If you plan
# to use arrays of multirange types, load the pg_array extension before the
# pg_multirange extension:
#
#   DB.extension :pg_array, :pg_multirange
#
# The pg_multirange extension will automatically load the pg_range extension.
#
# Related module: Sequel::Postgres::PGMultiRange

require 'delegate'
require 'strscan'

module Sequel
  module Postgres
    class PGMultiRange < DelegateClass(Array)
      include Sequel::SQL::AliasMethods

      # Converts strings into PGMultiRange instances.
      class Parser < StringScanner
        def initialize(source, converter)
          super(source)
          @converter = converter 
        end

        # Parse the multirange type input string into a PGMultiRange value.
        def parse
          raise Sequel::Error, "invalid multirange, doesn't start with {" unless get_byte == '{'
          ranges = []

          unless scan(/\}/)
            while true
              raise Sequel::Error, "unfinished multirange" unless range_string = scan_until(/[\]\)]/)
              ranges << @converter.call(range_string)
              
              case sep = get_byte
              when '}'
                break
              when ','
                # nothing
              else
                raise Sequel::Error, "invalid multirange separator: #{sep.inspect}"
              end
            end
          end

          raise Sequel::Error, "invalid multirange, remaining data after }" unless eos?
          ranges
        end
      end

      # Callable object that takes the input string and parses it using Parser.
      class Creator
        # The database type to set on the PGMultiRange instances returned.
        attr_reader :type

        def initialize(type, converter=nil)
          @type = type
          @converter = converter
        end

        # Parse the string using Parser with the appropriate
        # converter, and return a PGMultiRange with the appropriate database
        # type.
        def call(string)
          PGMultiRange.new(Parser.new(string, @converter).parse, @type)
        end
      end

      module DatabaseMethods
        # Add the default multirange conversion procs to the database
        def self.extended(db)
          db.instance_exec do
            raise Error, "multiranges not supported on this database" unless server_version >= 140000

            extension :pg_range
            @pg_multirange_schema_types ||= {}

            register_multirange_type('int4multirange', :range_oid=>3904, :oid=>4451)
            register_multirange_type('nummultirange', :range_oid=>3906, :oid=>4532)
            register_multirange_type('tsmultirange', :range_oid=>3908, :oid=>4533)
            register_multirange_type('tstzmultirange', :range_oid=>3910, :oid=>4534)
            register_multirange_type('datemultirange', :range_oid=>3912, :oid=>4535)
            register_multirange_type('int8multirange', :range_oid=>3926, :oid=>4536)

            if respond_to?(:register_array_type)
              register_array_type('int4multirange', :oid=>6150, :scalar_oid=>4451, :scalar_typecast=>:int4multirange)
              register_array_type('nummultirange', :oid=>6151, :scalar_oid=>4532, :scalar_typecast=>:nummultirange)
              register_array_type('tsmultirange', :oid=>6152, :scalar_oid=>4533, :scalar_typecast=>:tsmultirange)
              register_array_type('tstzmultirange', :oid=>6153, :scalar_oid=>4534, :scalar_typecast=>:tstzmultirange)
              register_array_type('datemultirange', :oid=>6155, :scalar_oid=>4535, :scalar_typecast=>:datemultirange)
              register_array_type('int8multirange', :oid=>6157, :scalar_oid=>4536, :scalar_typecast=>:int8multirange)
            end

            [:int4multirange, :nummultirange, :tsmultirange, :tstzmultirange, :datemultirange, :int8multirange].each do |v|
              @schema_type_classes[v] = PGMultiRange
            end

            procs = conversion_procs
            add_conversion_proc(4533, PGMultiRange::Creator.new("tsmultirange", procs[3908]))
            add_conversion_proc(4534, PGMultiRange::Creator.new("tstzmultirange", procs[3910]))

            if respond_to?(:register_array_type) && defined?(PGArray::Creator)
              add_conversion_proc(6152, PGArray::Creator.new("tsmultirange", procs[4533]))
              add_conversion_proc(6153, PGArray::Creator.new("tstzmultirange", procs[4534]))
            end
          end
        end

        # Handle PGMultiRange values in bound variables
        def bound_variable_arg(arg, conn)
          case arg
          when PGMultiRange 
            arg.unquoted_literal(schema_utility_dataset)
          else
            super
          end
        end

        # Freeze the pg multirange schema types to prevent adding new ones.
        def freeze
          @pg_multirange_schema_types.freeze
          super
        end

        # Register a database specific multirange type.  This can be used to support
        # different multirange types per Database.  Options:
        #
        # :converter :: A callable object (e.g. Proc), that is called with the PostgreSQL range string,
        #               and should return a PGRange instance.
        # :oid :: The PostgreSQL OID for the multirange type.  This is used by Sequel to set up automatic type
        #         conversion on retrieval from the database.
        # :range_oid :: Should be the PostgreSQL OID for the multirange subtype (the range type). If given,
        #               automatically sets the :converter option by looking for scalar conversion
        #               proc.
        #
        # If a block is given, it is treated as the :converter option.
        def register_multirange_type(db_type, opts=OPTS, &block)
          oid = opts[:oid]
          soid = opts[:range_oid]

          if has_converter = opts.has_key?(:converter)
            raise Error, "can't provide both a block and :converter option to register_multirange_type" if block
            converter = opts[:converter]
          else
            has_converter = true if block
            converter = block
          end

          unless (soid || has_converter) && oid
            range_oid, subtype_oid = from(:pg_range).join(:pg_type, :oid=>:rngmultitypid).where(:typname=>db_type.to_s).get([:rngmultitypid, :rngtypid])
            soid ||= subtype_oid unless has_converter
            oid ||= range_oid
          end

          db_type = db_type.to_s.dup.freeze

          if soid
            raise Error, "can't provide both a converter and :range_oid option to register" if has_converter 
            raise Error, "no conversion proc for :range_oid=>#{soid.inspect} in conversion_procs" unless converter = conversion_procs[soid]
          end

          raise Error, "cannot add a multirange type without a convertor (use :converter or :range_oid option or pass block)" unless converter
          creator = Creator.new(db_type, converter)
          add_conversion_proc(oid, creator)

          @pg_multirange_schema_types[db_type] = db_type.to_sym

          singleton_class.class_eval do
            meth = :"typecast_value_#{db_type}"
            scalar_typecast_method = :"typecast_value_#{opts.fetch(:scalar_typecast, db_type.sub('multirange', 'range'))}"
            define_method(meth){|v| typecast_value_pg_multirange(v, creator, scalar_typecast_method)}
            private meth
          end

          @schema_type_classes[db_type] = PGMultiRange
          nil
        end

        private

        # Recognize the registered database multirange types.
        def schema_multirange_type(db_type)
          @pg_multirange_schema_types[db_type] || super
        end

        # Set the :ruby_default value if the default value is recognized as a multirange.
        def schema_post_process(_)
          super.each do |a|
            h = a[1]
            db_type = h[:db_type]
            if @pg_multirange_schema_types[db_type] && h[:default] =~ /\A#{db_type}\(.*\)\z/
              h[:ruby_default] = get(Sequel.lit(h[:default])) 
            end
          end
        end

        # Given a value to typecast and the type of PGMultiRange subclass:
        # * If given a PGMultiRange with a matching type, use it directly.
        # * If given a PGMultiRange with a different type, return a PGMultiRange
        #   with the creator's type.
        # * If given an Array, create a new PGMultiRange instance for it, typecasting
        #   each instance using the scalar_typecast_method.
        def typecast_value_pg_multirange(value, creator, scalar_typecast_method=nil)
          case value
          when PGMultiRange
            return value if value.db_type == creator.type
          when Array
            # nothing
          else
            raise Sequel::InvalidValue, "invalid value for multirange type: #{value.inspect}"
          end

          if scalar_typecast_method && respond_to?(scalar_typecast_method, true)
            value = value.map{|v| send(scalar_typecast_method, v)}
          end
          PGMultiRange.new(value, creator.type)
        end
      end

      # The type of this multirange (e.g. 'int4multirange').
      attr_accessor :db_type

      # Set the array of ranges to delegate to, and the database type.
      def initialize(ranges, db_type)
        super(ranges)
        @db_type = db_type.to_s
      end

      # Append the multirange SQL to the given sql string. 
      def sql_literal_append(ds, sql)
        sql << db_type << '('
        joiner = nil
        conversion_meth = nil
        each do |range|
          if joiner
            sql << joiner
          else
            joiner = ', '
          end

          unless range.is_a?(PGRange)
            conversion_meth ||= :"typecast_value_#{db_type.sub('multi', '')}"
            range = ds.db.send(conversion_meth, range)
          end

          ds.literal_append(sql, range)
        end
        sql << ')'
      end

      # Return whether the value is inside any of the ranges in the multirange.
      def cover?(value)
        any?{|range| range.cover?(value)}
      end
      alias === cover?

      # Don't consider multiranges with different database types equal.
      def eql?(other)
        if PGMultiRange === other
          return false unless other.db_type == db_type
          other = other.__getobj__
        end
        __getobj__.eql?(other)
      end

      # Don't consider multiranges with different database types equal.
      def ==(other)
        return false if PGMultiRange === other && other.db_type != db_type
        super
      end

      # Return a string containing the unescaped version of the multirange.
      # Separated out for use by the bound argument code.
      def unquoted_literal(ds)
        val = String.new
        val << "{"

        joiner = nil
        conversion_meth = nil
        each do |range|
          if joiner
            val << joiner
          else
            joiner = ', '
          end

          unless range.is_a?(PGRange)
            conversion_meth ||= :"typecast_value_#{db_type.sub('multi', '')}"
            range = ds.db.send(conversion_meth, range)
          end

          val << range.unquoted_literal(ds)
        end
         
        val << "}"
      end

      # Allow automatic parameterization.
      def sequel_auto_param_type(ds)
        "::#{db_type}"
      end
    end
  end

  module SQL::Builders
    # Convert the object to a Postgres::PGMultiRange.
    def pg_multirange(v, db_type)
      case v
      when Postgres::PGMultiRange
        if v.db_type == db_type
          v
        else
          Postgres::PGMultiRange.new(v, db_type)
        end
      when Array
        Postgres::PGMultiRange.new(v, db_type)
      else
        # May not be defined unless the pg_range_ops extension is used
        pg_range_op(v)
      end
    end
  end

  Database.register_extension(:pg_multirange, Postgres::PGMultiRange::DatabaseMethods)
end
