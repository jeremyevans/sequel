# frozen-string-literal: true

Sequel.require %w'emulate_offset_with_reverse_and_count unmodified_identifiers', 'adapters/utils'

module Sequel
  module Access
    Sequel::Database.set_shared_adapter_scheme(:access, self)

    module DatabaseMethods
      include UnmodifiedIdentifiers::DatabaseMethods

      # Access uses type :access as the database_type
      def database_type
        :access
      end

      # Doesn't work, due to security restrictions on MSysObjects
      #def tables
      #  from(:MSysObjects).where(:Type=>1, :Flags=>0).select_map(:Name).map(&:to_sym)
      #end
      
      # Access doesn't support renaming tables from an SQL query,
      # so create a copy of the table and then drop the from table.
      def rename_table(from_table, to_table)
        create_table(to_table, :as=>from(from_table))
        drop_table(from_table)
      end

      # Access uses type Counter for an autoincrementing keys
      def serial_primary_key_options
        {:primary_key => true, :type=>:Counter}
      end

      private

      def alter_table_set_column_type_sql(table, op)
        "ALTER COLUMN #{quote_identifier(op[:name])} #{type_literal(op)}"
      end

      # Access doesn't support CREATE TABLE AS, it only supports SELECT INTO.
      # Emulating CREATE TABLE AS using SELECT INTO is only possible if a dataset
      # is given as the argument, it can't work with a string, so raise an
      # Error if a string is given.
      def create_table_as(name, ds, options)
        raise(Error, "must provide dataset instance as value of create_table :as option on Access") unless ds.is_a?(Sequel::Dataset)
        run(ds.into(name).sql)
      end
    
      DATABASE_ERROR_REGEXPS = {
        /The changes you requested to the table were not successful because they would create duplicate values in the index, primary key, or relationship/ => UniqueConstraintViolation,
        /You cannot add or change a record because a related record is required|The record cannot be deleted or changed because table/ => ForeignKeyConstraintViolation,
        /One or more values are prohibited by the validation rule/ => CheckConstraintViolation,
        /You must enter a value in the .+ field|cannot contain a Null value because the Required property for this field is set to True/ => NotNullConstraintViolation,
      }.freeze
      def database_error_regexps
        DATABASE_ERROR_REGEXPS
      end

      # The SQL to drop an index for the table.
      def drop_index_sql(table, op)
        "DROP INDEX #{quote_identifier(op[:name] || default_index_name(table, op[:columns]))} ON #{quote_schema_table(table)}"
      end
      
      # Access doesn't have a 64-bit integer type, so use integer and hope
      # the user isn't using more than 32 bits.
      def type_literal_generic_bignum_symbol(column)
        :integer
      end

      # Access doesn't have a true boolean class, so it uses bit
      def type_literal_generic_trueclass(column)
        :bit
      end
      
      # Access uses image type for blobs
      def type_literal_generic_file(column)
        :image
      end
    end
  
    module DatasetMethods
      include(Module.new do
        Dataset.def_sql_method(self, :select, %w'select distinct limit columns into from join where group order having compounds')
      end)
      include EmulateOffsetWithReverseAndCount
      include UnmodifiedIdentifiers::DatasetMethods

      DATE_FORMAT = '#%Y-%m-%d#'.freeze
      TIMESTAMP_FORMAT = '#%Y-%m-%d %H:%M:%S#'.freeze
      TOP = " TOP ".freeze
      BRACKET_CLOSE = Dataset::BRACKET_CLOSE
      BRACKET_OPEN = Dataset::BRACKET_OPEN
      PAREN_CLOSE = Dataset::PAREN_CLOSE
      PAREN_OPEN = Dataset::PAREN_OPEN
      INTO = Dataset::INTO
      FROM = Dataset::FROM
      SPACE = Dataset::SPACE
      NOT_EQUAL = ' <> '.freeze
      OPS = {:'%'=>' Mod '.freeze, :'||'=>' & '.freeze}
      BOOL_FALSE = '0'.freeze
      BOOL_TRUE = '-1'.freeze
      DATE_FUNCTION = 'Date()'.freeze
      NOW_FUNCTION = 'Now()'.freeze
      TIME_FUNCTION = 'Time()'.freeze
      CAST_TYPES = {String=>:CStr, Integer=>:CLng, Date=>:CDate, Time=>:CDate, DateTime=>:CDate, Numeric=>:CDec, BigDecimal=>:CDec, File=>:CStr, Float=>:CDbl, TrueClass=>:CBool, FalseClass=>:CBool}

      EMULATED_FUNCTION_MAP = {:char_length=>:len}
      EXTRACT_MAP = {:year=>"'yyyy'", :month=>"'m'", :day=>"'d'", :hour=>"'h'", :minute=>"'n'", :second=>"'s'"}
      COMMA = Dataset::COMMA
      DATEPART_OPEN = "datepart(".freeze

      # Access doesn't support CASE, but it can be emulated with nested
      # IIF function calls.
      def case_expression_sql_append(sql, ce)
        literal_append(sql, ce.with_merged_expression.conditions.reverse.inject(ce.default){|exp,(cond,val)| Sequel::SQL::Function.new(:IIF, cond, val, exp)})
      end

      # Access doesn't support CAST, it uses separate functions for
      # type conversion
      def cast_sql_append(sql, expr, type)
        sql << CAST_TYPES.fetch(type, type).to_s
        sql << PAREN_OPEN
        literal_append(sql, expr)
        sql << PAREN_CLOSE
      end

      def complex_expression_sql_append(sql, op, args)
        case op
        when :ILIKE
          complex_expression_sql_append(sql, :LIKE, args)
        when :'NOT ILIKE'
          complex_expression_sql_append(sql, :'NOT LIKE', args)
        when :LIKE, :'NOT LIKE'
          sql << PAREN_OPEN
          literal_append(sql, args.at(0))
          sql << SPACE << op.to_s << SPACE
          literal_append(sql, args.at(1))
          sql << PAREN_CLOSE
        when :'!='
          sql << PAREN_OPEN
          literal_append(sql, args.at(0))
          sql << NOT_EQUAL
          literal_append(sql, args.at(1))
          sql << PAREN_CLOSE
        when :'%', :'||'
          sql << PAREN_OPEN
          c = false
          op_str = OPS[op]
          args.each do |a|
            sql << op_str if c
            literal_append(sql, a)
            c ||= true
          end
          sql << PAREN_CLOSE
        when :**
          sql << PAREN_OPEN
          literal_append(sql, args[0])
          sql << ' ^ '
          literal_append(sql, args[1])
          sql << PAREN_CLOSE
        when :extract
          part = args.at(0)
          raise(Sequel::Error, "unsupported extract argument: #{part.inspect}") unless format = EXTRACT_MAP[part]
          sql << DATEPART_OPEN << format.to_s << COMMA
          literal_append(sql, args.at(1))
          sql << PAREN_CLOSE
        else
          super
        end
      end

      # Use Date() and Now() for CURRENT_DATE and CURRENT_TIMESTAMP
      def constant_sql_append(sql, constant)
        case constant
        when :CURRENT_DATE
          sql << DATE_FUNCTION
        when :CURRENT_TIMESTAMP
          sql << NOW_FUNCTION
        when :CURRENT_TIME
          sql << TIME_FUNCTION
        else
          super
        end
      end

      # Emulate cross join by using multiple tables in the FROM clause.
      def cross_join(table)
        clone(:from=>@opts[:from] + [table])
      end

      # Access uses [] to escape metacharacters, instead of backslashes.
      def escape_like(string)
        string.gsub(/[\\*#?\[]/){|m| "[#{m}]"}
      end
   
      # Specify a table for a SELECT ... INTO query.
      def into(table)
        clone(:into => table)
      end

      # Access does not support derived column lists.
      def supports_derived_column_lists?
        false
      end

      # Access doesn't support INTERSECT or EXCEPT
      def supports_intersect_except?
        false
      end

      # Access does not support IS TRUE
      def supports_is_true?
        false
      end
      
      # Access doesn't support JOIN USING
      def supports_join_using?
        false
      end

      # Access does not support multiple columns for the IN/NOT IN operators
      def supports_multiple_column_in?
        false
      end

      # Access doesn't support truncate, so do a delete instead.
      def truncate
        delete
        nil
      end
      
      private

      # Access uses # to quote dates
      def literal_date(d)
        d.strftime(DATE_FORMAT)
      end

      # Access uses # to quote datetimes
      def literal_datetime(t)
        t.strftime(TIMESTAMP_FORMAT)
      end
      alias literal_time literal_datetime

      # Use 0 for false on MSSQL
      def literal_false
        BOOL_FALSE
      end

      # Use 0 for false on MSSQL
      def literal_true
        BOOL_TRUE
      end

      # Access requires parentheses when joining more than one table
      def select_from_sql(sql)
        if f = @opts[:from]
          sql << FROM
          if (j = @opts[:join]) && !j.empty?
            sql << (PAREN_OPEN * j.length)
          end
          source_list_append(sql, f)
        end
      end

      def select_into_sql(sql)
        if i = @opts[:into]
          sql << INTO
          identifier_append(sql, i)
        end
      end

      # Access requires parentheses when joining more than one table
      def select_join_sql(sql)
        if js = @opts[:join]
          js.each do |j|
            literal_append(sql, j)
            sql << PAREN_CLOSE
          end
        end
      end

      # Access uses TOP for limits
      def select_limit_sql(sql)
        if l = @opts[:limit]
          sql << TOP
          literal_append(sql, l)
        end
      end

      # Access uses [] for quoting identifiers
      def quoted_identifier_append(sql, v)
        sql << BRACKET_OPEN << v.to_s << BRACKET_CLOSE
      end
    end
  end
end
