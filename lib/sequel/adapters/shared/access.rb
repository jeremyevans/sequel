module Sequel
  module Access
    module DatabaseMethods
      # Access uses type :access as the database_type
      def database_type
        :access
      end

      # Doesn't work, due to security restrictions on MSysObjects
      #def tables
      #  from(:MSysObjects).filter(:Type=>1, :Flags=>0).select_map(:Name).map{|x| x.to_sym}
      #end

      # Access uses type Counter for an autoincrementing keys
      def serial_primary_key_options
        {:primary_key => true, :type=>:Counter}
      end

      private

      def identifier_input_method_default
        nil
      end
      
      def identifier_output_method_default
        nil
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
      SELECT_CLAUSE_METHODS = Dataset.clause_methods(:select, %w'select distinct limit columns from join where group order having compounds')
      DATE_FORMAT = '#%Y-%m-%d#'.freeze
      TIMESTAMP_FORMAT = '#%Y-%m-%d %H:%M:%S#'.freeze
      TOP = " TOP ".freeze
      BRACKET_CLOSE = Dataset::BRACKET_CLOSE
      BRACKET_OPEN = Dataset::BRACKET_OPEN
      PAREN_CLOSE = Dataset::PAREN_CLOSE
      PAREN_OPEN = Dataset::PAREN_OPEN
      FROM = Dataset::FROM
      NOT_EQUAL = ' <> '.freeze
      OPS = {:'%'=>' Mod '.freeze, :'||'=>' & '.freeze}
      BOOL_FALSE = '0'.freeze
      BOOL_TRUE = '-1'.freeze
      DATE_FUNCTION = 'Date()'.freeze
      NOW_FUNCTION = 'Now()'.freeze
      TIME_FUNCTION = 'Time()'.freeze
      CAST_TYPES = {String=>:CStr, Integer=>:CLng, Date=>:CDate, Time=>:CDate, DateTime=>:CDate, Numeric=>:CDec, BigDecimal=>:CDec, File=>:CStr, Float=>:CDbl, TrueClass=>:CBool, FalseClass=>:CBool}

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
          super(sql, :LIKE, args)
        when :'NOT ILIKE'
          super(sql, :'NOT LIKE', args)
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

      def emulated_function_sql_append(sql, f)
        case f.f
        when :char_length
          literal_append(sql, SQL::Function.new(:len, f.args.first))
        else
          super
        end
      end
      
      # Access doesn't support INTERSECT or EXCEPT
      def supports_intersect_except?
        false
      end

      # Access does not support IS TRUE
      def supports_is_true?
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

      # Access requires the limit clause come before other clauses
      def select_clause_methods
        SELECT_CLAUSE_METHODS
      end
    end
  end
end
