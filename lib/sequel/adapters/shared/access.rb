module Sequel
  module Access
    module DatabaseMethods
      # Access uses type :access as the database_type
      def database_type
        :access
      end

      # Doesn't work, due to security restrictions on MSysObjects
      def tables
        from(:MSysObjects).filter(:Type=>1, :Flags=>0).select_map(:Name).map{|x| x.to_sym}
      end

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
    end

    module DatasetMethods
      SELECT_CLAUSE_METHODS = Dataset.clause_methods(:select, %w'select limit distinct columns from join where group order having compounds')
      DATE_FORMAT = '#%Y-%m-%d#'.freeze
      TIMESTAMP_FORMAT = '#%Y-%m-%d %H:%M:%S#'.freeze
      TOP = " TOP ".freeze
      BRACKET_CLOSE = Dataset::BRACKET_CLOSE
      BRACKET_OPEN = Dataset::BRACKET_OPEN

      # Access doesn't support INTERSECT or EXCEPT
      def supports_intersect_except?
        false
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
