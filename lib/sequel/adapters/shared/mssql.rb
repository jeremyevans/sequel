module Sequel
  module MSSQL
    module DatabaseMethods
      AUTO_INCREMENT = 'IDENTITY(1,1)'.freeze
      SQL_BEGIN = "BEGIN TRANSACTION".freeze
      SQL_COMMIT = "COMMIT TRANSACTION".freeze
      SQL_ROLLBACK = "ROLLBACK TRANSACTION".freeze
      TEMPORARY = "#".freeze
      
      # Microsoft SQL Server uses the :mssql type.
      def database_type
        :mssql
      end

      def dataset(opts = nil)
        ds = super
        ds.extend(DatasetMethods)
        ds
      end

      private

      def auto_increment_sql
        AUTO_INCREMENT
      end

      # SQL to BEGIN a transaction.
      def begin_transaction_sql
        SQL_BEGIN
      end

      # SQL to COMMIT a transaction.
      def commit_transaction_sql
        SQL_COMMIT
      end
      
      # SQL to ROLLBACK a transaction.
      def rollback_transaction_sql
        SQL_ROLLBACK
      end

      # SQL fragment for marking a table as temporary
      def temporary_table_sql
        TEMPORARY
      end
    end
  
    module DatasetMethods
      SELECT_CLAUSE_ORDER = %w'with limit distinct columns from table_options join where group order having compounds'.freeze

      def complex_expression_sql(op, args)
        case op
        when :'||'
          super(:+, args)
        else
          super(op, args)
        end
      end
      
      def full_text_search(cols, terms, opts = {})
        filter("CONTAINS (#{literal(cols)}, #{literal(terms)})")
      end
      
      def multi_insert_sql(columns, values)
        values = values.map {|r| "SELECT #{expression_list(r)}" }.join(" UNION ALL ")
        ["#{insert_sql_base}#{source_list(@opts[:from])} (#{identifier_list(columns)}) #{values}"]
      end

      # Allows you to do .nolock on a query
      def nolock
        clone(:table_options => "(NOLOCK)")
      end

      def quoted_identifier(name)
        "[#{name}]"
      end

      # Microsoft SQL Server does not support INTERSECT or EXCEPT
      def supports_intersect_except?
        false
      end
      
      # MSSQL 2005+ supports window functions
      def supports_window_functions?
        true
      end

      private

      def literal_string(v)
        "N#{super}"
      end

      def select_clause_order
        SELECT_CLAUSE_ORDER
      end

      # MSSQL uses TOP for limit, with no offset support
      def select_limit_sql(sql)
        raise(Error, "OFFSET not supported") if @opts[:offset]
        sql << " TOP #{@opts[:limit]}" if @opts[:limit]
      end

      # MSSQL uses the WITH statement to lock tables
      def select_table_options_sql(sql)
        sql << " WITH #{@opts[:table_options]}" if @opts[:table_options]
      end
    end
  end
end
