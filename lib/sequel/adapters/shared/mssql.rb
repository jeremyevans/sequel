Sequel.require 'adapters/utils/unsupported'

module Sequel
  module MSSQL
    module DatabaseMethods
      AUTO_INCREMENT = 'IDENTITY(1,1)'.freeze
      SQL_BEGIN = "BEGIN TRANSACTION".freeze
      SQL_COMMIT = "COMMIT TRANSACTION".freeze
      SQL_ROLLBACK = "ROLLBACK TRANSACTION".freeze
      TEMPORARY = "#".freeze
      
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
      
      # Use mssql specific syntax for create table for temporary table
      def create_table_sql_list(name, columns, indexes = nil, options = {})
        sql = ["CREATE TABLE #{TEMPORARY if options[:temporary]}#{quote_schema_table(name)} (#{column_list_sql(columns)})"]
        sql.concat(index_list_sql_list(name, indexes)) if indexes && !indexes.empty?
        sql
      end
    end
  
    module DatasetMethods
      include Dataset::UnsupportedIntersectExcept

      SELECT_CLAUSE_ORDER = %w'limit distinct columns from with join where group order having compounds'.freeze

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
        ["INSERT INTO #{source_list(@opts[:from])} (#{identifier_list(columns)}) #{values}"]
      end

      # Allows you to do .nolock on a query
      def nolock
        clone(:with => "(NOLOCK)")
      end

      def quoted_identifier(name)
        "[#{name}]"
      end

      private

      def literal_string(v)
        "N#{super}"
      end

      def select_clause_order
        SELECT_CLAUSE_ORDER
      end

      # MSSQL uses TOP for limit, with no offset support
      def select_limit_sql(sql, opts)
        raise(Error, "OFFSET not supported") if opts[:offset]
        sql << " TOP #{opts[:limit]}" if opts[:limit]
      end

      # MSSQL uses the WITH statement to lock tables
      def select_with_sql(sql, opts)
        sql << " WITH #{opts[:with]}" if opts[:with]
      end
    end
  end
end
