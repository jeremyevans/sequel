Sequel.require 'adapters/utils/split_alter_table'

module Sequel
  module Cubrid
    module DatabaseMethods
      include Sequel::Database::SplitAlterTable

      AUTOINCREMENT = 'AUTO_INCREMENT'.freeze
      COLUMN_DEFINITION_ORDER = [:auto_increment, :default, :null, :unique, :primary_key, :references]

      def database_type
        :cubrid
      end

      def supports_savepoints?
        false
      end
      
      private

      def alter_table_op_sql(table, op)
        case op[:op]
        when :rename_column
          "RENAME COLUMN #{quote_identifier(op[:name])} AS #{quote_identifier(op[:new_name])}"
        when :set_column_type, :set_column_null, :set_column_default
          o = op[:op]
          opts = schema(table).find{|x| x.first == op[:name]}
          opts = opts ? opts.last.dup : {}
          opts[:name] = o == :rename_column ? op[:new_name] : op[:name]
          opts[:type] = o == :set_column_type ? op[:type] : opts[:db_type]
          opts[:null] = o == :set_column_null ? op[:null] : opts[:allow_null]
          opts[:default] = o == :set_column_default ? op[:default] : opts[:ruby_default]
          opts.delete(:default) if opts[:default] == nil
          "CHANGE COLUMN #{quote_identifier(op[:name])} #{column_definition_sql(op.merge(opts))}"
        else
          super
        end
      end

      def alter_table_sql(table, op)
        case op[:op]
        when :drop_index
          "ALTER TABLE #{quote_schema_table(table)} #{drop_index_sql(table, op)}"
        else
          super
        end
      end

      def auto_increment_sql
        AUTOINCREMENT
      end

      # CUBRID requires auto increment before primary key
      def column_definition_order
        COLUMN_DEFINITION_ORDER
      end

      # CUBRID requires FOREIGN KEY keywords before a column reference
      def column_references_sql(column)
        sql = super
        sql = " FOREIGN KEY#{sql}" unless column[:columns]
        sql
      end

      def connection_execute_method
        :query
      end

      # CUBRID is case insensitive, so don't modify identifiers
      def identifier_input_method_default
        nil
      end

      # CUBRID is case insensitive, so don't modify identifiers
      def identifier_output_method_default
        nil
      end

      # CUBRID doesn't support booleans, it recommends using smallint.
      def type_literal_generic_trueclass(column)
        :smallint
      end

      # CUBRID uses clob for text types.
      def uses_clob_for_text?
        true
      end
    end
    
    module DatasetMethods
      SELECT_CLAUSE_METHODS = Sequel::Dataset.clause_methods(:select, %w'select distinct columns from join where group having compounds order limit')
      LIMIT = Sequel::Dataset::LIMIT
      COMMA = Sequel::Dataset::COMMA
      BOOL_FALSE = '0'.freeze
      BOOL_TRUE = '1'.freeze

      def complex_expression_sql_append(sql, op, args)
        case op
        when :ILIKE
          super(sql, :LIKE, [SQL::Function.new(:upper, args.at(0)), SQL::Function.new(:upper, args.at(1))])
        when :"NOT ILIKE"
          super(sql, :"NOT LIKE", [SQL::Function.new(:upper, args.at(0)), SQL::Function.new(:upper, args.at(1))])
        else
          super
        end
      end

      def supports_join_using?
        false
      end

      def supports_multiple_column_in?
        false
      end

      def supports_timestamp_usecs?
        false
      end

      # CUBRID supposedly supports TRUNCATE, but it appears not to work in my testing.
      # Fallback to using DELETE.
      def truncate
        delete
        nil
      end

      private

      def literal_false
        BOOL_FALSE
      end

      def literal_true
        BOOL_TRUE
      end
     
      # CUBRID doesn't support CTEs or FOR UPDATE.
      def select_clause_methods
        SELECT_CLAUSE_METHODS
      end

      # CUBRID requires a limit to use an offset,
      # and requires a FROM table if a limit is used.
      def select_limit_sql(sql)
        if @opts[:from] && (l = @opts[:limit])
          sql << LIMIT
          if o = @opts[:offset]
            literal_append(sql, o)
            sql << COMMA
          end
          literal_append(sql, l)
        end
      end
    end
  end
end
