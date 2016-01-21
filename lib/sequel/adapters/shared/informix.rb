# frozen-string-literal: true

module Sequel
  module Informix
    module DatabaseMethods
      extend Sequel::Database::ResetIdentifierMangling

      TEMPORARY = 'TEMP '.freeze

      # Informix uses the :informix database type
      def database_type
        :informix
      end

      private

      # Informix has issues with quoted identifiers, so
      # turn off database quoting by default.
      def quote_identifiers_default
        false
      end

      # SQL fragment for showing a table is temporary
      def temporary_table_sql
        TEMPORARY
      end
    end
    
    module DatasetMethods
      FIRST = " FIRST ".freeze
      SKIP = " SKIP ".freeze

      Dataset.def_sql_method(self, :select, %w'select limit distinct columns from join where having group compounds order')

      # Informix does not support INTERSECT or EXCEPT
      def supports_intersect_except?
        false
      end

      private

      def select_limit_sql(sql)
        if o = @opts[:offset]
          sql << SKIP
          literal_append(sql, o)
        end
        if l = @opts[:limit]
          sql << FIRST
          literal_append(sql, l)
        end
      end
    end
  end
end
