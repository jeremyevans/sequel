# frozen-string-literal: true

Sequel::Deprecation.deprecate("Sequel support for Informix", "Please consider maintaining it yourself as an external sequel adapter if you want to continue using it")

module Sequel
  module Informix
    Sequel::Database.set_shared_adapter_scheme(:informix, self)

    module DatabaseMethods
      TEMPORARY = 'TEMP '.freeze
      Sequel::Deprecation.deprecate_constant(self, :TEMPORARY)

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
        'TEMP '
      end
    end
    
    module DatasetMethods
      FIRST = " FIRST ".freeze
      Sequel::Deprecation.deprecate_constant(self, :FIRST)
      SKIP = " SKIP ".freeze
      Sequel::Deprecation.deprecate_constant(self, :SKIP)

      Dataset.def_sql_method(self, :select, %w'select limit distinct columns from join where having group compounds order')

      def quote_identifiers?
        @opts.fetch(:quote_identifiers, false)
      end

      # Informix does not support INTERSECT or EXCEPT
      def supports_intersect_except?
        false
      end

      private

      def select_limit_sql(sql)
        if o = @opts[:offset]
          sql << " SKIP "
          literal_append(sql, o)
        end
        if l = @opts[:limit]
          sql << " FIRST "
          literal_append(sql, l)
        end
      end
    end
  end
end
