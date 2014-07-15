module Sequel
  module OpenEdge
    module DatabaseMethods
      extend Sequel::Database::ResetIdentifierMangling

      # OpenEdge uses the :openedge database type.
      def database_type
        :openedge
      end
    end
  
    module DatasetMethods
      Dataset.def_sql_method(self, :select, %w'select limit distinct columns from join where group order having with compounds')

      # OpenEdge requires SQL standard datetimes
      def requires_sql_standard_datetimes?
        true
      end

      # OpenEdge does not support INTERSECT or EXCEPT
      def supports_intersect_except?
        false
      end

      private

      # OpenEdge uses TOP for limit.
      def select_limit_sql(sql)
        raise(Error, "OFFSET not supported") if @opts[:offset]
        if l = @opts[:limit]
          sql << " TOP "
          literal_append(sql, l)
        end
      end
    end
  end
end
