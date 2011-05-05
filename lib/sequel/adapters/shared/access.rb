module Sequel
  module Access
    module DatabaseMethods
      # Access uses type :access as the database_type
      def database_type
        :access
      end

      def dataset(opts = nil)
        ds = super
        ds.extend(DatasetMethods)
        ds
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
      SELECT_CLAUSE_METHODS = Dataset.clause_methods(:select, %w'limit distinct columns from join where group order having compounds')

      # Access doesn't support INTERSECT or EXCEPT
      def supports_intersect_except?
        false
      end

      private

      # Access uses TOP for limits
      def select_limit_sql(sql)
        sql << " TOP #{@opts[:limit]}" if @opts[:limit]
      end

      # Access uses [] for quoting identifiers
      def quoted_identifier(v)
        "[#{v}]"
      end

      # Access requires the limit clause come before other clauses
      def select_clause_methods
        SELECT_CLAUSE_METHODS
      end
    end
  end
end
