module Sequel
  module Access
    module DatabaseMethods
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

      def supports_intersect_except?
        false
      end

      private

      def quoted_identifier(v)
        "[#{v}]"
      end

      def select_clause_methods
        SELECT_CLAUSE_METHODS
      end
    end
  end
end
