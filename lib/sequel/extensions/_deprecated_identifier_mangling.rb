# frozen-string-literal: true

#
module Sequel
  module DeprecatedIdentifierMangling
    module DatabaseMethods
      def self.extended(db)
        db.instance_exec do
          @identifier_input_method = nil
          @identifier_output_method = nil
          @quote_identifiers = nil
          reset_identifier_mangling
          extend_datasets(DatasetMethods)
        end
      end

      attr_reader :identifier_input_method
      attr_reader :identifier_output_method

      def identifier_input_method=(v)
        Sequel::Deprecation.deprecate("Database#identifier_input_method=", "Explicitly load the identifier_mangling extension if you would like to use this")
        reset_default_dataset
        @identifier_input_method = v
      end
      
      def identifier_output_method=(v)
        Sequel::Deprecation.deprecate("Database#identifier_output_method=", "Explicitly load the identifier_mangling extension if you would like to use this")
        reset_default_dataset
        @identifier_output_method = v
      end

      def quote_identifiers=(v)
        Sequel::Deprecation.deprecate("Database#quote_identifiers=", "Explicitly load the identifier_mangling extension if you would like to use this")
        reset_default_dataset
        @quote_identifiers = v
      end
      
      def quote_identifiers?
        @quote_identifiers
      end

      private

      def _metadata_dataset
        super.clone(:identifier_input_method=>identifier_input_method_default, :identifier_output_method=>identifier_output_method_default, :skip_symbol_cache=>true)
      end

      def identifier_input_method_default
        return super if defined?(super)
        :upcase if folds_unquoted_identifiers_to_uppercase?
      end

      def identifier_output_method_default
        return super if defined?(super)
        :downcase if folds_unquoted_identifiers_to_uppercase?
      end

      def reset_identifier_mangling
        @quote_identifiers = @opts.fetch(:quote_identifiers){(qi = Database.quote_identifiers).nil? ? quote_identifiers_default : qi}
        @identifier_input_method = @opts.fetch(:identifier_input_method){(iim = Database.identifier_input_method).nil? ? identifier_input_method_default : (iim if iim)}
        @identifier_output_method = @opts.fetch(:identifier_output_method){(iom = Database.identifier_output_method).nil? ? identifier_output_method_default : (iom if iom)}
        reset_default_dataset
      end
    end

    module DatasetMethods
      def identifier_input_method
        @opts.fetch(:identifier_input_method, db.identifier_input_method)
      end
      
      def identifier_input_method=(v)
        Sequel::Deprecation.deprecate("Dataset#identifier_input_method=", "Explicitly load the identifier_mangling extension if you would like to use this")
        raise_if_frozen!(%w"identifier_input_method= with_identifier_input_method")
        skip_symbol_cache!
        @opts[:identifier_input_method] = v
      end
      
      def identifier_output_method
        @opts.fetch(:identifier_output_method, db.identifier_output_method)
      end
    
      def identifier_output_method=(v)
        Sequel::Deprecation.deprecate("Dataset#identifier_output_method=", "Explicitly load the identifier_mangling extension if you would like to use this")
        raise_if_frozen!(%w"identifier_output_method= with_identifier_output_method")
        @opts[:identifier_output_method] = v
      end

      def quote_identifiers?
        @opts.fetch(:quote_identifiers, db.quote_identifiers?)
      end

      def with_identifier_input_method(meth)
        Sequel::Deprecation.deprecate("Dataset#with_identifier_input_method", "Explicitly load the identifier_mangling extension if you would like to use this")
        clone(:identifier_input_method=>meth, :skip_symbol_cache=>true)
      end

      def with_identifier_output_method(meth)
        Sequel::Deprecation.deprecate("Dataset#with_identifier_output_method", "Explicitly load the identifier_mangling extension if you would like to use this")
        clone(:identifier_output_method=>meth)
      end
      
      private

      def input_identifier(v)
        (i = identifier_input_method) ? v.to_s.send(i) : v.to_s
      end

      def output_identifier(v)
        v = 'untitled' if v == ''
        (i = identifier_output_method) ? v.to_s.send(i).to_sym : v.to_sym
      end
    end
  end

  Database.register_extension(:_deprecated_identifier_mangling, DeprecatedIdentifierMangling::DatabaseMethods)
end

