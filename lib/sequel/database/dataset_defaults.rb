module Sequel
  class Database
    # ---------------------
    # :section: Methods that set defaults for created datasets
    # This methods change the default behavior of this database's datasets.
    # ---------------------

    # The identifier input method to use by default
    @@identifier_input_method = nil

    # The identifier output method to use by default
    @@identifier_output_method = nil

    # Whether to quote identifiers (columns and tables) by default
    @@quote_identifiers = nil

    # The method to call on identifiers going into the database
    def self.identifier_input_method
      @@identifier_input_method
    end
    
    # Set the method to call on identifiers going into the database
    # See Sequel.identifier_input_method=.
    def self.identifier_input_method=(v)
      @@identifier_input_method = v || ""
    end
    
    # The method to call on identifiers coming from the database
    def self.identifier_output_method
      @@identifier_output_method
    end
    
    # Set the method to call on identifiers coming from the database
    # See Sequel.identifier_output_method=.
    def self.identifier_output_method=(v)
      @@identifier_output_method = v || ""
    end

    # Sets the default quote_identifiers mode for new databases.
    # See Sequel.quote_identifiers=.
    def self.quote_identifiers=(value)
      @@quote_identifiers = value
    end

    # The default schema to use, generally should be nil.
    attr_accessor :default_schema

    # The method to call on identifiers going into the database
    def identifier_input_method
      case @identifier_input_method
      when nil
        @identifier_input_method = @opts.fetch(:identifier_input_method, (@@identifier_input_method.nil? ? identifier_input_method_default : @@identifier_input_method))
        @identifier_input_method == "" ? nil : @identifier_input_method
      when ""
        nil
      else
        @identifier_input_method
      end
    end
    
    # Set the method to call on identifiers going into the database
    def identifier_input_method=(v)
      reset_schema_utility_dataset
      @identifier_input_method = v || ""
    end
    
    # The method to call on identifiers coming from the database
    def identifier_output_method
      case @identifier_output_method
      when nil
        @identifier_output_method = @opts.fetch(:identifier_output_method, (@@identifier_output_method.nil? ? identifier_output_method_default : @@identifier_output_method))
        @identifier_output_method == "" ? nil : @identifier_output_method
      when ""
        nil
      else
        @identifier_output_method
      end
    end
    
    # Set the method to call on identifiers coming from the database
    def identifier_output_method=(v)
      reset_schema_utility_dataset
      @identifier_output_method = v || ""
    end

    # Whether to quote identifiers (columns and tables) for this database
    def quote_identifiers=(v)
      reset_schema_utility_dataset
      @quote_identifiers = v
    end
    
    # Returns true if the database quotes identifiers.
    def quote_identifiers?
      return @quote_identifiers unless @quote_identifiers.nil?
      @quote_identifiers = @opts.fetch(:quote_identifiers, (@@quote_identifiers.nil? ? quote_identifiers_default : @@quote_identifiers))
    end
    
    private
    
    # The default value for default_schema.
    def default_schema_default
      nil
    end

    # The method to apply to identifiers going into the database by default.
    # Should be overridden in subclasses for databases that fold unquoted
    # identifiers to lower case instead of uppercase, such as
    # MySQL, PostgreSQL, and SQLite.
    def identifier_input_method_default
      :upcase
    end
    
    # The method to apply to identifiers coming the database by default.
    # Should be overridden in subclasses for databases that fold unquoted
    # identifiers to lower case instead of uppercase, such as
    # MySQL, PostgreSQL, and SQLite.
    def identifier_output_method_default
      :downcase
    end
    
    # Whether to quote identifiers by default for this database, true
    # by default.
    def quote_identifiers_default
      true
    end
  end
end
