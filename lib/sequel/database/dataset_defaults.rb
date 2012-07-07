module Sequel
  class Database
    # ---------------------
    # :section: 5 - Methods that set defaults for created datasets
    # This methods change the default behavior of this database's datasets.
    # ---------------------

    # The default class to use for datasets
    DatasetClass = Sequel::Dataset

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

    # The class to use for creating datasets.  Should respond to
    # new with the Database argument as the first argument, and
    # an optional options hash.
    attr_reader :dataset_class

    # The default schema to use, generally should be nil.  This sets
    # the default schema used for some schema modification and
    # introspection queries, but does not effect most dataset code.
    attr_accessor :default_schema

    # If the database has any dataset modules associated with it,
    # use a subclass of the given class that includes the modules
    # as the dataset class.
    def dataset_class=(c)
      unless @dataset_modules.empty?
        c = Class.new(c)
        @dataset_modules.each{|m| c.send(:include, m)}
      end
      @dataset_class = c
    end

    # Equivalent to extending all datasets produced by the database with a
    # module.  What it actually does is use a subclass of the current dataset_class
    # as the new dataset_class, and include the module in the subclass.
    # Instead of a module, you can provide a block that is used to create an
    # anonymous module.
    #
    # This allows you to override any of the dataset methods even if they are
    # defined directly on the dataset class that this Database object uses.
    #
    # Examples:
    #
    #   # Introspec columns for all of DB's datasets
    #   DB.extend_datasets(Sequel::ColumnsIntrospection)
    #
    #   # Trace all SELECT queries by printing the SQL and the full backtrace
    #   DB.extend_datasets do
    #     def fetch_rows(sql)
    #       puts sql
    #       puts caller
    #       super
    #     end
    #   end
    def extend_datasets(mod=nil, &block)
      raise(Error, "must provide either mod or block, not both") if mod && block
      reset_schema_utility_dataset
      mod = Module.new(&block) if block
      if @dataset_modules.empty?
       @dataset_modules = [mod]
       @dataset_class = Class.new(@dataset_class)
      else
       @dataset_modules << mod
      end
      @dataset_class.send(:include, mod)
    end

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

    # Set the method to call on identifiers going into the database:
    #
    #   DB[:items] # SELECT * FROM items
    #   DB.identifier_input_method = :upcase
    #   DB[:items] # SELECT * FROM ITEMS
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

    # Set the method to call on identifiers coming from the database:
    #
    #   DB[:items].first # {:id=>1, :name=>'foo'}
    #   DB.identifier_output_method = :upcase
    #   DB[:items].first # {:ID=>1, :NAME=>'foo'}
    def identifier_output_method=(v)
      reset_schema_utility_dataset
      @identifier_output_method = v || ""
    end

    # Set whether to quote identifiers (columns and tables) for this database:
    #
    #   DB[:items] # SELECT * FROM items
    #   DB.quote_identifiers = true
    #   DB[:items] # SELECT * FROM "items"
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

    # The default dataset class to use for the database
    def dataset_class_default
      self.class.const_get(:DatasetClass)
    end

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
