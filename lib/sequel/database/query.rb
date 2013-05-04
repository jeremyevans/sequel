module Sequel
  class Database
    # ---------------------
    # :section: 1 - Methods that execute queries and/or return results
    # This methods generally execute SQL code on the database server.
    # ---------------------

    STRING_DEFAULT_RE = /\A'(.*)'\z/
    CURRENT_TIMESTAMP_RE = /now|CURRENT|getdate|\ADate\(\)\z/io
    COLUMN_SCHEMA_DATETIME_TYPES = [:date, :datetime]
    COLUMN_SCHEMA_STRING_TYPES = [:string, :blob, :date, :datetime, :time, :enum, :set, :interval]

    # The prepared statement object hash for this database, keyed by name symbol
    attr_reader :prepared_statements
    
    # Whether the schema should be cached for this database.  True by default
    # for performance, can be set to false to always issue a database query to
    # get the schema.
    attr_accessor :cache_schema
    
    # Runs the supplied SQL statement string on the database server.
    # Returns self so it can be safely chained:
    #
    #   DB << "UPDATE albums SET artist_id = NULL" << "DROP TABLE artists"
    def <<(sql)
      run(sql)
      self
    end
    
    # Call the prepared statement with the given name with the given hash
    # of arguments.
    #
    #   DB[:items].filter(:id=>1).prepare(:first, :sa)
    #   DB.call(:sa) # SELECT * FROM items WHERE id = 1
    def call(ps_name, hash={}, &block)
      prepared_statement(ps_name).call(hash, &block)
    end
    
    # Executes the given SQL on the database. This method should be overridden in descendants.
    # This method should not be called directly by user code.
    def execute(sql, opts={})
      Sequel::Deprecation.deprecate('Database#execute default implementation and Sequel::NotImplemented', 'All database instances can be assumed to implement execute')
      raise NotImplemented, "#execute should be overridden by adapters"
    end
    
    # Method that should be used when submitting any DDL (Data Definition
    # Language) SQL, such as +create_table+.  By default, calls +execute_dui+.
    # This method should not be called directly by user code.
    def execute_ddl(sql, opts={}, &block)
      execute_dui(sql, opts, &block)
    end

    # Method that should be used when issuing a DELETE, UPDATE, or INSERT
    # statement.  By default, calls execute.
    # This method should not be called directly by user code.
    def execute_dui(sql, opts={}, &block)
      execute(sql, opts, &block)
    end

    # Method that should be used when issuing a INSERT
    # statement.  By default, calls execute_dui.
    # This method should not be called directly by user code.
    def execute_insert(sql, opts={}, &block)
      execute_dui(sql, opts, &block)
    end

    # Returns an array of hashes containing foreign key information from the
    # table.  Each hash will contain at least the following fields:
    #
    # :columns :: An array of columns in the given table
    # :table :: The table referenced by the columns
    # :key :: An array of columns referenced (in the table specified by :table),
    #         but can be nil on certain adapters if the primary key is referenced.
    #
    # The hash may also contain entries for:
    #
    # :deferrable :: Whether the constraint is deferrable
    # :name :: The name of the constraint
    # :on_delete :: The action to take ON DELETE
    # :on_update :: The action to take ON UPDATE
    def foreign_key_list(table, opts={})
      Sequel::Deprecation.deprecate('Database#foreign_key_list default implementation and Sequel::NotImplemented', 'Use Database#supports_foreign_key_parsing? to check for support')
      raise NotImplemented, "#foreign_key_list should be overridden by adapters"
    end
    
    # Returns a single value from the database, e.g.:
    #
    #   DB.get(1) # SELECT 1
    #   # => 1
    #   DB.get{server_version{}} # SELECT server_version()
    def get(*args, &block)
      @default_dataset.get(*args, &block)
    end
    
    # Return a hash containing index information for the table. Hash keys are index name symbols.
    # Values are subhashes with two keys, :columns and :unique.  The value of :columns
    # is an array of symbols of column names.  The value of :unique is true or false
    # depending on if the index is unique.
    #
    # Should not include the primary key index, functional indexes, or partial indexes.
    #
    #   DB.indexes(:artists)
    #   # => {:artists_name_ukey=>{:columns=>[:name], :unique=>true}}
    def indexes(table, opts={})
      Sequel::Deprecation.deprecate('Database#indexes default implementation and Sequel::NotImplemented', 'Use Database#supports_index_parsing? to check for support')
      raise NotImplemented, "#indexes should be overridden by adapters"
    end
    
    # Runs the supplied SQL statement string on the database server. Returns nil.
    # Options:
    # :server :: The server to run the SQL on.
    #
    #   DB.run("SET some_server_variable = 42")
    def run(sql, opts={})
      execute_ddl(sql, opts)
      nil
    end
    
    # Returns the schema for the given table as an array with all members being arrays of length 2,
    # the first member being the column name, and the second member being a hash of column information.
    # The table argument can also be a dataset, as long as it only has one table.
    # Available options are:
    #
    # :reload :: Ignore any cached results, and get fresh information from the database.
    # :schema :: An explicit schema to use.  It may also be implicitly provided
    #            via the table name.
    #
    # If schema parsing is supported by the database, the column information should hash at least contain the
    # following entries:
    #
    # :allow_null :: Whether NULL is an allowed value for the column.
    # :db_type :: The database type for the column, as a database specific string.
    # :default :: The database default for the column, as a database specific string.
    # :primary_key :: Whether the columns is a primary key column.  If this column is not present,
    #                 it means that primary key information is unavailable, not that the column
    #                 is not a primary key.
    # :ruby_default :: The database default for the column, as a ruby object.  In many cases, complex
    #                  database defaults cannot be parsed into ruby objects, in which case nil will be
    #                  used as the value.
    # :type :: A symbol specifying the type, such as :integer or :string.
    #
    # Example:
    #
    #   DB.schema(:artists)
    #   # [[:id,
    #   #   {:type=>:integer,
    #   #    :primary_key=>true,
    #   #    :default=>"nextval('artist_id_seq'::regclass)",
    #   #    :ruby_default=>nil,
    #   #    :db_type=>"integer",
    #   #    :allow_null=>false}],
    #   #  [:name,
    #   #   {:type=>:string,
    #   #    :primary_key=>false,
    #   #    :default=>nil,
    #   #    :ruby_default=>nil,
    #   #    :db_type=>"text",
    #   #    :allow_null=>false}]]
    def schema(table, opts={})
      raise(Error, 'schema parsing is not implemented on this database') unless supports_schema_parsing?

      opts = opts.dup
      tab = if table.is_a?(Dataset)
        o = table.opts
        from = o[:from]
        raise(Error, "can only parse the schema for a dataset with a single from table") unless from && from.length == 1 && !o.include?(:join) && !o.include?(:sql)
        table.first_source_table
      else
        table
      end

      qualifiers = split_qualifiers(tab)
      table_name = qualifiers.pop
      sch = qualifiers.pop
      information_schema_schema = case qualifiers.length
      when 1
        Sequel.identifier(*qualifiers)
      when 2
        Sequel.qualify(*qualifiers)
      end

      if table.is_a?(Dataset)
        quoted_name = table.literal(tab)
        opts[:dataset] = table
      else
        quoted_name = schema_utility_dataset.literal(table)
      end

      opts[:schema] = sch if sch && !opts.include?(:schema)
      opts[:information_schema_schema] = information_schema_schema if information_schema_schema && !opts.include?(:information_schema_schema)

      Sequel.synchronize{@schemas.delete(quoted_name)} if opts[:reload]
      if v = Sequel.synchronize{@schemas[quoted_name]}
        return v
      end

      cols = schema_parse_table(table_name, opts)
      raise(Error, 'schema parsing returned no columns, table probably doesn\'t exist') if cols.nil? || cols.empty?
      cols.each{|_,c| c[:ruby_default] = column_schema_to_ruby_default(c[:default], c[:type])}
      Sequel.synchronize{@schemas[quoted_name] = cols} if cache_schema
      cols
    end

    # Returns true if a table with the given name exists.  This requires a query
    # to the database.
    #
    #   DB.table_exists?(:foo) # => false
    #   # SELECT NULL FROM foo LIMIT 1
    #
    # Note that since this does a SELECT from the table, it can give false negatives
    # if you don't have permission to SELECT from the table.
    def table_exists?(name)
      sch, table_name = schema_and_table(name)
      name = SQL::QualifiedIdentifier.new(sch, table_name) if sch
      _table_exists?(from(name))
      true
    rescue DatabaseError
      false
    end

    # Return all tables in the database as an array of symbols.
    #
    #   DB.tables # => [:albums, :artists]
    def tables(opts={})
      Sequel::Deprecation.deprecate('Database#tables default implementation and Sequel::NotImplemented', 'Use Database#supports_table_listing? to check for support')
      raise NotImplemented, "#tables should be overridden by adapters"
    end
    
    # Return all views in the database as an array of symbols.
    #
    #   DB.views # => [:gold_albums, :artists_with_many_albums]
    def views(opts={})
      Sequel::Deprecation.deprecate('Database#views default implementation and Sequel::NotImplemented', 'Use Database#supports_view_listing? to check for support')
      raise NotImplemented, "#views should be overridden by adapters"
    end
    
    private
    
    # Should raise an error if the table doesn't not exist,
    # and not raise an error if the table does exist.
    def _table_exists?(ds)
      ds.get(SQL::AliasedExpression.new(Sequel::NULL, :nil))
    end
    
    # Whether the type should be treated as a string type when parsing the
    # column schema default value.
    def column_schema_default_string_type?(type)
      COLUMN_SCHEMA_STRING_TYPES.include?(type)
    end

    # Transform the given normalized default string into a ruby object for the
    # given type.
    def column_schema_default_to_ruby_value(default, type)
      case type
      when :boolean
        case default 
        when /[f0]/i
          false
        when /[t1]/i
          true
        end
      when :string, :enum, :set, :interval
        default
      when :blob
        Sequel::SQL::Blob.new(default)
      when :integer
        Integer(default)
      when :float
        Float(default)
      when :date
        Sequel.string_to_date(default)
      when :datetime
        DateTime.parse(default)
      when :time
        Sequel.string_to_time(default)
      when :decimal
        BigDecimal.new(default)
      end
    end
   
    # Normalize the default value string for the given type
    # and return the normalized value.
    def column_schema_normalize_default(default, type)
      if column_schema_default_string_type?(type)
        return unless m = STRING_DEFAULT_RE.match(default)
        m[1].gsub("''", "'")
      else
        default
      end
    end

    # Convert the given default, which should be a database specific string, into
    # a ruby object.
    def column_schema_to_ruby_default(default, type)
      return default unless default.is_a?(String)
      if COLUMN_SCHEMA_DATETIME_TYPES.include?(type)
        if CURRENT_TIMESTAMP_RE.match(default)
          if type == :date
            return Sequel::CURRENT_DATE
          else
            return Sequel::CURRENT_TIMESTAMP
          end
        end
      end
      default = column_schema_normalize_default(default, type)
      column_schema_default_to_ruby_value(default, type) rescue nil
    end

    # Return a Method object for the dataset's output_identifier_method.
    # Used in metadata parsing to make sure the returned information is in the
    # correct format.
    def input_identifier_meth(ds=nil)
      (ds || dataset).method(:input_identifier)
    end
    
    # Return a dataset that uses the default identifier input and output methods
    # for this database.  Used when parsing metadata so that column symbols are
    # returned as expected.
    def metadata_dataset
      @metadata_dataset ||= (
        ds = dataset;
        ds.identifier_input_method = identifier_input_method_default;
        ds.identifier_output_method = identifier_output_method_default;
        ds
      )
    end

    # Return a Method object for the dataset's output_identifier_method.
    # Used in metadata parsing to make sure the returned information is in the
    # correct format.
    def output_identifier_meth(ds=nil)
      (ds || dataset).method(:output_identifier)
    end

    # Remove the cached schema for the given schema name
    def remove_cached_schema(table)
      Sequel.synchronize{@schemas.delete(quote_schema_table(table))} if @schemas
    end
    
    # Match the database's column type to a ruby type via a
    # regular expression, and return the ruby type as a symbol
    # such as :integer or :string.
    def schema_column_type(db_type)
      case db_type
      when /\A(character( varying)?|n?(var)?char|n?text|string|clob)/io
        :string
      when /\A(int(eger)?|(big|small|tiny)int)/io
        :integer
      when /\Adate\z/io
        :date
      when /\A((small)?datetime|timestamp( with(out)? time zone)?)(\(\d+\))?\z/io
        :datetime
      when /\Atime( with(out)? time zone)?\z/io
        :time
      when /\A(bool(ean)?)\z/io
        :boolean
      when /\A(real|float|double( precision)?|double\(\d+,\d+\)( unsigned)?)\z/io
        :float
      when /\A(?:(?:(?:num(?:ber|eric)?|decimal)(?:\(\d+,\s*(\d+|false|true)\))?))\z/io
        $1 && ['0', 'false'].include?($1) ? :integer : :decimal
      when /bytea|blob|image|(var)?binary/io
        :blob
      when /\Aenum/io
        :enum
      end
    end
  end
end
