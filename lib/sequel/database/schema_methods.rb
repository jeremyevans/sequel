module Sequel
  class Database
    # ---------------------
    # :section: 2 - Methods that modify the database schema
    # These methods execute code on the database that modifies the database's schema.
    # ---------------------

    AUTOINCREMENT = 'AUTOINCREMENT'.freeze
    COMMA_SEPARATOR = ', '.freeze
    NOT_NULL = ' NOT NULL'.freeze
    NULL = ' NULL'.freeze
    PRIMARY_KEY = ' PRIMARY KEY'.freeze
    TEMPORARY = 'TEMPORARY '.freeze
    UNDERSCORE = '_'.freeze
    UNIQUE = ' UNIQUE'.freeze
    UNSIGNED = ' UNSIGNED'.freeze

    # The order of column modifiers to use when defining a column.
    COLUMN_DEFINITION_ORDER = [:collate, :default, :null, :unique, :primary_key, :auto_increment, :references]

    # The default options for join table columns.
    DEFAULT_JOIN_TABLE_COLUMN_OPTIONS = {:null=>false}

    # The alter table operations that are combinable.
    COMBINABLE_ALTER_TABLE_OPS = [:add_column, :drop_column, :rename_column,
      :set_column_type, :set_column_default, :set_column_null,
      :add_constraint, :drop_constraint]

    # Adds a column to the specified table. This method expects a column name,
    # a datatype and optionally a hash with additional constraints and options:
    #
    #   DB.add_column :items, :name, :text, :unique => true, :null => false
    #   DB.add_column :items, :category, :text, :default => 'ruby'
    #
    # See <tt>alter_table</tt>.
    def add_column(table, *args)
      alter_table(table) {add_column(*args)}
    end
    
    # Adds an index to a table for the given columns:
    # 
    #   DB.add_index :posts, :title
    #   DB.add_index :posts, [:author, :title], :unique => true
    #
    # Options:
    # :ignore_errors :: Ignore any DatabaseErrors that are raised
    #
    # See <tt>alter_table</tt>.
    def add_index(table, columns, options={})
      e = options[:ignore_errors]
      begin
        alter_table(table){add_index(columns, options)}
      rescue DatabaseError
        raise unless e
      end
    end
    
    # Alters the given table with the specified block. Example:
    #
    #   DB.alter_table :items do
    #     add_column :category, :text, :default => 'ruby'
    #     drop_column :category
    #     rename_column :cntr, :counter
    #     set_column_type :value, :float
    #     set_column_default :value, :float
    #     add_index [:group, :category]
    #     drop_index [:group, :category]
    #   end
    #
    # Note that +add_column+ accepts all the options available for column
    # definitions using <tt>create_table</tt>, and +add_index+ accepts all the options
    # available for index definition.
    #
    # See <tt>Schema::AlterTableGenerator</tt> and the {"Migrations and Schema Modification" guide}[link:files/doc/migration_rdoc.html].
    def alter_table(name, generator=nil, &block)
      generator ||= alter_table_generator(&block)
      remove_cached_schema(name)
      apply_alter_table_generator(name, generator)
      nil
    end

    # Return a new Schema::AlterTableGenerator instance with the receiver as
    # the database and the given block.
    def alter_table_generator(&block)
      alter_table_generator_class.new(self, &block)
    end

    # Create a join table using a hash of foreign keys to referenced
    # table names.  Example:
    #
    #   create_join_table(:cat_id=>:cats, :dog_id=>:dogs)
    #   # CREATE TABLE cats_dogs (
    #   #  cat_id integer NOT NULL REFERENCES cats,
    #   #  dog_id integer NOT NULL REFERENCES dogs,
    #   #  PRIMARY KEY (cat_id, dog_id)
    #   # )
    #   # CREATE INDEX cats_dogs_dog_id_cat_id_index ON cats_dogs(dog_id, cat_id)
    #
    # The primary key and index are used so that almost all operations
    # on the table can benefit from one of the two indexes, and the primary
    # key ensures that entries in the table are unique, which is the typical
    # desire for a join table.
    #
    # You can provide column options by making the values in the hash
    # be option hashes, so long as the option hashes have a :table
    # entry giving the table referenced:
    #
    #   create_join_table(:cat_id=>{:table=>:cats, :type=>Bignum}, :dog_id=>:dogs)
    #   
    # You can provide a second argument which is a table options hash:
    #
    #   create_join_table({:cat_id=>:cats, :dog_id=>:dogs}, :temp=>true)
    #
    # Some table options are handled specially:
    #
    # :index_options :: The options to pass to the index
    # :name :: The name of the table to create
    # :no_index :: Set to true not to create the second index.
    # :no_primary_key :: Set to true to not create the primary key.
    def create_join_table(hash, options={})
      keys = hash.keys.sort_by{|k| k.to_s}
      create_table(join_table_name(hash, options), options) do
        keys.each do |key|
          v = hash[key]
          unless v.is_a?(Hash)
            v = {:table=>v}
          end
          v = DEFAULT_JOIN_TABLE_COLUMN_OPTIONS.merge(v)
          foreign_key(key, v)
        end
        primary_key(keys) unless options[:no_primary_key]
        index(keys.reverse, options[:index_options] || {}) unless options[:no_index]
      end
    end

    # Creates a table with the columns given in the provided block:
    #
    #   DB.create_table :posts do
    #     primary_key :id
    #     column :title, :text
    #     String :content
    #     index :title
    #   end
    #
    # General options:
    # :as :: Create the table using the value, which should be either a
    #        dataset or a literal SQL string.  If this option is used,
    #        a block should not be given to the method.
    # :ignore_index_errors :: Ignore any errors when creating indexes.
    # :temp :: Create the table as a temporary table.
    #
    # MySQL specific options:
    # :charset :: The character set to use for the table.
    # :collate :: The collation to use for the table.
    # :engine :: The table engine to use for the table.
    #
    # PostgreSQL specific options:
    # :unlogged :: Create the table as an unlogged table.
    #
    # See <tt>Schema::Generator</tt> and the {"Schema Modification" guide}[link:files/doc/schema_modification_rdoc.html].
    def create_table(name, options={}, &block)
      remove_cached_schema(name)
      options = {:generator=>options} if options.is_a?(Schema::CreateTableGenerator)
      if sql = options[:as]
        raise(Error, "can't provide both :as option and block to create_table") if block
        create_table_as(name, sql, options)
      else
        generator = options[:generator] || create_table_generator(&block)
        create_table_from_generator(name, generator, options)
        create_table_indexes_from_generator(name, generator, options)
        nil
      end
    end

    # Forcibly create a table, attempting to drop it if it already exists, then creating it.
    # 
    #   DB.create_table!(:a){Integer :a} 
    #   # SELECT NULL FROM a LIMIT 1 -- check existence
    #   # DROP TABLE a -- drop table if already exists
    #   # CREATE TABLE a (a integer)
    def create_table!(name, options={}, &block)
      drop_table?(name)
      create_table(name, options, &block)
    end
    
    # Creates the table unless the table already exists.
    # 
    #   DB.create_table?(:a){Integer :a} 
    #   # SELECT NULL FROM a LIMIT 1 -- check existence
    #   # CREATE TABLE a (a integer) -- if it doesn't already exist
    def create_table?(name, options={}, &block)
      if supports_create_table_if_not_exists?
        create_table(name, options.merge(:if_not_exists=>true), &block)
      elsif !table_exists?(name)
        create_table(name, options, &block)
      end
    end

    # Return a new Schema::CreateTableGenerator instance with the receiver as
    # the database and the given block.
    def create_table_generator(&block)
      create_table_generator_class.new(self, &block)
    end
    
    # Creates a view, replacing it if it already exists:
    #
    #   DB.create_or_replace_view(:cheap_items, "SELECT * FROM items WHERE price < 100")
    #   DB.create_or_replace_view(:ruby_items, DB[:items].filter(:category => 'ruby'))
    def create_or_replace_view(name, source)
      source = source.sql if source.is_a?(Dataset)
      execute_ddl("CREATE OR REPLACE VIEW #{quote_schema_table(name)} AS #{source}")
      remove_cached_schema(name)
      nil
    end
    
    # Creates a view based on a dataset or an SQL string:
    #
    #   DB.create_view(:cheap_items, "SELECT * FROM items WHERE price < 100")
    #   DB.create_view(:ruby_items, DB[:items].filter(:category => 'ruby'))
    def create_view(name, source)
      source = source.sql if source.is_a?(Dataset)
      execute_ddl("CREATE VIEW #{quote_schema_table(name)} AS #{source}")
    end
    
    # Removes a column from the specified table:
    #
    #   DB.drop_column :items, :category
    #
    # See <tt>alter_table</tt>.
    def drop_column(table, *args)
      alter_table(table) {drop_column(*args)}
    end
    
    # Removes an index for the given table and column/s:
    #
    #   DB.drop_index :posts, :title
    #   DB.drop_index :posts, [:author, :title]
    #
    # See <tt>alter_table</tt>.
    def drop_index(table, columns, options={})
      alter_table(table){drop_index(columns, options)}
    end

    # Drop the join table that would have been created with the
    # same arguments to create_join_table:
    #
    #   drop_join_table(:cat_id=>:cats, :dog_id=>:dogs)
    #   # DROP TABLE cats_dogs
    def drop_join_table(hash, options={})
      drop_table(join_table_name(hash, options), options)
    end
    
    # Drops one or more tables corresponding to the given names:
    #
    #   DB.drop_table(:posts) # DROP TABLE posts
    #   DB.drop_table(:posts, :comments)
    #   DB.drop_table(:posts, :comments, :cascade=>true)
    def drop_table(*names)
      options = names.last.is_a?(Hash) ? names.pop : {}
      names.each do |n|
        execute_ddl(drop_table_sql(n, options))
        remove_cached_schema(n)
      end
      nil
    end
    
    # Drops the table if it already exists.  If it doesn't exist,
    # does nothing.
    # 
    #   DB.drop_table?(:a)
    #   # SELECT NULL FROM a LIMIT 1 -- check existence
    #   # DROP TABLE a -- if it already exists
    def drop_table?(*names)
      options = names.last.is_a?(Hash) ? names.pop : {}
      if supports_drop_table_if_exists?
        options = options.merge(:if_exists=>true)
        names.each do |name|
          drop_table(name, options)
        end
      else
        names.each do |name|
          drop_table(name, options) if table_exists?(name)
        end
      end
    end
    
    # Drops one or more views corresponding to the given names:
    #
    #   DB.drop_view(:cheap_items)
    #   DB.drop_view(:cheap_items, :pricey_items)
    #   DB.drop_view(:cheap_items, :pricey_items, :cascade=>true)
    def drop_view(*names)
      options = names.last.is_a?(Hash) ? names.pop : {}
      names.each do |n|
        execute_ddl(drop_view_sql(n, options))
        remove_cached_schema(n)
      end
      nil
    end

    # Renames a table:
    #
    #   DB.tables #=> [:items]
    #   DB.rename_table :items, :old_items
    #   DB.tables #=> [:old_items]
    def rename_table(name, new_name)
      execute_ddl(rename_table_sql(name, new_name))
      remove_cached_schema(name)
      nil
    end
    
    # Renames a column in the specified table. This method expects the current
    # column name and the new column name:
    #
    #   DB.rename_column :items, :cntr, :counter
    #
    # See <tt>alter_table</tt>.
    def rename_column(table, *args)
      alter_table(table) {rename_column(*args)}
    end
    
    # Sets the default value for the given column in the given table:
    #
    #   DB.set_column_default :items, :category, 'perl!'
    #
    # See <tt>alter_table</tt>.
    def set_column_default(table, *args)
      alter_table(table) {set_column_default(*args)}
    end
    
    # Set the data type for the given column in the given table:
    #
    #   DB.set_column_type :items, :price, :float
    #
    # See <tt>alter_table</tt>.
    def set_column_type(table, *args)
      alter_table(table) {set_column_type(*args)}
    end

    private

    # Apply the changes in the given alter table ops to the table given by name.
    def apply_alter_table(name, ops)
      alter_table_sql_list(name, ops).each{|sql| execute_ddl(sql)}
    end
    
    # Apply the operations in the given generator to the table given by name.
    def apply_alter_table_generator(name, generator)
      apply_alter_table(name, generator.operations)
    end

    # The class used for alter_table generators.
    def alter_table_generator_class
      Schema::AlterTableGenerator
    end
    
    # SQL fragment for given alter table operation.
    def alter_table_op_sql(table, op)
      quoted_name = quote_identifier(op[:name]) if op[:name]
      case op[:op]
      when :add_column
        "ADD COLUMN #{column_definition_sql(op)}"
      when :drop_column
        "DROP COLUMN #{quoted_name}#{' CASCADE' if op[:cascade]}"
      when :rename_column
        "RENAME COLUMN #{quoted_name} TO #{quote_identifier(op[:new_name])}"
      when :set_column_type
        "ALTER COLUMN #{quoted_name} TYPE #{type_literal(op)}"
      when :set_column_default
        "ALTER COLUMN #{quoted_name} SET DEFAULT #{literal(op[:default])}"
      when :set_column_null
        "ALTER COLUMN #{quoted_name} #{op[:null] ? 'DROP' : 'SET'} NOT NULL"
      when :add_constraint
        "ADD #{constraint_definition_sql(op)}"
      when :drop_constraint
        "DROP CONSTRAINT #{quoted_name}#{' CASCADE' if op[:cascade]}"
      else
        raise Error, "Unsupported ALTER TABLE operation: #{op[:op]}"
      end
    end

    # The SQL to execute to modify the DDL for the given table name.  op
    # should be one of the operations returned by the AlterTableGenerator.
    def alter_table_sql(table, op)
      case op[:op]
      when :add_index
        index_definition_sql(table, op)
      when :drop_index
        drop_index_sql(table, op)
      else
        "ALTER TABLE #{quote_schema_table(table)} #{alter_table_op_sql(table, op)}"
      end
    end

    # Array of SQL DDL modification statements for the given table,
    # corresponding to the DDL changes specified by the operations.
    def alter_table_sql_list(table, operations)
      if supports_combining_alter_table_ops?
        grouped_ops = []
        last_combinable = false
        operations.each do |op|
          if combinable_alter_table_op?(op)
            if sql = alter_table_op_sql(table, op)
              grouped_ops << [] unless last_combinable
              grouped_ops.last << sql
              last_combinable = true
            end
          elsif sql = alter_table_sql(table, op)
            grouped_ops << sql
            last_combinable = false
          end
        end
        grouped_ops.map do |gop|
          if gop.is_a?(Array)
            "ALTER TABLE #{quote_schema_table(table)} #{gop.join(', ')}"
          else
            gop
          end
        end
      else
        operations.map{|op| alter_table_sql(table, op)}.flatten.compact
      end
    end
    
    # The SQL string specify the autoincrement property, generally used by
    # primary keys.
    def auto_increment_sql
      AUTOINCREMENT
    end
    
    # The order of the column definition, as an array of symbols.
    def column_definition_order
      self.class.const_get(:COLUMN_DEFINITION_ORDER)
    end

    # SQL DDL fragment containing the column creation SQL for the given column.
    def column_definition_sql(column)
      sql = "#{quote_identifier(column[:name])} #{type_literal(column)}"
      column_definition_order.each{|m| send(:"column_definition_#{m}_sql", sql, column)}
      sql
    end

    # Add auto increment SQL fragment to column creation SQL.
    def column_definition_auto_increment_sql(sql, column)
      sql << " #{auto_increment_sql}" if column[:auto_increment]
    end

    # Add collate SQL fragment to column creation SQL.
    def column_definition_collate_sql(sql, column)
      sql << " COLLATE #{column[:collate]}" if column[:collate]
    end

    # Add default SQL fragment to column creation SQL.
    def column_definition_default_sql(sql, column)
      sql << " DEFAULT #{literal(column[:default])}" if column.include?(:default)
    end
    
    # Add null/not null SQL fragment to column creation SQL.
    def column_definition_null_sql(sql, column)
      null = column.fetch(:null, column[:allow_null])
      sql << NOT_NULL if null == false
      sql << NULL if null == true
    end
    
    # Add primary key SQL fragment to column creation SQL.
    def column_definition_primary_key_sql(sql, column)
      sql << PRIMARY_KEY if column[:primary_key]
    end
    
    # Add foreign key reference SQL fragment to column creation SQL.
    def column_definition_references_sql(sql, column)
      sql << column_references_column_constraint_sql(column) if column[:table]
    end
    
    # Add unique constraint SQL fragment to column creation SQL.
    def column_definition_unique_sql(sql, column)
      sql << UNIQUE if column[:unique]
    end
    
    # SQL for all given columns, used inside a CREATE TABLE block.
    def column_list_sql(generator)
      (generator.columns.map{|c| column_definition_sql(c)} + generator.constraints.map{|c| constraint_definition_sql(c)}).join(COMMA_SEPARATOR)
    end

    # SQL DDL fragment for column foreign key references (column constraints)
    def column_references_column_constraint_sql(column)
      column_references_sql(column)
    end

    # SQL DDL fragment for column foreign key references
    def column_references_sql(column)
      sql = " REFERENCES #{quote_schema_table(column[:table])}"
      sql << "(#{Array(column[:key]).map{|x| quote_identifier(x)}.join(COMMA_SEPARATOR)})" if column[:key]
      sql << " ON DELETE #{on_delete_clause(column[:on_delete])}" if column[:on_delete]
      sql << " ON UPDATE #{on_update_clause(column[:on_update])}" if column[:on_update]
      sql << " DEFERRABLE INITIALLY DEFERRED" if column[:deferrable]
      sql
    end
  
    # SQL DDL fragment for table foreign key references (table constraints)
    def column_references_table_constraint_sql(constraint)
      "FOREIGN KEY #{literal(constraint[:columns])}#{column_references_sql(constraint)}"
    end

    # Whether the given alter table operation is combinable.
    def combinable_alter_table_op?(op)
      # Use a dynamic lookup for easier overriding in adapters
      COMBINABLE_ALTER_TABLE_OPS.include?(op[:op])
    end

    # SQL DDL fragment specifying a constraint on a table.
    def constraint_definition_sql(constraint)
      sql = constraint[:name] ? "CONSTRAINT #{quote_identifier(constraint[:name])} " : ""
      case constraint[:type]
      when :check
        check = constraint[:check]
        sql << "CHECK #{filter_expr((check.is_a?(Array) && check.length == 1) ? check.first : check)}"
      when :primary_key
        sql << "PRIMARY KEY #{literal(constraint[:columns])}"
      when :foreign_key
        sql << column_references_table_constraint_sql(constraint)
      when :unique
        sql << "UNIQUE #{literal(constraint[:columns])}"
      else
        raise Error, "Invalid constriant type #{constraint[:type]}, should be :check, :primary_key, :foreign_key, or :unique"
      end
      sql
    end

    # Execute the create table statements using the generator.
    def create_table_from_generator(name, generator, options)
      execute_ddl(create_table_sql(name, generator, options))
    end

    # The class used for create_table generators.
    def create_table_generator_class
      Schema::CreateTableGenerator
    end
    
    # Execute the create index statements using the generator.
    def create_table_indexes_from_generator(name, generator, options)
      e = options[:ignore_index_errors] || options[:if_not_exists]
      generator.indexes.each do |index|
        begin
          index_sql_list(name, [index]).each{|sql| execute_ddl(sql)}
        rescue Error
          raise unless e
        end
      end
    end

    # DDL statement for creating a table with the given name, columns, and options
    def create_table_sql(name, generator, options)
      "#{create_table_prefix_sql(name, options)} (#{column_list_sql(generator)})"
    end

    # Run a command to create the table with the given name from the given
    # SELECT sql statement.
    def create_table_as(name, sql, options)
      sql = sql.sql if sql.is_a?(Sequel::Dataset)
      run(create_table_as_sql(name, sql, options))
    end
    
    # DDL statement for creating a table from the result of a SELECT statement.
    # +sql+ should be a string representing a SELECT query.
    def create_table_as_sql(name, sql, options)
      "#{create_table_prefix_sql(name, options)} AS #{sql}"
    end

    # DDL statement for creating a table with the given name, columns, and options
    def create_table_prefix_sql(name, options)
      "CREATE #{temporary_table_sql if options[:temp]}TABLE#{' IF NOT EXISTS' if options[:if_not_exists]} #{options[:temp] ? quote_identifier(name) : quote_schema_table(name)}"
    end

    # Default index name for the table and columns, may be too long
    # for certain databases.
    def default_index_name(table_name, columns)
      schema, table = schema_and_table(table_name)
      "#{"#{schema}_" if schema and schema != default_schema}#{table}_#{columns.map{|c| [String, Symbol].any?{|cl| c.is_a?(cl)} ? c : literal(c).gsub(/\W/, '_')}.join(UNDERSCORE)}_index"
    end
  
    # The SQL to drop an index for the table.
    def drop_index_sql(table, op)
      "DROP INDEX #{quote_identifier(op[:name] || default_index_name(table, op[:columns]))}"
    end

    # SQL DDL statement to drop the table with the given name.
    def drop_table_sql(name, options)
      "DROP TABLE#{' IF EXISTS' if options[:if_exists]} #{quote_schema_table(name)}#{' CASCADE' if options[:cascade]}"
    end
    
    # SQL DDL statement to drop a view with the given name.
    def drop_view_sql(name, options)
      "DROP VIEW #{quote_schema_table(name)}#{' CASCADE' if options[:cascade]}"
    end

    # Proxy the filter_expr call to the dataset, used for creating constraints.
    def filter_expr(*args, &block)
      schema_utility_dataset.literal(schema_utility_dataset.send(:filter_expr, *args, &block))
    end

    # SQL DDL statement for creating an index for the table with the given name
    # and index specifications.
    def index_definition_sql(table_name, index)
      index_name = index[:name] || default_index_name(table_name, index[:columns])
      if index[:type]
        raise Error, "Index types are not supported for this database"
      elsif index[:where]
        raise Error, "Partial indexes are not supported for this database"
      else
        "CREATE #{'UNIQUE ' if index[:unique]}INDEX #{quote_identifier(index_name)} ON #{quote_schema_table(table_name)} #{literal(index[:columns])}"
      end
    end
  
    # Array of SQL DDL statements, one for each index specification,
    # for the given table.
    def index_sql_list(table_name, indexes)
      indexes.map{|i| index_definition_sql(table_name, i)}
    end

    # Extract the join table name from the arguments given to create_join_table.
    # Also does argument validation for the create_join_table method.
    def join_table_name(hash, options)
      entries = hash.values
      raise Error, "must have 2 entries in hash given to (create|drop)_join_table" unless entries.length == 2
      if options[:name]
        options[:name]
      else
        table_names = entries.map{|e| join_table_name_extract(e)}
        table_names.map{|t| t.to_s}.sort.join('_')
      end
    end

    # Extract an individual join table name, which should either be a string
    # or symbol, or a hash containing one of those as the value for :table.
    def join_table_name_extract(entry)
      case entry
      when Symbol, String
        entry
      when Hash
        join_table_name_extract(entry[:table])
      else
        raise Error, "can't extract table name from #{entry.inspect}"
      end
    end
    
    # SQL DDL ON DELETE fragment to use, based on the given action.
    # The following actions are recognized:
    # 
    # * :cascade - Delete rows referencing this row.
    # * :no_action (default) - Raise an error if other rows reference this
    #   row, allow deferring of the integrity check.
    # * :restrict - Raise an error if other rows reference this row,
    #   but do not allow deferring the integrity check.
    # * :set_default - Set columns referencing this row to their default value.
    # * :set_null - Set columns referencing this row to NULL.
    #
    # Any other object given is just converted to a string, with "_" converted to " " and upcased.
    def on_delete_clause(action)
      action.to_s.gsub("_", " ").upcase
    end

    # Alias of #on_delete_clause, since the two usually behave the same.
    def on_update_clause(action)
      on_delete_clause(action)
    end
    
    # Proxy the quote_schema_table method to the dataset
    def quote_schema_table(table)
      schema_utility_dataset.quote_schema_table(table)
    end
    
    # Proxy the quote_identifier method to the dataset, used for quoting tables and columns.
    def quote_identifier(v)
      schema_utility_dataset.quote_identifier(v)
    end
    
    # SQL DDL statement for renaming a table.
    def rename_table_sql(name, new_name)
      "ALTER TABLE #{quote_schema_table(name)} RENAME TO #{quote_schema_table(new_name)}"
    end

    # Remove the cached schema_utility_dataset, because the identifier
    # quoting has changed.
    def reset_schema_utility_dataset
      @schema_utility_dataset = nil
    end
    
    # Split the schema information from the table
    def schema_and_table(table_name)
      schema_utility_dataset.schema_and_table(table_name)
    end

    # Return true if the given column schema represents an autoincrementing primary key.
    def schema_autoincrementing_primary_key?(schema)
      !!(schema[:primary_key] && schema[:db_type] =~ /int/io)
    end

    # The dataset to use for proxying certain schema methods.
    def schema_utility_dataset
      @schema_utility_dataset ||= dataset
    end

    # Whether the database supports combining multiple alter table
    # operations into a single query, false by default.
    def supports_combining_alter_table_ops?
      false
    end

    # SQL DDL fragment for temporary table
    def temporary_table_sql
      self.class.const_get(:TEMPORARY)
    end

    # SQL fragment specifying the type of a given column.
    def type_literal(column)
      column[:type].is_a?(Class) ? type_literal_generic(column) : type_literal_specific(column)
    end
    
    # SQL fragment specifying the full type of a column,
    # consider the type with possible modifiers.
    def type_literal_generic(column)
      meth = "type_literal_generic_#{column[:type].name.to_s.downcase}"
      if respond_to?(meth, true)
        send(meth, column)
      else
        raise Error, "Unsupported ruby class used as database type: #{column[:type]}"
      end
    end

    # Alias for type_literal_generic_numeric, to make overriding in a subclass easier.
    def type_literal_generic_bigdecimal(column)
      type_literal_generic_numeric(column)
    end

    # Sequel uses the bigint type by default for Bignums.
    def type_literal_generic_bignum(column)
      :bigint
    end

    # Sequel uses the date type by default for Dates.
    def type_literal_generic_date(column)
      :date
    end

    # Sequel uses the timestamp type by default for DateTimes.
    def type_literal_generic_datetime(column)
      :timestamp
    end

    # Alias for type_literal_generic_trueclass, to make overriding in a subclass easier.
    def type_literal_generic_falseclass(column)
      type_literal_generic_trueclass(column)
    end

    # Sequel uses the blob type by default for Files.
    def type_literal_generic_file(column)
      :blob
    end

    # Alias for type_literal_generic_integer, to make overriding in a subclass easier.
    def type_literal_generic_fixnum(column)
      type_literal_generic_integer(column)
    end

    # Sequel uses the double precision type by default for Floats.
    def type_literal_generic_float(column)
      :"double precision"
    end

    # Sequel uses the integer type by default for integers
    def type_literal_generic_integer(column)
      :integer
    end

    # Sequel uses the numeric type by default for Numerics and BigDecimals.
    # If a size is given, it is used, otherwise, it will default to whatever
    # the database default is for an unsized value.
    def type_literal_generic_numeric(column)
      column[:size] ? "numeric(#{Array(column[:size]).join(', ')})" : :numeric
    end

    # Sequel uses the varchar type by default for Strings.  If a
    # size isn't present, Sequel assumes a size of 255.  If the
    # :fixed option is used, Sequel uses the char type.  If the
    # :text option is used, Sequel uses the :text type.
    def type_literal_generic_string(column)
      if column[:text]
        uses_clob_for_text? ? :clob : :text
      elsif column[:fixed]
        "char(#{column[:size]||255})"
      else
        "varchar(#{column[:size]||255})"
      end
    end
    
    # Sequel uses the timestamp type by default for Time values.
    # If the :only_time option is used, the time type is used.
    def type_literal_generic_time(column)
      column[:only_time] ? :time : :timestamp
    end

    # Sequel uses the boolean type by default for TrueClass and FalseClass.
    def type_literal_generic_trueclass(column)
      :boolean
    end

    # SQL fragment for the given type of a column if the column is not one of the
    # generic types specified with a ruby class.
    def type_literal_specific(column)
      type = column[:type]
      type = "double precision" if type.to_s == 'double'
      column[:size] ||= 255 if type.to_s == 'varchar'
      elements = column[:size] || column[:elements]
      "#{type}#{literal(Array(elements)) if elements}#{UNSIGNED if column[:unsigned]}"
    end

    # Whether clob should be used for String :text=>true columns.
    def uses_clob_for_text?
      false
    end
  end
end
