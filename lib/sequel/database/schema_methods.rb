module Sequel
  class Database
    # ---------------------
    # :section: Methods that modify the database schema
    # These methods execute code on the database that modifies the database's schema.
    # ---------------------

    AUTOINCREMENT = 'AUTOINCREMENT'.freeze
    CASCADE = 'CASCADE'.freeze
    COMMA_SEPARATOR = ', '.freeze
    NO_ACTION = 'NO ACTION'.freeze
    NOT_NULL = ' NOT NULL'.freeze
    NULL = ' NULL'.freeze
    PRIMARY_KEY = ' PRIMARY KEY'.freeze
    RESTRICT = 'RESTRICT'.freeze
    SET_DEFAULT = 'SET DEFAULT'.freeze
    SET_NULL = 'SET NULL'.freeze
    TEMPORARY = 'TEMPORARY '.freeze
    UNDERSCORE = '_'.freeze
    UNIQUE = ' UNIQUE'.freeze
    UNSIGNED = ' UNSIGNED'.freeze

    # The order of column modifiers to use when defining a column.
    COLUMN_DEFINITION_ORDER = [:collate, :default, :null, :unique, :primary_key, :auto_increment, :references]

    # Adds a column to the specified table. This method expects a column name,
    # a datatype and optionally a hash with additional constraints and options:
    #
    #   DB.add_column :items, :name, :text, :unique => true, :null => false
    #   DB.add_column :items, :category, :text, :default => 'ruby'
    #
    # See alter_table.
    def add_column(table, *args)
      alter_table(table) {add_column(*args)}
    end
    
    # Adds an index to a table for the given columns:
    # 
    #   DB.add_index :posts, :title
    #   DB.add_index :posts, [:author, :title], :unique => true
    #
    # Options:
    # * :ignore_errors - Ignore any DatabaseErrors that are raised
    #
    # See alter_table.
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
    # definitions using create_table, and +add_index+ accepts all the options
    # available for index definition.
    #
    # See Schema::AlterTableGenerator and the {"Migrations and Schema Modification" guide}[link:files/doc/migration_rdoc.html].
    def alter_table(name, generator=nil, &block)
      generator ||= Schema::AlterTableGenerator.new(self, &block)
      alter_table_sql_list(name, generator.operations).flatten.each {|sql| execute_ddl(sql)}
      remove_cached_schema(name)
      nil
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
    # Options:
    # :temp :: Create the table as a temporary table.
    # :ignore_index_errors :: Ignore any errors when creating indexes.
    #
    # See Schema::Generator and the {"Migrations and Schema Modification" guide}[link:files/doc/migration_rdoc.html].
    def create_table(name, options={}, &block)
      remove_cached_schema(name)
      options = {:generator=>options} if options.is_a?(Schema::Generator)
      generator = options[:generator] || Schema::Generator.new(self, &block)
      create_table_from_generator(name, generator, options)
      create_table_indexes_from_generator(name, generator, options)
      nil
    end
    
    # Forcibly create a table, attempting to drop it if it already exists, then creating it.
    # 
    #   DB.create_table!(:a){Integer :a} 
    #   # SELECT * FROM a LIMIT a -- check existence
    #   # DROP TABLE a -- drop table if already exists
    #   # CREATE TABLE a (a integer)
    def create_table!(name, options={}, &block)
      drop_table(name) if table_exists?(name)
      create_table(name, options, &block)
    end
    
    # Creates the table unless the table already exists.
    # 
    #   DB.create_table?(:a){Integer :a} 
    #   # SELECT * FROM a LIMIT a -- check existence
    #   # CREATE TABLE a (a integer) -- if it doesn't already exist
    def create_table?(name, options={}, &block)
      if supports_create_table_if_not_exists?
        create_table(name, options.merge(:if_not_exists=>true), &block)
      elsif !table_exists?(name)
        create_table(name, options, &block)
      end
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
    # See alter_table.
    def drop_column(table, *args)
      alter_table(table) {drop_column(*args)}
    end
    
    # Removes an index for the given table and column/s:
    #
    #   DB.drop_index :posts, :title
    #   DB.drop_index :posts, [:author, :title]
    #
    # See alter_table.
    def drop_index(table, columns, options={})
      alter_table(table){drop_index(columns, options)}
    end
    
    # Drops one or more tables corresponding to the given names:
    #
    #   DB.drop_table(:posts)
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
    # See alter_table.
    def rename_column(table, *args)
      alter_table(table) {rename_column(*args)}
    end
    
    # Sets the default value for the given column in the given table:
    #
    #   DB.set_column_default :items, :category, 'perl!'
    #
    # See alter_table.
    def set_column_default(table, *args)
      alter_table(table) {set_column_default(*args)}
    end
    
    # Set the data type for the given column in the given table:
    #
    #   DB.set_column_type :items, :price, :float
    #
    # See alter_table.
    def set_column_type(table, *args)
      alter_table(table) {set_column_type(*args)}
    end

    private

    # The SQL to execute to modify the DDL for the given table name.  op
    # should be one of the operations returned by the AlterTableGenerator.
    def alter_table_sql(table, op)
      quoted_name = quote_identifier(op[:name]) if op[:name]
      alter_table_op = case op[:op]
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
      when :add_index
        return index_definition_sql(table, op)
      when :drop_index
        return drop_index_sql(table, op)
      when :add_constraint
        "ADD #{constraint_definition_sql(op)}"
      when :drop_constraint
        "DROP CONSTRAINT #{quoted_name}#{' CASCADE' if op[:cascade]}"
      else
        raise Error, "Unsupported ALTER TABLE operation"
      end
      "ALTER TABLE #{quote_schema_table(table)} #{alter_table_op}"
    end

    # Array of SQL DDL modification statements for the given table,
    # corresponding to the DDL changes specified by the operations.
    def alter_table_sql_list(table, operations)
      operations.map{|op| alter_table_sql(table, op)}
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
      sql << " ON UPDATE #{on_delete_clause(column[:on_update])}" if column[:on_update]
      sql << " DEFERRABLE INITIALLY DEFERRED" if column[:deferrable]
      sql
    end
  
    # SQL DDL fragment for table foreign key references (table constraints)
    def column_references_table_constraint_sql(constraint)
      "FOREIGN KEY #{literal(constraint[:columns])}#{column_references_sql(constraint)}"
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
      "CREATE #{temporary_table_sql if options[:temp]}TABLE#{' IF NOT EXISTS' if options[:if_not_exists]} #{options[:temp] ? quote_identifier(name) : quote_schema_table(name)} (#{column_list_sql(generator)})"
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
      "DROP TABLE #{quote_schema_table(name)}#{' CASCADE' if options[:cascade]}"
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
    def on_delete_clause(action)
      case action
      when :restrict
        RESTRICT
      when :cascade
        CASCADE
      when :set_null
        SET_NULL
      when :set_default
        SET_DEFAULT
      else
        NO_ACTION
      end
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
      !!schema[:primary_key]
    end

    # The dataset to use for proxying certain schema methods.
    def schema_utility_dataset
      @schema_utility_dataset ||= dataset
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
        :text
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
  end
end
