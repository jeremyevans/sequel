module Sequel
  class Database
    # Adds a column to the specified table. This method expects a column name,
    # a datatype and optionally a hash with additional constraints and options:
    #
    #   DB.add_column :items, :name, :text, :unique => true, :null => false
    #   DB.add_column :items, :category, :text, :default => 'ruby'
    def add_column(table, *args)
      alter_table(table) {add_column(*args)}
    end
    
    # Adds an index to a table for the given columns:
    # 
    #   DB.add_index :posts, :title
    #   DB.add_index :posts, [:author, :title], :unique => true
    def add_index(table, *args)
      alter_table(table) {add_index(*args)}
    end
    
    # Alters the given table with the specified block. Here are the currently
    # available operations:
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
    # Note that #add_column accepts all the options available for column
    # definitions using create_table, and #add_index accepts all the options
    # available for index definition.
    def alter_table(name, &block)
      g = Schema::AlterTableGenerator.new(self, &block)
      alter_table_sql_list(name, g.operations).each {|sql| execute(sql)}
    end
    
    # Creates a table. The easiest way to use this method is to provide a
    # block:
    #   DB.create_table :posts do
    #     primary_key :id, :serial
    #     column :title, :text
    #     column :content, :text
    #     index :title
    #   end
    def create_table(name, &block)
      g = Schema::Generator.new(self, &block)
      create_table_sql_list(name, *g.create_info).each {|sql| execute(sql)}
    end
    
    # Forcibly creates a table. If the table already exists it is dropped.
    def create_table!(name, &block)
      drop_table(name) rescue nil
      create_table(name, &block)
    end
    
    # Creates a view, replacing it if it already exists:
    #
    #   DB.create_or_replace_view(:cheap_items, "SELECT * FROM items WHERE price < 100")
    #   DB.create_or_replace_view(:ruby_items, DB[:items].filter(:category => 'ruby'))
    def create_or_replace_view(name, source)
      source = source.sql if source.is_a?(Dataset)
      execute("CREATE OR REPLACE VIEW #{name} AS #{source}")
    end
    
    # Creates a view based on a dataset or an SQL string:
    #
    #   DB.create_view(:cheap_items, "SELECT * FROM items WHERE price < 100")
    #   DB.create_view(:ruby_items, DB[:items].filter(:category => 'ruby'))
    def create_view(name, source)
      source = source.sql if source.is_a?(Dataset)
      execute("CREATE VIEW #{name} AS #{source}")
    end
    
    # Removes a column from the specified table:
    #
    #   DB.drop_column :items, :category
    def drop_column(table, *args)
      alter_table(table) {drop_column(*args)}
    end
    
    # Removes an index for the given table and column/s:
    #
    #   DB.drop_index :posts, :title
    #   DB.drop_index :posts, [:author, :title]
    def drop_index(table, columns)
      alter_table(table) {drop_index(columns)}
    end
    
    # Drops one or more tables corresponding to the given table names.
    def drop_table(*names)
      names.each {|n| execute(drop_table_sql(n))}
    end
    
    # Drops a view:
    #
    #   DB.drop_view(:cheap_items)
    def drop_view(name)
      execute("DROP VIEW #{name}")
    end

    # Renames a table:
    #
    #   DB.tables #=> [:items]
    #   DB.rename_table :items, :old_items
    #   DB.tables #=> [:old_items]
    def rename_table(*args)
      execute(rename_table_sql(*args))
    end
    
    # Renames a column in the specified table. This method expects the current
    # column name and the new column name:
    #
    #   DB.rename_column :items, :cntr, :counter
    def rename_column(table, *args)
      alter_table(table) {rename_column(*args)}
    end
    
    # Sets the default value for the given column in the given table:
    #
    #   DB.set_column_default :items, :category, 'perl!'
    def set_column_default(table, *args)
      alter_table(table) {set_column_default(*args)}
    end
    
    # Set the data type for the given column in the given table:
    #
    #   DB.set_column_type :items, :price, :float
    def set_column_type(table, *args)
      alter_table(table) {set_column_type(*args)}
    end
  end
end
