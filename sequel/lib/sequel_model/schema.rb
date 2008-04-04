module Sequel
  class Model
    # Defines a table schema (see Schema::Generator for more information).
    #
    # This is only needed if you want to use the create_table or drop_table
    # methods.
    def self.set_schema(name = nil, &block)
      if name
        set_dataset(db[name])
      end
      @schema = Schema::Generator.new(db, &block)
      if @schema.primary_key_name
        set_primary_key @schema.primary_key_name
      end
    end
    
    # Returns table schema for direct descendant of Model.
    def self.schema
      @schema || ((superclass != Model) && (superclass.schema))
    end

    # Returns name of table.
    def self.table_name
      dataset.opts[:from].first
    end
    
    # Returns true if table exists, false otherwise.
    def self.table_exists?
      db.table_exists?(table_name)
    end
    
    # Creates table.
    def self.create_table
      db.create_table_sql_list(table_name, *schema.create_info).each {|s| db << s} 
    end
    
    # Drops table.
    def self.drop_table
      db.execute db.drop_table_sql(table_name)
    end
    
    # Like create_table but invokes drop_table when table_exists? is true.
    def self.create_table!
      drop_table if table_exists?
      create_table
    end
  end
end
