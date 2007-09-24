module Sequel
  class Model
    def self.set_schema(name = nil, &block)
      name ? set_dataset(db[name]) : name = table_name
      @schema = Schema::Generator.new(db, name, &block)
      if @schema.primary_key_name
        set_primary_key @schema.primary_key_name
      end
    end
    
    def self.schema
      @schema || ((superclass != Model) && (superclass.schema))
    end

    def self.table_name
      dataset.opts[:from].first
    end
    
    def self.table_exists?
      db.table_exists?(table_name)
    end
    
    def self.create_table
      db.create_table_sql_list(*schema.create_info).each {|s| db << s} 
    end
    
    def self.drop_table
      db.execute db.drop_table_sql(table_name)
    end
    
    def self.create_table!
      drop_table if table_exists?
      create_table
      
    end
    
    def self.recreate_table
      warn "Model.recreate_table is deprecated. Please use Model.create_table! instead."
      create_table!
    end
  end
end