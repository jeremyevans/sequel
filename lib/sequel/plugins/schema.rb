module Sequel
  module Plugins
    module Schema
      module ClassMethods
        # Creates table, using the column information from set_schema.
        def create_table
          db.create_table(table_name, :generator=>@schema)
          @db_schema = get_db_schema(true)
          columns
        end
        
        # Drops the table if it exists and then runs create_table.  Should probably
        # not be used except in testing.
        def create_table!
          drop_table rescue nil
          create_table
        end

        # Creates the table unless the table already exists
        def create_table?
          create_table unless table_exists?
        end
        
        # Drops table.
        def drop_table
          db.drop_table(table_name)
        end
    
        # Returns table schema created with set_schema for direct descendant of Model.
        # Does not retreive schema information from the database, see db_schema if you
        # want that.
        def schema
          @schema || (superclass.schema unless superclass == Model)
        end
    
        # Defines a table schema (see Schema::Generator for more information).
        #
        # This is only needed if you want to use the create_table/create_table! methods.
        # Will also set the dataset if you provide a name, as well as setting
        # the primary key if you defined one in the passed block.
        #
        # In general, it is a better idea to use migrations for production code, as
        # migrations allow changes to existing schema.  set_schema is mostly useful for
        # test code or simple examples.
        def set_schema(name = nil, &block)
          set_dataset(db[name]) if name
          @schema = Sequel::Schema::Generator.new(db, &block)
          set_primary_key(@schema.primary_key_name) if @schema.primary_key_name
        end
        
        # Returns true if table exists, false otherwise.
        def table_exists?
          db.table_exists?(table_name)
        end
      end
    end
  end
end
