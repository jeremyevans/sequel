# frozen-string-literal: true

module Sequel
  module Plugins
    # Sequel's built in schema plugin allows you to define your schema
    # directly in the model using Model.set_schema (which takes a block
    # similar to Database#create_table), and use Model.create_table to
    # create a table using the schema information.
    #
    # This plugin is mostly suited to test code.  If there is any
    # chance that your application's schema could change, you should
    # be using the migration extension instead.
    # 
    # Usage:
    #
    #   # Add the schema methods to all model subclasses (called before loading subclasses)
    #   Sequel::Model.plugin :schema
    #
    #   # Add the schema methods to the Album class
    #   Album.plugin :schema
    module Schema
      module ClassMethods
        # Creates table, using the column information from set_schema.
        def create_table(*args, &block)
          set_schema(*args, &block) if block
          db.create_table(table_name, :generator=>@schema)
          @db_schema = get_db_schema(true)
          columns
        end
        
        # Drops the table if it exists and then runs create_table.  Should probably
        # not be used except in testing.
        def create_table!(*args, &block)
          drop_table?
          create_table(*args, &block)
        end

        # Creates the table unless the table already exists
        def create_table?(*args, &block)
          create_table(*args, &block) unless table_exists?
        end
        
        # Drops table.  If the table doesn't exist, this will probably raise an error.
        def drop_table
          db.drop_table(table_name)
        end
    
        # Drops table if it already exists, do nothing if it doesn't exist.
        def drop_table?
          db.drop_table?(table_name)
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
          @schema = db.create_table_generator(&block)
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
