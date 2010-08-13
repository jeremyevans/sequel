module Sequel
  module Plugins
    # The class_table_inheritance plugin allows you to model inheritance in the
    # database using a table per model class in the hierarchy, with only columns
    # unique to that model class (or subclass hierarchy) being stored in the related
    # table.  For example, with this hierarchy:
    #
    #       Employee
    #      /        \ 
    #   Staff     Manager
    #                |
    #            Executive
    #
    # the following database schema may be used (table - columns):
    #
    # * employees - id, name, kind
    # * staff - id, manager_id
    # * managers - id, num_staff
    # * executives - id, num_managers
    #
    # The class_table_inheritance plugin assumes that the main table
    # (e.g. employees) has a primary key field (usually autoincrementing),
    # and all other tables have a foreign key of the same name that points
    # to the same key in their superclass's table.  For example:
    #
    # * employees.id  - primary key, autoincrementing
    # * staff.id - foreign key referencing employees(id)
    # * managers.id - foreign key referencing employees(id)
    # * executives.id - foreign key referencing managers(id)
    #
    # When using the class_table_inheritance plugin, subclasses use joined 
    # datasets:
    #
    #   Employee.dataset.sql  # SELECT * FROM employees
    #   Manager.dataset.sql   # SELECT * FROM employees
    #                         # INNER JOIN managers USING (id)
    #   Executive.dataset.sql # SELECT * FROM employees 
    #                         # INNER JOIN managers USING (id)
    #                         # INNER JOIN executives USING (id)
    #
    # This allows Executive.all to return instances with all attributes
    # loaded.  The plugin overrides the deleting, inserting, and updating
    # in the model to work with multiple tables, by handling each table
    # individually.
    #
    # This plugin allows the use of a :key option when loading to mark
    # a column holding a class name.  This allows methods on the
    # superclass to return instances of specific subclasses.
    # This plugin also requires the lazy_attributes plugin and uses it to
    # return subclass specific attributes that would not be loaded
    # when calling superclass methods (since those wouldn't join
    # to the subclass tables).  For example:
    #
    #   a = Employee.all # [<#Staff>, <#Manager>, <#Executive>]
    #   a.first.values # {:id=>1, name=>'S', :kind=>'Staff'}
    #   a.first.manager_id # Loads the manager_id attribute from the database
    # 
    # Usage:
    #
    #   # Set up class table inheritance in the parent class
    #   # (Not in the subclasses)
    #   Employee.plugin :class_table_inheritance
    #
    #   # Set the +kind+ column to hold the class name, and
    #   # set the subclass table to map to for each subclass 
    #   Employee.plugin :class_table_inheritance, :key=>:kind, :table_map=>{:Staff=>:staff}
    module ClassTableInheritance
      # The class_table_inheritance plugin requires the lazy_attributes plugin
      # to handle lazily-loaded attributes for subclass instances returned
      # by superclass methods.
      def self.apply(model, opts={}, &block)
        model.plugin :lazy_attributes
      end
      
      # Initialize the per-model data structures and set the dataset's row_proc
      # to check for the :key option column for the type of class when loading objects.
      # Options:
      # * :key - The column symbol holding the name of the model class this
      #   is an instance of.  Necessary if you want to call model methods
      #   using the superclass, but have them return subclass instances.
      # * :table_map - Hash with class name symbol keys and table name symbol
      #   values.  Necessary if the implicit table name for the model class
      #   does not match the database table name
      def self.configure(model, opts={}, &block)
        model.instance_eval do
          m = method(:constantize)
          @cti_base_model = self
          @cti_key = key = opts[:key] 
          @cti_tables = [table_name]
          @cti_columns = {table_name=>columns}
          @cti_table_map = opts[:table_map] || {}
          dataset.row_proc = if key
            lambda{|r| (m.call(r[key]) rescue model).load(r)}
          else
            lambda{|r| model.load(r)}
          end
        end
      end

      module ClassMethods
        # The parent/root/base model for this class table inheritance hierarchy.
        # This is the only model in the hierarchy that load the
        # class_table_inheritance plugin.
        attr_reader :cti_base_model
        
        # Hash with table name symbol keys and arrays of column symbol values,
        # giving the columns to update in each backing database table.
        attr_reader :cti_columns
        
        # The column containing the class name as a string.  Used to
        # return instances of subclasses when calling the superclass's
        # load method.
        attr_reader :cti_key
        
        # An array of table symbols that back this model.  The first is
        # cti_base_model table symbol, and the last is the current model
        # table symbol.
        attr_reader :cti_tables
        
        # A hash with class name symbol keys and table name symbol values.
        # Specified with the :table_map option to the plugin, and used if
        # the implicit naming is incorrect.
        attr_reader :cti_table_map
        
        # Add the appropriate data structures to the subclass.  Does not
        # allow anonymous subclasses to be created, since they would not
        # be mappable to a table.
        def inherited(subclass)
          cc = cti_columns
          ck = cti_key
          ct = cti_tables.dup
          ctm = cti_table_map.dup
          cbm = cti_base_model
          pk = primary_key
          ds = dataset
          subclass.instance_eval do
            raise(Error, "cannot create anonymous subclass for model class using class_table_inheritance") if !(n = name) || n.empty?
            table = ctm[n.to_sym] || implicit_table_name
            columns = db.from(table).columns
            @cti_key = ck 
            @cti_tables = ct + [table]
            @cti_columns = cc.merge(table=>columns)
            @cti_table_map = ctm
            @cti_base_model = cbm
            # Need to set dataset and columns before calling super so that
            # the main column accessor module is included in the class before any
            # plugin accessor modules (such as the lazy attributes accessor module).
            set_dataset(ds.join(table, [pk]))
            set_columns(self.columns)
          end
          super
          subclass.instance_eval do
            m = method(:constantize)
            dataset.row_proc = if cti_key
              lambda{|r| (m.call(r[ck]) rescue subclass).load(r)}
            else
              lambda{|r| subclass.load(r)}
            end
            (columns - [cbm.primary_key]).each{|a| define_lazy_attribute_getter(a)}
            cti_tables.reverse.each do |table|
              db.schema(table).each{|k,v| db_schema[k] = v}
            end
          end
        end
        
        # The primary key in the parent/base/root model, which should have a
        # foreign key with the same name referencing it in each model subclass.
        def primary_key
          return super if self == cti_base_model
          cti_base_model.primary_key
        end
        
        # The table name for the current model class's main table (not used
        # by any superclasses).
        def table_name
          self == cti_base_model ? super : cti_tables.last
        end
      end

      module InstanceMethods
        # Set the cti_key column to the name of the model.
        def before_create
          send("#{model.cti_key}=", model.name.to_s) if model.cti_key
          super
        end
        
        # Delete the row from all backing tables, starting from the
        # most recent table and going through all superclasses.
        def delete
          m = model
          m.cti_tables.reverse.each do |table|
            m.db.from(table).filter(m.primary_key=>pk).delete
          end
          self
        end
        
        private
        
        # Insert rows into all backing tables, using the columns
        # in each table.  
        def _insert
          return super if model == model.cti_base_model
          iid = @values[primary_key] 
          m = model
          m.cti_tables.each do |table|
            h = {}
            h[m.primary_key] ||= iid if iid
            m.cti_columns[table].each{|c| h[c] = @values[c] if @values.include?(c)}
            nid = m.db.from(table).insert(h)
            iid ||= nid
          end
          @values[primary_key] = iid
        end
        
        # Update rows in all backing tables, using the columns in each table.
        def _update(columns)
          pkh = pk_hash
          m = model
          m.cti_tables.each do |table|
            h = {}
            m.cti_columns[table].each{|c| h[c] = columns[c] if columns.include?(c)}
            m.db.from(table).filter(pkh).update(h) unless h.empty?
          end
        end
      end
    end
  end
end
