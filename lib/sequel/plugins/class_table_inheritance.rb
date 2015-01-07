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
    # employees :: id, name, kind
    # staff :: id, manager_id
    # managers :: id, num_staff
    # executives :: id, num_managers
    #
    # The class_table_inheritance plugin assumes that the main table
    # (e.g. employees) has a primary key field (usually autoincrementing),
    # and all other tables have a foreign key of the same name that points
    # to the same key in their superclass's table.  For example:
    #
    # employees.id :: primary key, autoincrementing
    # staff.id :: foreign key referencing employees(id)
    # managers.id :: foreign key referencing employees(id)
    # executives.id :: foreign key referencing managers(id)
    #
    # When using the class_table_inheritance plugin, subclasses use joined 
    # datasets:
    #
    #   Employee.dataset.sql
    #   # SELECT employees.id, employees.name, employees.kind
    #   # FROM employees
    #
    #   Manager.dataset.sql
    #   # SELECT employees.id, employees.name, employees.kind, managers.num_staff
    #   # FROM employees
    #   # JOIN managers ON (managers.id = employees.id)
    #
    #   Executive.dataset.sql
    #   # SELECT employees.id, employees.name, employees.kind, managers.num_staff, executives.num_managers
    #   # FROM employees
    #   # JOIN managers ON (managers.id = employees.id)
    #   # JOIN executives ON (executives.id = managers.id)
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
    # If you want to get all columns in a subclass instance after loading
    # via the superclass, call Model#refresh.
    #
    #   a = Employee.first
    #   a.values # {:id=>1, name=>'S', :kind=>'Executive'}
    #   a.refresh.values # {:id=>1, name=>'S', :kind=>'Executive', :num_staff=>4, :num_managers=>2}
    # 
    # Usage:
    #
    #   # Set up class table inheritance in the parent class
    #   # (Not in the subclasses)
    #   class Employee < Sequel::Model
    #     plugin :class_table_inheritance
    #   end
    #
    #   # Have subclasses inherit from the appropriate class
    #   class Staff < Employee; end
    #   class Manager < Employee; end
    #   class Executive < Manager; end
    #
    #   # You can also set options when loading the plugin:
    #   # :kind :: column to hold the class name
    #   # :table_map :: map of class name symbols to table name symbols
    #   # :model_map :: map of column values to class name symbols
    #   Employee.plugin :class_table_inheritance, :key=>:kind, :table_map=>{:Staff=>:staff},
    #     :model_map=>{1=>:Employee, 2=>:Manager, 3=>:Executive, 4=>:Staff}
    module ClassTableInheritance
      # The class_table_inheritance plugin requires the lazy_attributes plugin
      # to handle lazily-loaded attributes for subclass instances returned
      # by superclass methods.
      def self.apply(model, opts=OPTS)
        model.plugin :lazy_attributes
      end
      
      # Initialize the per-model data structures and set the dataset's row_proc
      # to check for the :key option column for the type of class when loading objects.
      # Options:
      # :key :: The column symbol holding the name of the model class this
      #         is an instance of.  Necessary if you want to call model methods
      #         using the superclass, but have them return subclass instances.
      # :table_map :: Hash with class name symbol keys and table name symbol
      #               values.  Necessary if the implicit table name for the model class
      #               does not match the database table name
      # :model_map :: Hash with keys being values of the cti_key column, and values
      #               being class name strings or symbols.  Used if you don't want to
      #               store class names in the database.  If you use this option, you
      #               are responsible for setting the values of the cti_key column
      #               manually (usually in a before_create hook).
      def self.configure(model, opts=OPTS)
        model.instance_eval do
          @cti_base_model = self
          @cti_key = opts[:key] 
          @cti_tables = [table_name]
          @cti_columns = {table_name=>columns}
          @cti_table_map = opts[:table_map] || {}
          @cti_model_map = opts[:model_map]
          set_dataset_cti_row_proc
          set_dataset(dataset.select(*columns.map{|c| Sequel.qualify(table_name, Sequel.identifier(c))}))
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
        
        # A hash with keys being values of the cti_key column, and values
        # being class name strings or symbols.  Used if you don't want to
        # store class names in the database.
        attr_reader :cti_model_map
        
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
          cmm = cti_model_map
          pk = primary_key
          ds = dataset
          table = nil
          columns = nil
          subclass.instance_eval do
            raise(Error, "cannot create anonymous subclass for model class using class_table_inheritance") if !(n = name) || n.empty?
            table = ctm[n.to_sym] || implicit_table_name
            columns = db.from(table).columns
            @cti_key = ck 
            @cti_tables = ct + [table]
            @cti_columns = cc.merge(table=>columns)
            @cti_table_map = ctm
            @cti_base_model = cbm
            @cti_model_map = cmm
            # Need to set dataset and columns before calling super so that
            # the main column accessor module is included in the class before any
            # plugin accessor modules (such as the lazy attributes accessor module).
            set_dataset(ds.join(table, pk=>pk).select_append(*(columns - [primary_key]).map{|c| Sequel.qualify(table, Sequel.identifier(c))}))
            set_columns(self.columns)
          end
          super
          subclass.instance_eval do
            set_dataset_cti_row_proc
            (columns - [cbm.primary_key]).each{|a| define_lazy_attribute_getter(a, :dataset=>dataset, :table=>table)}
            cti_tables.reverse.each do |t|
              db.schema(t).each{|k,v| db_schema[k] = v}
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

        private

        # If calling set_dataset manually, make sure to set the dataset
        # row proc to one that handles inheritance correctly.
        def set_dataset_row_proc(ds)
          ds.row_proc = @dataset.row_proc if @dataset
        end

        # Set the row_proc for the model's dataset appropriately
        # based on the cti key and model map.
        def set_dataset_cti_row_proc
          m = method(:constantize)
          dataset.row_proc = if ck = cti_key
            if model_map = cti_model_map
              lambda do |r|
                mod = if name = model_map[r[ck]]
                  m.call(name)
                else
                  self
                end
                mod.call(r)
              end
            else
              lambda{|r| (m.call(r[ck]) rescue self).call(r)}
            end
          else
            self
          end
        end
      end

      module InstanceMethods
        # Delete the row from all backing tables, starting from the
        # most recent table and going through all superclasses.
        def delete
          raise Sequel::Error, "can't delete frozen object" if frozen?
          m = model
          m.cti_tables.reverse.each do |table|
            m.db.from(table).filter(m.primary_key=>pk).delete
          end
          self
        end
        
        private
        
        # Set the cti_key column to the name of the model.
        def _before_validation
          if new? && model.cti_key && !model.cti_model_map
            set_column_value("#{model.cti_key}=", model.name.to_s)
          end
          super
        end
        
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
