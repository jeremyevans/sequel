module Sequel
  class Model
    # Associations are used in order to specify relationships between model classes
    # that reflect relations between tables in the database using foreign keys.
    module Associations
      # Map of association type symbols to association reflection classes.
      ASSOCIATION_TYPES = {}
    
      # Set an empty association reflection hash in the model
      def self.apply(model)
        model.instance_variable_set(:@association_reflections, {})
      end

      # AssociationReflection is a Hash subclass that keeps information on Sequel::Model associations. It
      # provides methods to reduce internal code duplication.  It should not
      # be instantiated by the user.
      class AssociationReflection < Hash
        include Sequel::Inflections
    
        # Name symbol for the _add internal association method
        def _add_method
          :"_add_#{singularize(self[:name])}"
        end
      
        # Name symbol for the _dataset association method
        def _dataset_method
          :"_#{self[:name]}_dataset"
        end
      
        # Name symbol for the _remove_all internal association method
        def _remove_all_method
          :"_remove_all_#{self[:name]}"
        end
      
        # Name symbol for the _remove internal association method
        def _remove_method
          :"_remove_#{singularize(self[:name])}"
        end
      
        # Name symbol for the _setter association method
        def _setter_method
          :"_#{self[:name]}="
        end
      
        # Name symbol for the add association method
        def add_method
          :"add_#{singularize(self[:name])}"
        end
      
        # Name symbol for association method, the same as the name of the association.
        def association_method
          self[:name]
        end
      
        # The class associated to the current model class via this association
        def associated_class
          self[:class] ||= constantize(self[:class_name])
        end
        
        # Whether this association can have associated objects, given the current
        # object.  Should be false if obj cannot have associated objects because
        # the necessary key columns are NULL.
        def can_have_associated_objects?(obj)
          true
        end

        # Name symbol for the dataset association method
        def dataset_method
          :"#{self[:name]}_dataset"
        end
      
        # Name symbol for the _helper internal association method
        def dataset_helper_method
          :"_#{self[:name]}_dataset_helper"
        end
      
        # Whether the dataset needs a primary key to function, true by default.
        def dataset_need_primary_key?
          true
        end
    
        # By default associations do not need to select a key in an associated table
        # to eagerly load.
        def eager_loading_use_associated_key?
          false
        end

        # Whether to eagerly graph a lazy dataset, true by default.  If this
        # is false, the association won't respect the :eager_graph option
        # when loading the association for a single record.
        def eager_graph_lazy_dataset?
          true
        end
    
        # Whether the associated object needs a primary key to be added/removed,
        # false by default.
        def need_associated_primary_key?
          false
        end
    
        # Returns the reciprocal association variable, if one exists. The reciprocal
        # association is the association in the associated class that is the opposite
        # of the current association.  For example, Album.many_to_one :artist and
        # Artist.one_to_many :albums are reciprocal associations.  This information is
        # to populate reciprocal associations.  For example, when you do this_artist.add_album(album)
        # it sets album.artist to this_artist.
        def reciprocal
          return self[:reciprocal] if include?(:reciprocal)
          r_types = Array(reciprocal_type)
          keys = self[:keys]
          associated_class.all_association_reflections.each do |assoc_reflect|
            if r_types.include?(assoc_reflect[:type]) && assoc_reflect[:keys] == keys && assoc_reflect.associated_class == self[:model]
              self[:reciprocal_type] = assoc_reflect[:type]
              return self[:reciprocal] = assoc_reflect[:name]
            end
          end
          self[:reciprocal] = nil
        end
    
        # Whether the reciprocal of this association returns an array of objects instead of a single object,
        # true by default.
        def reciprocal_array?
          true
        end
    
        # Name symbol for the remove_all_ association method
        def remove_all_method
          :"remove_all_#{self[:name]}"
        end
      
        # Name symbol for the remove_ association method
        def remove_method
          :"remove_#{singularize(self[:name])}"
        end
      
        # Whether to check that an object to be disassociated is already associated to this object, false by default.
        def remove_should_check_existing?
          false
        end

        # Whether this association returns an array of objects instead of a single object,
        # true by default.
        def returns_array?
          true
        end
    
        # The columns to select when loading the association, nil by default.
        def select
          self[:select]
        end
    
        # Whether to set the reciprocal association to self when loading associated
        # records, false by default.
        def set_reciprocal_to_self?
          false
        end
    
        # Name symbol for the setter association method
        def setter_method
          :"#{self[:name]}="
        end
      end
    
      class ManyToOneAssociationReflection < AssociationReflection
        ASSOCIATION_TYPES[:many_to_one] = self
    
        # many_to_one associations can only have associated objects if none of
        # the :keys options have a nil value.
        def can_have_associated_objects?(obj)
          !self[:keys].any?{|k| obj.send(k).nil?}
        end
        
        # Whether the dataset needs a primary key to function, false for many_to_one associations.
        def dataset_need_primary_key?
          false
        end
    
        # Default foreign key name symbol for foreign key in current model's table that points to
        # the given association's table's primary key.
        def default_key
          :"#{self[:name]}_id"
        end
      
        # Whether to eagerly graph a lazy dataset, true for many_to_one associations
        # only if the key is nil.
        def eager_graph_lazy_dataset?
          self[:key].nil?
        end
    
        # The key to use for the key hash when eager loading
        def eager_loader_key
          self[:eager_loader_key] ||= self[:key]
        end
    
        # The column(s) in the associated table that the key in the current table references (either a symbol or an array).
        def primary_key
         self[:primary_key] ||= associated_class.primary_key
        end
       
        # The columns in the associated table that the key in the current table references (always an array).
        def primary_keys
         self[:primary_keys] ||= Array(primary_key)
        end
        alias associated_object_keys primary_keys
        
        # True only if the reciprocal is a one_to_many association.
        def reciprocal_array?
          !set_reciprocal_to_self?
        end
      
        # Whether this association returns an array of objects instead of a single object,
        # false for a many_to_one association.
        def returns_array?
          false
        end
        
        # True only if the reciprocal is a one_to_one association.
        def set_reciprocal_to_self?
          reciprocal
          self[:reciprocal_type] == :one_to_one
        end
    
        private
    
        # The reciprocal type of a many_to_one association is either
        # a one_to_many or a one_to_one association.
        def reciprocal_type
          self[:reciprocal_type] ||= [:one_to_many, :one_to_one]
        end
      end
    
      class OneToManyAssociationReflection < AssociationReflection
        ASSOCIATION_TYPES[:one_to_many] = self
        
        # The keys in the associated model's table related to this association
        def associated_object_keys
          self[:keys]
        end

        # one_to_many associations can only have associated objects if none of
        # the :keys options have a nil value.
        def can_have_associated_objects?(obj)
          !self[:primary_keys].any?{|k| obj.send(k).nil?}
        end
    
        # Default foreign key name symbol for key in associated table that points to
        # current table's primary key.
        def default_key
          :"#{underscore(demodulize(self[:model].name))}_id"
        end
        
        # The key to use for the key hash when eager loading
        def eager_loader_key
          self[:eager_loader_key] ||= primary_key
        end
    
        # The column in the current table that the key in the associated table references.
        def primary_key
         self[:primary_key] ||= self[:model].primary_key
        end
      
        # One to many associations set the reciprocal to self when loading associated records.
        def set_reciprocal_to_self?
          true
        end
    
        # Whether the reciprocal of this association returns an array of objects instead of a single object,
        # false for a one_to_many association.
        def reciprocal_array?
          false
        end
    
        # The one_to_many association needs to check that an object to be removed already is associated.
        def remove_should_check_existing?
          true
        end

        private
    
        # The reciprocal type of a one_to_many association is a many_to_one association.
        def reciprocal_type
          :many_to_one
        end
      end
      
      class OneToOneAssociationReflection < OneToManyAssociationReflection
        ASSOCIATION_TYPES[:one_to_one] = self
        
        # one_to_one associations return a single object, not an array
        def returns_array?
          false
        end
      end
    
      class ManyToManyAssociationReflection < AssociationReflection
        ASSOCIATION_TYPES[:many_to_many] = self
    
        # The alias to use for the associated key when eagerly loading
        def associated_key_alias
          self[:left_key_alias]
        end

        # The column to use for the associated key when eagerly loading
        def associated_key_column
          self[:left_key]
        end

        # The table containing the column to use for the associated key when eagerly loading
        def associated_key_table
          self[:join_table]
        end
        
        # many_to_many associations can only have associated objects if none of
        # the :left_primary_keys options have a nil value.
        def can_have_associated_objects?(obj)
          !self[:left_primary_keys].any?{|k| obj.send(k).nil?}
        end

        # The default associated key alias(es) to use when eager loading
        # associations via eager.
        def default_associated_key_alias
          self[:uses_left_composite_keys] ? (0...self[:left_keys].length).map{|i| :"x_foreign_key_#{i}_x"} : :x_foreign_key_x
        end
      
        # Default name symbol for the join table.
        def default_join_table
          [self[:class_name], self[:model].name].map{|i| underscore(pluralize(demodulize(i)))}.sort.join('_').to_sym
        end

        # Default foreign key name symbol for key in join table that points to
        # current table's primary key (or :left_primary_key column).
        def default_left_key
          :"#{underscore(demodulize(self[:model].name))}_id"
        end
    
        # Default foreign key name symbol for foreign key in join table that points to
        # the association's table's primary key (or :right_primary_key column).
        def default_right_key
          :"#{singularize(self[:name])}_id"
        end
      
        # The key to use for the key hash when eager loading
        def eager_loader_key
          self[:eager_loader_key] ||= self[:left_primary_key]
        end
    
        # many_to_many associations need to select a key in an associated table to eagerly load
        def eager_loading_use_associated_key?
          true
        end

        # Whether the associated object needs a primary key to be added/removed,
        # true for many_to_many associations.
        def need_associated_primary_key?
          true
        end
    
        # Returns the reciprocal association symbol, if one exists.
        def reciprocal
          return self[:reciprocal] if include?(:reciprocal)
          left_keys = self[:left_keys]
          right_keys = self[:right_keys]
          join_table = self[:join_table]
          associated_class.all_association_reflections.each do |assoc_reflect|
            if assoc_reflect[:type] == :many_to_many && assoc_reflect[:left_keys] == right_keys &&
               assoc_reflect[:right_keys] == left_keys && assoc_reflect[:join_table] == join_table &&
               assoc_reflect.associated_class == self[:model]
              return self[:reciprocal] = assoc_reflect[:name]
            end
          end
          self[:reciprocal] = nil
        end
    
        # The primary key column(s) to use in the associated table (can be symbol or array).
        def right_primary_key
          self[:right_primary_key] ||= associated_class.primary_key
        end
        
        # The primary key columns to use in the associated table (always array).
        def right_primary_keys
          self[:right_primary_keys] ||= Array(right_primary_key)
        end
        alias associated_object_keys right_primary_keys
    
        # The columns to select when loading the association, associated_class.table_name.* by default.
        def select
         return self[:select] if include?(:select)
         self[:select] ||= Sequel::SQL::ColumnAll.new(associated_class.table_name)
        end
      end
  
      # This module contains methods added to all association datasets
      module AssociationDatasetMethods
        # The model object that created the association dataset
        attr_accessor :model_object
    
        # The association reflection related to the association dataset
        attr_accessor :association_reflection
      end
      
      # Each kind of association adds a number of instance methods to the model class which
      # are specialized according to the association type and optional parameters
      # given in the definition. Example:
      # 
      #   class Project < Sequel::Model
      #     many_to_one :portfolio
      #     # or: one_to_one :portfolio
      #     one_to_many :milestones
      #     # or: many_to_many :milestones 
      #   end
      # 
      # The project class now has the following instance methods:
      # * portfolio - Returns the associated portfolio.
      # * portfolio=(obj) - Sets the associated portfolio to the object,
      #   but the change is not persisted until you save the record (for many_to_one associations).
      # * portfolio_dataset - Returns a dataset that would return the associated
      #   portfolio, only useful in fairly specific circumstances.
      # * milestones - Returns an array of associated milestones
      # * add_milestone(obj) - Associates the passed milestone with this object.
      # * remove_milestone(obj) - Removes the association with the passed milestone.
      # * remove_all_milestones - Removes associations with all associated milestones.
      # * milestones_dataset - Returns a dataset that would return the associated
      #   milestones, allowing for further filtering/limiting/etc.
      #
      # If you want to override the behavior of the add_/remove_/remove_all_/ methods
      # or the association setter method, there are private instance methods created that are prepended
      # with an underscore (e.g. _add_milestone or _portfolio=).  The private instance methods can be
      # easily overridden, but you shouldn't override the public instance methods without
      # calling super, as they deal with callbacks and caching.
      #
      # By default the classes for the associations are inferred from the association
      # name, so for example the Project#portfolio will return an instance of 
      # Portfolio, and Project#milestones will return an array of Milestone 
      # instances.  You can use the :class option to change which class is used.
      #
      # Association definitions are also reflected by the class, e.g.:
      #
      #   Project.associations
      #   => [:portfolio, :milestones]
      #   Project.association_reflection(:portfolio)
      #   => {:type => :many_to_one, :name => :portfolio, :class_name => "Portfolio"}
      module ClassMethods
        # All association reflections defined for this model (default: none).
        attr_reader :association_reflections

        # Array of all association reflections for this model class
        def all_association_reflections
          association_reflections.values
        end
        
        # Associates a related model with the current model. The following types are
        # supported:
        #
        # * :many_to_one - Foreign key in current model's table points to 
        #   associated model's primary key.  Each associated model object can
        #   be associated with more than one current model objects.  Each current
        #   model object can be associated with only one associated model object.
        # * :one_to_many - Foreign key in associated model's table points to this
        #   model's primary key.   Each current model object can be associated with
        #   more than one associated model objects.  Each associated model object
        #   can be associated with only one current model object.
        # * :one_to_one - Similar to one_to_many in terms of foreign keys, but
        #   only one object is associated to the current object through the
        #   association.  The methods created are similar to many_to_one, except
        #   that the one_to_one setter method saves the passed object.
        # * :many_to_many - A join table is used that has a foreign key that points
        #   to this model's primary key and a foreign key that points to the
        #   associated model's primary key.  Each current model object can be
        #   associated with many associated model objects, and each associated
        #   model object can be associated with many current model objects.
        #
        # The following options can be supplied:
        # * *ALL types*:
        #   - :after_add - Symbol, Proc, or array of both/either specifying a callback to call
        #     after a new item is added to the association.
        #   - :after_load - Symbol, Proc, or array of both/either specifying a callback to call
        #     after the associated record(s) have been retrieved from the database.  Not called
        #     when eager loading via eager_graph, but called when eager loading via eager.
        #   - :after_remove - Symbol, Proc, or array of both/either specifying a callback to call
        #     after an item is removed from the association.
        #   - :after_set - Symbol, Proc, or array of both/either specifying a callback to call
        #     after an item is set using the association setter method.
        #   - :allow_eager - If set to false, you cannot load the association eagerly
        #     via eager or eager_graph
        #   - :before_add - Symbol, Proc, or array of both/either specifying a callback to call
        #     before a new item is added to the association.
        #   - :before_remove - Symbol, Proc, or array of both/either specifying a callback to call
        #     before an item is removed from the association.
        #   - :before_set - Symbol, Proc, or array of both/either specifying a callback to call
        #     before an item is set using the association setter method.
        #   - :cartesian_product_number - the number of joins completed by this association that could cause more
        #     than one row for each row in the current table (default: 0 for many_to_one associations,
        #     1 for *_to_many associations).
        #   - :class - The associated class or its name. If not
        #     given, uses the association's name, which is camelized (and
        #     singularized unless the type is :many_to_one)
        #   - :clone - Merge the current options and block into the options and block used in defining
        #     the given association.  Can be used to DRY up a bunch of similar associations that
        #     all share the same options such as :class and :key, while changing the order and block used.
        #   - :conditions - The conditions to use to filter the association, can be any argument passed to filter.
        #   - :dataset - A proc that is instance_evaled to get the base dataset
        #     to use for the _dataset method (before the other options are applied).
        #   - :distinct - Use the DISTINCT clause when selecting associating object, both when
        #     lazy loading and eager loading via .eager (but not when using .eager_graph).
        #   - :eager - The associations to eagerly load via #eager when loading the associated object(s).
        #     For many_to_one associations, this is ignored unless this association is
        #     being eagerly loaded, as it doesn't save queries unless multiple objects
        #     can be loaded at once.
        #   - :eager_block - If given, use the block instead of the default block when
        #     eagerly loading.  To not use a block when eager loading (when one is used normally),
        #     set to nil.
        #   - :eager_graph - The associations to eagerly load via #eager_graph when loading the associated object(s).
        #     For many_to_one associations, this is ignored unless this association is
        #     being eagerly loaded, as it doesn't save queries unless multiple objects
        #     can be loaded at once.
        #   - :eager_grapher - A proc to use to implement eager loading via eager graph, overriding the default.
        #     Takes three arguments, a dataset, an alias to use for the table to graph for this association,
        #     and the alias that was used for the current table (since you can cascade associations),
        #     Should return a copy of the dataset with the association graphed into it.
        #   - :eager_loader - A proc to use to implement eager loading, overriding the default.  Takes three arguments,
        #     a key hash (used solely to enhance performance), an array of records,
        #     and a hash of dependent associations.  The associated records should
        #     be queried from the database and the associations cache for each
        #     record should be populated for this to work correctly.
        #   - :eager_loader_key - A symbol for the key column to use to populate the key hash
        #     for the eager loader.
        #   - :extend - A module or array of modules to extend the dataset with.
        #   - :graph_block - The block to pass to join_table when eagerly loading
        #     the association via eager_graph.
        #   - :graph_conditions - The additional conditions to use on the SQL join when eagerly loading
        #     the association via eager_graph.  Should be a hash or an array of all two pairs. If not
        #     specified, the :conditions option is used if it is a hash or array of all two pairs.
        #   - :graph_join_type - The type of SQL join to use when eagerly loading the association via
        #     eager_graph.  Defaults to :left_outer.
        #   - :graph_only_conditions - The conditions to use on the SQL join when eagerly loading
        #     the association via eager_graph, instead of the default conditions specified by the
        #     foreign/primary keys.  This option causes the :graph_conditions option to be ignored.
        #   - :graph_select - A column or array of columns to select from the associated table
        #     when eagerly loading the association via eager_graph. Defaults to all
        #     columns in the associated table.
        #   - :limit - Limit the number of records to the provided value.  Use
        #     an array with two arguments for the value to specify a limit and an offset.
        #   - :order - the column(s) by which to order the association dataset.  Can be a
        #     singular column or an array.
        #   - :order_eager_graph - Whether to add the order to the dataset's order when graphing
        #     via eager graph.  Defaults to true, so set to false to disable.
        #   - :read_only - Do not add a setter method (for many_to_one or one_to_many with :one_to_one),
        #     or add_/remove_/remove_all_ methods (for one_to_many, many_to_many)
        #   - :reciprocal - the symbol name of the reciprocal association,
        #     if it exists.  By default, sequel will try to determine it by looking at the
        #     associated model's assocations for a association that matches
        #     the current association's key(s).  Set to nil to not use a reciprocal.
        #   - :select - the attributes to select.  Defaults to the associated class's
        #     table_name.* in a many_to_many association, which means it doesn't include the attributes from the
        #     join table.  If you want to include the join table attributes, you can
        #     use this option, but beware that the join table attributes can clash with
        #     attributes from the model table, so you should alias any attributes that have
        #     the same name in both the join table and the associated table.
        #   - :validate - Set to false to not validate when implicitly saving any associated object.
        # * :many_to_one:
        #   - :key - foreign_key in current model's table that references
        #     associated model's primary key, as a symbol.  Defaults to :"#{name}_id".  Can use an
        #     array of symbols for a composite key association.
        #   - :primary_key - column in the associated table that :key option references, as a symbol.
        #     Defaults to the primary key of the associated table. Can use an
        #     array of symbols for a composite key association.
        # * :one_to_many:
        #   - :key - foreign key in associated model's table that references
        #     current model's primary key, as a symbol.  Defaults to
        #     :"#{self.name.underscore}_id".  Can use an
        #     array of symbols for a composite key association.
        #   - :primary_key - column in the current table that :key option references, as a symbol.
        #     Defaults to primary key of the current table. Can use an
        #     array of symbols for a composite key association.
        # * :many_to_many:
        #   - :graph_join_table_block - The block to pass to join_table for
        #     the join table when eagerly loading the association via eager_graph.
        #   - :graph_join_table_conditions - The additional conditions to use on the SQL join for
        #     the join table when eagerly loading the association via eager_graph. Should be a hash
        #     or an array of all two pairs.
        #   - :graph_join_table_join_type - The type of SQL join to use for the join table when eagerly
        #     loading the association via eager_graph.  Defaults to the :graph_join_type option or
        #     :left_outer.
        #   - :graph_join_table_only_conditions - The conditions to use on the SQL join for the join
        #     table when eagerly loading the association via eager_graph, instead of the default
        #     conditions specified by the foreign/primary keys.  This option causes the 
        #     :graph_join_table_conditions option to be ignored.
        #   - :join_table - name of table that includes the foreign keys to both
        #     the current model and the associated model, as a symbol.  Defaults to the name
        #     of current model and name of associated model, pluralized,
        #     underscored, sorted, and joined with '_'.
        #   - :left_key - foreign key in join table that points to current model's
        #     primary key, as a symbol. Defaults to :"#{self.name.underscore}_id".  
        #     Can use an array of symbols for a composite key association.
        #   - :left_primary_key - column in current table that :left_key points to, as a symbol.
        #     Defaults to primary key of current table.  Can use an
        #     array of symbols for a composite key association.
        #   - :right_key - foreign key in join table that points to associated
        #     model's primary key, as a symbol.  Defaults to Defaults to :"#{name.to_s.singularize}_id".
        #     Can use an array of symbols for a composite key association.
        #   - :right_primary_key - column in associated table that :right_key points to, as a symbol.
        #     Defaults to primary key of the associated table.  Can use an
        #     array of symbols for a composite key association.
        #   - :uniq - Adds a after_load callback that makes the array of objects unique.
        def associate(type, name, opts = {}, &block)
          raise(Error, 'one_to_many association type with :one_to_one option removed, used one_to_one association type') if opts[:one_to_one] && type == :one_to_many
          raise(Error, 'invalid association type') unless assoc_class = ASSOCIATION_TYPES[type]
          raise(Error, 'Model.associate name argument must be a symbol') unless Symbol === name
      
          # merge early so we don't modify opts
          orig_opts = opts.dup
          orig_opts = association_reflection(opts[:clone])[:orig_opts].merge(orig_opts) if opts[:clone]
          opts = orig_opts.merge(:type => type, :name => name, :cache => true, :model => self)
          opts[:block] = block if block
          opts = assoc_class.new.merge!(opts)
          opts[:eager_block] = block unless opts.include?(:eager_block)
          opts[:graph_join_type] ||= :left_outer
          opts[:order_eager_graph] = true unless opts.include?(:order_eager_graph)
          conds = opts[:conditions]
          opts[:graph_conditions] = conds if !opts.include?(:graph_conditions) and Sequel.condition_specifier?(conds)
          opts[:graph_conditions] = opts.fetch(:graph_conditions, []).to_a
          opts[:graph_select] = Array(opts[:graph_select]) if opts[:graph_select]
          [:before_add, :before_remove, :after_add, :after_remove, :after_load, :before_set, :after_set, :extend].each do |cb_type|
            opts[cb_type] = Array(opts[cb_type])
          end
          late_binding_class_option(opts, opts.returns_array? ? singularize(name) : name)
          
          send(:"def_#{type}", opts)
      
          orig_opts.delete(:clone)
          orig_opts.merge!(:class_name=>opts[:class_name], :class=>opts[:class], :block=>block)
          opts[:orig_opts] = orig_opts
          # don't add to association_reflections until we are sure there are no errors
          association_reflections[name] = opts
        end
        
        # The association reflection hash for the association of the given name.
        def association_reflection(name)
          association_reflections[name]
        end
        
        # Array of association name symbols
        def associations
          association_reflections.keys
        end

        # Modify and return eager loading dataset based on association options. Options:
        def eager_loading_dataset(opts, ds, select, associations)
          ds = ds.select(*select) if select
          if c = opts[:conditions]
            ds = (c.is_a?(Array) && !Sequel.condition_specifier?(c)) ? ds.filter(*c) : ds.filter(c)
          end
          ds = ds.order(*opts[:order]) if opts[:order]
          ds = ds.eager(opts[:eager]) if opts[:eager]
          ds = ds.distinct if opts[:distinct]
          if opts[:eager_graph]
            ds = ds.eager_graph(opts[:eager_graph])
            ds = ds.add_graph_aliases(opts.associated_key_alias=>[opts.associated_class.table_name, opts.associated_key_alias, SQL::QualifiedIdentifier.new(opts.associated_key_table, opts.associated_key_column)]) if opts.eager_loading_use_associated_key?
          elsif opts.eager_loading_use_associated_key?
            ds = if opts[:uses_left_composite_keys]
              t = opts.associated_key_table
              ds.select_more(*opts.associated_key_alias.zip(opts.associated_key_column).map{|a, c| SQL::AliasedExpression.new(SQL::QualifiedIdentifier.new(t, c), a)})
            else
              ds.select_more(SQL::AliasedExpression.new(SQL::QualifiedIdentifier.new(opts.associated_key_table, opts.associated_key_column), opts.associated_key_alias)) 
            end
          end
          ds = ds.eager(associations) unless Array(associations).empty?
          ds = opts[:eager_block].call(ds) if opts[:eager_block]
          ds
        end

        # Copy the association reflections to the subclass
        def inherited(subclass)
          super
          subclass.instance_variable_set(:@association_reflections, @association_reflections.dup)
        end
      
        # Shortcut for adding a many_to_many association, see associate
        def many_to_many(name, opts={}, &block)
          associate(:many_to_many, name, opts, &block)
        end
        
        # Shortcut for adding a many_to_one association, see associate
        def many_to_one(name, opts={}, &block)
          associate(:many_to_one, name, opts, &block)
        end
        
        # Shortcut for adding a one_to_many association, see associate
        def one_to_many(name, opts={}, &block)
          associate(:one_to_many, name, opts, &block)
        end

        # Shortcut for adding a one_to_one association, see associate.
        def one_to_one(name, opts={}, &block)
          associate(:one_to_one, name, opts, &block)
        end
        
        private
      
        # Add a method to the module included in the class, so the method
        # can be easily overridden in the class itself while allowing for
        # super to be called.
        def association_module_def(name, &block)
          overridable_methods_module.module_eval{define_method(name, &block)}
        end
      
        # Add a private method to the module included in the class.
        def association_module_private_def(name, &block)
          association_module_def(name, &block)
          overridable_methods_module.send(:private, name)
        end
      
        # Add the add_ instance method 
        def def_add_method(opts)
          association_module_def(opts.add_method){|o,*args| add_associated_object(opts, o, *args)}
        end
      
        # Adds methods related to the association's dataset to the module included in the class.
        def def_association_dataset_methods(opts)
          # If a block is given, define a helper method for it, because it takes
          # an argument.  This is unnecessary in Ruby 1.9, as that has instance_exec.
          association_module_private_def(opts.dataset_helper_method, &opts[:block]) if opts[:block]
          association_module_private_def(opts._dataset_method, &opts[:dataset])
          association_module_def(opts.dataset_method){_dataset(opts)}
          def_association_method(opts)
        end

        # Adds method for retrieving the associated objects to the module included in the class.
        def def_association_method(opts)
          association_module_def(opts.association_method){|*reload| load_associated_objects(opts, reload[0])}
        end
      
        # Adds many_to_many association instance methods
        def def_many_to_many(opts)
          name = opts[:name]
          model = self
          left = (opts[:left_key] ||= opts.default_left_key)
          lcks = opts[:left_keys] = Array(left)
          right = (opts[:right_key] ||= opts.default_right_key)
          rcks = opts[:right_keys] = Array(right)
          left_pk = (opts[:left_primary_key] ||= self.primary_key)
          lcpks = opts[:left_primary_keys] = Array(left_pk)
          raise(Error, 'mismatched number of left composite keys') unless lcks.length == lcpks.length
          raise(Error, 'mismatched number of right composite keys') if opts[:right_primary_key] && rcks.length != Array(opts[:right_primary_key]).length
          uses_lcks = opts[:uses_left_composite_keys] = lcks.length > 1
          uses_rcks = opts[:uses_right_composite_keys] = rcks.length > 1
          opts[:cartesian_product_number] ||= 1
          join_table = (opts[:join_table] ||= opts.default_join_table)
          left_key_alias = opts[:left_key_alias] ||= opts.default_associated_key_alias
          graph_jt_conds = opts[:graph_join_table_conditions] = opts.fetch(:graph_join_table_conditions, []).to_a
          opts[:graph_join_table_join_type] ||= opts[:graph_join_type]
          opts[:after_load].unshift(:array_uniq!) if opts[:uniq]
          opts[:dataset] ||= proc{opts.associated_class.inner_join(join_table, rcks.zip(opts.right_primary_keys) + lcks.zip(lcpks.map{|k| send(k)}))}
          database = db
          
          opts[:eager_loader] ||= proc do |key_hash, records, associations|
            h = key_hash[left_pk]
            records.each{|object| object.associations[name] = []}
            r = uses_rcks ? rcks.zip(opts.right_primary_keys) : [[right, opts.right_primary_key]]
            l = uses_lcks ? [[lcks.map{|k| SQL::QualifiedIdentifier.new(join_table, k)}, SQL::SQLArray.new(h.keys)]] : [[left, h.keys]]
            model.eager_loading_dataset(opts, opts.associated_class.inner_join(join_table, r + l), Array(opts.select), associations).all do |assoc_record|
              hash_key = if uses_lcks
                left_key_alias.map{|k| assoc_record.values.delete(k)}
              else
                assoc_record.values.delete(left_key_alias)
              end
              next unless objects = h[hash_key]
              objects.each{|object| object.associations[name].push(assoc_record)}
            end
          end
          
          join_type = opts[:graph_join_type]
          select = opts[:graph_select]
          use_only_conditions = opts.include?(:graph_only_conditions)
          only_conditions = opts[:graph_only_conditions]
          conditions = opts[:graph_conditions]
          graph_block = opts[:graph_block]
          use_jt_only_conditions = opts.include?(:graph_join_table_only_conditions)
          jt_only_conditions = opts[:graph_join_table_only_conditions]
          jt_join_type = opts[:graph_join_table_join_type]
          jt_graph_block = opts[:graph_join_table_block]
          opts[:eager_grapher] ||= proc do |ds, assoc_alias, table_alias|
            ds = ds.graph(join_table, use_jt_only_conditions ? jt_only_conditions : lcks.zip(lcpks) + graph_jt_conds, :select=>false, :table_alias=>ds.unused_table_alias(join_table), :join_type=>jt_join_type, :implicit_qualifier=>table_alias, :from_self_alias=>ds.opts[:eager_graph][:master], &jt_graph_block)
            ds.graph(opts.associated_class, use_only_conditions ? only_conditions : opts.right_primary_keys.zip(rcks) + conditions, :select=>select, :table_alias=>assoc_alias, :join_type=>join_type, &graph_block)
          end
      
          def_association_dataset_methods(opts)
      
          return if opts[:read_only]
      
          association_module_private_def(opts._add_method) do |o|
            h = {}
            lcks.zip(lcpks).each{|k, pk| h[k] = send(pk)}
            rcks.zip(opts.right_primary_keys).each{|k, pk| h[k] = o.send(pk)}
            database.dataset.from(join_table).insert(h)
          end
          association_module_private_def(opts._remove_method) do |o|
            database.dataset.from(join_table).filter(lcks.zip(lcpks.map{|k| send(k)}) + rcks.zip(opts.right_primary_keys.map{|k| o.send(k)})).delete
          end
          association_module_private_def(opts._remove_all_method) do
            _apply_association_options(opts, database.dataset.from(join_table).filter(lcks.zip(lcpks.map{|k| send(k)}))).delete
          end
      
          def_add_method(opts)
          def_remove_methods(opts)
        end
        
        # Adds many_to_one association instance methods
        def def_many_to_one(opts)
          name = opts[:name]
          model = self
          opts[:key] = opts.default_key unless opts.include?(:key)
          key = opts[:key]
          cks = opts[:keys] = Array(opts[:key])
          raise(Error, 'mismatched number of composite keys') if opts[:primary_key] && cks.length != Array(opts[:primary_key]).length
          uses_cks = opts[:uses_composite_keys] = cks.length > 1
          opts[:cartesian_product_number] ||= 0
          opts[:dataset] ||= proc do
            klass = opts.associated_class
            klass.filter(opts.primary_keys.map{|k| SQL::QualifiedIdentifier.new(klass.table_name, k)}.zip(cks.map{|k| send(k)}))
          end
          opts[:eager_loader] ||= proc do |key_hash, records, associations|
            h = key_hash[key]
            keys = h.keys
            # Default the cached association to nil, so any object that doesn't have it
            # populated will have cached the negative lookup.
            records.each{|object| object.associations[name] = nil}
            # Skip eager loading if no objects have a foreign key for this association
            unless keys.empty?
              klass = opts.associated_class
              model.eager_loading_dataset(opts, klass.filter(uses_cks ? {opts.primary_keys.map{|k| SQL::QualifiedIdentifier.new(klass.table_name, k)}=>SQL::SQLArray.new(keys)} : {SQL::QualifiedIdentifier.new(klass.table_name, opts.primary_key)=>keys}), opts.select, associations).all do |assoc_record|
                hash_key = uses_cks ? opts.primary_keys.map{|k| assoc_record.send(k)} : assoc_record.send(opts.primary_key)
                next unless objects = h[hash_key]
                objects.each{|object| object.associations[name] = assoc_record}
              end
            end
          end
      
          join_type = opts[:graph_join_type]
          select = opts[:graph_select]
          use_only_conditions = opts.include?(:graph_only_conditions)
          only_conditions = opts[:graph_only_conditions]
          conditions = opts[:graph_conditions]
          graph_block = opts[:graph_block]
          opts[:eager_grapher] ||= proc do |ds, assoc_alias, table_alias|
            ds.graph(opts.associated_class, use_only_conditions ? only_conditions : opts.primary_keys.zip(cks) + conditions, :select=>select, :table_alias=>assoc_alias, :join_type=>join_type, :implicit_qualifier=>table_alias, :from_self_alias=>ds.opts[:eager_graph][:master], &graph_block)
          end
      
          def_association_dataset_methods(opts)
          
          return if opts[:read_only]
      
          association_module_private_def(opts._setter_method){|o| cks.zip(opts.primary_keys).each{|k, pk| send(:"#{k}=", (o.send(pk) if o))}}
          association_module_def(opts.setter_method){|o| set_associated_object(opts, o)}
        end
        
        # Adds one_to_many association instance methods
        def def_one_to_many(opts)
          one_to_one = opts[:type] == :one_to_one
          name = opts[:name]
          model = self
          key = (opts[:key] ||= opts.default_key)
          cks = opts[:keys] = Array(key)
          primary_key = (opts[:primary_key] ||= self.primary_key)
          cpks = opts[:primary_keys] = Array(primary_key)
          raise(Error, 'mismatched number of composite keys') unless cks.length == cpks.length
          uses_cks = opts[:uses_composite_keys] = cks.length > 1
          opts[:dataset] ||= proc do
            klass = opts.associated_class
            klass.filter(cks.map{|k| SQL::QualifiedIdentifier.new(klass.table_name, k)}.zip(cpks.map{|k| send(k)}))
          end
          opts[:eager_loader] ||= proc do |key_hash, records, associations|
            h = key_hash[primary_key]
            if one_to_one
              records.each{|object| object.associations[name] = nil}
            else
              records.each{|object| object.associations[name] = []}
            end
            reciprocal = opts.reciprocal
            klass = opts.associated_class
            model.eager_loading_dataset(opts, klass.filter(uses_cks ? {cks.map{|k| SQL::QualifiedIdentifier.new(klass.table_name, k)}=>SQL::SQLArray.new(h.keys)} : {SQL::QualifiedIdentifier.new(klass.table_name, key)=>h.keys}), opts.select, associations).all do |assoc_record|
              hash_key = uses_cks ? cks.map{|k| assoc_record.send(k)} : assoc_record.send(key)
              next unless objects = h[hash_key]
              if one_to_one
                objects.each do |object| 
                  object.associations[name] = assoc_record
                  assoc_record.associations[reciprocal] = object if reciprocal
                end
              else
                objects.each do |object| 
                  object.associations[name].push(assoc_record)
                  assoc_record.associations[reciprocal] = object if reciprocal
                end
              end
            end
          end
          
          join_type = opts[:graph_join_type]
          select = opts[:graph_select]
          use_only_conditions = opts.include?(:graph_only_conditions)
          only_conditions = opts[:graph_only_conditions]
          conditions = opts[:graph_conditions]
          opts[:cartesian_product_number] ||= one_to_one ? 0 : 1
          graph_block = opts[:graph_block]
          opts[:eager_grapher] ||= proc do |ds, assoc_alias, table_alias|
            ds = ds.graph(opts.associated_class, use_only_conditions ? only_conditions : cks.zip(cpks) + conditions, :select=>select, :table_alias=>assoc_alias, :join_type=>join_type, :implicit_qualifier=>table_alias, :from_self_alias=>ds.opts[:eager_graph][:master], &graph_block)
            # We only load reciprocals for one_to_many associations, as other reciprocals don't make sense
            ds.opts[:eager_graph][:reciprocals][assoc_alias] = opts.reciprocal
            ds
          end
      
          def_association_dataset_methods(opts)
          
          ck_nil_hash ={}
          cks.each{|k| ck_nil_hash[k] = nil}

          unless opts[:read_only]
            validate = opts[:validate]

            if one_to_one
              association_module_private_def(opts._setter_method) do |o|
                up_ds = _apply_association_options(opts, opts.associated_class.filter(cks.zip(cpks.map{|k| send(k)})))
                if o
                  up_ds = up_ds.exclude(o.pk_hash)
                  cks.zip(cpks).each{|k, pk| o.send(:"#{k}=", send(pk))}
                end
                update_database = lambda do
                  up_ds.update(ck_nil_hash)
                  o.save(:validate=>validate) || raise(Sequel::Error, "invalid associated object, cannot save") if o
                end
                use_transactions && o ? db.transaction(opts){update_database.call} : update_database.call
              end
              association_module_def(opts.setter_method){|o| set_one_to_one_associated_object(opts, o)}
            else 
              association_module_private_def(opts._add_method) do |o|
                cks.zip(cpks).each{|k, pk| o.send(:"#{k}=", send(pk))}
                o.save(:validate=>validate) || raise(Sequel::Error, "invalid associated object, cannot save")
              end
              def_add_method(opts)
      
              association_module_private_def(opts._remove_method) do |o|
                cks.each{|k| o.send(:"#{k}=", nil)}
                o.save(:validate=>validate) || raise(Sequel::Error, "invalid associated object, cannot save")
              end
              association_module_private_def(opts._remove_all_method) do
                _apply_association_options(opts, opts.associated_class.filter(cks.zip(cpks.map{|k| send(k)}))).update(ck_nil_hash)
              end
              def_remove_methods(opts)
            end
          end
        end
        alias def_one_to_one def_one_to_many
        
        # Add the remove_ and remove_all instance methods
        def def_remove_methods(opts)
          association_module_def(opts.remove_method){|o,*args| remove_associated_object(opts, o, *args)}
          association_module_def(opts.remove_all_method){|*args| remove_all_associated_objects(opts, *args)}
        end
      end

      # Private instance methods used to implement the associations support.
      module InstanceMethods
        # The currently cached associations.  A hash with the keys being the
        # association name symbols and the values being the associated object
        # or nil (many_to_one), or the array of associated objects (*_to_many).
        def associations
          @associations ||= {}
        end
      
        # Used internally by the associations code, like pk but doesn't raise
        # an Error if the model has no primary key.
        def pk_or_nil
          key = primary_key
          key.is_a?(Array) ? key.map{|k| @values[k]} : @values[key]
        end

        private
        
        def _apply_association_options(opts, ds)
          ds.extend(AssociationDatasetMethods)
          ds.model_object = self
          ds.association_reflection = opts
          opts[:extend].each{|m| ds.extend(m)}
          ds = ds.select(*opts.select) if opts.select
          if c = opts[:conditions]
            ds = (c.is_a?(Array) && !Sequel.condition_specifier?(c)) ? ds.filter(*c) : ds.filter(c)
          end
          ds = ds.order(*opts[:order]) if opts[:order]
          ds = ds.limit(*opts[:limit]) if opts[:limit]
          ds = ds.limit(1) if !opts.returns_array? && opts[:key]
          ds = ds.eager(*opts[:eager]) if opts[:eager]
          ds = ds.distinct if opts[:distinct]
          ds = ds.eager_graph(opts[:eager_graph]) if opts[:eager_graph] && opts.eager_graph_lazy_dataset?
          ds = send(opts.dataset_helper_method, ds) if opts[:block]
          ds
        end

        # Backbone behind association dataset methods
        def _dataset(opts)
          raise(Sequel::Error, "model object #{inspect} does not have a primary key") if opts.dataset_need_primary_key? && !pk
          _apply_association_options(opts, send(opts._dataset_method))
        end

        # Return the associated objects from the dataset, without callbacks, reciprocals, and caching.
        def _load_associated_objects(opts)
          if opts.returns_array?
            opts.can_have_associated_objects?(self) ? send(opts.dataset_method).all : []
          else
            if opts.can_have_associated_objects?(self)
              send(opts.dataset_method).all.first
            end
          end
        end
        
        # Clear the associations cache when refreshing
        def _refresh(dataset)
          associations.clear
          super
        end

        # Add the given associated object to the given association
        def add_associated_object(opts, o, *args)
          klass = opts.associated_class
          if o.is_a?(Hash)
            o = klass.new(o)
          elsif !o.is_a?(klass)
            raise(Sequel::Error, "associated object #{o.inspect} not of correct type #{klass}")
          end
          raise(Sequel::Error, "model object #{inspect} does not have a primary key") unless pk
          if opts.need_associated_primary_key?
            o.save(:validate=>opts[:validate]) if o.new?
            raise(Sequel::Error, "associated object #{o.inspect} does not have a primary key") unless o.pk
          end
          return if run_association_callbacks(opts, :before_add, o) == false
          send(opts._add_method, o, *args)
          if array = associations[opts[:name]] and !array.include?(o)
            array.push(o)
          end
          add_reciprocal_object(opts, o)
          run_association_callbacks(opts, :after_add, o)
          o
        end

        # Add/Set the current object to/as the given object's reciprocal association.
        def add_reciprocal_object(opts, o)
          return unless reciprocal = opts.reciprocal
          if opts.reciprocal_array?
            if array = o.associations[reciprocal] and !array.include?(self)
              array.push(self)
            end
          else
            o.associations[reciprocal] = self
          end
        end
        
        # Call uniq! on the given array. This is used by the :uniq option,
        # and is an actual method for memory reasons.
        def array_uniq!(a)
          a.uniq!
        end

        # Load the associated objects using the dataset, handling callbacks, reciprocals, and caching.
        def load_associated_objects(opts, reload=false)
          name = opts[:name]
          if associations.include?(name) and !reload
            associations[name]
          else
            objs = _load_associated_objects(opts)
            run_association_callbacks(opts, :after_load, objs)
            if opts.set_reciprocal_to_self?
              if opts.returns_array?
                objs.each{|o| add_reciprocal_object(opts, o)}
              elsif objs
                add_reciprocal_object(opts, objs)
              end
            end
            associations[name] = objs
          end
        end

        # Remove all associated objects from the given association
        def remove_all_associated_objects(opts, *args)
          raise(Sequel::Error, "model object #{inspect} does not have a primary key") unless pk
          send(opts._remove_all_method, *args)
          ret = associations[opts[:name]].each{|o| remove_reciprocal_object(opts, o)} if associations.include?(opts[:name])
          associations[opts[:name]] = []
          ret
        end

        # Remove the given associated object from the given association
        def remove_associated_object(opts, o, *args)
          klass = opts.associated_class
          if o.is_a?(Integer) || o.is_a?(String) || o.is_a?(Array)
            key = o
            pkh = klass.primary_key_hash(key)
            raise(Sequel::Error, "no object with key(s) #{key} is currently associated to #{inspect}") unless o = (opts.remove_should_check_existing? ? send(opts.dataset_method) : klass).first(pkh)
          elsif !o.is_a?(klass)
            raise(Sequel::Error, "associated object #{o.inspect} not of correct type #{klass}")
          elsif opts.remove_should_check_existing? && send(opts.dataset_method).filter(o.pk_hash).empty?
            raise(Sequel::Error, "associated object #{o.inspect} is not currently associated to #{inspect}")
          end
          raise(Sequel::Error, "model object #{inspect} does not have a primary key") unless pk
          raise(Sequel::Error, "associated object #{o.inspect} does not have a primary key") if opts.need_associated_primary_key? && !o.pk
          return if run_association_callbacks(opts, :before_remove, o) == false
          send(opts._remove_method, o, *args)
          associations[opts[:name]].delete_if{|x| o === x} if associations.include?(opts[:name])
          remove_reciprocal_object(opts, o)
          run_association_callbacks(opts, :after_remove, o)
          o
        end

        # Remove/unset the current object from/as the given object's reciprocal association.
        def remove_reciprocal_object(opts, o)
          return unless reciprocal = opts.reciprocal
          if opts.reciprocal_array?
            if array = o.associations[reciprocal]
              array.delete_if{|x| self === x}
            end
          else
            o.associations[reciprocal] = nil
          end
        end

        # Run the callback for the association with the object.
        def run_association_callbacks(reflection, callback_type, object)
          raise_error = raise_on_save_failure || !reflection.returns_array?
          stop_on_false = [:before_add, :before_remove, :before_set].include?(callback_type)
          reflection[callback_type].each do |cb|
            res = case cb
            when Symbol
              send(cb, object)
            when Proc
              cb.call(self, object)
            else
              raise Error, "callbacks should either be Procs or Symbols"
            end
            if res == false and stop_on_false
              raise(BeforeHookFailed, "Unable to modify association for #{inspect}: one of the #{callback_type} hooks returned false") if raise_error
              return false
            end
          end
        end

        # Set the given object as the associated object for the given association
        def set_associated_object(opts, o)
          raise(Error, "associated object #{o.inspect} does not have a primary key") if o && !o.pk
          run_association_callbacks(opts, :before_set, o)
          if a = associations[opts[:name]]
            remove_reciprocal_object(opts, a)
          end
          send(opts._setter_method, o)
          associations[opts[:name]] = o
          add_reciprocal_object(opts, o) if o
          run_association_callbacks(opts, :after_set, o)
          o
        end
        
        # Set the given object as the associated object for the given association
        def set_one_to_one_associated_object(opts, o)
          raise(Error, "object #{inspect} does not have a primary key") unless pk
          run_association_callbacks(opts, :before_set, o)
          if a = associations[opts[:name]]
            remove_reciprocal_object(opts, a)
          end
          send(opts._setter_method, o)
          associations[opts[:name]] = o
          add_reciprocal_object(opts, o) if o
          run_association_callbacks(opts, :after_set, o)
          o
        end
      end

      # Eager loading makes it so that you can load all associated records for a
      # set of objects in a single query, instead of a separate query for each object.
      #
      # Two separate implementations are provided.  #eager should be used most of the
      # time, as it loads associated records using one query per association.  However,
      # it does not allow you the ability to filter based on columns in associated tables.  #eager_graph loads
      # all records in one query.  Using #eager_graph you can filter based on columns in associated
      # tables.  However, #eager_graph can be slower than #eager, especially if multiple
      # *_to_many associations are joined.
      #
      # You can cascade the eager loading (loading associations' associations)
      # with no limit to the depth of the cascades.  You do this by passing a hash to #eager or #eager_graph
      # with the keys being associations of the current model and values being
      # associations of the model associated with the current model via the key.
      #  
      # The arguments can be symbols or hashes with symbol keys (for cascaded
      # eager loading). Examples:
      #
      #   Album.eager(:artist).all
      #   Album.eager_graph(:artist).all
      #   Album.eager(:artist, :genre).all
      #   Album.eager_graph(:artist, :genre).all
      #   Album.eager(:artist).eager(:genre).all
      #   Album.eager_graph(:artist).eager(:genre).all
      #   Artist.eager(:albums=>:tracks).all
      #   Artist.eager_graph(:albums=>:tracks).all
      #   Artist.eager(:albums=>{:tracks=>:genre}).all
      #   Artist.eager_graph(:albums=>{:tracks=>:genre}).all
      module DatasetMethods
        # Add the #eager! and #eager_graph! mutation methods to the dataset.
        def self.extended(obj)
          obj.def_mutation_method(:eager, :eager_graph)
        end
      
        # The preferred eager loading method.  Loads all associated records using one
        # query for each association.
        #
        # The basic idea for how it works is that the dataset is first loaded normally.
        # Then it goes through all associations that have been specified via eager.
        # It loads each of those associations separately, then associates them back
        # to the original dataset via primary/foreign keys.  Due to the necessity of
        # all objects being present, you need to use .all to use eager loading, as it
        # can't work with .each.
        #
        # This implementation avoids the complexity of extracting an object graph out
        # of a single dataset, by building the object graph out of multiple datasets,
        # one for each association.  By using a separate dataset for each association,
        # it avoids problems such as aliasing conflicts and creating cartesian product
        # result sets if multiple *_to_many eager associations are requested.
        #
        # One limitation of using this method is that you cannot filter the dataset
        # based on values of columns in an associated table, since the associations are loaded
        # in separate queries.  To do that you need to load all associations in the
        # same query, and extract an object graph from the results of that query. If you
        # need to filter based on columns in associated tables, look at #eager_graph
        # or join the tables you need to filter on manually. 
        #
        # Each association's order, if defined, is respected. Eager also works
        # on a limited dataset, but does not use any :limit options for associations.
        # If the association uses a block or has an :eager_block argument, it is used.
        def eager(*associations)
          opt = @opts[:eager]
          opt = opt ? opt.dup : {}
          associations.flatten.each do |association|
            case association
              when Symbol
                check_association(model, association)
                opt[association] = nil
              when Hash
                association.keys.each{|assoc| check_association(model, assoc)}
                opt.merge!(association)
              else raise(Sequel::Error, 'Associations must be in the form of a symbol or hash')
            end
          end
          clone(:eager=>opt)
        end
      
        # The secondary eager loading method.  Loads all associations in a single query. This
        # method should only be used if you need to filter based on columns in associated tables.
        #
        # This method builds an object graph using Dataset#graph.  Then it uses the graph
        # to build the associations, and finally replaces the graph with a simple array
        # of model objects.
        #
        # Be very careful when using this with multiple *_to_many associations, as you can
        # create large cartesian products.  If you must graph multiple *_to_many associations,
        # make sure your filters are specific if you have a large database.
        # 
        # Each association's order, if definied, is respected. #eager_graph probably
        # won't work correctly on a limited dataset, unless you are
        # only graphing many_to_one associations.
        # 
        # Does not use the block defined for the association, since it does a single query for
        # all objects.  You can use the :graph_* association options to modify the SQL query.
        #
        # Like eager, you need to call .all on the dataset for the eager loading to work.  If you just
        # call each, you will get a normal graphed result back (a hash with model object values).
        def eager_graph(*associations)
          ds = if @opts[:eager_graph]
            self
          else
            # Each of the following have a symbol key for the table alias, with the following values: 
            # :reciprocals - the reciprocal instance variable to use for this association
            # :requirements - array of requirements for this association
            # :alias_association_type_map - the type of association for this association
            # :alias_association_name_map - the name of the association for this association
            clone(:eager_graph=>{:requirements=>{}, :master=>alias_symbol(first_source), :alias_association_type_map=>{}, :alias_association_name_map=>{}, :reciprocals=>{}, :cartesian_product_number=>0})
          end
          ds.eager_graph_associations(ds, model, ds.opts[:eager_graph][:master], [], *associations)
        end
        
        # Do not attempt to split the result set into associations,
        # just return results as simple objects.  This is useful if you
        # want to use eager_graph as a shortcut to have all of the joins
        # and aliasing set up, but want to do something else with the dataset.
        def ungraphed
          super.clone(:eager_graph=>nil)
        end
      
        protected
      
        # Call graph on the association with the correct arguments,
        # update the eager_graph data structure, and recurse into
        # eager_graph_associations if there are any passed in associations
        # (which would be dependencies of the current association)
        #
        # Arguments:
        # * ds - Current dataset
        # * model - Current Model
        # * ta - table_alias used for the parent association
        # * requirements - an array, used as a stack for requirements
        # * r - association reflection for the current association
        # * *associations - any associations dependent on this one
        def eager_graph_association(ds, model, ta, requirements, r, *associations)
          klass = r.associated_class
          assoc_name = r[:name]
          assoc_table_alias = ds.unused_table_alias(assoc_name)
          ds = r[:eager_grapher].call(ds, assoc_table_alias, ta)
          ds = ds.order_more(*qualified_expression(r[:order], assoc_table_alias)) if r[:order] and r[:order_eager_graph]
          eager_graph = ds.opts[:eager_graph]
          eager_graph[:requirements][assoc_table_alias] = requirements.dup
          eager_graph[:alias_association_name_map][assoc_table_alias] = assoc_name
          eager_graph[:alias_association_type_map][assoc_table_alias] = r.returns_array?
          eager_graph[:cartesian_product_number] += r[:cartesian_product_number] || 2
          ds = ds.eager_graph_associations(ds, r.associated_class, assoc_table_alias, requirements + [assoc_table_alias], *associations) unless associations.empty?
          ds
        end
      
        # Check the associations are valid for the given model.
        # Call eager_graph_association on each association.
        #
        # Arguments:
        # * ds - Current dataset
        # * model - Current Model
        # * ta - table_alias used for the parent association
        # * requirements - an array, used as a stack for requirements
        # * *associations - the associations to add to the graph
        def eager_graph_associations(ds, model, ta, requirements, *associations)
          return ds if associations.empty?
          associations.flatten.each do |association|
            ds = case association
            when Symbol
              ds.eager_graph_association(ds, model, ta, requirements, check_association(model, association))
            when Hash
              association.each do |assoc, assoc_assocs|
                ds = ds.eager_graph_association(ds, model, ta, requirements, check_association(model, assoc), assoc_assocs)
              end
              ds
            else raise(Sequel::Error, 'Associations must be in the form of a symbol or hash')
            end
          end
          ds
        end
      
        # Build associations out of the array of returned object graphs.
        def eager_graph_build_associations(record_graphs)
          eager_graph = @opts[:eager_graph]
          master = eager_graph[:master]
          requirements = eager_graph[:requirements]
          alias_map = eager_graph[:alias_association_name_map]
          type_map = eager_graph[:alias_association_type_map]
          reciprocal_map = eager_graph[:reciprocals]
      
          # Make dependency map hash out of requirements array for each association.
          # This builds a tree of dependencies that will be used for recursion
          # to ensure that all parts of the object graph are loaded into the
          # appropriate subordinate association.
          dependency_map = {}
          # Sort the associations by requirements length, so that
          # requirements are added to the dependency hash before their
          # dependencies.
          requirements.sort_by{|a| a[1].length}.each do |ta, deps|
            if deps.empty?
              dependency_map[ta] = {}
            else
              deps = deps.dup
              hash = dependency_map[deps.shift]
              deps.each do |dep|
                hash = hash[dep]
              end
              hash[ta] = {}
            end
          end
      
          # This mapping is used to make sure that duplicate entries in the
          # result set are mapped to a single record.  For example, using a
          # single one_to_many association with 10 associated records,
          # the main object will appear in the object graph 10 times.
          # We map by primary key, if available, or by the object's entire values,
          # if not. The mapping must be per table, so create sub maps for each table
          # alias.
          records_map = {master=>{}}
          alias_map.keys.each{|ta| records_map[ta] = {}}
      
          # This will hold the final record set that we will be replacing the object graph with.
          records = []
          record_graphs.each do |record_graph|
            primary_record = record_graph[master]
            key = primary_record.pk_or_nil || primary_record.values.sort_by{|x| x[0].to_s}
            if cached_pr = records_map[master][key]
              primary_record = cached_pr
            else
              records_map[master][key] = primary_record
              # Only add it to the list of records to return if it is a new record
              records.push(primary_record)
            end
            # Build all associations for the current object and it's dependencies
            eager_graph_build_associations_graph(dependency_map, alias_map, type_map, reciprocal_map, records_map, primary_record, record_graph)
          end
      
          # Remove duplicate records from all associations if this graph could possibly be a cartesian product
          eager_graph_make_associations_unique(records, dependency_map, alias_map, type_map) if eager_graph[:cartesian_product_number] > 1
          
          # Replace the array of object graphs with an array of model objects
          record_graphs.replace(records)
        end
      
        private
      
        # Make sure the association is valid for this model, and return the related AssociationReflection.
        def check_association(model, association)
          raise(Sequel::Error, "Invalid association #{association} for #{model.name}") unless reflection = model.association_reflection(association)
          raise(Sequel::Error, "Eager loading is not allowed for #{model.name} association #{association}") if reflection[:allow_eager] == false
          reflection
        end
      
        # Build associations for the current object.  This is called recursively
        # to build all dependencies.
        def eager_graph_build_associations_graph(dependency_map, alias_map, type_map, reciprocal_map, records_map, current, record_graph)
          return if dependency_map.empty?
          # Don't clobber the instance variable array for *_to_many associations if it has already been setup
          dependency_map.keys.each do |ta|
            assoc_name = alias_map[ta]
            current.associations[assoc_name] = type_map[ta] ? [] : nil unless current.associations.include?(assoc_name)
          end
          dependency_map.each do |ta, deps|
            next unless rec = record_graph[ta]
            key = rec.pk_or_nil || rec.values.sort_by{|x| x[0].to_s}
            if cached_rec = records_map[ta][key]
              rec = cached_rec
            else
              records_map[ta][key] = rec
            end
            assoc_name = alias_map[ta]
            if type_map[ta]
              current.associations[assoc_name].push(rec) 
              if reciprocal = reciprocal_map[ta]
                rec.associations[reciprocal] = current
              end
            else
              current.associations[assoc_name] = rec
            end
            # Recurse into dependencies of the current object
            eager_graph_build_associations_graph(deps, alias_map, type_map, reciprocal_map, records_map, rec, record_graph)
          end
        end
      
        # If the result set is the result of a cartesian product, then it is possible that
        # there are multiple records for each association when there should only be one.
        # In that case, for each object in all associations loaded via #eager_graph, run
        # uniq! on the association to make sure no duplicate records show up.
        # Note that this can cause legitimate duplicate records to be removed.
        def eager_graph_make_associations_unique(records, dependency_map, alias_map, type_map)
          records.each do |record|
            dependency_map.each do |ta, deps|
              list = record.send(alias_map[ta])
              list = if type_map[ta]
                list.uniq!
              else
                [list] if list
              end
              # Recurse into dependencies
              eager_graph_make_associations_unique(list, deps, alias_map, type_map) if list
            end
          end
        end
      
        # Eagerly load all specified associations 
        def eager_load(a, eager_assoc=@opts[:eager])
          return if a.empty?
          # Key is foreign/primary key name symbol
          # Value is hash with keys being foreign/primary key values (generally integers)
          #  and values being an array of current model objects with that
          #  specific foreign/primary key
          key_hash = {}
          # Reflections for all associations to eager load
          reflections = eager_assoc.keys.collect{|assoc| model.association_reflection(assoc)}
      
          # Populate keys to monitor
          reflections.each{|reflection| key_hash[reflection.eager_loader_key] ||= Hash.new{|h,k| h[k] = []}}
          
          # Associate each object with every key being monitored
          a.each do |rec|
            key_hash.each do |key, id_map|
              case key
              when Array
                id_map[key.map{|k| rec[k]}] << rec if key.all?{|k| rec[k]}
              when Symbol
                id_map[rec[key]] << rec if rec[key]
              end
            end
          end
          
          reflections.each do |r|
            r[:eager_loader].call(key_hash, a, eager_assoc[r[:name]])
            a.each{|object| object.send(:run_association_callbacks, r, :after_load, object.associations[r[:name]])} unless r[:after_load].empty?
          end 
        end
      
        # Build associations from the graph if #eager_graph was used, 
        # and/or load other associations if #eager was used.
        def post_load(all_records)
          eager_graph_build_associations(all_records) if @opts[:eager_graph]
          eager_load(all_records) if @opts[:eager]
          super
        end
      end
    end
  end
end
