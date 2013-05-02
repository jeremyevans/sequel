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
          cached_fetch(:class){constantize(self[:class_name])}
        end

        # The dataset associated via this association, with the non-instance specific
        # changes already applied.
        def associated_dataset
          cached_fetch(:_dataset){apply_dataset_changes(associated_class.dataset.clone)}
        end

        # Apply all non-instance specific changes to the given dataset and return it.
        def apply_dataset_changes(ds)
          ds.extend(AssociationDatasetMethods)
          ds.association_reflection = self
          self[:extend].each{|m| ds.extend(m)}
          ds = ds.select(*select) if select
          if c = self[:conditions]
            ds = (c.is_a?(Array) && !Sequel.condition_specifier?(c)) ? ds.where(*c) : ds.where(c)
          end
          ds = ds.order(*self[:order]) if self[:order]
          ds = ds.limit(*self[:limit]) if self[:limit]
          ds = ds.limit(1) if !returns_array? && self[:key]
          ds = ds.eager(*self[:eager]) if self[:eager]
          ds = ds.distinct if self[:distinct]
          ds
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
      
        # Whether the dataset needs a primary key to function, true by default.
        def dataset_need_primary_key?
          true
        end
    
        # The eager limit strategy to use for this dataset.
        def eager_limit_strategy
          cached_fetch(:_eager_limit_strategy) do
            if self[:limit]
              case s = cached_fetch(:eager_limit_strategy){self[:model].default_eager_limit_strategy || :ruby}
              when true
                ds = associated_class.dataset
                if ds.supports_window_functions?
                  :window_function
                else
                  :ruby
                end
              else
                s
              end
            else
              nil
            end
          end
        end

        # The key to use for the key hash when eager loading
        def eager_loader_key
          self[:eager_loader_key]
        end
    
        # By default associations do not need to select a key in an associated table
        # to eagerly load.
        def eager_loading_use_associated_key?
          false
        end

        # Alias of predicate_key, only for backwards compatibility.
        def eager_loading_predicate_key
          predicate_key
        end

        # Whether to eagerly graph a lazy dataset, true by default.  If this
        # is false, the association won't respect the :eager_graph option
        # when loading the association for a single record.
        def eager_graph_lazy_dataset?
          true
        end
    
        # The limit and offset for this association (returned as a two element array).
        def limit_and_offset
          if (v = self[:limit]).is_a?(Array)
            v
          else
            [v, nil]
          end
        end

        # Whether the associated object needs a primary key to be added/removed,
        # false by default.
        def need_associated_primary_key?
          false
        end

        # The keys to use for loading of the regular dataset, as an array.
        def predicate_keys
          cached_fetch(:predicate_keys){Array(predicate_key)}
        end

        # Qualify +col+ with the given table name.  If +col+ is an array of columns,
        # return an array of qualified columns.  Only qualifies Symbols and SQL::Identifier
        # values, other values are not modified.
        def qualify(table, col)
          transform(col) do |k|
            case k
            when Symbol, SQL::Identifier
              SQL::QualifiedIdentifier.new(table, k)
            else
              Sequel::Qualifier.new(self[:model].dataset, table).transform(k)
            end
          end
        end

        # Qualify col with the associated model's table name.
        def qualify_assoc(col)
          qualify(associated_class.table_name, col)
        end
        
        # Qualify col with the current model's table name.
        def qualify_cur(col)
          qualify(self[:model].table_name, col)
        end
        
        # Returns the reciprocal association variable, if one exists. The reciprocal
        # association is the association in the associated class that is the opposite
        # of the current association.  For example, Album.many_to_one :artist and
        # Artist.one_to_many :albums are reciprocal associations.  This information is
        # to populate reciprocal associations.  For example, when you do this_artist.add_album(album)
        # it sets album.artist to this_artist.
        def reciprocal
          cached_fetch(:reciprocal) do
            r_types = Array(reciprocal_type)
            keys = self[:keys]
            recip = nil
            associated_class.all_association_reflections.each do |assoc_reflect|
              if r_types.include?(assoc_reflect[:type]) && assoc_reflect[:keys] == keys && assoc_reflect.associated_class == self[:model]
                cached_set(:reciprocal_type, assoc_reflect[:type])
                recip = assoc_reflect[:name]
                break
              end
            end
            recip
          end
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
      
        # Whether associated objects need to be removed from the association before
        # being destroyed in order to preserve referential integrity.
        def remove_before_destroy?
          true
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
    
        # The columns to select when loading the association.
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
        
        private

        if defined?(RUBY_ENGINE) && RUBY_ENGINE != 'ruby'
        # :nocov:
          # On non-GVL rubies, assume the need to synchronize access.  Store the key
          # in a special sub-hash that always uses this method to synchronize access.
          def cached_fetch(key)
            fetch(key) do
              h = self[:cache]
              Sequel.synchronize{return h[key] if h.has_key?(key)}
              value = yield
              Sequel.synchronize{h[key] = value}
            end
          end

          # Cache the value at the given key, synchronizing access.
          def cached_set(key, value)
            h = self[:cache]
            Sequel.synchronize{h[key] = value}
          end
        # :nocov:
        else
          # On MRI, use a plain fetch, since the GVL will synchronize access.
          def cached_fetch(key)
            fetch(key) do 
              h = self[:cache]
              h.fetch(key){h[key] = yield}
            end
          end

          # On MRI, just set the value at the key in the cache, since the GVL
          # will synchronize access.
          def cached_set(key, value)
            self[:cache][key] = value
          end
        end

        # If +s+ is an array, map +s+ over the block.  Otherwise, just call the
        # block with +s+.
        def transform(s)
          s.is_a?(Array) ? s.map(&Proc.new) : (yield s)
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
    
        # many_to_one associations don't need an eager limit strategy
        def eager_limit_strategy
          nil
        end

        # The expression to use on the left hand side of the IN lookup when eager loading
        def predicate_key
          cached_fetch(:predicate_key){qualified_primary_key}
        end

        # The column(s) in the associated table that the key in the current table references (either a symbol or an array).
        def primary_key
         cached_fetch(:primary_key){associated_class.primary_key}
        end
       
        # The columns in the associated table that the key in the current table references (always an array).
        def primary_keys
         cached_fetch(:primary_keys){Array(primary_key)}
        end
        alias associated_object_keys primary_keys

        # The method symbol or array of method symbols to call on the associated object
        # to get the value to use for the foreign keys.
        def primary_key_method
         cached_fetch(:primary_key_method){primary_key}
        end
       
        # The array of method symbols to call on the associated object
        # to get the value to use for the foreign keys.
        def primary_key_methods
         cached_fetch(:primary_key_methods){Array(primary_key_method)}
        end
       
        # #primary_key qualified by the associated table
        def qualified_primary_key
          cached_fetch(:qualified_primary_key){self[:qualify] == false ? primary_key : qualify_assoc(primary_key)}
        end
        
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
          reciprocal_type == :one_to_one
        end
    
        private
    
        # The reciprocal type of a many_to_one association is either
        # a one_to_many or a one_to_one association.
        def reciprocal_type
          cached_fetch(:reciprocal_type){[:one_to_many, :one_to_one]}
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
        
        # The hash key to use for the eager loading predicate (left side of IN (1, 2, 3))
        def predicate_key
          cached_fetch(:predicate_key){qualify_assoc(self[:key])}
        end
        alias qualified_key predicate_key
    
        # The column in the current table that the key in the associated table references.
        def primary_key
         self[:primary_key]
        end

        # #primary_key qualified by the current table
        def qualified_primary_key
          cached_fetch(:qualified_primary_key){qualify_cur(primary_key)}
        end
      
        # Whether the reciprocal of this association returns an array of objects instead of a single object,
        # false for a one_to_many association.
        def reciprocal_array?
          false
        end
    
        # Destroying one_to_many associated objects automatically deletes the foreign key.
        def remove_before_destroy?
          false
        end
    
        # The one_to_many association needs to check that an object to be removed already is associated.
        def remove_should_check_existing?
          true
        end

        # One to many associations set the reciprocal to self when loading associated records.
        def set_reciprocal_to_self?
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
        
        # one_to_one associations don't use an eager limit strategy by default, but
        # support both DISTINCT ON and window functions as strategies.
        def eager_limit_strategy
          cached_fetch(:_eager_limit_strategy) do
            case s = self[:eager_limit_strategy]
            when Symbol
              s
            when true
              ds = associated_class.dataset
              if ds.supports_ordered_distinct_on?
                :distinct_on
              elsif ds.supports_window_functions?
                :window_function
              end
            else
              nil
            end
          end
        end

        # The limit and offset for this association (returned as a two element array).
        def limit_and_offset
          [1, nil]
        end

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

        # Alias of right_primary_keys
        def associated_object_keys
          right_primary_keys
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
      
        # The hash key to use for the eager loading predicate (left side of IN (1, 2, 3)).
        # The left key qualified by the join table.
        def predicate_key
          cached_fetch(:predicate_key){qualify(join_table_alias, self[:left_key])}
        end
        alias qualified_left_key predicate_key

        # The right key qualified by the join table.
        def qualified_right_key
          cached_fetch(:qualified_right_key){qualify(join_table_alias, self[:right_key])}
        end
    
        # many_to_many associations need to select a key in an associated table to eagerly load
        def eager_loading_use_associated_key?
          true
        end

        # The source of the join table.  This is the join table itself, unless it
        # is aliased, in which case it is the unaliased part.
        def join_table_source
          cached_fetch(:join_table_source){split_join_table_alias[0]}
        end

        # The join table itself, unless it is aliased, in which case this
        # is the alias.
        def join_table_alias
          cached_fetch(:join_table_alias) do
            s, a = split_join_table_alias
            a || s
          end
        end
        alias associated_key_table join_table_alias
        
        # Whether the associated object needs a primary key to be added/removed,
        # true for many_to_many associations.
        def need_associated_primary_key?
          true
        end
    
        # Returns the reciprocal association symbol, if one exists.
        def reciprocal
          cached_fetch(:reciprocal) do
            left_keys = self[:left_keys]
            right_keys = self[:right_keys]
            join_table = self[:join_table]
            recip = nil
            associated_class.all_association_reflections.each do |assoc_reflect|
              if assoc_reflect[:type] == :many_to_many && assoc_reflect[:left_keys] == right_keys &&
                 assoc_reflect[:right_keys] == left_keys && assoc_reflect[:join_table] == join_table &&
                 assoc_reflect.associated_class == self[:model]
                recip = assoc_reflect[:name]
                break
              end
            end
            recip
          end
        end

        # #right_primary_key qualified by the associated table
        def qualified_right_primary_key
          cached_fetch(:qualified_right_primary_key){qualify_assoc(right_primary_key)}
        end
    
        # The primary key column(s) to use in the associated table (can be symbol or array).
        def right_primary_key
          cached_fetch(:right_primary_key){associated_class.primary_key}
        end
        
        # The primary key columns to use in the associated table (always array).
        def right_primary_keys
          cached_fetch(:right_primary_keys){Array(right_primary_key)}
        end
    
        # The method symbol or array of method symbols to call on the associated objects
        # to get the foreign key values for the join table. 
        def right_primary_key_method
          cached_fetch(:right_primary_key_method){right_primary_key}
        end

        # The array of method symbols to call on the associated objects
        # to get the foreign key values for the join table. 
        def right_primary_key_methods
          cached_fetch(:right_primary_key_methods){Array(right_primary_key_method)}
        end
        
        # The columns to select when loading the association, associated_class.table_name.* by default.
        def select
         cached_fetch(:select){Sequel::SQL::ColumnAll.new(associated_class.table_name)}
        end

        private

        # Split the join table into source and alias parts.
        def split_join_table_alias
          associated_class.dataset.split_alias(self[:join_table])
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
      # portfolio :: Returns the associated portfolio.
      # portfolio=(obj) :: Sets the associated portfolio to the object,
      #                    but the change is not persisted until you save the record (for many_to_one associations).
      # portfolio_dataset :: Returns a dataset that would return the associated
      #                      portfolio, only useful in fairly specific circumstances.
      # milestones :: Returns an array of associated milestones
      # add_milestone(obj) :: Associates the passed milestone with this object.
      # remove_milestone(obj) :: Removes the association with the passed milestone.
      # remove_all_milestones :: Removes associations with all associated milestones.
      # milestones_dataset :: Returns a dataset that would return the associated
      #                       milestones, allowing for further filtering/limiting/etc.
      #
      # If you want to override the behavior of the add_/remove_/remove_all_/ methods
      # or the association setter method, use the :adder, :remover, :clearer, and/or :setter
      # options.  These options override the default behavior.
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
      #   => {:type => :many_to_one, :name => :portfolio, ...}
      #
      # Associations should not have the same names as any of the columns in the
      # model's current table they reference. If you are dealing with an existing schema that
      # has a column named status, you can't name the association status, you'd
      # have to name it foo_status or something else.  If you give an association the same name
      # as a column, you will probably end up with an association that doesn't work, or a SystemStackError.
      #
      # For a more in depth general overview, as well as a reference guide,
      # see the {Association Basics guide}[link:files/doc/association_basics_rdoc.html].
      # For examples of advanced usage, see the {Advanced Associations guide}[link:files/doc/advanced_associations_rdoc.html].
      module ClassMethods
        # All association reflections defined for this model (default: {}).
        attr_reader :association_reflections

        # The default :eager_limit_strategy option to use for *_many associations (default: nil)
        attr_accessor :default_eager_limit_strategy

        # Array of all association reflections for this model class
        def all_association_reflections
          association_reflections.values
        end
        
        # Given an association reflection and a dataset, apply the
        # :select, :conditions, :order, :eager, :distinct, and :eager_block
        # association options to the given dataset and return the dataset
        # or a modified copy of it.
        def apply_association_dataset_opts(opts, ds)
          ds = ds.select(*opts.select) if opts.select
          if c = opts[:conditions]
            ds = (c.is_a?(Array) && !Sequel.condition_specifier?(c)) ? ds.where(*c) : ds.where(c)
          end
          ds = ds.order(*opts[:order]) if opts[:order]
          ds = ds.eager(opts[:eager]) if opts[:eager]
          ds = ds.distinct if opts[:distinct]
          ds = opts[:eager_block].call(ds) if opts[:eager_block]
          ds
        end

        # Associates a related model with the current model. The following types are
        # supported:
        #
        # :many_to_one :: Foreign key in current model's table points to 
        #                 associated model's primary key.  Each associated model object can
        #                 be associated with more than one current model objects.  Each current
        #                 model object can be associated with only one associated model object.
        # :one_to_many :: Foreign key in associated model's table points to this
        #                 model's primary key.   Each current model object can be associated with
        #                 more than one associated model objects.  Each associated model object
        #                 can be associated with only one current model object.
        # :one_to_one :: Similar to one_to_many in terms of foreign keys, but
        #                only one object is associated to the current object through the
        #                association.  The methods created are similar to many_to_one, except
        #                that the one_to_one setter method saves the passed object.
        # :many_to_many :: A join table is used that has a foreign key that points
        #                  to this model's primary key and a foreign key that points to the
        #                  associated model's primary key.  Each current model object can be
        #                  associated with many associated model objects, and each associated
        #                  model object can be associated with many current model objects.
        #
        # The following options can be supplied:
        # === Multiple Types
        # :adder :: Proc used to define the private _add_* method for doing the database work
        #           to associate the given object to the current object (*_to_many assocations).
        # :after_add :: Symbol, Proc, or array of both/either specifying a callback to call
        #               after a new item is added to the association.
        # :after_load :: Symbol, Proc, or array of both/either specifying a callback to call
        #                after the associated record(s) have been retrieved from the database.
        # :after_remove :: Symbol, Proc, or array of both/either specifying a callback to call
        #                  after an item is removed from the association.
        # :after_set :: Symbol, Proc, or array of both/either specifying a callback to call
        #               after an item is set using the association setter method.
        # :allow_eager :: If set to false, you cannot load the association eagerly
        #                 via eager or eager_graph
        # :before_add :: Symbol, Proc, or array of both/either specifying a callback to call
        #                before a new item is added to the association.
        # :before_remove :: Symbol, Proc, or array of both/either specifying a callback to call
        #                   before an item is removed from the association.
        # :before_set :: Symbol, Proc, or array of both/either specifying a callback to call
        #                before an item is set using the association setter method.
        # :cartesian_product_number :: the number of joins completed by this association that could cause more
        #                              than one row for each row in the current table (default: 0 for
        #                              many_to_one and one_to_one associations, 1 for one_to_many and
        #                              many_to_many associations).
        # :class :: The associated class or its name as a string or symbol. If not
        #           given, uses the association's name, which is camelized (and
        #           singularized unless the type is :many_to_one or :one_to_one).  If this is specified
        #           as a string or symbol, you must specify the full class name (e.g. "SomeModule::MyModel"). 
        # :clearer :: Proc used to define the private _remove_all_* method for doing the database work
        #             to remove all objects associated to the current object (*_to_many assocations).
        # :clone :: Merge the current options and block into the options and block used in defining
        #           the given association.  Can be used to DRY up a bunch of similar associations that
        #           all share the same options such as :class and :key, while changing the order and block used.
        # :conditions :: The conditions to use to filter the association, can be any argument passed to where.
        # :dataset :: A proc that is instance_execed to get the base dataset to use (before the other
        #             options are applied).  If the proc accepts an argument, it is passed the related
        #             association reflection.
        # :distinct :: Use the DISTINCT clause when selecting associating object, both when
        #              lazy loading and eager loading via .eager (but not when using .eager_graph).
        # :eager :: The associations to eagerly load via +eager+ when loading the associated object(s).
        # :eager_block :: If given, use the block instead of the default block when
        #                 eagerly loading.  To not use a block when eager loading (when one is used normally),
        #                 set to nil.
        # :eager_graph :: The associations to eagerly load via +eager_graph+ when loading the associated object(s).
        #                 many_to_many associations with this option cannot be eagerly loaded via +eager+.
        # :eager_grapher :: A proc to use to implement eager loading via +eager_graph+, overriding the default.
        #                   Takes one or three arguments. If three arguments, they are a dataset, an alias to use for
        #                   the table to graph for this association, and the alias that was used for the current table
        #                   (since you can cascade associations). If one argument, is passed a hash with keys :self,
        #                   :table_alias, and :implicit_qualifier, corresponding to the three arguments, and an optional
        #                   additional key :eager_block, a callback accepting one argument, the associated dataset. This
        #                   is used to customize the association at query time.
        #                   Should return a copy of the dataset with the association graphed into it.
        # :eager_limit_strategy :: Determines the strategy used for enforcing limits when eager loading associations via
        #                          the +eager+ method.  For one_to_one associations, no strategy is used by default, and
        #                          for *_many associations, the :ruby strategy is used by default, which still retrieves
        #                          all records but slices the resulting array after the association is retrieved.  You
        #                          can pass a +true+ value for this option to have Sequel pick what it thinks is the best
        #                          choice for the database, or specify a specific symbol to manually select a strategy.
        #                          one_to_one associations support :distinct_on and :window_function.
        #                          *_many associations support :ruby, and :window_function.
        # :eager_loader :: A proc to use to implement eager loading, overriding the default.  Takes a single hash argument,
        #                  with at least the keys: :rows, which is an array of current model instances, :associations,
        #                  which is a hash of dependent associations, :self, which is the dataset doing the eager loading,
        #                  :eager_block, which is a dynamic callback that should be called with the dataset, and :id_map,
        #                  which is a mapping of key values to arrays of current model instances. In the proc, the
        #                  associated records should be queried from the database and the associations cache for each
        #                  record should be populated.
        # :eager_loader_key :: A symbol for the key column to use to populate the key_hash
        #                      for the eager loader.  Can be set to nil to not populate the key_hash.
        # :extend :: A module or array of modules to extend the dataset with.
        # :graph_alias_base :: The base name to use for the table alias when eager graphing.  Defaults to the name
        #                      of the association.  If the alias name has already been used in the query, Sequel will create
        #                      a unique alias by appending a numeric suffix (e.g. alias_0, alias_1, ...) until the alias is
        #                      unique.
        # :graph_block :: The block to pass to join_table when eagerly loading
        #                 the association via +eager_graph+.
        # :graph_conditions :: The additional conditions to use on the SQL join when eagerly loading
        #                      the association via +eager_graph+.  Should be a hash or an array of two element arrays. If not
        #                      specified, the :conditions option is used if it is a hash or array of two element arrays.
        # :graph_join_type :: The type of SQL join to use when eagerly loading the association via
        #                     eager_graph.  Defaults to :left_outer.
        # :graph_only_conditions :: The conditions to use on the SQL join when eagerly loading
        #                           the association via +eager_graph+, instead of the default conditions specified by the
        #                           foreign/primary keys.  This option causes the :graph_conditions option to be ignored.
        # :graph_select :: A column or array of columns to select from the associated table
        #                  when eagerly loading the association via +eager_graph+. Defaults to all
        #                  columns in the associated table.
        # :limit :: Limit the number of records to the provided value.  Use
        #           an array with two elements for the value to specify a
        #           limit (first element) and an offset (second element).
        # :methods_module :: The module that methods the association creates will be placed into. Defaults
        #                    to the module containing the model's columns.
        # :order :: the column(s) by which to order the association dataset.  Can be a
        #           singular column symbol or an array of column symbols.
        # :order_eager_graph :: Whether to add the association's order to the graphed dataset's order when graphing
        #                       via +eager_graph+.  Defaults to true, so set to false to disable.
        # :read_only :: Do not add a setter method (for many_to_one or one_to_one associations),
        #               or add_/remove_/remove_all_ methods (for one_to_many and many_to_many associations).
        # :reciprocal :: the symbol name of the reciprocal association,
        #                if it exists.  By default, Sequel will try to determine it by looking at the
        #                associated model's assocations for a association that matches
        #                the current association's key(s).  Set to nil to not use a reciprocal.
        # :remover :: Proc used to define the private _remove_* method for doing the database work
        #             to remove the association between the given object and the current object (*_to_many assocations).
        # :select :: the columns to select.  Defaults to the associated class's
        #            table_name.* in a many_to_many association, which means it doesn't include the attributes from the
        #            join table.  If you want to include the join table attributes, you can
        #            use this option, but beware that the join table attributes can clash with
        #            attributes from the model table, so you should alias any attributes that have
        #            the same name in both the join table and the associated table.
        # :setter :: Proc used to define the private _*= method for doing the work to setup the assocation
        #            between the given object and the current object (*_to_one associations).
        # :validate :: Set to false to not validate when implicitly saving any associated object.
        # === :many_to_one
        # :key :: foreign key in current model's table that references
        #         associated model's primary key, as a symbol.  Defaults to :"#{name}_id".  Can use an
        #         array of symbols for a composite key association.
        # :key_column :: Similar to, and usually identical to, :key, but :key refers to the model method
        #                to call, where :key_column refers to the underlying column.  Should only be
        #                used if the the model method differs from the foreign key column, in conjunction
        #                with defining a model alias method for the key column.
        # :primary_key :: column in the associated table that :key option references, as a symbol.
        #                 Defaults to the primary key of the associated table. Can use an
        #                 array of symbols for a composite key association.
        # :primary_key_method :: the method symbol or array of method symbols to call on the associated
        #                        object to get the foreign key values.  Defaults to :primary_key option.
        # :qualify :: Whether to use qualifier primary keys when loading the association.  The default
        #             is true, so you must set to false to not qualify.  Qualification rarely causes
        #             problems, but it's necessary to disable in some cases, such as when you are doing
        #             a JOIN USING operation on the column on Oracle.
        # === :one_to_many and :one_to_one
        # :key :: foreign key in associated model's table that references
        #         current model's primary key, as a symbol.  Defaults to
        #         :"#{self.name.underscore}_id".  Can use an
        #         array of symbols for a composite key association.
        # :key_method :: the method symbol or array of method symbols to call on the associated
        #                object to get the foreign key values.  Defaults to :key option.
        # :primary_key :: column in the current table that :key option references, as a symbol.
        #                 Defaults to primary key of the current table. Can use an
        #                 array of symbols for a composite key association.
        # :primary_key_column :: Similar to, and usually identical to, :primary_key, but :primary_key refers
        #                        to the model method call, where :primary_key_column refers to the underlying column.
        #                        Should only be used if the the model method differs from the primary key column, in
        #                        conjunction with defining a model alias method for the primary key column.
        # === :many_to_many
        # :graph_join_table_block :: The block to pass to +join_table+ for
        #                            the join table when eagerly loading the association via +eager_graph+.
        # :graph_join_table_conditions :: The additional conditions to use on the SQL join for
        #                                 the join table when eagerly loading the association via +eager_graph+.
        #                                 Should be a hash or an array of two element arrays.
        # :graph_join_table_join_type :: The type of SQL join to use for the join table when eagerly
        #                                loading the association via +eager_graph+.  Defaults to the
        #                                :graph_join_type option or :left_outer.
        # :graph_join_table_only_conditions :: The conditions to use on the SQL join for the join
        #                                      table when eagerly loading the association via +eager_graph+,
        #                                      instead of the default conditions specified by the
        #                                      foreign/primary keys.  This option causes the
        #                                      :graph_join_table_conditions option to be ignored.
        # :join_table :: name of table that includes the foreign keys to both
        #                the current model and the associated model, as a symbol.  Defaults to the name
        #                of current model and name of associated model, pluralized,
        #                underscored, sorted, and joined with '_'.
        # :join_table_block :: proc that can be used to modify the dataset used in the add/remove/remove_all
        #                      methods.  Should accept a dataset argument and return a modified dataset if present.
        # :left_key :: foreign key in join table that points to current model's
        #              primary key, as a symbol. Defaults to :"#{self.name.underscore}_id".
        #              Can use an array of symbols for a composite key association.
        # :left_primary_key :: column in current table that :left_key points to, as a symbol.
        #                      Defaults to primary key of current table.  Can use an
        #                      array of symbols for a composite key association.
        # :left_primary_key_column :: Similar to, and usually identical to, :left_primary_key, but :left_primary_key refers to
        #                             the model method to call, where :left_primary_key_column refers to the underlying column.  Should only
        #                             be used if the model method differs from the left primary key column, in conjunction
        #                             with defining a model alias method for the left primary key column.
        # :right_key :: foreign key in join table that points to associated
        #               model's primary key, as a symbol.  Defaults to :"#{name.to_s.singularize}_id".
        #               Can use an array of symbols for a composite key association.
        # :right_primary_key :: column in associated table that :right_key points to, as a symbol.
        #                       Defaults to primary key of the associated table.  Can use an
        #                       array of symbols for a composite key association.
        # :right_primary_key_method :: the method symbol or array of method symbols to call on the associated
        #                              object to get the foreign key values for the join table.
        #                              Defaults to :right_primary_key option.
        # :uniq :: Adds a after_load callback that makes the array of objects unique.
        def associate(type, name, opts = {}, &block)
          raise(Error, 'one_to_many association type with :one_to_one option removed, used one_to_one association type') if opts[:one_to_one] && type == :one_to_many
          raise(Error, 'invalid association type') unless assoc_class = ASSOCIATION_TYPES[type]
          raise(Error, 'Model.associate name argument must be a symbol') unless Symbol === name
          raise(Error, ':eager_loader option must have an arity of 1 or 3') if opts[:eager_loader] && ![1, 3].include?(opts[:eager_loader].arity)
          raise(Error, ':eager_grapher option must have an arity of 1 or 3') if opts[:eager_grapher] && ![1, 3].include?(opts[:eager_grapher].arity)

          # dup early so we don't modify opts
          orig_opts = opts.dup
          if opts[:clone]
            cloned_assoc = association_reflection(opts[:clone])
            raise(Error, "cannot clone an association to an association of different type (association #{name} with type #{type} cloning #{opts[:clone]} with type #{cloned_assoc[:type]})") unless cloned_assoc[:type] == type || [cloned_assoc[:type], type].all?{|t| [:one_to_many, :one_to_one].include?(t)}
            orig_opts = cloned_assoc[:orig_opts].merge(orig_opts)
          end
          opts = orig_opts.merge(:type => type, :name => name, :cache=>{}, :model => self)
          opts[:block] = block if block
          opts = assoc_class.new.merge!(opts)
          opts[:eager_block] = block unless opts.include?(:eager_block)
          if !opts.has_key?(:predicate_key) && opts.has_key?(:eager_loading_predicate_key)
            opts[:predicate_key] = opts[:eager_loading_predicate_key]
          end
          opts[:graph_join_type] ||= :left_outer
          opts[:order_eager_graph] = true unless opts.include?(:order_eager_graph)
          conds = opts[:conditions]
          opts[:graph_alias_base] ||= name
          opts[:graph_conditions] = conds if !opts.include?(:graph_conditions) and Sequel.condition_specifier?(conds)
          opts[:graph_conditions] = opts.fetch(:graph_conditions, []).to_a
          opts[:graph_select] = Array(opts[:graph_select]) if opts[:graph_select]
          [:before_add, :before_remove, :after_add, :after_remove, :after_load, :before_set, :after_set, :extend].each do |cb_type|
            opts[cb_type] = Array(opts[cb_type])
          end
          late_binding_class_option(opts, opts.returns_array? ? singularize(name) : name)
          
          # Remove :class entry if it exists and is nil, to work with cached_fetch
          opts.delete(:class) unless opts[:class]
          
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

        # Modify and return eager loading dataset based on association options.
        def eager_loading_dataset(opts, ds, select, associations, eager_options={})
          ds = apply_association_dataset_opts(opts, ds)
          ds = ds.select(*select) if select
          if opts[:eager_graph]
            raise(Error, "cannot eagerly load a #{opts[:type]} association that uses :eager_graph") if opts.eager_loading_use_associated_key?
            ds = ds.eager_graph(opts[:eager_graph])
          end
          ds = ds.eager(associations) unless Array(associations).empty?
          ds = eager_options[:eager_block].call(ds) if eager_options[:eager_block]
          if opts.eager_loading_use_associated_key?
            ds = if opts[:uses_left_composite_keys]
              ds.select_append(*opts.associated_key_alias.zip(opts.predicate_keys).map{|a, k| SQL::AliasedExpression.new(k, a)})
            else
              ds.select_append(SQL::AliasedExpression.new(opts.predicate_key, opts.associated_key_alias))
            end
          end
          ds
        end

        # Shortcut for adding a many_to_many association, see #associate
        def many_to_many(name, opts={}, &block)
          associate(:many_to_many, name, opts, &block)
        end
        
        # Shortcut for adding a many_to_one association, see #associate
        def many_to_one(name, opts={}, &block)
          associate(:many_to_one, name, opts, &block)
        end
        
        # Shortcut for adding a one_to_many association, see #associate
        def one_to_many(name, opts={}, &block)
          associate(:one_to_many, name, opts, &block)
        end

        # Shortcut for adding a one_to_one association, see #associate.
        def one_to_one(name, opts={}, &block)
          associate(:one_to_one, name, opts, &block)
        end

        Plugins.inherited_instance_variables(self, :@association_reflections=>:dup, :@default_eager_limit_strategy=>nil)
        Plugins.def_dataset_methods(self, [:eager, :eager_graph])
        
        private
      
        # Use a correlated subquery to limit the results of the eager loading dataset.
        def apply_correlated_subquery_eager_limit_strategy(ds, opts)
          Sequel::Deprecation.deprecate('The correlated_subquery eager limit strategy',  'Switch to another eager limit strategy.')
          klass = opts.associated_class
          kds = klass.dataset
          dsa = ds.send(:dataset_alias, 1)
          raise Error, "can't use a correlated subquery if the associated class (#{opts.associated_class.inspect}) does not have a primary key" unless pk = klass.primary_key
          pka = Array(pk)
          raise Error, "can't use a correlated subquery if the associated class (#{opts.associated_class.inspect}) has a composite primary key and the database does not support multiple column IN" if pka.length > 1 && !ds.supports_multiple_column_in?
          table = kds.opts[:from]
          raise Error, "can't use a correlated subquery unless the associated class (#{opts.associated_class.inspect}) uses a single FROM table" unless table && table.length == 1
          table = table.first
          if order = ds.opts[:order]
            oproc = lambda do |x|
              case x
              when Symbol
                t, c, _ = ds.send(:split_symbol, x)
                if t && t.to_sym == table
                  SQL::QualifiedIdentifier.new(dsa, c)
                else
                  x
                end
              when SQL::QualifiedIdentifier
                if x.table == table
                  SQL::QualifiedIdentifier.new(dsa, x.column)
                else
                  x
                end
              when SQL::OrderedExpression
                SQL::OrderedExpression.new(oproc.call(x.expression), x.descending, :nulls=>x.nulls)
              else
                x
              end
            end
            order = order.map(&oproc) 
          end
          limit, offset = opts.limit_and_offset

          subquery = yield kds.
            unlimited.
            from(SQL::AliasedExpression.new(table, dsa)).
            select(*pka.map{|k| SQL::QualifiedIdentifier.new(dsa, k)}).
            order(*order).
            limit(limit, offset)

          pk = if pk.is_a?(Array)
            pk.map{|k| SQL::QualifiedIdentifier.new(table, k)}
          else
            SQL::QualifiedIdentifier.new(table, pk)
          end
          ds.where(pk=>subquery)
        end

        # Use a window function to limit the results of the eager loading dataset.
        def apply_window_function_eager_limit_strategy(ds, opts)
          rn = ds.row_number_column 
          limit, offset = opts.limit_and_offset
          ds = ds.unordered.select_append{row_number(:over, :partition=>opts.predicate_key, :order=>ds.opts[:order]){}.as(rn)}.from_self
          ds = if opts[:type] == :one_to_one
            ds.where(rn => 1)
          elsif offset
            offset += 1
            ds.where(rn => (offset...(offset+limit))) 
          else
            ds.where{SQL::Identifier.new(rn) <= limit} 
          end
        end

        # The module to use for the association's methods.  Defaults to
        # the overridable_methods_module.
        def association_module(opts={})
          opts.fetch(:methods_module, overridable_methods_module)
        end

        # Add a method to the module included in the class, so the method
        # can be easily overridden in the class itself while allowing for
        # super to be called.
        def association_module_def(name, opts={}, &block)
          association_module(opts).module_eval{define_method(name, &block)}
        end
      
        # Add a private method to the module included in the class.
        def association_module_private_def(name, opts={}, &block)
          association_module_def(name, opts, &block)
          association_module(opts).send(:private, name)
        end
      
        # Add the add_ instance method 
        def def_add_method(opts)
          association_module_def(opts.add_method, opts){|o,*args| add_associated_object(opts, o, *args)}
        end
      
        # Adds the association dataset methods to the association methods module.
        def def_association_dataset_methods(opts)
          association_module_def(opts.dataset_method, opts){_dataset(opts)}
          def_association_method(opts)
        end

        # Adds the association method to the association methods module.
        def def_association_method(opts)
          association_module_def(opts.association_method, opts){|*dynamic_opts, &block| load_associated_objects(opts, dynamic_opts[0], &block)}
        end
      
        # Configures many_to_many association reflection and adds the related association methods
        def def_many_to_many(opts)
          name = opts[:name]
          model = self
          left = (opts[:left_key] ||= opts.default_left_key)
          lcks = opts[:left_keys] = Array(left)
          right = (opts[:right_key] ||= opts.default_right_key)
          rcks = opts[:right_keys] = Array(right)
          left_pk = (opts[:left_primary_key] ||= self.primary_key)
          opts[:eager_loader_key] = left_pk unless opts.has_key?(:eager_loader_key)
          lcpks = opts[:left_primary_keys] = Array(left_pk)
          lpkc = opts[:left_primary_key_column] ||= left_pk
          lpkcs = opts[:left_primary_key_columns] ||= Array(lpkc)
          raise(Error, "mismatched number of left keys: #{lcks.inspect} vs #{lcpks.inspect}") unless lcks.length == lcpks.length
          if opts[:right_primary_key]
            rcpks = Array(opts[:right_primary_key])
            raise(Error, "mismatched number of right keys: #{rcks.inspect} vs #{rcpks.inspect}") unless rcks.length == rcpks.length
          end
          uses_lcks = opts[:uses_left_composite_keys] = lcks.length > 1
          opts[:uses_right_composite_keys] = rcks.length > 1
          opts[:cartesian_product_number] ||= 1
          join_table = (opts[:join_table] ||= opts.default_join_table)
          left_key_alias = opts[:left_key_alias] ||= opts.default_associated_key_alias
          graph_jt_conds = opts[:graph_join_table_conditions] = opts.fetch(:graph_join_table_conditions, []).to_a
          opts[:graph_join_table_join_type] ||= opts[:graph_join_type]
          opts[:after_load].unshift(:array_uniq!) if opts[:uniq]
          opts[:dataset] ||= proc{opts.associated_dataset.inner_join(join_table, rcks.zip(opts.right_primary_keys) + opts.predicate_keys.zip(lcpks.map{|k| send(k)}), :qualify=>:deep)}

          opts[:eager_loader] ||= proc do |eo|
            h = eo[:id_map]
            rows = eo[:rows]
            rows.each{|object| object.associations[name] = []}
            r = rcks.zip(opts.right_primary_keys)
            l = [[opts.predicate_key, h.keys]]
            ds = model.eager_loading_dataset(opts, opts.associated_class.inner_join(join_table, r + l, :qualify=>:deep), nil, eo[:associations], eo)
            case opts.eager_limit_strategy
            when :window_function
              delete_rn = true
              rn = ds.row_number_column
              ds = apply_window_function_eager_limit_strategy(ds, opts)
            when :correlated_subquery
              ds = apply_correlated_subquery_eager_limit_strategy(ds, opts) do |xds|
                dsa = ds.send(:dataset_alias, 2)
                xds.inner_join(join_table, r + lcks.map{|k| [k, SQL::QualifiedIdentifier.new(opts.join_table_alias, k)]}, :table_alias=>dsa, :qualify=>:deep)
              end
            end
            ds.all do |assoc_record|
              assoc_record.values.delete(rn) if delete_rn
              hash_key = if uses_lcks
                left_key_alias.map{|k| assoc_record.values.delete(k)}
              else
                assoc_record.values.delete(left_key_alias)
              end
              next unless objects = h[hash_key]
              objects.each{|object| object.associations[name].push(assoc_record)}
            end
            if opts.eager_limit_strategy == :ruby
              limit, offset = opts.limit_and_offset
              rows.each{|o| o.associations[name] = o.associations[name].slice(offset||0, limit) || []}
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
          opts[:eager_grapher] ||= proc do |eo|
            ds = eo[:self]
            ds = ds.graph(join_table, use_jt_only_conditions ? jt_only_conditions : lcks.zip(lpkcs) + graph_jt_conds, :select=>false, :table_alias=>ds.unused_table_alias(join_table, [eo[:table_alias]]), :join_type=>jt_join_type, :implicit_qualifier=>eo[:implicit_qualifier], :qualify=>:deep, :from_self_alias=>ds.opts[:eager_graph][:master], &jt_graph_block)
            ds.graph(eager_graph_dataset(opts, eo), use_only_conditions ? only_conditions : opts.right_primary_keys.zip(rcks) + conditions, :select=>select, :table_alias=>eo[:table_alias], :qualify=>:deep, :join_type=>join_type, &graph_block)
          end
      
          def_association_dataset_methods(opts)
      
          return if opts[:read_only]
      
          adder = opts[:adder] || proc do |o|
            h = {}
            lcks.zip(lcpks).each{|k, pk| h[k] = send(pk)}
            rcks.zip(opts.right_primary_key_methods).each{|k, pk| h[k] = o.send(pk)}
            _join_table_dataset(opts).insert(h)
          end
          association_module_private_def(opts._add_method, opts, &adder) 

          remover = opts[:remover] || proc do |o|
            _join_table_dataset(opts).where(lcks.zip(lcpks.map{|k| send(k)}) + rcks.zip(opts.right_primary_key_methods.map{|k| o.send(k)})).delete
          end
          association_module_private_def(opts._remove_method, opts, &remover)

          clearer = opts[:clearer] || proc do
            _join_table_dataset(opts).where(lcks.zip(lcpks.map{|k| send(k)})).delete
          end
          association_module_private_def(opts._remove_all_method, opts, &clearer)
      
          def_add_method(opts)
          def_remove_methods(opts)
        end
        
        # Configures many_to_one association reflection and adds the related association methods
        def def_many_to_one(opts)
          name = opts[:name]
          model = self
          opts[:key] = opts.default_key unless opts.has_key?(:key)
          key = opts[:key]
          opts[:eager_loader_key] = key unless opts.has_key?(:eager_loader_key)
          cks = opts[:graph_keys] = opts[:keys] = Array(key)
          opts[:key_column] ||= key
          opts[:graph_keys] = opts[:key_columns] = Array(opts[:key_column])
          opts[:qualified_key] = opts.qualify_cur(key)
          if opts[:primary_key]
            cpks = Array(opts[:primary_key])
            raise(Error, "mismatched number of keys: #{cks.inspect} vs #{cpks.inspect}") unless cks.length == cpks.length
          end
          uses_cks = opts[:uses_composite_keys] = cks.length > 1
          opts[:cartesian_product_number] ||= 0
          opts[:dataset] ||= proc do
            opts.associated_dataset.where(opts.predicate_keys.zip(cks.map{|k| send(k)}))
          end
          opts[:eager_loader] ||= proc do |eo|
            h = eo[:id_map]
            keys = h.keys
            # Default the cached association to nil, so any object that doesn't have it
            # populated will have cached the negative lookup.
            eo[:rows].each{|object| object.associations[name] = nil}
            # Skip eager loading if no objects have a foreign key for this association
            unless keys.empty?
              klass = opts.associated_class
              model.eager_loading_dataset(opts, klass.where(opts.predicate_key=>keys), nil, eo[:associations], eo).all do |assoc_record|
                hash_key = uses_cks ? opts.primary_key_methods.map{|k| assoc_record.send(k)} : assoc_record.send(opts.primary_key_method)
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
          graph_cks = opts[:graph_keys]
          opts[:eager_grapher] ||= proc do |eo|
            ds = eo[:self]
            ds.graph(eager_graph_dataset(opts, eo), use_only_conditions ? only_conditions : opts.primary_keys.zip(graph_cks) + conditions, eo.merge(:select=>select, :join_type=>join_type, :qualify=>:deep, :from_self_alias=>ds.opts[:eager_graph][:master]), &graph_block)
          end
      
          def_association_dataset_methods(opts)
          
          return if opts[:read_only]
      
          setter = opts[:setter] || proc{|o| cks.zip(opts.primary_key_methods).each{|k, pk| send(:"#{k}=", (o.send(pk) if o))}}
          association_module_private_def(opts._setter_method, opts, &setter)
          association_module_def(opts.setter_method, opts){|o| set_associated_object(opts, o)}
        end
        
        # Configures one_to_many and one_to_one association reflections and adds the related association methods
        def def_one_to_many(opts)
          one_to_one = opts[:type] == :one_to_one
          name = opts[:name]
          model = self
          key = (opts[:key] ||= opts.default_key)
          km = opts[:key_method] ||= opts[:key]
          cks = opts[:keys] = Array(key)
          opts[:key_methods] = Array(opts[:key_method])
          primary_key = (opts[:primary_key] ||= self.primary_key)
          opts[:eager_loader_key] = primary_key unless opts.has_key?(:eager_loader_key)
          cpks = opts[:primary_keys] = Array(primary_key)
          pkc = opts[:primary_key_column] ||= primary_key
          pkcs = opts[:primary_key_columns] ||= Array(pkc)
          raise(Error, "mismatched number of keys: #{cks.inspect} vs #{cpks.inspect}") unless cks.length == cpks.length
          uses_cks = opts[:uses_composite_keys] = cks.length > 1
          opts[:dataset] ||= proc do
            opts.associated_dataset.where(opts.predicate_keys.zip(cpks.map{|k| send(k)}))
          end
          opts[:eager_loader] ||= proc do |eo|
            h = eo[:id_map]
            rows = eo[:rows]
            if one_to_one
              rows.each{|object| object.associations[name] = nil}
            else
              rows.each{|object| object.associations[name] = []}
            end
            reciprocal = opts.reciprocal
            klass = opts.associated_class
            filter_keys = opts.predicate_key
            ds = model.eager_loading_dataset(opts, klass.where(filter_keys=>h.keys), nil, eo[:associations], eo)
            case opts.eager_limit_strategy
            when :distinct_on
              ds = ds.distinct(*filter_keys).order_prepend(*filter_keys)
            when :window_function
              delete_rn = true
              rn = ds.row_number_column
              ds = apply_window_function_eager_limit_strategy(ds, opts)
            when :correlated_subquery
              ds = apply_correlated_subquery_eager_limit_strategy(ds, opts) do |xds|
                xds.where(opts.associated_object_keys.map{|k| [SQL::QualifiedIdentifier.new(xds.first_source_alias, k), SQL::QualifiedIdentifier.new(xds.first_source_table, k)]})
              end
            end
            ds.all do |assoc_record|
              assoc_record.values.delete(rn) if delete_rn
              hash_key = uses_cks ? km.map{|k| assoc_record.send(k)} : assoc_record.send(km)
              next unless objects = h[hash_key]
              if one_to_one
                objects.each do |object| 
                  unless object.associations[name]
                    object.associations[name] = assoc_record
                    assoc_record.associations[reciprocal] = object if reciprocal
                  end
                end
              else
                objects.each do |object| 
                  object.associations[name].push(assoc_record)
                  assoc_record.associations[reciprocal] = object if reciprocal
                end
              end
            end
            if opts.eager_limit_strategy == :ruby
              limit, offset = opts.limit_and_offset
              rows.each{|o| o.associations[name] = o.associations[name].slice(offset||0, limit) || []}
            end
          end
          
          join_type = opts[:graph_join_type]
          select = opts[:graph_select]
          use_only_conditions = opts.include?(:graph_only_conditions)
          only_conditions = opts[:graph_only_conditions]
          conditions = opts[:graph_conditions]
          opts[:cartesian_product_number] ||= one_to_one ? 0 : 1
          graph_block = opts[:graph_block]
          opts[:eager_grapher] ||= proc do |eo|
            ds = eo[:self]
            ds = ds.graph(eager_graph_dataset(opts, eo), use_only_conditions ? only_conditions : cks.zip(pkcs) + conditions, eo.merge(:select=>select, :join_type=>join_type, :qualify=>:deep, :from_self_alias=>ds.opts[:eager_graph][:master]), &graph_block)
            # We only load reciprocals for one_to_many associations, as other reciprocals don't make sense
            ds.opts[:eager_graph][:reciprocals][eo[:table_alias]] = opts.reciprocal
            ds
          end
      
          def_association_dataset_methods(opts)
          
          ck_nil_hash ={}
          cks.each{|k| ck_nil_hash[k] = nil}

          unless opts[:read_only]
            validate = opts[:validate]

            if one_to_one
              setter = opts[:setter] || proc do |o|
                up_ds = _apply_association_options(opts, opts.associated_dataset.where(cks.zip(cpks.map{|k| send(k)})))
                if o
                  up_ds = up_ds.exclude(o.pk_hash) unless o.new?
                  cks.zip(cpks).each{|k, pk| o.send(:"#{k}=", send(pk))}
                end
                checked_transaction do
                  up_ds.update(ck_nil_hash)
                  o.save(:validate=>validate) || raise(Sequel::Error, "invalid associated object, cannot save") if o
                end
              end
              association_module_private_def(opts._setter_method, opts, &setter)
              association_module_def(opts.setter_method, opts){|o| set_one_to_one_associated_object(opts, o)}
            else 
              adder = opts[:adder] || proc do |o|
                cks.zip(cpks).each{|k, pk| o.send(:"#{k}=", send(pk))}
                o.save(:validate=>validate) || raise(Sequel::Error, "invalid associated object, cannot save")
              end
              association_module_private_def(opts._add_method, opts, &adder)
      
              remover = opts[:remover] || proc do |o|
                cks.each{|k| o.send(:"#{k}=", nil)}
                o.save(:validate=>validate) || raise(Sequel::Error, "invalid associated object, cannot save")
              end
              association_module_private_def(opts._remove_method, opts, &remover)

              clearer = opts[:clearer] || proc do
                _apply_association_options(opts, opts.associated_dataset.where(cks.zip(cpks.map{|k| send(k)}))).update(ck_nil_hash)
              end
              association_module_private_def(opts._remove_all_method, opts, &clearer)

              def_add_method(opts)
              def_remove_methods(opts)
            end
          end
        end

        # Alias of def_one_to_many, since they share pretty much the same code.
        def def_one_to_one(opts)
          def_one_to_many(opts)
        end
        
        # Add the remove_ and remove_all instance methods
        def def_remove_methods(opts)
          association_module_def(opts.remove_method, opts){|o,*args| remove_associated_object(opts, o, *args)}
          association_module_def(opts.remove_all_method, opts){|*args| remove_all_associated_objects(opts, *args)}
        end

        # Return dataset to graph into given the association reflection, applying the :callback option if set.
        def eager_graph_dataset(opts, eager_options)
          ds = opts.associated_class.dataset
          if cb = eager_options[:callback]
            ds = cb.call(ds)
          end
          ds
        end
      end

      # Instance methods used to implement the associations support.
      module InstanceMethods
        # The currently cached associations.  A hash with the keys being the
        # association name symbols and the values being the associated object
        # or nil (many_to_one), or the array of associated objects (*_to_many).
        def associations
          @associations ||= {}
        end

        # Freeze the associations cache when freezing the object.  Note that
        # retrieving associations after freezing will still work in most cases,
        # but the associations will not be cached in the association cache.
        def freeze
          associations.freeze
          super
        end
      
        # Clear the associations cache when refreshing
        def set_values(hash)
          @associations.clear if @associations
          super
        end

        # Formally used internally by the associations code, like pk but doesn't raise
        # an Error if the model has no primary key.  Not used any longer, deprecated.
        def pk_or_nil
          key = primary_key
          key.is_a?(Array) ? key.map{|k| @values[k]} : @values[key]
        end

        private
        
        # Apply the association options such as :order and :limit to the given dataset, returning a modified dataset.
        def _apply_association_options(opts, ds)
          unless ds.kind_of?(AssociationDatasetMethods)
            ds = opts.apply_dataset_changes(ds)
          end
          ds.model_object = self
          ds = ds.eager_graph(opts[:eager_graph]) if opts[:eager_graph] && opts.eager_graph_lazy_dataset?
          ds = instance_exec(ds, &opts[:block]) if opts[:block]
          ds
        end

        # Return a dataset for the association after applying any dynamic callback.
        def _associated_dataset(opts, dynamic_opts)
          ds = send(opts.dataset_method)
          if callback = dynamic_opts[:callback]
            ds = callback.call(ds)
          end
          ds
        end
        
        # Return an association dataset for the given association reflection
        def _dataset(opts)
          raise(Sequel::Error, "model object #{inspect} does not have a primary key") if opts.dataset_need_primary_key? && !pk
          ds = if opts[:dataset].arity == 1
            instance_exec(opts, &opts[:dataset])
          else
            instance_exec(&opts[:dataset])
          end
          _apply_association_options(opts, ds)
        end

        # Dataset for the join table of the given many to many association reflection
        def _join_table_dataset(opts)
          ds = model.db.from(opts.join_table_source)
          opts[:join_table_block] ? opts[:join_table_block].call(ds) : ds
        end

        # Return the associated single object for the given association reflection and dynamic options
        # (or nil if no associated object).
        def _load_associated_object(opts, dynamic_opts)
          _load_associated_object_array(opts, dynamic_opts).first
        end

        # Load the associated objects for the given association reflection and dynamic options
        # as an array.
        def _load_associated_object_array(opts, dynamic_opts)
          _associated_dataset(opts, dynamic_opts).all
        end

        # Return the associated objects from the dataset, without association callbacks, reciprocals, and caching.
        # Still apply the dynamic callback if present.
        def _load_associated_objects(opts, dynamic_opts={})
          if opts.can_have_associated_objects?(self)
            if opts.returns_array?
              _load_associated_object_array(opts, dynamic_opts)
            else
              _load_associated_object(opts, dynamic_opts)
            end
          elsif opts.returns_array?
            []
          end
        end
        
        # Add the given associated object to the given association
        def add_associated_object(opts, o, *args)
          klass = opts.associated_class
          if o.is_a?(Hash)
            o = klass.new(o)
          elsif o.is_a?(Integer) || o.is_a?(String) || o.is_a?(Array)
            o = klass[o]
          elsif !o.is_a?(klass)
            raise(Sequel::Error, "associated object #{o.inspect} not of correct type #{klass}")
          end
          raise(Sequel::Error, "model object #{inspect} does not have a primary key") unless pk
          ensure_associated_primary_key(opts, o, *args)
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
          return if o.frozen?
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

        # Save the associated object if the associated object needs a primary key
        # and the associated object is new and does not have one.  Raise an error if
        # the object still does not have a primary key
        def ensure_associated_primary_key(opts, o, *args)
          if opts.need_associated_primary_key?
            o.save(:validate=>opts[:validate]) if o.new?
            raise(Sequel::Error, "associated object #{o.inspect} does not have a primary key") unless o.pk
          end
        end

        # Load the associated objects using the dataset, handling callbacks, reciprocals, and caching.
        def load_associated_objects(opts, dynamic_opts=nil)
          if dynamic_opts == true or dynamic_opts == false or dynamic_opts == nil
            dynamic_opts = {:reload=>dynamic_opts}
          elsif dynamic_opts.respond_to?(:call)
            dynamic_opts = {:callback=>dynamic_opts}
          end
          if block_given?
            dynamic_opts = dynamic_opts.merge(:callback=>Proc.new)
          end
          name = opts[:name]
          if associations.include?(name) and !dynamic_opts[:callback] and !dynamic_opts[:reload]
            associations[name]
          else
            objs = _load_associated_objects(opts, dynamic_opts)
            if opts.set_reciprocal_to_self?
              if opts.returns_array?
                objs.each{|o| add_reciprocal_object(opts, o)}
              elsif objs
                add_reciprocal_object(opts, objs)
              end
            end

            # If the current object is frozen, you can't update the associations
            # cache.  This can cause issues for after_load procs that expect
            # the objects to be already cached in the associations, but
            # unfortunately that case cannot be handled.
            associations[name] = objs unless frozen?
            run_association_callbacks(opts, :after_load, objs)
            frozen? ? objs : associations[name]
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
            o = remove_check_existing_object_from_pk(opts, o, *args)
          elsif !o.is_a?(klass)
            raise(Sequel::Error, "associated object #{o.inspect} not of correct type #{klass}")
          elsif opts.remove_should_check_existing? && send(opts.dataset_method).where(o.pk_hash).empty?
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

        # Check that the object from the associated table specified by the primary key
        # is currently associated to the receiver.  If it is associated, return the object, otherwise
        # raise an error.
        def remove_check_existing_object_from_pk(opts, o, *args)
          key = o
          pkh = opts.associated_class.qualified_primary_key_hash(key)
          raise(Sequel::Error, "no object with key(s) #{key.inspect} is currently associated to #{inspect}") unless o = send(opts.dataset_method).first(pkh)
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

        # Set the given object as the associated object for the given *_to_one association reflection
        def _set_associated_object(opts, o)
          a = associations[opts[:name]]
          return if a && a == o && !set_associated_object_if_same?
          run_association_callbacks(opts, :before_set, o)
          remove_reciprocal_object(opts, a) if a
          send(opts._setter_method, o)
          associations[opts[:name]] = o
          add_reciprocal_object(opts, o) if o
          run_association_callbacks(opts, :after_set, o)
          o
        end

        # Whether run the associated object setter code if passed the same object as the one already
        # cached in the association.  Usually not set (so nil), can be set on a per-object basis
        # if necessary.
        def set_associated_object_if_same?
          @set_associated_object_if_same
        end
        
        # Set the given object as the associated object for the given many_to_one association reflection
        def set_associated_object(opts, o)
          raise(Error, "associated object #{o.inspect} does not have a primary key") if o && !o.pk
          _set_associated_object(opts, o)
        end

        # Set the given object as the associated object for the given one_to_one association reflection
        def set_one_to_one_associated_object(opts, o)
          raise(Error, "object #{inspect} does not have a primary key") unless pk
          _set_associated_object(opts, o)
        end
      end

      # Eager loading makes it so that you can load all associated records for a
      # set of objects in a single query, instead of a separate query for each object.
      #
      # Two separate implementations are provided.  +eager+ should be used most of the
      # time, as it loads associated records using one query per association.  However,
      # it does not allow you the ability to filter or order based on columns in associated tables.  +eager_graph+ loads
      # all records in a single query using JOINs, allowing you to filter or order based on columns in associated
      # tables.  However, +eager_graph+ is usually slower than +eager+, especially if multiple
      # one_to_many or many_to_many associations are joined.
      #
      # You can cascade the eager loading (loading associations on associated objects)
      # with no limit to the depth of the cascades.  You do this by passing a hash to +eager+ or +eager_graph+
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
      #
      # You can also pass a callback as a hash value in order to customize the dataset being
      # eager loaded at query time, analogous to the way the :eager_block association option
      # allows you to customize it at association definition time. For example,
      # if you wanted artists with their albums since 1990:
      #
      #   Artist.eager(:albums => proc{|ds| ds.where{year > 1990}})
      #
      # Or if you needed albums and their artist's name only, using a single query:
      #
      #   Albums.eager_graph(:artist => proc{|ds| ds.select(:name)})
      #
      # To cascade eager loading while using a callback, you substitute the cascaded
      # associations with a single entry hash that has the proc callback as the key and 
      # the cascaded associations as the value.  This will load artists with their albums
      # since 1990, and also the tracks on those albums and the genre for those tracks:
      #
      #   Artist.eager(:albums => {proc{|ds| ds.where{year > 1990}}=>{:tracks => :genre}})
      module DatasetMethods
        Sequel::Dataset.def_mutation_method(:eager, :eager_graph, :module=>self)
      
        # If the expression is in the form <tt>x = y</tt> where +y+ is a <tt>Sequel::Model</tt>
        # instance, array of <tt>Sequel::Model</tt> instances, or a <tt>Sequel::Model</tt> dataset,
        # assume +x+ is an association symbol and look up the association reflection
        # via the dataset's model.  From there, return the appropriate SQL based on the type of
        # association and the values of the foreign/primary keys of +y+.  For most association
        # types, this is a simple transformation, but for +many_to_many+ associations this 
        # creates a subquery to the join table.
        def complex_expression_sql_append(sql, op, args)
          r = args.at(1)
          if (((op == :'=' || op == :'!=') and r.is_a?(Sequel::Model)) ||
              (multiple = ((op == :IN || op == :'NOT IN') and ((is_ds = r.is_a?(Sequel::Dataset)) or r.all?{|x| x.is_a?(Sequel::Model)}))))
            l = args.at(0)
            if ar = model.association_reflections[l]
              if multiple
                klass = ar.associated_class
                if is_ds
                  if r.respond_to?(:model)
                    unless r.model <= klass
                      # A dataset for a different model class, could be a valid regular query
                      return super
                    end
                  else
                    # Not a model dataset, could be a valid regular query
                    return super
                  end
                else
                  unless r.all?{|x| x.is_a?(klass)}
                    raise Sequel::Error, "invalid association class for one object for association #{l.inspect} used in dataset filter for model #{model.inspect}, expected class #{klass.inspect}"
                  end
                end
              elsif !r.is_a?(ar.associated_class)
                raise Sequel::Error, "invalid association class #{r.class.inspect} for association #{l.inspect} used in dataset filter for model #{model.inspect}, expected class #{ar.associated_class.inspect}"
              end

              if exp = association_filter_expression(op, ar, r)
                literal_append(sql, exp)
              else
                raise Sequel::Error, "invalid association type #{ar[:type].inspect} for association #{l.inspect} used in dataset filter for model #{model.inspect}"
              end
            elsif multiple && (is_ds || r.empty?)
              # Not a query designed for this support, could be a valid regular query
              super
            else
              raise Sequel::Error, "invalid association #{l.inspect} used in dataset filter for model #{model.inspect}"
            end
          else
            super
          end
        end

        # The preferred eager loading method.  Loads all associated records using one
        # query for each association.
        #
        # The basic idea for how it works is that the dataset is first loaded normally.
        # Then it goes through all associations that have been specified via +eager+.
        # It loads each of those associations separately, then associates them back
        # to the original dataset via primary/foreign keys.  Due to the necessity of
        # all objects being present, you need to use +all+ to use eager loading, as it
        # can't work with +each+.
        #
        # This implementation avoids the complexity of extracting an object graph out
        # of a single dataset, by building the object graph out of multiple datasets,
        # one for each association.  By using a separate dataset for each association,
        # it avoids problems such as aliasing conflicts and creating cartesian product
        # result sets if multiple one_to_many or many_to_many eager associations are requested.
        #
        # One limitation of using this method is that you cannot filter the dataset
        # based on values of columns in an associated table, since the associations are loaded
        # in separate queries.  To do that you need to load all associations in the
        # same query, and extract an object graph from the results of that query. If you
        # need to filter based on columns in associated tables, look at +eager_graph+
        # or join the tables you need to filter on manually. 
        #
        # Each association's order, if defined, is respected.
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
            else
              raise(Sequel::Error, 'Associations must be in the form of a symbol or hash')
            end
          end
          clone(:eager=>opt)
        end
      
        # The secondary eager loading method.  Loads all associations in a single query. This
        # method should only be used if you need to filter or order based on columns in associated tables.
        #
        # This method uses <tt>Dataset#graph</tt> to create appropriate aliases for columns in all the
        # tables.  Then it uses the graph's metadata to build the associations from the single hash, and
        # finally replaces the array of hashes with an array model objects inside all.
        #
        # Be very careful when using this with multiple one_to_many or many_to_many associations, as you can
        # create large cartesian products.  If you must graph multiple one_to_many and many_to_many associations,
        # make sure your filters are narrow if you have a large database.
        # 
        # Each association's order, if definied, is respected. +eager_graph+ probably
        # won't work correctly on a limited dataset, unless you are
        # only graphing many_to_one and one_to_one associations.
        # 
        # Does not use the block defined for the association, since it does a single query for
        # all objects.  You can use the :graph_* association options to modify the SQL query.
        #
        # Like +eager+, you need to call +all+ on the dataset for the eager loading to work.  If you just
        # call +each+, it will yield plain hashes, each containing all columns from all the tables.
        def eager_graph(*associations)
          ds = if eg = @opts[:eager_graph]
            eg = eg.dup
            [:requirements, :reflections, :reciprocals].each{|k| eg[k] = eg[k].dup}
            clone(:eager_graph=>eg)
          else
            # Each of the following have a symbol key for the table alias, with the following values: 
            # :reciprocals - the reciprocal instance variable to use for this association
            # :reflections - AssociationReflection instance related to this association
            # :requirements - array of requirements for this association
            clone(:eager_graph=>{:requirements=>{}, :master=>alias_symbol(first_source), :reflections=>{}, :reciprocals=>{}, :cartesian_product_number=>0})
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
        # ds :: Current dataset
        # model :: Current Model
        # ta :: table_alias used for the parent association
        # requirements :: an array, used as a stack for requirements
        # r :: association reflection for the current association, or an SQL::AliasedExpression
        #      with the reflection as the expression and the alias base as the aliaz.
        # *associations :: any associations dependent on this one
        def eager_graph_association(ds, model, ta, requirements, r, *associations)
          if r.is_a?(SQL::AliasedExpression)
            alias_base = r.aliaz
            r = r.expression
          else
            alias_base = r[:graph_alias_base]
          end
          assoc_table_alias = ds.unused_table_alias(alias_base)
          loader = r[:eager_grapher]
          if !associations.empty?
            if associations.first.respond_to?(:call)
              callback = associations.first
              associations = {}
            elsif associations.length == 1 && (assocs = associations.first).is_a?(Hash) && assocs.length == 1 && (pr_assoc = assocs.to_a.first) && pr_assoc.first.respond_to?(:call)
              callback, assoc = pr_assoc
              associations = assoc.is_a?(Array) ? assoc : [assoc]
            end
          end
          ds = if loader.arity == 1
            loader.call(:self=>ds, :table_alias=>assoc_table_alias, :implicit_qualifier=>ta, :callback=>callback)
          else
            loader.call(ds, assoc_table_alias, ta)
          end
          ds = ds.order_more(*qualified_expression(r[:order], assoc_table_alias)) if r[:order] and r[:order_eager_graph]
          eager_graph = ds.opts[:eager_graph]
          eager_graph[:requirements][assoc_table_alias] = requirements.dup
          eager_graph[:reflections][assoc_table_alias] = r
          eager_graph[:cartesian_product_number] += r[:cartesian_product_number] || 2
          ds = ds.eager_graph_associations(ds, r.associated_class, assoc_table_alias, requirements + [assoc_table_alias], *associations) unless associations.empty?
          ds
        end

        # Check the associations are valid for the given model.
        # Call eager_graph_association on each association.
        #
        # Arguments:
        # ds :: Current dataset
        # model :: Current Model
        # ta :: table_alias used for the parent association
        # requirements :: an array, used as a stack for requirements
        # *associations :: the associations to add to the graph
        def eager_graph_associations(ds, model, ta, requirements, *associations)
          return ds if associations.empty?
          associations.flatten.each do |association|
            ds = case association
            when Symbol, SQL::AliasedExpression
              ds.eager_graph_association(ds, model, ta, requirements, eager_graph_check_association(model, association))
            when Hash
              association.each do |assoc, assoc_assocs|
                ds = ds.eager_graph_association(ds, model, ta, requirements, eager_graph_check_association(model, assoc), assoc_assocs)
              end
              ds
            else
              raise(Sequel::Error, 'Associations must be in the form of a symbol or hash')
            end
          end
          ds
        end

        # Replace the array of plain hashes with an array of model objects will all eager_graphed
        # associations set in the associations cache for each object.
        def eager_graph_build_associations(hashes)
          hashes.replace(EagerGraphLoader.new(self).load(hashes))
        end
      
        private
      
        # Return an expression for filtering by the given association reflection and associated object.
        def association_filter_expression(op, ref, obj)
          meth = :"#{ref[:type]}_association_filter_expression"
          send(meth, op, ref, obj) if respond_to?(meth, true)
        end

        # Handle inversion for association filters by returning an inverted expression,
        # plus also handling cases where the referenced columns are NULL.
        def association_filter_handle_inversion(op, exp, cols)
          if op == :'!=' || op == :'NOT IN'
            if exp == SQL::Constants::FALSE
              ~exp
            else
              ~exp | Sequel::SQL::BooleanExpression.from_value_pairs(cols.zip([]), :OR)
            end
          else
            exp
          end
        end

        # Return an expression for making sure that the given keys match the value of
        # the given methods for either the single object given or for any of the objects
        # given if +obj+ is an array.
        def association_filter_key_expression(keys, meths, obj)
          vals = if obj.is_a?(Sequel::Dataset)
            {(keys.length == 1 ? keys.first : keys)=>obj.select(*meths).exclude(Sequel::SQL::BooleanExpression.from_value_pairs(meths.zip([]), :OR))}
          else
            vals = Array(obj).reject{|o| !meths.all?{|m| o.send(m)}}
            return SQL::Constants::FALSE if vals.empty?
            if obj.is_a?(Array)
              if keys.length == 1
                meth = meths.first
                {keys.first=>vals.map{|o| o.send(meth)}}
              else
                {keys=>vals.map{|o| meths.map{|m| o.send(m)}}}
              end  
            else
              keys.zip(meths.map{|k| obj.send(k)})
            end
          end
          SQL::BooleanExpression.from_value_pairs(vals)
        end

        # Make sure the association is valid for this model, and return the related AssociationReflection.
        def check_association(model, association)
          raise(Sequel::UndefinedAssociation, "Invalid association #{association} for #{model.name}") unless reflection = model.association_reflection(association)
          raise(Sequel::Error, "Eager loading is not allowed for #{model.name} association #{association}") if reflection[:allow_eager] == false
          reflection
        end
      
        # Allow associations that are eagerly graphed to be specified as an SQL::AliasedExpression, for
        # per-call determining of the alias base.
        def eager_graph_check_association(model, association)
          if association.is_a?(SQL::AliasedExpression)
            SQL::AliasedExpression.new(check_association(model, association.expression), association.aliaz)
          else
            check_association(model, association)
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
          reflections = eager_assoc.keys.collect{|assoc| model.association_reflection(assoc) || (raise Sequel::UndefinedAssociation, "Model: #{self}, Association: #{assoc}")}
      
          # Populate the key_hash entry for each association being eagerly loaded
          reflections.each do |r|
            if key = r.eager_loader_key
              # key_hash for this key has already been populated,
              # skip populating again so that duplicate values
              # aren't added.
              unless id_map = key_hash[key]
                id_map = key_hash[key] = Hash.new{|h,k| h[k] = []}

                # Supporting both single (Symbol) and composite (Array) keys.
                a.each do |rec|
                  case key
                  when Array
                    if (k = key.map{|k2| rec.send(k2)}) && k.all?
                      id_map[k] << rec
                    end
                  when Symbol
                    if k = rec.send(key)
                      id_map[k] << rec
                    end
                  else
                    raise Error, "unhandled eager_loader_key #{key.inspect} for association #{r[:name]}"
                  end
                end
              end
            else
              id_map = nil
            end
          
            loader = r[:eager_loader]
            associations = eager_assoc[r[:name]]
            if associations.respond_to?(:call)
              eager_block = associations
              associations = {}
            elsif associations.is_a?(Hash) && associations.length == 1 && (pr_assoc = associations.to_a.first) && pr_assoc.first.respond_to?(:call)
              eager_block, associations = pr_assoc
            end
            if loader.arity == 1
              loader.call(:key_hash=>key_hash, :rows=>a, :associations=>associations, :self=>self, :eager_block=>eager_block, :id_map=>id_map)
            else
              loader.call(key_hash, a, associations)
            end
            a.each{|object| object.send(:run_association_callbacks, r, :after_load, object.associations[r[:name]])} unless r[:after_load].empty?
          end 
        end

        # Return plain hashes instead of calling the row_proc if eager_graph is being used.
        def graph_each(&block)
          @opts[:eager_graph] ? fetch_rows(select_sql, &block) : super
        end

        # Return a subquery expression for filering by a many_to_many association
        def many_to_many_association_filter_expression(op, ref, obj)
          lpks, lks, rks = ref.values_at(:left_primary_key_columns, :left_keys, :right_keys)
          jt = ref.join_table_alias
          lpks = lpks.first if lpks.length == 1
          lpks = ref.qualify(model.table_name, lpks)

          meths = if obj.is_a?(Sequel::Dataset)
            ref.qualify(obj.model.table_name, ref.right_primary_keys)
          else
            ref.right_primary_key_methods
          end

          exp = association_filter_key_expression(ref.qualify(jt, rks), meths, obj)
          if exp == SQL::Constants::FALSE
            association_filter_handle_inversion(op, exp, Array(lpks))
          else
            association_filter_handle_inversion(op, SQL::BooleanExpression.from_value_pairs(lpks=>model.db.from(ref[:join_table]).select(*ref.qualify(jt, lks)).where(exp).exclude(SQL::BooleanExpression.from_value_pairs(ref.qualify(jt, lks).zip([]), :OR))), Array(lpks))
          end
        end

        # Return a simple equality expression for filering by a many_to_one association
        def many_to_one_association_filter_expression(op, ref, obj)
          keys = ref.qualify(model.table_name, ref[:key_columns])
          meths = if obj.is_a?(Sequel::Dataset)
            ref.qualify(obj.model.table_name, ref.primary_keys)
          else
            ref.primary_key_methods
          end
          association_filter_handle_inversion(op, association_filter_key_expression(keys, meths, obj), keys)
        end

        # Return a simple equality expression for filering by a one_to_* association
        def one_to_many_association_filter_expression(op, ref, obj)
          keys = ref.qualify(model.table_name, ref[:primary_key_columns])
          meths = if obj.is_a?(Sequel::Dataset)
            ref.qualify(obj.model.table_name, ref[:keys])
          else
            ref[:key_methods]
          end
          association_filter_handle_inversion(op, association_filter_key_expression(keys, meths, obj), keys)
        end
        alias one_to_one_association_filter_expression one_to_many_association_filter_expression

        # Build associations from the graph if #eager_graph was used, 
        # and/or load other associations if #eager was used.
        def post_load(all_records)
          eager_graph_build_associations(all_records) if @opts[:eager_graph]
          eager_load(all_records) if @opts[:eager]
          super
        end
      end

      # This class is the internal implementation of eager_graph.  It is responsible for taking an array of plain
      # hashes and returning an array of model objects with all eager_graphed associations already set in the
      # association cache.
      class EagerGraphLoader
        # Hash with table alias symbol keys and after_load hook values
        attr_reader :after_load_map
        
        # Hash with table alias symbol keys and association name values
        attr_reader :alias_map
        
        # Hash with table alias symbol keys and subhash values mapping column_alias symbols to the
        # symbol of the real name of the column
        attr_reader :column_maps
        
        # Recursive hash with table alias symbol keys mapping to hashes with dependent table alias symbol keys.
        attr_reader :dependency_map
        
        # Hash with table alias symbol keys and [limit, offset] values
        attr_reader :limit_map
        
        # Hash with table alias symbol keys and callable values used to create model instances
        # The table alias symbol for the primary model
        attr_reader :master
        
        # Hash with table alias symbol keys and primary key symbol values (or arrays of primary key symbols for
        # composite key tables)
        attr_reader :primary_keys

        # Hash with table alias symbol keys and reciprocal association symbol values,
        # used for setting reciprocals for one_to_many associations.
        attr_reader :reciprocal_map
        
        # Hash with table alias symbol keys and subhash values mapping primary key symbols (or array of symbols)
        # to model instances.  Used so that only a single model instance is created for each object.
        attr_reader :records_map
        
        # Hash with table alias symbol keys and AssociationReflection values
        attr_reader :reflection_map
        
        # Hash with table alias symbol keys and callable values used to create model instances
        attr_reader :row_procs
        
        # Hash with table alias symbol keys and true/false values, where true means the
        # association represented by the table alias uses an array of values instead of
        # a single value (i.e. true => *_many, false => *_to_one).
        attr_reader :type_map

        # Initialize all of the data structures used during loading.
        def initialize(dataset)
          opts = dataset.opts
          eager_graph = opts[:eager_graph]
          @master =  eager_graph[:master]
          requirements = eager_graph[:requirements]
          reflection_map = @reflection_map = eager_graph[:reflections]
          reciprocal_map = @reciprocal_map = eager_graph[:reciprocals]
          @unique = eager_graph[:cartesian_product_number] > 1
      
          alias_map = @alias_map = {}
          type_map = @type_map = {}
          after_load_map = @after_load_map = {}
          limit_map = @limit_map = {}
          reflection_map.each do |k, v|
            alias_map[k] = v[:name]
            type_map[k] = v.returns_array?
            after_load_map[k] = v[:after_load] unless v[:after_load].empty?
            limit_map[k] = v.limit_and_offset if v[:limit]
          end

          # Make dependency map hash out of requirements array for each association.
          # This builds a tree of dependencies that will be used for recursion
          # to ensure that all parts of the object graph are loaded into the
          # appropriate subordinate association.
          @dependency_map = {}
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
          # the main object column values appear in the object graph 10 times.
          # We map by primary key, if available, or by the object's entire values,
          # if not. The mapping must be per table, so create sub maps for each table
          # alias.
          records_map = {@master=>{}}
          alias_map.keys.each{|ta| records_map[ta] = {}}
          @records_map = records_map

          datasets = opts[:graph][:table_aliases].to_a.reject{|ta,ds| ds.nil?}
          column_aliases = opts[:graph_aliases] || opts[:graph][:column_aliases]
          primary_keys = {}
          column_maps = {}
          models = {}
          row_procs = {}
          datasets.each do |ta, ds|
            models[ta] = ds.model
            primary_keys[ta] = []
            column_maps[ta] = {}
            row_procs[ta] = ds.row_proc
          end
          column_aliases.each do |col_alias, tc|
            ta, column = tc
            column_maps[ta][col_alias] = column
          end
          column_maps.each do |ta, h|
            pk = models[ta].primary_key
            if pk.is_a?(Array)
              primary_keys[ta] = []
              h.select{|ca, c| primary_keys[ta] << ca if pk.include?(c)}
            else
              h.select{|ca, c| primary_keys[ta] = ca if pk == c}
            end
          end
          @column_maps = column_maps
          @primary_keys = primary_keys
          @row_procs = row_procs

          # For performance, create two special maps for the master table,
          # so you can skip a hash lookup.
          @master_column_map = column_maps[master]
          @master_primary_keys = primary_keys[master]

          # Add a special hash mapping table alias symbols to 5 element arrays that just
          # contain the data in other data structures for that table alias.  This is
          # used for performance, to get all values in one hash lookup instead of
          # separate hash lookups for each data structure.
          ta_map = {}
          alias_map.keys.each do |ta|
            ta_map[ta] = [records_map[ta], row_procs[ta], alias_map[ta], type_map[ta], reciprocal_map[ta]]
          end
          @ta_map = ta_map
        end

        # Return an array of primary model instances with the associations cache prepopulated
        # for all model objects (both primary and associated).
        def load(hashes)
          master = master()
      
          # Assign to local variables for speed increase
          rp = row_procs[master]
          rm = records_map[master]
          dm = dependency_map

          # This will hold the final record set that we will be replacing the object graph with.
          records = []

          hashes.each do |h|
            unless key = master_pk(h)
              key = hkey(master_hfor(h))
            end
            unless primary_record = rm[key]
              primary_record = rm[key] = rp.call(master_hfor(h))
              # Only add it to the list of records to return if it is a new record
              records.push(primary_record)
            end
            # Build all associations for the current object and it's dependencies
            _load(dm, primary_record, h)
          end
      
          # Remove duplicate records from all associations if this graph could possibly be a cartesian product
          # Run after_load procs if there are any
          post_process(records, dm) if @unique || !after_load_map.empty? || !limit_map.empty?

          records
        end
      
        private

        # Recursive method that creates associated model objects and associates them to the current model object.
        def _load(dependency_map, current, h)
          dependency_map.each do |ta, deps|
            unless key = pk(ta, h)
              ta_h = hfor(ta, h)
              unless ta_h.values.any?
                assoc_name = alias_map[ta]
                unless (assoc = current.associations).has_key?(assoc_name)
                  assoc[assoc_name] = type_map[ta] ? [] : nil
                end
                next
              end
              key = hkey(ta_h)
            end
            rm, rp, assoc_name, tm, rcm = @ta_map[ta]
            unless rec = rm[key]
              rec = rm[key] = rp.call(hfor(ta, h))
            end

            if tm
              unless (assoc = current.associations).has_key?(assoc_name)
                assoc[assoc_name] = []
              end
              assoc[assoc_name].push(rec) 
              rec.associations[rcm] = current if rcm
            else
              current.associations[assoc_name] ||= rec
            end
            # Recurse into dependencies of the current object
            _load(deps, rec, h) unless deps.empty?
          end
        end
      
        # Return the subhash for the specific table alias +ta+ by parsing the values out of the main hash +h+
        def hfor(ta, h)
          out = {}
          @column_maps[ta].each{|ca, c| out[c] = h[ca]}
          out
        end

        # Return a suitable hash key for any subhash +h+, which is an array of values by column order.
        # This is only used if the primary key cannot be used.
        def hkey(h)
          h.sort_by{|x| x[0].to_s}
        end

        # Return the subhash for the master table by parsing the values out of the main hash +h+
        def master_hfor(h)
          out = {}
          @master_column_map.each{|ca, c| out[c] = h[ca]}
          out
        end

        # Return a primary key value for the master table by parsing it out of the main hash +h+.
        def master_pk(h)
          x = @master_primary_keys
          if x.is_a?(Array)
            unless x == []
              x = x.map{|ca| h[ca]}
              x if x.all?
            end
          else
            h[x]
          end
        end

        # Return a primary key value for the given table alias by parsing it out of the main hash +h+.
        def pk(ta, h)
          x = primary_keys[ta]
          if x.is_a?(Array)
            unless x == []
              x = x.map{|ca| h[ca]}
              x if x.all?
            end
          else
            h[x]
          end
        end

        # If the result set is the result of a cartesian product, then it is possible that
        # there are multiple records for each association when there should only be one.
        # In that case, for each object in all associations loaded via +eager_graph+, run
        # uniq! on the association to make sure no duplicate records show up.
        # Note that this can cause legitimate duplicate records to be removed.
        def post_process(records, dependency_map)
          records.each do |record|
            dependency_map.each do |ta, deps|
              assoc_name = alias_map[ta]
              list = record.send(assoc_name)
              rec_list = if type_map[ta]
                list.uniq!
                if lo = limit_map[ta]
                  limit, offset = lo
                  list.replace(list[offset||0, limit])
                end
                list
              elsif list
                [list]
              else
                []
              end
              record.send(:run_association_callbacks, reflection_map[ta], :after_load, list) if after_load_map[ta]
              post_process(rec_list, deps) if !rec_list.empty? && !deps.empty?
            end
          end
        end
      end
    end
  end
end
