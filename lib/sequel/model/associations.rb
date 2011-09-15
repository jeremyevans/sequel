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
    
        # The eager limit strategy to use for this dataset.
        def eager_limit_strategy
          fetch(:_eager_limit_strategy) do
            self[:_eager_limit_strategy] = if self[:limit]
              if s = self[:eager_limit_strategy]
                s
              else
                :ruby
              end
            else
              nil
            end
          end
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
    
        # The limit and offset for this association (returned as a two element array).
        def limit_and_offset
          if (v = self[:limit]).is_a?(Array)
            v
          else
            [v, 0]
          end
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
        
        # one_to_one associations don't need an eager limit strategy
        def eager_limit_strategy
          fetch(:_eager_limit_strategy) do
            self[:_eager_limit_strategy] = case s = self[:eager_limit_strategy]
            when Symbol
              s
            when true
              if associated_class.dataset.supports_ordered_distinct_on?
                :distinct_on
              end
            else
              nil
            end
          end
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

        # The table containing the column to use for the associated key when eagerly loading
        def associated_key_table
          self[:associated_key_table] ||= (
          syms = associated_class.dataset.split_alias(self[:join_table]);
          syms[1] || syms[0])
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
      #   => {:type => :many_to_one, :name => :portfolio, ...}
      #
      # For a more in depth general overview, as well as a reference guide,
      # see the {Association Basics guide}[link:files/doc/association_basics_rdoc.html].
      # For examples of advanced usage, see the {Advanced Associations guide}[link:files/doc/advanced_associations_rdoc.html].
      module ClassMethods
        # All association reflections defined for this model (default: {}).
        attr_reader :association_reflections

        # Array of all association reflections for this model class
        def all_association_reflections
          association_reflections.values
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
        # === All Types
        # :after_add :: Symbol, Proc, or array of both/either specifying a callback to call
        #               after a new item is added to the association.
        # :after_load :: Symbol, Proc, or array of both/either specifying a callback to call
        #                after the associated record(s) have been retrieved from the database.  Not called
        #                when eager loading via eager_graph, but called when eager loading via eager.
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
        # :class :: The associated class or its name. If not
        #           given, uses the association's name, which is camelized (and
        #           singularized unless the type is :many_to_one or :one_to_one)
        # :clone :: Merge the current options and block into the options and block used in defining
        #           the given association.  Can be used to DRY up a bunch of similar associations that
        #            all share the same options such as :class and :key, while changing the order and block used.
        # :conditions :: The conditions to use to filter the association, can be any argument passed to filter.
        # :dataset :: A proc that is instance_evaled to get the base dataset
        #             to use for the _dataset method (before the other options are applied).
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
        #                          one_to_one supports :distinct_on.
        #                          Other strategies will be added in the future.
        # :eager_loader :: A proc to use to implement eager loading, overriding the default.  Takes one or three arguments.
        #                  If three arguments, the first should be a key hash (used solely to enhance performance), the
        #                  second an array of records, and the third a hash of dependent associations. If one argument, is
        #                  passed a hash with keys :key_hash, :rows, and :associations, corresponding to the three
        #                  arguments, and an additional key :self, which is the dataset doing the eager loading. In the
        #                  proc, the associated records should be queried from the database and the associations cache for
        #                  each record should be populated.
        # :eager_loader_key :: A symbol for the key column to use to populate the key hash
        #                      for the eager loader.
        # :extend :: A module or array of modules to extend the dataset with.
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
        # :select :: the columns to select.  Defaults to the associated class's
        #            table_name.* in a many_to_many association, which means it doesn't include the attributes from the
        #            join table.  If you want to include the join table attributes, you can
        #            use this option, but beware that the join table attributes can clash with
        #            attributes from the model table, so you should alias any attributes that have
        #            the same name in both the join table and the associated table.
        # :validate :: Set to false to not validate when implicitly saving any associated object.
        # === :many_to_one
        # :key :: foreign key in current model's table that references
        #         associated model's primary key, as a symbol.  Defaults to :"#{name}_id".  Can use an
        #         array of symbols for a composite key association.
        # :primary_key :: column in the associated table that :key option references, as a symbol.
        #                 Defaults to the primary key of the associated table. Can use an
        #                 array of symbols for a composite key association.
        # === :one_to_many and :one_to_one
        # :key :: foreign key in associated model's table that references
        #         current model's primary key, as a symbol.  Defaults to
        #         :"#{self.name.underscore}_id".  Can use an
        #         array of symbols for a composite key association.
        # :primary_key :: column in the current table that :key option references, as a symbol.
        #                 Defaults to primary key of the current table. Can use an
        #                 array of symbols for a composite key association.
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
        # :right_key :: foreign key in join table that points to associated
        #               model's primary key, as a symbol.  Defaults to Defaults to :"#{name.to_s.singularize}_id".
        #               Can use an array of symbols for a composite key association.
        # :right_primary_key :: column in associated table that :right_key points to, as a symbol.
        #                       Defaults to primary key of the associated table.  Can use an
        #                       array of symbols for a composite key association.
        # :uniq :: Adds a after_load callback that makes the array of objects unique.
        def associate(type, name, opts = {}, &block)
          raise(Error, 'one_to_many association type with :one_to_one option removed, used one_to_one association type') if opts[:one_to_one] && type == :one_to_many
          raise(Error, 'invalid association type') unless assoc_class = ASSOCIATION_TYPES[type]
          raise(Error, 'Model.associate name argument must be a symbol') unless Symbol === name
          raise(Error, ':eager_loader option must have an arity of 1 or 3') if opts[:eager_loader] && ![1, 3].include?(opts[:eager_loader].arity)
          raise(Error, ':eager_grapher option must have an arity of 1 or 3') if opts[:eager_grapher] && ![1, 3].include?(opts[:eager_grapher].arity)

          # dup early so we don't modify opts
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

        # Modify and return eager loading dataset based on association options.
        def eager_loading_dataset(opts, ds, select, associations, eager_options={})
          ds = ds.select(*select) if select
          if c = opts[:conditions]
            ds = (c.is_a?(Array) && !Sequel.condition_specifier?(c)) ? ds.filter(*c) : ds.filter(c)
          end
          ds = ds.order(*opts[:order]) if opts[:order]
          ds = ds.eager(opts[:eager]) if opts[:eager]
          ds = ds.distinct if opts[:distinct]
          if opts[:eager_graph]
            raise(Error, "cannot eagerly load a #{opts[:type]} association that uses :eager_graph") if opts.eager_loading_use_associated_key?
            ds = ds.eager_graph(opts[:eager_graph])
          end
          ds = ds.eager(associations) unless Array(associations).empty?
          ds = opts[:eager_block].call(ds) if opts[:eager_block]
          ds = eager_options[:eager_block].call(ds) if eager_options[:eager_block]
          if opts.eager_loading_use_associated_key?
            ds = if opts[:uses_left_composite_keys]
              t = opts.associated_key_table
              ds.select_append(*opts.associated_key_alias.zip(opts.associated_key_column).map{|a, c| SQL::AliasedExpression.new(SQL::QualifiedIdentifier.new(t, c), a)})
            else
              ds.select_append(SQL::AliasedExpression.new(SQL::QualifiedIdentifier.new(opts.associated_key_table, opts.associated_key_column), opts.associated_key_alias))
            end
          end
          ds
        end

        # Copy the association reflections to the subclass
        def inherited(subclass)
          super
          subclass.instance_variable_set(:@association_reflections, @association_reflections.dup)
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
        
        private
      
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
          # If a block is given, define a helper method for it, because it takes
          # an argument.  This is unnecessary in Ruby 1.9, as that has instance_exec.
          association_module_private_def(opts.dataset_helper_method, opts, &opts[:block]) if opts[:block]
          association_module_private_def(opts._dataset_method, opts, &opts[:dataset])
          association_module_def(opts.dataset_method, opts){_dataset(opts)}
          def_association_method(opts)
        end

        # Adds the association method to the association methods module. Be backwards
        # compatible with ruby 1.8.6, which doesn't support blocks taking block arguments.
        if RUBY_VERSION >= '1.8.7'
          class_eval <<-END, __FILE__, __LINE__+1
            def def_association_method(opts)
              association_module_def(opts.association_method, opts){|*dynamic_opts, &block| load_associated_objects(opts, dynamic_opts[0], &block)}
            end
          END
        else
          class_eval <<-END, __FILE__, __LINE__+1
            def def_association_method(opts)
              association_module_def(opts.association_method, opts){|*dynamic_opts| load_associated_objects(opts, dynamic_opts[0])}
            end
          END
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
          lcpks = opts[:left_primary_keys] = Array(left_pk)
          raise(Error, "mismatched number of left composite keys: #{lcks.inspect} vs #{lcpks.inspect}") unless lcks.length == lcpks.length
          if opts[:right_primary_key]
            rcpks = Array(opts[:right_primary_key])
            raise(Error, "mismatched number of right composite keys: #{rcks.inspect} vs #{rcpks.inspect}") unless rcks.length == rcpks.length
          end
          uses_lcks = opts[:uses_left_composite_keys] = lcks.length > 1
          uses_rcks = opts[:uses_right_composite_keys] = rcks.length > 1
          opts[:cartesian_product_number] ||= 1
          join_table = (opts[:join_table] ||= opts.default_join_table)
          left_key_alias = opts[:left_key_alias] ||= opts.default_associated_key_alias
          graph_jt_conds = opts[:graph_join_table_conditions] = opts.fetch(:graph_join_table_conditions, []).to_a
          opts[:graph_join_table_join_type] ||= opts[:graph_join_type]
          opts[:after_load].unshift(:array_uniq!) if opts[:uniq]
          opts[:dataset] ||= proc{opts.associated_class.inner_join(join_table, rcks.zip(opts.right_primary_keys) + lcks.zip(lcpks.map{|k| send(k)}))}

          opts[:eager_loader] ||= proc do |eo|
            h = eo[:key_hash][left_pk]
            rows = eo[:rows]
            rows.each{|object| object.associations[name] = []}
            r = uses_rcks ? rcks.zip(opts.right_primary_keys) : [[right, opts.right_primary_key]]
            l = uses_lcks ? [[lcks.map{|k| SQL::QualifiedIdentifier.new(join_table, k)}, h.keys]] : [[left, h.keys]]
            model.eager_loading_dataset(opts, opts.associated_class.inner_join(join_table, r + l), Array(opts.select), eo[:associations], eo).all do |assoc_record|
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
              rows.each{|o| o.associations[name] = o.associations[name].slice(offset, limit) || []}
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
            ds = ds.graph(join_table, use_jt_only_conditions ? jt_only_conditions : lcks.zip(lcpks) + graph_jt_conds, :select=>false, :table_alias=>ds.unused_table_alias(join_table), :join_type=>jt_join_type, :implicit_qualifier=>eo[:implicit_qualifier], :from_self_alias=>ds.opts[:eager_graph][:master], &jt_graph_block)
            ds.graph(eager_graph_dataset(opts, eo), use_only_conditions ? only_conditions : opts.right_primary_keys.zip(rcks) + conditions, :select=>select, :table_alias=>eo[:table_alias], :join_type=>join_type, &graph_block)
          end
      
          def_association_dataset_methods(opts)
      
          return if opts[:read_only]
      
          association_module_private_def(opts._add_method, opts) do |o|
            h = {}
            lcks.zip(lcpks).each{|k, pk| h[k] = send(pk)}
            rcks.zip(opts.right_primary_keys).each{|k, pk| h[k] = o.send(pk)}
            _join_table_dataset(opts).insert(h)
          end
          association_module_private_def(opts._remove_method, opts) do |o|
            _join_table_dataset(opts).filter(lcks.zip(lcpks.map{|k| send(k)}) + rcks.zip(opts.right_primary_keys.map{|k| o.send(k)})).delete
          end
          association_module_private_def(opts._remove_all_method, opts) do
            _join_table_dataset(opts).filter(lcks.zip(lcpks.map{|k| send(k)})).delete
          end
      
          def_add_method(opts)
          def_remove_methods(opts)
        end
        
        # Configures many_to_one association reflection and adds the related association methods
        def def_many_to_one(opts)
          name = opts[:name]
          model = self
          opts[:key] = opts.default_key unless opts.include?(:key)
          key = opts[:key]
          cks = opts[:keys] = Array(opts[:key])
          if opts[:primary_key]
            cpks = Array(opts[:primary_key])
            raise(Error, "mismatched number of composite keys: #{cks.inspect} vs #{cpks.inspect}") unless cks.length == cpks.length
          end
          uses_cks = opts[:uses_composite_keys] = cks.length > 1
          opts[:cartesian_product_number] ||= 0
          opts[:dataset] ||= proc do
            klass = opts.associated_class
            klass.filter(opts.primary_keys.map{|k| SQL::QualifiedIdentifier.new(klass.table_name, k)}.zip(cks.map{|k| send(k)}))
          end
          opts[:eager_loader] ||= proc do |eo|
            h = eo[:key_hash][key]
            keys = h.keys
            # Default the cached association to nil, so any object that doesn't have it
            # populated will have cached the negative lookup.
            eo[:rows].each{|object| object.associations[name] = nil}
            # Skip eager loading if no objects have a foreign key for this association
            unless keys.empty?
              klass = opts.associated_class
              model.eager_loading_dataset(opts, klass.filter(uses_cks ? {opts.primary_keys.map{|k| SQL::QualifiedIdentifier.new(klass.table_name, k)}=>keys} : {SQL::QualifiedIdentifier.new(klass.table_name, opts.primary_key)=>keys}), opts.select, eo[:associations], eo).all do |assoc_record|
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
          opts[:eager_grapher] ||= proc do |eo|
            ds = eo[:self]
            ds.graph(eager_graph_dataset(opts, eo), use_only_conditions ? only_conditions : opts.primary_keys.zip(cks) + conditions, eo.merge(:select=>select, :join_type=>join_type, :from_self_alias=>ds.opts[:eager_graph][:master]), &graph_block)
          end
      
          def_association_dataset_methods(opts)
          
          return if opts[:read_only]
      
          association_module_private_def(opts._setter_method, opts){|o| cks.zip(opts.primary_keys).each{|k, pk| send(:"#{k}=", (o.send(pk) if o))}}
          association_module_def(opts.setter_method, opts){|o| set_associated_object(opts, o)}
        end
        
        # Configures one_to_many and one_to_one association reflections and adds the related association methods
        def def_one_to_many(opts)
          one_to_one = opts[:type] == :one_to_one
          name = opts[:name]
          model = self
          key = (opts[:key] ||= opts.default_key)
          cks = opts[:keys] = Array(key)
          primary_key = (opts[:primary_key] ||= self.primary_key)
          cpks = opts[:primary_keys] = Array(primary_key)
          raise(Error, "mismatched number of composite keys: #{cks.inspect} vs #{cpks.inspect}") unless cks.length == cpks.length
          uses_cks = opts[:uses_composite_keys] = cks.length > 1
          opts[:dataset] ||= proc do
            klass = opts.associated_class
            klass.filter(cks.map{|k| SQL::QualifiedIdentifier.new(klass.table_name, k)}.zip(cpks.map{|k| send(k)}))
          end
          opts[:eager_loader] ||= proc do |eo|
            h = eo[:key_hash][primary_key]
            rows = eo[:rows]
            if one_to_one
              rows.each{|object| object.associations[name] = nil}
            else
              rows.each{|object| object.associations[name] = []}
            end
            reciprocal = opts.reciprocal
            klass = opts.associated_class
            filter_keys = uses_cks ? cks.map{|k| SQL::QualifiedIdentifier.new(klass.table_name, k)} : SQL::QualifiedIdentifier.new(klass.table_name, key)
            ds = model.eager_loading_dataset(opts, klass.filter(filter_keys=>h.keys), opts.select, eo[:associations], eo)
            case opts.eager_limit_strategy
            when :distinct_on
              filter_keys = Array(filter_keys)
              ds = ds.distinct(*filter_keys).order_prepend(*filter_keys)
            end
            ds.all do |assoc_record|
              hash_key = uses_cks ? cks.map{|k| assoc_record.send(k)} : assoc_record.send(key)
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
              rows.each{|o| o.associations[name] = o.associations[name].slice(offset, limit) || []}
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
            ds = ds.graph(eager_graph_dataset(opts, eo), use_only_conditions ? only_conditions : cks.zip(cpks) + conditions, eo.merge(:select=>select, :join_type=>join_type, :from_self_alias=>ds.opts[:eager_graph][:master]), &graph_block)
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
              association_module_private_def(opts._setter_method, opts) do |o|
                up_ds = _apply_association_options(opts, opts.associated_class.filter(cks.zip(cpks.map{|k| send(k)})))
                if o
                  up_ds = up_ds.exclude(o.pk_hash)
                  cks.zip(cpks).each{|k, pk| o.send(:"#{k}=", send(pk))}
                end
                checked_transaction do
                  up_ds.update(ck_nil_hash)
                  o.save(:validate=>validate) || raise(Sequel::Error, "invalid associated object, cannot save") if o
                end
              end
              association_module_def(opts.setter_method, opts){|o| set_one_to_one_associated_object(opts, o)}
            else 
              association_module_private_def(opts._add_method, opts) do |o|
                cks.zip(cpks).each{|k, pk| o.send(:"#{k}=", send(pk))}
                o.save(:validate=>validate) || raise(Sequel::Error, "invalid associated object, cannot save")
              end
              def_add_method(opts)
      
              association_module_private_def(opts._remove_method, opts) do |o|
                cks.each{|k| o.send(:"#{k}=", nil)}
                o.save(:validate=>validate) || raise(Sequel::Error, "invalid associated object, cannot save")
              end
              association_module_private_def(opts._remove_all_method, opts) do
                _apply_association_options(opts, opts.associated_class.filter(cks.zip(cpks.map{|k| send(k)}))).update(ck_nil_hash)
              end
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
      
        # Formally used internally by the associations code, like pk but doesn't raise
        # an Error if the model has no primary key.  Not used any longer, deprecated.
        def pk_or_nil
          key = primary_key
          key.is_a?(Array) ? key.map{|k| @values[k]} : @values[key]
        end

        private
        
        # Apply the association options such as :order and :limit to the given dataset, returning a modified dataset.
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
          _apply_association_options(opts, send(opts._dataset_method))
        end

        # Dataset for the join table of the given many to many association reflection
        def _join_table_dataset(opts)
          ds = model.db.from(opts[:join_table])
          opts[:join_table_block] ? opts[:join_table_block].call(ds) : ds
        end

        # Return the associated objects from the dataset, without association callbacks, reciprocals, and caching.
        # Still apply the dynamic callback if present.
        def _load_associated_objects(opts, dynamic_opts={})
          if opts.returns_array?
            opts.can_have_associated_objects?(self) ? _associated_dataset(opts, dynamic_opts).all : []
          else
            if opts.can_have_associated_objects?(self)
              _associated_dataset(opts, dynamic_opts).all.first
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
            associations[name] = objs
            run_association_callbacks(opts, :after_load, objs)
            associations[name]
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

        # Set the given object as the associated object for the given many_to_one association reflection
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
        
        # Set the given object as the associated object for the given one_to_one association reflection
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
      #   Artist.eager(:albums => proc{|ds| ds.filter{year > 1990}})
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
      #   Artist.eager(:albums => {proc{|ds| ds.filter{year > 1990}}=>{:tracks => :genre}})
      module DatasetMethods
        # Add the <tt>eager!</tt> and <tt>eager_graph!</tt> mutation methods to the dataset.
        def self.extended(obj)
          obj.def_mutation_method(:eager, :eager_graph)
        end
      
        # If the expression is in the form <tt>x = y</tt> where +y+ is a <tt>Sequel::Model</tt>
        # instance, array of <tt>Sequel::Model</tt> instances, or a <tt>Sequel::Model</tt> dataset,
        # assume +x+ is an association symbol and look up the association reflection
        # via the dataset's model.  From there, return the appropriate SQL based on the type of
        # association and the values of the foreign/primary keys of +y+.  For most association
        # types, this is a simple transformation, but for +many_to_many+ associations this 
        # creates a subquery to the join table.
        def complex_expression_sql(op, args)
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
                literal(exp)
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
        # ds :: Current dataset
        # model :: Current Model
        # ta :: table_alias used for the parent association
        # requirements :: an array, used as a stack for requirements
        # r :: association reflection for the current association
        # *associations :: any associations dependent on this one
        def eager_graph_association(ds, model, ta, requirements, r, *associations)
          assoc_name = r[:name]
          assoc_table_alias = ds.unused_table_alias(assoc_name)
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
        # ds :: Current dataset
        # model :: Current Model
        # ta :: table_alias used for the parent association
        # requirements :: an array, used as a stack for requirements
        # *associations :: the associations to add to the graph
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
            loader = r[:eager_loader]
            associations = eager_assoc[r[:name]]
            if associations.respond_to?(:call)
              eager_block = associations
              associations = {}
            elsif associations.is_a?(Hash) && associations.length == 1 && (pr_assoc = associations.to_a.first) && pr_assoc.first.respond_to?(:call)
              eager_block, associations = pr_assoc
            end
            if loader.arity == 1
              loader.call(:key_hash=>key_hash, :rows=>a, :associations=>associations, :self=>self, :eager_block=>eager_block)
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
          lpks, lks, rks = ref.values_at(:left_primary_keys, :left_keys, :right_keys)
          lpks = lpks.first if lpks.length == 1
          exp = association_filter_key_expression(rks, ref.right_primary_keys, obj)
          if exp == SQL::Constants::FALSE
            association_filter_handle_inversion(op, exp, Array(lpks))
          else
            association_filter_handle_inversion(op, SQL::BooleanExpression.from_value_pairs(lpks=>model.db[ref[:join_table]].select(*lks).where(exp).exclude(SQL::BooleanExpression.from_value_pairs(lks.zip([]), :OR))), Array(lpks))
          end
        end

        # Return a simple equality expression for filering by a many_to_one association
        def many_to_one_association_filter_expression(op, ref, obj)
          keys = ref[:keys]
          association_filter_handle_inversion(op, association_filter_key_expression(keys, ref.primary_keys, obj), keys)
        end

        # Return a simple equality expression for filering by a one_to_* association
        def one_to_many_association_filter_expression(op, ref, obj)
          keys = ref[:primary_keys]
          association_filter_handle_inversion(op, association_filter_key_expression(keys, ref[:keys], obj), keys)
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
        # Hash with table alias symbol keys and association name values
        attr_reader :alias_map
        
        # Hash with table alias symbol keys and subhash values mapping column_alias symbols to the
        # symbol of the real name of the column
        attr_reader :column_maps
        
        # Recursive hash with table alias symbol keys mapping to hashes with dependent table alias symbol keys.
        attr_reader :dependency_map
        
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
          alias_map = @alias_map = eager_graph[:alias_association_name_map]
          type_map = @type_map = eager_graph[:alias_association_type_map]
          reciprocal_map = @reciprocal_map = eager_graph[:reciprocals]
          @unique = eager_graph[:cartesian_product_number] > 1
      
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
          unique(records, dm) if @unique
          
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
              current.associations[assoc_name] = rec
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
        def unique(records, dependency_map)
          records.each do |record|
            dependency_map.each do |ta, deps|
              list = record.send(alias_map[ta])
              list = if type_map[ta]
                list.uniq!
                unique(list, deps) if !list.empty? && !deps.empty?
              elsif list
                unique([list], deps) unless deps.empty?
              end
            end
          end
        end
      end
    end
  end
end
