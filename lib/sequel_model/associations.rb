# Associations are used in order to specify relationships between model classes
# that reflect relations between tables in the database using foreign keys.
#
# Each kind of association adds a number of methods to the model class which
# are specialized according to the association type and optional parameters
# given in the definition. Example:
# 
#   class Project < Sequel::Model
#     many_to_one :portfolio
#     one_to_many :milestones
#   end
# 
# The project class now has the following instance methods:
# * portfolio - Returns the associated portfolio.
# * portfolio=(obj) - Sets the associated portfolio to the object,
#   but the change is not persisted until you save the record.
# * portfolio_dataset - Returns a dataset that would return the associated
#   portfolio, only useful in fairly specific circumstances.
# * milestones - Returns an array of associated milestones
# * add_milestone(obj) - Associates the passed milestone with this object.
# * remove_milestone(obj) - Removes the association with the passed milestone.
# * remove_all_milestones - Removes associations with all associated milestones.
# * milestones_dataset - Returns a dataset that would return the associated
#   milestones, allowing for further filtering/limiting/etc.
#
# If you want to override the behavior of the add_/remove_/remove_all_ methods,
# there are private instance methods created that a prepended with an
# underscore (e.g. _add_milestone).  The private instance methods can be
# easily overridden, but you shouldn't override the public instance methods,
# as they deal with how associations are cached.
#
# By default the classes for the associations are inferred from the association
# name, so for example the Project#portfolio will return an instance of 
# Portfolio, and Project#milestones will return an array of Milestone 
# instances, in similar fashion to how ActiveRecord infers class names.
#
# Association definitions are also reflected by the class, e.g.:
#
#   Project.associations
#   => [:portfolio, :milestones]
#   Project.association_reflection(:portfolio)
#   => {:type => :many_to_one, :name => :portfolio, :class_name => "Portfolio"}
#
# Associations can be defined by either using the associate method, or by
# calling one of the three methods: many_to_one, one_to_many, many_to_many.
# Sequel::Model also provides aliases for these methods that conform to
# ActiveRecord conventions: belongs_to, has_many, has_and_belongs_to_many.
# For example, the following three statements are equivalent:
#
#   associate :one_to_many, :attributes
#   one_to_many :attributes
#   has_many :attributes
module Sequel::Model::Associations
  # This module contains methods added to all association datasets
  module DatasetMethods
    # The model object that created the association dataset
    attr_accessor :model_object

    # The association reflection related to the association dataset
    attr_accessor :association_reflection
  end

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
  #   Similar to ActiveRecord's belongs_to.
  # * :one_to_many - Foreign key in associated model's table points to this
  #   model's primary key.   Each current model object can be associated with
  #   more than one associated model objects.  Each associated model object
  #   can be associated with only one current model object.
  #   Similar to ActiveRecord's has_many.
  # * :many_to_many - A join table is used that has a foreign key that points
  #   to this model's primary key and a foreign key that points to the
  #   associated model's primary key.  Each current model object can be
  #   associated with many associated model objects, and each associated
  #   model object can be associated with many current model objects.
  #   Similar to ActiveRecord's has_and_belongs_to_many.
  #
  # A one to one relationship can be set up with a many_to_one association
  # on the table with the foreign key, and a one_to_many association with the
  # :one_to_one option specified on the table without the foreign key.  The
  # two associations will operate similarly, except that the many_to_one
  # association setter doesn't update the database until you call save manually.
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
  #   - :allow_eager - If set to false, you cannot load the association eagerly
  #     via eager or eager_graph
  #   - :before_add - Symbol, Proc, or array of both/either specifying a callback to call
  #     before a new item is added to the association.
  #   - :before_remove - Symbol, Proc, or array of both/either specifying a callback to call
  #     before an item is removed from the association.
  #   - :class - The associated class or its name. If not
  #     given, uses the association's name, which is camelized (and
  #     singularized unless the type is :many_to_one)
  #   - :clone - Merge the current options and block into the options and block used in defining
  #     the given association.  Can be used to DRY up a bunch of similar associations that
  #     all share the same options such as :class and :key, while changing the order and block used.
  #   - :conditions - The conditions to use to filter the association, can be any argument passed to filter.
  #   - :dataset - A proc that is instance_evaled to get the base dataset
  #     to use for the _dataset method (before the other options are applied).
  #   - :eager - The associations to eagerly load via EagerLoading#eager when loading the associated object(s).
  #     For many_to_one associations, this is ignored unless this association is
  #     being eagerly loaded, as it doesn't save queries unless multiple objects
  #     can be loaded at once.
  #   - :eager_block - If given, use the block instead of the default block when
  #     eagerly loading.  To not use a block when eager loading (when one is used normally),
  #     set to nil.
  #   - :eager_graph - The associations to eagerly load via EagerLoading#eager_graph when loading the associated object(s).
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
  #     an array with two arguments for the value to specify a limit and offset.
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
  #     table_name.*, which means it doesn't include the attributes from the
  #     join table in a many_to_many association.  If you want to include the join table attributes, you can
  #     use this option, but beware that the join table attributes can clash with
  #     attributes from the model table, so you should alias any attributes that have
  #     the same name in both the join table and the associated table.
  # * :many_to_one:
  #   - :key - foreign_key in current model's table that references
  #     associated model's primary key, as a symbol.  Defaults to :"#{name}_id".
  #   - :primary_key - column in the associated table that :key option references, as a symbol.
  #     Defaults to the primary key of the associated table.
  # * :one_to_many:
  #   - :key - foreign key in associated model's table that references
  #     current model's primary key, as a symbol.  Defaults to
  #     :"#{self.name.underscore}_id".
  #   - :one_to_one: Create a getter and setter similar to those of many_to_one
  #     associations.  The getter returns a singular matching record, or raises an
  #     error if multiple records match.  The setter updates the record given and removes
  #     associations with all other records. When this option is used, the other
  #     association methods usually added are either removed or made private,
  #     so using this is similar to using many_to_one, in terms of the methods
  #     it adds, the main difference is that the foreign key is in the associated
  #     table instead of the current table.
  #   - :primary_key - column in the current table that :key option references, as a symbol.
  #     Defaults to primary key of the current table.
  # * :many_to_many:
  #   - :graph_join_table_block - The block to pass to join_table for
  #     the join table when eagerly loading the association via eager_graph.
  #   - :graph_join_table_conditions - The additional conditions to use on the SQL join for
  #     the join table when eagerly loading the association via eager_graph. Should be a hash
  #     or an array of all two pairs.
  #   - :graph_join_type - The type of SQL join to use for the join table when eagerly
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
  #   - :left_primary_key - column in current table that :left_key points to, as a symbol.
  #     Defaults to primary key of current table.
  #   - :right_key - foreign key in join table that points to associated
  #     model's primary key, as a symbol.  Defaults to Defaults to :"#{name.to_s.singularize}_id".
  #   - :right_primary_key - column in associated table that :right_key points to, as a symbol.
  #     Defaults to primary key of the associated table.
  #   - :uniq - Adds a after_load callback that makes the array of objects unique.
  def associate(type, name, opts = {}, &block)
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
    opts[:graph_conditions] = conds if !opts.include?(:graph_conditions) and (conds.is_a?(Hash) or (conds.is_a?(Array) and conds.all_two_pairs?))
    opts[:graph_conditions] = opts[:graph_conditions] ? opts[:graph_conditions].to_a : []
    opts[:graph_select] = Array(opts[:graph_select]) if opts[:graph_select]
    [:before_add, :before_remove, :after_add, :after_remove, :after_load, :extend].each do |cb_type|
      opts[cb_type] = Array(opts[cb_type])
    end

    # find class
    case opts[:class]
      when String, Symbol
        # Delete :class to allow late binding
        opts[:class_name] ||= opts.delete(:class).to_s
      when Class
        opts[:class_name] ||= opts[:class].name
    end

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

  # Shortcut for adding a one_to_many association, see associate
  def one_to_many(*args, &block)
    associate(:one_to_many, *args, &block)
  end
  
  # Shortcut for adding a many_to_one association, see associate
  def many_to_one(*args, &block)
    associate(:many_to_one, *args, &block)
  end
  
  # Shortcut for adding a many_to_many association, see associate
  def many_to_many(*args, &block)
    associate(:many_to_many, *args, &block)
  end
  
  private

  # Add a method to the association module
  def association_module_def(name, &block)
    overridable_methods_module.module_eval{define_method(name, &block)}
  end

  # Add a method to the association module
  def association_module_private_def(name, &block)
    association_module_def(name, &block)
    overridable_methods_module.send(:private, name)
  end

  # Add the add_ instance method 
  def def_add_method(opts)
    association_module_def(opts.add_method){|o| add_associated_object(opts, o)}
  end

  # Adds association methods to the model for *_to_many associations.
  def def_association_dataset_methods(opts)
    # If a block is given, define a helper method for it, because it takes
    # an argument.  This is unnecessary in Ruby 1.9, as that has instance_exec.
    association_module_private_def(opts.dataset_helper_method, &opts[:block]) if opts[:block]
    association_module_private_def(opts._dataset_method, &opts[:dataset])
    association_module_def(opts.dataset_method){_dataset(opts)}
    association_module_def(opts.association_method){|*reload| load_associated_objects(opts, reload[0])}
  end

  # Adds many_to_many association instance methods
  def def_many_to_many(opts)
    name = opts[:name]
    model = self
    left = (opts[:left_key] ||= opts.default_left_key)
    right = (opts[:right_key] ||= opts.default_right_key)
    left_pk = (opts[:left_primary_key] ||= self.primary_key)
    opts[:class_name] ||= camelize(singularize(name))
    join_table = (opts[:join_table] ||= opts.default_join_table)
    left_key_alias = opts[:left_key_alias] ||= :x_foreign_key_x
    left_key_select = opts[:left_key_select] ||= left.qualify(join_table).as(opts[:left_key_alias])
    graph_jt_conds = opts[:graph_join_table_conditions] = opts[:graph_join_table_conditions] ? opts[:graph_join_table_conditions].to_a : []
    opts[:graph_join_table_join_type] ||= opts[:graph_join_type]
    opts[:after_load].unshift(:array_uniq!) if opts[:uniq]
    opts[:dataset] ||= proc{opts.associated_class.inner_join(join_table, [[right, opts.right_primary_key], [left, send(left_pk)]])}
    database = db
    
    opts[:eager_loader] ||= proc do |key_hash, records, associations|
      h = key_hash[left_pk]
      records.each{|object| object.associations[name] = []}
      model.eager_loading_dataset(opts, opts.associated_class.inner_join(join_table, [[right, opts.right_primary_key], [left, h.keys]]), Array(opts.select) + Array(left_key_select), associations).all do |assoc_record|
        next unless objects = h[assoc_record.values.delete(left_key_alias)]
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
      ds = ds.graph(join_table, use_jt_only_conditions ? jt_only_conditions : [[left, left_pk]] + graph_jt_conds, :select=>false, :table_alias=>ds.send(:eager_unique_table_alias, ds, join_table), :join_type=>jt_join_type, :implicit_qualifier=>table_alias, &jt_graph_block)
      ds.graph(opts.associated_class, use_only_conditions ? only_conditions : [[opts.right_primary_key, right]] + conditions, :select=>select, :table_alias=>assoc_alias, :join_type=>join_type, &graph_block)
    end

    def_association_dataset_methods(opts)

    return if opts[:read_only]

    association_module_private_def(opts._add_method) do |o|
      database.dataset.from(join_table).insert(left=>send(left_pk), right=>o.send(opts.right_primary_key))
    end
    association_module_private_def(opts._remove_method) do |o|
      database.dataset.from(join_table).filter([[left, send(left_pk)], [right, o.send(opts.right_primary_key)]]).delete
    end
    association_module_private_def(opts._remove_all_method) do
      database.dataset.from(join_table).filter(left=>send(left_pk)).delete
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
    opts[:class_name] ||= camelize(name)
    opts[:dataset] ||= proc do
      klass = opts.associated_class
      klass.filter(opts.primary_key.qualify(klass.table_name)=>send(key))
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
        model.eager_loading_dataset(opts, klass.filter(opts.primary_key.qualify(klass.table_name)=>keys), opts.select, associations).all do |assoc_record|
          next unless objects = h[assoc_record.send(opts.primary_key)]
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
      ds.graph(opts.associated_class, use_only_conditions ? only_conditions : [[opts.primary_key, key]] + conditions, :select=>select, :table_alias=>assoc_alias, :join_type=>join_type, :implicit_qualifier=>table_alias, &graph_block)
    end

    def_association_dataset_methods(opts)
    
    return if opts[:read_only]

    association_module_private_def(opts._setter_method){|o| send(:"#{key}=", (o.send(opts.primary_key) if o))}
    association_module_def(opts.setter_method){|o| set_associated_object(opts, o)}
  end
  
  # Adds one_to_many association instance methods
  def def_one_to_many(opts)
    name = opts[:name]
    model = self
    key = (opts[:key] ||= opts.default_key)
    primary_key = (opts[:primary_key] ||= self.primary_key)
    opts[:class_name] ||= camelize(singularize(name))
    opts[:dataset] ||= proc do
      klass = opts.associated_class
      klass.filter(key.qualify(klass.table_name) => send(primary_key))
    end
    opts[:eager_loader] ||= proc do |key_hash, records, associations|
      h = key_hash[primary_key]
      records.each{|object| object.associations[name] = []}
      reciprocal = opts.reciprocal
      klass = opts.associated_class
      model.eager_loading_dataset(opts, klass.filter(key.qualify(klass.table_name)=>h.keys), opts.select, associations).all do |assoc_record|
        next unless objects = h[assoc_record[key]]
        objects.each do |object| 
          object.associations[name].push(assoc_record)
          assoc_record.associations[reciprocal] = object if reciprocal
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
      ds = ds.graph(opts.associated_class, use_only_conditions ? only_conditions : [[key, primary_key]] + conditions, :select=>select, :table_alias=>assoc_alias, :join_type=>join_type, :implicit_qualifier=>table_alias, &graph_block)
      # We only load reciprocals for one_to_many associations, as other reciprocals don't make sense
      ds.opts[:eager_graph][:reciprocals][assoc_alias] = opts.reciprocal
      ds
    end

    def_association_dataset_methods(opts)
    
    unless opts[:read_only]
      association_module_private_def(opts._add_method) do |o|
        o.send(:"#{key}=", send(primary_key))
        o.save || raise(Sequel::Error, "invalid associated object, cannot save")
      end
      def_add_method(opts)

      unless opts[:one_to_one]
        association_module_private_def(opts._remove_method) do |o|
          o.send(:"#{key}=", nil)
          o.save || raise(Sequel::Error, "invalid associated object, cannot save")
        end
        association_module_private_def(opts._remove_all_method) do
          opts.associated_class.filter(key=>send(primary_key)).update(key=>nil)
        end
        def_remove_methods(opts)
      end
    end
    if opts[:one_to_one]
      overridable_methods_module.send(:private, opts.association_method, opts.dataset_method)
      n = singularize(name).to_sym
      raise(Sequel::Error, "one_to_many association names should still be plural even when using the :one_to_one option") if n == name
      association_module_def(n) do |*o|
        objs = send(name, *o)
        raise(Sequel::Error, "multiple values found for a one-to-one relationship") if objs.length > 1
        objs.first
      end
      unless opts[:read_only]
        overridable_methods_module.send(:private, opts.add_method)
        association_module_def(:"#{n}=") do |o|
          klass = opts.associated_class
          model.db.transaction do
            send(opts.add_method, o)
            klass.filter(Sequel::SQL::BooleanExpression.new(:AND, {key=>send(primary_key)}, ~{klass.primary_key=>o.pk}.sql_expr)).update(key=>nil)
          end
        end
      end
    end
  end
  
  # Add the remove_ and remove_all instance methods
  def def_remove_methods(opts)
    association_module_def(opts.remove_method){|o| remove_associated_object(opts, o)}
    association_module_def(opts.remove_all_method){remove_all_associated_objects(opts)}
  end
end

class Sequel::Model
  extend Associations
end
