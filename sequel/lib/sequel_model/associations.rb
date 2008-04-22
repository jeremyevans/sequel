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
# The project class now has the following methods:
# * Project#portfolio, Project#portfolio=
# * Project#milestones, Project#add_milestone, Project#remove_milestone,
#   Project#milestones_dataset
#
# By default the classes for the associations are inferred from the association
# name, so for example the Project#portfolio will return an instance of 
# Portfolio, and Project#milestones will return an array of Milestone 
# instances, in similar fashion to how ActiveRecord infers class names.
#
# Association definitions are also reflected by the class, e.g.:
#
#   >> Project.associations
#   => [:portfolio, :milestones]
#   >> Project.association_reflection(:portfolio)
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
  RECIPROCAL_ASSOCIATIONS = {:many_to_one=>:one_to_many, :one_to_many=>:many_to_one, :many_to_many=>:many_to_many}

  # Array of all association reflections
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
  #   Similar to ActiveRecord/DataMapper's belongs_to.
  # * :one_to_many - Foreign key in associated model's table points to this
  #   model's primary key.   Each current model object can be associated with
  #   more than one associated model objects.  Each associated model object
  #   can be associated with only one current model object.
  #   Similar to ActiveRecord/DataMapper's has_many.
  # * :many_to_many - A join table is used that has a foreign key that points
  #   to this model's primary key and a foreign key that points to the
  #   associated model's primary key.  Each current model object can be
  #   associated with many associated model objects, and each associated
  #   model object can be associated with many current model objects.
  #   Similar to ActiveRecord/DataMapper's has_and_belongs_to_many.
  #
  # The following options can be supplied:
  # * *ALL types*:
  #   - :class - The associated class or its name. If not
  #     given, uses the association's name, which is camelized (and
  #     singularized if type is :{one,many}_to_many)
  #   - :eager - The associations to eagerly load when loading the associated object.
  #     For many_to_one associations, this is ignored unless this association is
  #     being eagerly loaded, as it doesn't save queries unless multiple objects
  #     can be loaded at once.
  #   - :reciprocal - the symbol name of the instance variable of the reciprocal association,
  #     if it exists.  By default, sequel will try to determine it by looking at the
  #     associated model's assocations for a association that matches
  #     the current association's key(s).  Set to nil to not use a reciprocal.
  # * :one_to_many/:many_to_many:
  #   - :order - the column(s) by which to order the association dataset.  Can be a
  #     singular column or an array.
  # * :many_to_one:
  #   - :key - foreign_key in current model's table that references
  #     associated model's primary key, as a symbol.  Defaults to :"#{name}_id".
  # * :one_to_many:
  #   - :key - foreign key in associated model's table that references
  #     current model's primary key, as a symbol.  Defaults to
  #     :"#{self.name.underscore}_id".
  # * :many_to_many:
  #   - :join_table - name of table that includes the foreign keys to both
  #     the current model and the associated model, as a symbol.  Defaults to the name
  #     of current model and name of associated model, pluralized,
  #     underscored, sorted, and joined with '_'.
  #   - :left_key - foreign key in join table that points to current model's
  #     primary key, as a symbol.
  #   - :right_key - foreign key in join table that points to associated
  #     model's primary key, as a symbol.
  #   - :select - the attributes to select.  Defaults to the associated class's
  #     table_name.*, which means it doesn't include the attributes from the join
  #     join table.  If you want to include the join table attributes, you can
  #     use this option, but beware that the join table attributes can clash with
  #     attributes from the model table, so you should alias any attributes that have
  #     the same name in both the join table and the associated table.
  def associate(type, name, opts = {}, &block)
    # check arguments
    raise ArgumentError unless [:many_to_one, :one_to_many, :many_to_many].include?(type) && Symbol === name

    # merge early so we don't modify opts
    opts = opts.merge(:type => type, :name => name, :block => block, :cache => true)

    # deprecation
    if opts[:from]
      Sequel::Deprecation.deprecate("The :from option to Sequel::Model.associate is deprecated and will be removed in Sequel 2.0.  Use the :class option.")
      opts[:class] = opts[:from]
    end

    # find class
    case opts[:class]
      when String, Symbol
        # Delete :class to allow late binding
        opts[:class_name] ||= opts.delete(:class).to_s
      when Class
        opts[:class_name] ||= opts[:class].name
    end

    send(:"def_#{type}", name, opts)

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
  alias_method :has_many, :one_to_many
  
  # Shortcut for adding a many_to_one association, see associate
  def many_to_one(*args, &block)
    associate(:many_to_one, *args, &block)
  end
  alias_method :belongs_to, :many_to_one
  
  # Shortcut for adding a many_to_many association, see associate
  def many_to_many(*args, &block)
    associate(:many_to_many, *args, &block)
  end
  alias_method :has_and_belongs_to_many, :many_to_many
  
  private
  # The class related to the given association reflection
  def associated_class(opts)
    opts[:class] ||= opts[:class_name].constantize
  end

  # Name symbol for add association method
  def association_add_method_name(name)
    :"add_#{name.to_s.singularize}"
  end

  # Name symbol of association instance variable
  def association_ivar(name)
    :"@#{name}"
  end
  
  # Name symbol for remove_method_name
  def association_remove_method_name(name)
    :"remove_#{name.to_s.singularize}"
  end
  
  # Hash storing the association reflections.  Keys are association name
  # symbols, values are association reflection hashes.
  def association_reflections
    @association_reflections ||= {}
  end
  
  # Defines an association
  def def_association_dataset_methods(name, opts, &block)
    dataset_method = :"#{name}_dataset"
    helper_method = :"#{name}_helper"
    dataset_block = opts[:block]
    ivar = association_ivar(name)
    
    # define a method returning the association dataset (with optional order)
    if order = opts[:order]
      class_def(dataset_method) {instance_eval(&block).order(order)}
    else
      class_def(dataset_method, &block)
    end
    
    # If a block is given, define a helper method for it, because it takes
    # an argument.  This is unnecessary in Ruby 1.9, as that has instance_exec.
    if dataset_block
      class_def(helper_method, &dataset_block)
    end
    
    class_def(name) do |*reload|
      if !reload[0] && obj = instance_variable_get(ivar)
        obj
      else
        ds = send(dataset_method)
        # if the a dataset block was specified, we need to call it and use
        # the result as the dataset to fetch records from.
        if dataset_block
          ds = send(helper_method, ds)
        end
        if eager = opts[:eager]
          ds = ds.eager(eager)
        end
        objs = ds.all
        # Only one_to_many associations should set the reciprocal object
        if (opts[:type] == :one_to_many) && (reciprocal = model.send(:reciprocal_association, opts))
          objs.each{|o| o.instance_variable_set(reciprocal, self)}
        end
        instance_variable_set(ivar, objs)
      end
    end
  end
  
  # Defines an association getter method, caching the block result in an 
  # instance variable. The defined method takes an optional reload parameter
  # that can be set to true in order to bypass the cache.
  def def_association_getter(name, &block)
    ivar = association_ivar(name)
    class_def(name) do |*reload|
      if !reload[0] && obj = instance_variable_get(ivar)
        obj == :null ? nil : obj
      else
        obj = instance_eval(&block)
        instance_variable_set(ivar, obj || :null)
        obj
      end
    end
  end

  # Adds many_to_many association instance methods
  def def_many_to_many(name, opts)
    assoc_class = method(:associated_class) # late binding of association dataset
    recip_assoc = method(:reciprocal_association) # late binding of the reciprocal association
    ivar = association_ivar(name)
    left = (opts[:left_key] ||= default_remote_key)
    right = (opts[:right_key] ||= :"#{name.to_s.singularize}_id")
    opts[:class_name] ||= name.to_s.singularize.camelize
    join_table = (opts[:join_table] ||= default_join_table_name(opts))
    database = db
    
    def_association_dataset_methods(name, opts) do
      klass = assoc_class[opts]
      key = (opts[:right_primary_key] ||= :"#{klass.table_name}__#{klass.primary_key}")
      selection = (opts[:select] ||= klass.table_name.all)
      klass.select(selection).inner_join(join_table, right => key, left => pk)
    end

    class_def(association_add_method_name(name)) do |o|
      database[join_table].insert(left => pk, right => o.pk)
      if arr = instance_variable_get(ivar)
        arr.push(o)
      end
      if (reciprocal = recip_assoc[opts]) && (list = o.instance_variable_get(reciprocal)) \
         && !(list.include?(self))
        list.push(self)
      end
      o
    end
    class_def(association_remove_method_name(name)) do |o|
      database[join_table].filter(left => pk, right => o.pk).delete
      if arr = instance_variable_get(ivar)
        arr.delete(o)
      end
      if (reciprocal = recip_assoc[opts]) && (list = o.instance_variable_get(reciprocal))
        list.delete(self)
      end
      o
    end
  end
  
  # Adds many_to_one association instance methods
  def def_many_to_one(name, opts)
    assoc_class = method(:associated_class) # late binding of association dataset
    recip_assoc = method(:reciprocal_association) # late binding of the reciprocal association
    ivar = association_ivar(name)
    
    key = (opts[:key] ||= :"#{name}_id")
    opts[:class_name] ||= name.to_s.camelize
    
    def_association_getter(name) {(fk = send(key)) ? assoc_class[opts][fk] : nil}
    class_def(:"#{name}=") do |o|
      old_val = instance_variable_get(ivar) if reciprocal = recip_assoc[opts]
      instance_variable_set(ivar, o)
      send(:"#{key}=", (o.pk if o))
      if reciprocal && (old_val != o)
        if old_val && (list = old_val.instance_variable_get(reciprocal))
          list.delete(self)
        end
        if o && (list = o.instance_variable_get(reciprocal)) && !(list.include?(self))
          list.push(self) 
        end
      end
      o
    end
  end
  
  # Adds one_to_many association instance methods
  def def_one_to_many(name, opts)
    assoc_class = method(:associated_class) # late binding of association dataset
    recip_assoc = method(:reciprocal_association) # late binding of the reciprocal association
    ivar = association_ivar(name)
    key = (opts[:key] ||= default_remote_key)
    opts[:class_name] ||= name.to_s.singularize.camelize
    
    def_association_dataset_methods(name, opts) {assoc_class[opts].filter(key => pk)}
    
    class_def(association_add_method_name(name)) do |o|
      o.send(:"#{key}=", pk)
      o.save!
      if arr = instance_variable_get(ivar)
        arr.push(o)
      end
      if reciprocal = recip_assoc[opts]
        o.instance_variable_set(reciprocal, self)
      end
      o
    end
    class_def(association_remove_method_name(name)) do |o|
      o.send(:"#{key}=", nil)
      o.save!
      if arr = instance_variable_get(ivar)
        arr.delete(o)
      end
      if reciprocal = recip_assoc[opts]
        o.instance_variable_set(reciprocal, :null)
      end
      o
    end
  end
  
  # Name symbol for default join table
  def default_join_table_name(opts)
    ([opts[:class_name].demodulize, name.demodulize]. \
      map{|i| i.pluralize.underscore}.sort.join('_')).to_sym
  end
  
  # Name symbol for default foreign key
  def default_remote_key
    :"#{name.demodulize.underscore}_id"
  end

  # Sets the reciprocal association variable in the reflection, if one exists
  def reciprocal_association(reflection)
    return reflection[:reciprocal] if reflection.include?(:reciprocal)
    reciprocal_type = ::Sequel::Model::Associations::RECIPROCAL_ASSOCIATIONS[reflection[:type]]
    if reciprocal_type == :many_to_many
      left_key = reflection[:left_key]
      right_key = reflection[:right_key]
      join_table = reflection[:join_table]
      associated_class(reflection).all_association_reflections.each do |assoc_reflect|
        if assoc_reflect[:type] == :many_to_many && assoc_reflect[:left_key] == right_key \
           && assoc_reflect[:right_key] == left_key && assoc_reflect[:join_table] == join_table
          return reflection[:reciprocal] = association_ivar(assoc_reflect[:name]).to_s.freeze
        end
      end
    else
      key = reflection[:key]
      associated_class(reflection).all_association_reflections.each do |assoc_reflect|
        if assoc_reflect[:type] == reciprocal_type && assoc_reflect[:key] == key
          return reflection[:reciprocal] = association_ivar(assoc_reflect[:name])
        end
      end
    end
    reflection[:reciprocal] = nil
  end
end
