# Associations are used in order to specify relationships between model classes
# that reflect relations between tables in the database using foreign keys.
#
# Each kind of association adds a number of methods to the model class which
# are specialized according to the association type and optional parameters
# given in the definition. Example:
# 
#   class Project < Sequel::Model
#     belongs_to :portfolio
#     has_many   :milestones
#   end
# 
# The project class now has the following methods:
# * Project#portfolio, Project#portfolio=
# * Project#milestones, Project#add_milestone, Project#remove_milestone
#
# By default the classes for the associations are inferred from the association
# name, so for example the Project#portfolio will return an instance of 
# Portfolio, and Project#milestones will return a dataset of Milestone 
# instances, in similar fashion to how ActiveRecord infers class names.
#
# Association definitions are also reflected by the class, e.g.:
#
#   >> Project.associations
#   => [:portfolio, :milestones]
#   >> Project.association_reflection(:portfolio)
#   => {:kind => :many_to_one, :name => :portfolio, :class_name => "Portfolio"}
#
# The following association kinds are supported:
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
# Associations can be defined by either using the associate method, or by
# calling one of the three methods: many_to_one, one_to_many, many_to_many.
# Sequel::Model also provides aliases for these methods that conform to
# ActiveRecord conventions: belongs_to, has_many, has_and_belongs_to_many.
# For example, the following two statements are equivalent:
#
#   associate :one_to_many, :attributes
#   one_to_many :attributes
module Sequel::Model::Associations
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
  #   - :class_name - The name of the associated class as a string.  If not
  #     given, uses the association's name, which is camelized (and
  #     singularized if type is :{one,many}_to_many)
  #   - :class - The associated class itself.  Simpler than using
  #     :class_name, but can't be used always due to dependencies not being
  #     loaded.
  # * :many_to_one:
  #   - :key - foreign_key in current model's table that references
  #     associated model's primary key, as a symbol.  Defaults to :"#{name}_id".
  # * :one_to_many:
  #   - :key - foreign key in associated model's table that references
  #     current model's primary key, as a symbol.  Defaults to
  #     :"#{self.name.underscore}_id".
  #   - :order - the column by which to order the association dataset.
  #   - :cache - set to true to cache and return an array of objects instead of a dataset.
  # * :many_to_many:
  #   - :join_table - name of table that includes the foreign keys to both
  #     the current model and the associated model, as a symbol.  Defaults to the name
  #     of current model and name of associated model, pluralized,
  #     underscored, sorted, and joined with '_'.
  #   - :left_key - foreign key in join table that points to current model's
  #     primary key, as a symbol.
  #   - :right_key - foreign key in join table that points to associated
  #     model's primary key, as a symbol.
  #   - :order - the column by which to order the association dataset.
  #   - :cache - set to true to cache and return an array of objects instead of a dataset.
  def associate(type, name, opts = {}, &block)
    # check arguments
    raise ArgumentError unless [:many_to_one, :one_to_many, :many_to_many].include?(type) && Symbol === name

    # deprecation
    if opts[:from]
      STDERR << "The :from option is deprecated, please use the :class option instead.\r\n"
      opts[:class] = opts[:from]
    end

    # prepare options
    opts[:class_name] ||= opts[:class].name if opts[:class]
    opts = association_reflections[name] = opts.merge(:type => type, :name => name, :block => block)

    send(:"def_#{type}", name, opts)
  end
  
  # The association reflection hash for the association of the given name.
  def association_reflection(name)
    association_reflections[name]
  end
  
  # Array of association name symbols
  def associations
    association_reflections.keys
  end

  # Shortcut for adding a many_to_one association, see associate
  def many_to_one(*args, &block)
    associate(:many_to_one, *args, &block)
  end
  alias_method :belongs_to, :many_to_one
  
  # Shortcut for adding a one_to_many association, see associate
  def one_to_many(*args, &block)
    associate(:one_to_many, *args, &block)
  end
  alias_method :has_many, :one_to_many
  
  # deprecated, please use many_to_one instead
  def one_to_one(*args, &block)
    STDERR << "one_to_one relation definitions are deprecated, please use many_to_one instead.\r\n"
    many_to_one(*args, &block)
  end

  # Shortcut for adding a many_to_many association, see associate
  def many_to_many(*args, &block)
    associate(:many_to_many, *args, &block)
  end
  alias_method :has_and_belongs_to_many, :many_to_many
  
  private
  def association_ivar(name)
    :"@#{name}"
  end
  
  def association_add_method_name(name)
    :"add_#{name.to_s.singularize}"
  end

  def association_remove_method_name(name)
    :"remove_#{name.to_s.singularize}"
  end
  
  def default_remote_key
    :"#{name.demodulize.underscore}_id"
  end

  # The class related to the given association reflection
  def associated_class(opts)
    opts[:class] ||= opts[:class_name].constantize
  end

  # Hash storing the association reflections.  Keys are association name
  # symbols, values are association reflection hashes.
  def association_reflections
    @association_reflections ||= {}
  end
  
  def def_many_to_one(name, opts)
    assoc_class = method(:associated_class) # late binding of association dataset
    ivar = association_ivar(name)
    
    key = (opts[:key] ||= :"#{name}_id")
    opts[:class_name] ||= name.to_s.camelize
    
    def_association_getter(name) {(fk = send(key)) ? assoc_class[opts][fk] : nil}
    class_def(:"#{name}=") do |o|
      instance_variable_set(ivar, o)
      send(:"#{key}=", (o.pk if o))
    end
  end
  
  def def_one_to_many(name, opts)
    assoc_class = method(:associated_class) # late binding of association dataset
    ivar = association_ivar(name)
    key = (opts[:key] ||= default_remote_key)
    opts[:class_name] ||= name.to_s.singularize.camelize
    
    def_association_dataset_methods(name, opts) {assoc_class[opts].filter(key => pk)}
    
    # define add_xxx, remove_xxx methods
    class_def(association_add_method_name(name)) do |o|
      o.send(:"#{key}=", pk); o.save!
      if arr = instance_variable_get(ivar)
        arr.push(o)
      end
      o
    end
    class_def(association_remove_method_name(name)) do |o|
      o.send(:"#{key}=", nil); o.save!
      if arr = instance_variable_get(ivar)
        arr.delete(o)
      end
      o
    end
  end
  
  def default_join_table_name(opts)
    ([opts[:class_name], self.name.demodulize]. \
      map{|i| i.pluralize.underscore}.sort.join('_')).to_sym
  end
  
  def def_many_to_many(name, opts)
    assoc_class = method(:associated_class) # late binding of association dataset
    ivar = association_ivar(name)
    left = (opts[:left_key] ||= default_remote_key)
    right = (opts[:right_key] ||= :"#{name.to_s.singularize}_id")
    opts[:class_name] ||= name.to_s.singularize.camelize
    join_table = (opts[:join_table] ||= default_join_table_name(opts))
    database = db
    
    def_association_dataset_methods(name, opts) do
      klass = assoc_class[opts]
      key = (opts[:right_primary_key] ||= :"#{klass.table_name}__#{klass.primary_key}")
      klass.inner_join(join_table, right => key, left => pk)
    end

    class_def(association_add_method_name(name)) do |o|
      database[join_table].insert(left => pk, right => o.pk)
      if arr = instance_variable_get(ivar)
        arr.push(o)
      end
      o
    end
    class_def(association_remove_method_name(name)) do |o|
      database[join_table].filter(left => pk, right => o.pk).delete
      if arr = instance_variable_get(ivar)
        arr.delete(o)
      end
      o
    end
  end
  
  # Defines an association getter method, caching the block result in an 
  # instance variable. The defined method takes an optional reload parameter
  # that can be set to true in order to bypass the cache.
  def def_association_getter(name, &block)
    ivar = association_ivar(name)
    class_def(name) do |*reload|
      if !reload[0] && obj = instance_variable_get(ivar)
        obj
      else
        instance_variable_set(ivar, instance_eval(&block))
      end
    end
  end
  
  # Defines an association
  def def_association_dataset_methods(name, opts, &block)
    dataset_method = :"#{name}_dataset"
    dataset_block = opts[:block]
    ivar = association_ivar(name)
    
    # define a method returning the association dataset (with optional order)
    if order = opts[:order]
      class_def(dataset_method) {instance_eval(&block).order(order)}
    else
      class_def(dataset_method, &block)
    end

    if opts[:cache]
      # if the :cache option is set to true, the association method should return
      # an array of association objects
      class_def(name) do |*reload|
        if !reload[0] && obj = instance_variable_get(ivar)
          obj
        else
          ds = send(dataset_method)
          # if the a dataset block was specified, we need to call it and use
          # the result as the dataset to fetch records from.
          if dataset_block
            ds = dataset_block[ds]
          end
          instance_variable_set(ivar, ds.all)
        end
      end
    elsif dataset_block
      # no cache, but we still need to check if a dataset block was given.
      # define helper so the supplied block will be instance_eval'ed
      class_def(:"#{name}_helper", &dataset_block)
      class_def(name) {send(:"#{name}_helper", send(dataset_method))}
    else
      # otherwise (by default), the association method is an alias to the 
      # association dataset method.
      alias_method name, dataset_method
    end
  end
end
