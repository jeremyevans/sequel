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
# * portfolio - Returns the associated portfolio
# * portfolio=(obj) - Sets the associated portfolio to the object,
#   but the change is not persisted until you save the record.
# * milestones - Returns an array of associated milestones
# * milestones_dataset - Returns a dataset that would return the associated
#   milestones, allowing for further filtering/limiting/etc.
# * add_milestone(obj) - Associates the passed milestone with this object
# * remove_milestone(obj) - Removes the association with the passed milestone
# * remove_all_milestones - Removes associations with all associated milestones
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
  # A one to one relationship can be set up with a many_to_one association
  # on the table with the foreign key, and a one_to_many association with the
  # :one_to_one option specified on the table without the foreign key.  The
  # two associations will operate similarly, except that the many_to_one
  # association setter doesn't update the database until you call save manually.
  # 
  # The following options can be supplied:
  # * *ALL types*:
  #   - :allow_eager - If set to false, you cannot load the association eagerly
  #     via eager or eager_graph
  #   - :class - The associated class or its name. If not
  #     given, uses the association's name, which is camelized (and
  #     singularized unless the type is :many_to_one)
  #   - :eager - The associations to eagerly load when loading the associated object.
  #     For many_to_one associations, this is ignored unless this association is
  #     being eagerly loaded, as it doesn't save queries unless multiple objects
  #     can be loaded at once.
  #   - :eager_block - If given, use the block instead of the default block when
  #     eagerly loading.  To not use a block when eager loading (when one is used normally),
  #     set to nil.
  #   - :graph_conditions - The conditions to use on the SQL join when eagerly loading
  #     the association via eager_graph
  #   - :graph_join_type - The type of SQL join to use when eagerly loading the association via
  #     eager_graph
  #   - :graph_select - A column or array of columns to select from the associated table
  #     when eagerly loading the association via eager_graph. Defaults to all
  #     columns in the associated table.
  #   - :order - the column(s) by which to order the association dataset.  Can be a
  #     singular column or an array.
  #   - :read_only - Do not add a setter method (for many_to_one or one_to_many with :one_to_one),
  #     or add_/remove_/remove_all_ methods (for one_to_many, many_to_many)
  #   - :reciprocal - the symbol name of the instance variable of the reciprocal association,
  #     if it exists.  By default, sequel will try to determine it by looking at the
  #     associated model's assocations for a association that matches
  #     the current association's key(s).  Set to nil to not use a reciprocal.
  #   - :select - the attributes to select.  Defaults to the associated class's
  #     table_name.*, which means it doesn't include the attributes from the
  #     join table in a many_to_many association.  If you want to include the join table attributes, you can
  #     use this option, but beware that the join table attributes can clash with
  #     attributes from the model table, so you should alias any attributes that have
  #     the same name in both the join table and the associated table.
  # * :one_to_many, :many_to_many:
  #   - :limit - Limit the number of records to the provided value.  Use
  #     an array with two arguments for the value to specify a limit and offset.
  # * :many_to_one:
  #   - :key - foreign_key in current model's table that references
  #     associated model's primary key, as a symbol.  Defaults to :"#{name}_id".
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
  # * :many_to_many:
  #   - :join_table - name of table that includes the foreign keys to both
  #     the current model and the associated model, as a symbol.  Defaults to the name
  #     of current model and name of associated model, pluralized,
  #     underscored, sorted, and joined with '_'.
  #   - :left_key - foreign key in join table that points to current model's
  #     primary key, as a symbol. Defaults to :"#{self.name.underscore}_id".
  #   - :right_key - foreign key in join table that points to associated
  #     model's primary key, as a symbol.  Defaults to Defaults to :"#{name.to_s.singularize}_id".
  #   - :graph_join_table_conditions - The conditions to use on the SQL join for the join table when eagerly loading
  #     the association via eager_graph
  def associate(type, name, opts = {}, &block)
    # check arguments
    raise ArgumentError unless [:many_to_one, :one_to_many, :many_to_many].include?(type) && Symbol === name

    # merge early so we don't modify opts
    opts = opts.merge(:type => type, :name => name, :block => block, :cache => true, :model => self)
    opts = AssociationReflection.new.merge!(opts)
    opts[:eager_block] = block unless opts.include?(:eager_block)
    opts[:graph_join_type] ||= :left_outer
    opts[:graph_conditions] = opts[:graph_conditions] ? opts[:graph_conditions].to_a : []
    opts[:graph_select] = Array(opts[:graph_select]) if opts[:graph_select]

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

  # Name symbol for add association method
  def association_add_method_name(name)
    :"add_#{name.to_s.singularize}"
  end

  # Name symbol of association instance variable
  def association_ivar(name)
    :"@#{name}"
  end
  
  # Name symbol for remove_all association method
  def association_remove_all_method_name(name)
    :"remove_all_#{name}"
  end
  
  # Name symbol for remove association method
  def association_remove_method_name(name)
    :"remove_#{name.to_s.singularize}"
  end
  
  # Hash storing the association reflections.  Keys are association name
  # symbols, values are association reflection hashes.
  def association_reflections
    @association_reflections ||= {}
  end
  
  # Adds association methods to the model for *_to_many associations.
  def def_association_dataset_methods(name, opts, &block)
    dataset_method = :"#{name}_dataset"
    helper_method = :"#{name}_helper"
    dataset_block = opts[:block]
    ivar = association_ivar(name)
    order = opts[:order]
    eager = opts[:eager]
    limit = opts[:limit]

    # If a block is given, define a helper method for it, because it takes
    # an argument.  This is unnecessary in Ruby 1.9, as that has instance_exec.
    if dataset_block
      class_def(helper_method, &dataset_block)
      private helper_method
    end
    
    # define a method returning the association dataset (with optional order)
    class_def(dataset_method) do
      ds = instance_eval(&block)
      ds = ds.order(*order) if order
      ds = ds.limit(*limit) if limit
      ds = ds.eager(eager) if eager
      ds = send(helper_method, ds) if dataset_block
      ds
    end
    
    class_def(name) do |*reload|
      if !reload[0] && obj = instance_variable_get(ivar)
        obj
      else
        objs = send(dataset_method).all
        # Only one_to_many associations should set the reciprocal object
        if (opts[:type] == :one_to_many) && (reciprocal = opts.reciprocal)
          objs.each{|o| o.instance_variable_set(reciprocal, self)}
        end
        instance_variable_set(ivar, objs)
      end
    end
  end

  # Adds many_to_many association instance methods
  def def_many_to_many(name, opts)
    ivar = association_ivar(name)
    left = (opts[:left_key] ||= default_remote_key)
    right = (opts[:right_key] ||= default_foreign_key(opts))
    opts[:class_name] ||= name.to_s.singularize.camelize
    join_table = (opts[:join_table] ||= default_join_table_name(opts))
    opts[:left_key_alias] ||= :"x_foreign_key_x"
    opts[:left_key_select] ||= :"#{join_table}__#{left}___#{opts[:left_key_alias]}"
    opts[:graph_join_table_conditions] = opts[:graph_join_table_conditions] ? opts[:graph_join_table_conditions].to_a : []
    database = db
    
    def_association_dataset_methods(name, opts) do
      opts.associated_class.select(*opts.select).inner_join(join_table, [[right, opts.associated_primary_key], [left, pk]])
    end
    return if opts[:read_only]

    class_def(association_add_method_name(name)) do |o|
      database[join_table].insert(left=>pk, right=>o.pk)
      if arr = instance_variable_get(ivar)
        arr.push(o)
      end
      if (reciprocal = opts.reciprocal) && (list = o.instance_variable_get(reciprocal)) \
         && !(list.include?(self))
        list.push(self)
      end
      o
    end
    class_def(association_remove_method_name(name)) do |o|
      database[join_table].filter([[left, pk], [right, o.pk]]).delete
      if arr = instance_variable_get(ivar)
        arr.delete(o)
      end
      if (reciprocal = opts.reciprocal) && (list = o.instance_variable_get(reciprocal))
        list.delete(self)
      end
      o
    end
    class_def(association_remove_all_method_name(name)) do
      database[join_table].filter(left=>pk).delete
      if arr = instance_variable_get(ivar)
        reciprocal = opts.reciprocal
        ret = arr.dup
        arr.each do |o|
          if reciprocal && (list = o.instance_variable_get(reciprocal))
            list.delete(self)
          end
        end
      end
      instance_variable_set(ivar, [])
      ret
    end
  end
  
  # Adds many_to_one association instance methods
  def def_many_to_one(name, opts)
    ivar = association_ivar(name)
    
    key = (opts[:key] ||= default_foreign_key(opts))
    opts[:class_name] ||= name.to_s.camelize
    
    class_def(name) do |*reload|
      if !reload[0] && obj = instance_variable_get(ivar)
        obj == :null ? nil : obj
      else
        obj = if fk = send(key)
          opts.associated_class.select(*opts.select).filter(opts.associated_primary_key=>fk).first
        end
        instance_variable_set(ivar, obj || :null)
        obj
      end
    end
    return if opts[:read_only]

    class_def(:"#{name}=") do |o|
      old_val = instance_variable_get(ivar) if reciprocal = opts.reciprocal
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
    ivar = association_ivar(name)
    key = (opts[:key] ||= default_remote_key)
    opts[:class_name] ||= name.to_s.singularize.camelize
    add_meth = association_add_method_name(name)
    remove_meth = association_remove_method_name(name)
    remove_all_meth = association_remove_all_method_name(name)
    
    def_association_dataset_methods(name, opts) {opts.associated_class.select(*opts.select).filter(key => pk)}
    
    unless opts[:read_only]
      class_def(add_meth) do |o|
        o.send(:"#{key}=", pk)
        o.save || raise(Sequel::Error, "invalid associated object, cannot save")
        if arr = instance_variable_get(ivar)
          arr.push(o)
        end
        if reciprocal = opts.reciprocal
          o.instance_variable_set(reciprocal, self)
        end
        o
      end
      unless opts[:one_to_one]
        class_def(remove_meth) do |o|
          o.send(:"#{key}=", nil)
          o.save || raise(Sequel::Error, "invalid associated object, cannot save")
          if arr = instance_variable_get(ivar)
            arr.delete(o)
          end
          if reciprocal = opts.reciprocal
            o.instance_variable_set(reciprocal, :null)
          end
          o
        end
        class_def(remove_all_meth) do
          opts.associated_class.filter(key=>pk).update(key=>nil)
          if arr = instance_variable_get(ivar)
            ret = arr.dup
            if reciprocal = opts.reciprocal
              arr.each{|o| o.instance_variable_set(reciprocal, :null)} 
            end
          end
          instance_variable_set(ivar, [])
          ret
        end
      end
    end
    if opts[:one_to_one]
      private name, :"#{name}_dataset"
      n = name.to_s.singularize.to_sym
      raise(Sequel::Error, "one_to_many association names should still be plural even when using the :one_to_one option") if n == name
      class_def(n) do |*o|
        objs = send(name, *o)
        raise(Sequel::Error, "multiple values found for a one-to-one relationship") if objs.length > 1
        objs.first
      end
      unless opts[:read_only]
        private add_meth
        class_def(:"#{n}=") do |o|
          klass = opts.associated_class
          model.db.transaction do
            send(add_meth, o)
            klass.filter(Sequel::SQL::BooleanExpression.new(:AND, {key=>pk}, ~{klass.primary_key=>o.pk}.sql_expr)).update(key=>nil)
          end
        end
      end
    end
  end
  
  # Default foreign key name symbol for foreign key in current model's table that points to
  # the given association's table's primary key.
  def default_foreign_key(reflection)
    name = reflection[:name]
    :"#{reflection[:type] == :many_to_one ? name : name.to_s.singularize}_id"
  end

  # Name symbol for default join table
  def default_join_table_name(opts)
    ([opts[:class_name].demodulize, name.demodulize]. \
      map{|i| i.pluralize.underscore}.sort.join('_')).to_sym
  end
  
  # Default foreign key name symbol for key in associated table that points to
  # current table's primary key.
  def default_remote_key
    :"#{name.demodulize.underscore}_id"
  end
end
