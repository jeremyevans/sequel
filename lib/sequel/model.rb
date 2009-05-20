require 'sequel/core'

module Sequel
  # Lets you create a Model subclass with its dataset already set.
  # source can be an existing dataset or a symbol (in which case
  # it will create a dataset using the default database with 
  # the given symbol as the table name).
  #
  # The purpose of this method is to set the dataset automatically
  # for a model class, if the table name doesn't match the implicit
  # name.  This is neater than using set_dataset inside the class,
  # doesn't require a bogus query for the schema, and allows
  # it to work correctly in a system that uses code reloading.
  #
  # Example:
  #   class Comment < Sequel::Model(:something)
  #     table_name # => :something
  #   end
  def self.Model(source)
    Model::ANONYMOUS_MODEL_CLASSES[source] ||= Class.new(Model).set_dataset(source)
  end

  # Sequel::Model is an object relational mapper built on top of Sequel core.  Each
  # model class is backed by a dataset instance, and many dataset methods can be
  # called directly on the class.  Model datasets return rows as model instances,
  # which have fairly standard ORM instance behavior.
  #
  # Sequel::Model is built completely out of plugins, the only method not part of a
  # plugin is the plugin method itself.  Plugins can override any class, instance, or
  # dataset method defined by a previous plugin and call super to get the default
  # behavior.
  #
  # You can set the SEQUEL_NO_ASSOCIATIONS constant or environment variable to
  # make Sequel not load the associations plugin by default.
  class Model
    # Map that stores model classes created with Sequel::Model(), to allow the reopening
    # of classes when dealing with code reloading.
    ANONYMOUS_MODEL_CLASSES = {}

    # Class methods added to model that call the method of the same name on the dataset
    DATASET_METHODS = %w'<< all avg count delete distinct eager eager_graph
       each each_page empty? except exclude filter first from from_self
       full_outer_join get graph group group_and_count group_by having import
       inner_join insert insert_multiple intersect interval join join_table
       last left_outer_join limit map multi_insert naked order order_by
       order_more paginate print query range reverse_order right_outer_join
       select select_all select_more server set set_graph_aliases 
       single_value to_csv to_hash union unfiltered unordered 
       update where with_sql'.map{|x| x.to_sym}
  
    # Class instance variables to set to nil when a subclass is created, for -w compliance
    EMPTY_INSTANCE_VARIABLES = [:@overridable_methods_module, :@db]

    # Empty instance methods to create that the user can override to get hook/callback behavior.
    # Just like any other method defined by Sequel, if you override one of these, you should
    # call super to get the default behavior (while empty by default, they can also be defined
    # by plugins).  
    HOOKS = [:after_initialize, :before_create, :after_create, :before_update,
      :after_update, :before_save, :after_save, :before_destroy, :after_destroy,
      :before_validation, :after_validation]

    # Class instance variables that are inherited in subclasses.  If the value is :dup, dup is called
    # on the superclass's instance variable when creating the instance variable in the subclass.
    # If the value is nil, the superclass's instance variable is used directly in the subclass.
    INHERITED_INSTANCE_VARIABLES = {:@allowed_columns=>:dup, :@dataset_methods=>:dup, 
      :@dataset_method_modules=>:dup, :@primary_key=>nil, :@use_transactions=>nil,
      :@raise_on_save_failure=>nil, :@restricted_columns=>:dup, :@restrict_primary_key=>nil,
      :@simple_pk=>nil, :@simple_table=>nil, :@strict_param_setting=>nil,
      :@typecast_empty_string_to_nil=>nil, :@typecast_on_assignment=>nil,
      :@raise_on_typecast_failure=>nil, :@plugins=>:dup}

    # Regexp that determines if a method name is normal in the sense that
    # it could be called directly in ruby code without using send.  Used to
    # avoid problems when using eval with a string to define methods.
    NORMAL_METHOD_NAME_REGEXP = /\A[A-Za-z_][A-Za-z0-9_]*\z/

    # The setter methods (methods ending with =) that are never allowed
    # to be called automatically via set/update/new/etc..
    RESTRICTED_SETTER_METHODS = %w"== === []= taguri= typecast_empty_string_to_nil= typecast_on_assignment= strict_param_setting= raise_on_save_failure= raise_on_typecast_failure="

    # Regular expression that determines if the method is a valid setter name
    # (i.e. it ends with =).
    SETTER_METHOD_REGEXP = /=\z/

    @allowed_columns = nil
    @db = nil
    @db_schema = nil
    @dataset_method_modules = []
    @dataset_methods = {}
    @overridable_methods_module = nil
    @plugins = []
    @primary_key = :id
    @raise_on_save_failure = true
    @raise_on_typecast_failure = true
    @restrict_primary_key = true
    @restricted_columns = nil
    @simple_pk = nil
    @simple_table = nil
    @strict_param_setting = true
    @typecast_empty_string_to_nil = true
    @typecast_on_assignment = true
    @use_transactions = true
  end

  require %w"inflections plugins base exceptions errors", "model"
  if !defined?(::SEQUEL_NO_ASSOCIATIONS) && !ENV.has_key?('SEQUEL_NO_ASSOCIATIONS')
    require 'associations', 'model'
    Model.plugin Model::Associations
  end
end
