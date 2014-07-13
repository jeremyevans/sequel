require 'sequel/core'

module Sequel
  # Lets you create a Model subclass with its dataset already set.
  # +source+ should be an instance of one of the following classes:
  #
  # Database :: Sets the database for this model to +source+.
  #             Generally only useful when subclassing directly
  #             from the returned class, where the name of the
  #             subclass sets the table name (which is combined
  #             with the +Database+ in +source+ to create the
  #             dataset to use) 
  # Dataset :: Sets the dataset for this model to +source+. 
  # other :: Sets the table name for this model to +source+. The
  #          class will use the default database for model
  #          classes in order to create the dataset.
  #
  # The purpose of this method is to set the dataset/database automatically
  # for a model class, if the table name doesn't match the implicit
  # name.  This is neater than using set_dataset inside the class,
  # doesn't require a bogus query for the schema.
  #
  #   # Using a symbol
  #   class Comment < Sequel::Model(:something)
  #     table_name # => :something
  #   end
  #
  #   # Using a dataset
  #   class Comment < Sequel::Model(DB1[:something])
  #     dataset # => DB1[:something]
  #   end
  #
  #   # Using a database
  #   class Comment < Sequel::Model(DB1)
  #     dataset # => DB1[:comments]
  #   end
  def self.Model(source)
    if cache_anonymous_models && (klass = Model::ANONYMOUS_MODEL_CLASSES_MUTEX.synchronize{Model::ANONYMOUS_MODEL_CLASSES[source]})
      return klass
    end
    klass = if source.is_a?(Database)
      c = Class.new(Model)
      c.db = source
      c
    else
      Class.new(Model).set_dataset(source)
    end
    Model::ANONYMOUS_MODEL_CLASSES_MUTEX.synchronize{Model::ANONYMOUS_MODEL_CLASSES[source] = klass} if cache_anonymous_models
    klass
  end

  @cache_anonymous_models = true

  class << self
    # Whether to cache the anonymous models created by Sequel::Model().  This is
    # required for reloading them correctly (avoiding the superclass mismatch).  True
    # by default for backwards compatibility.
    attr_accessor :cache_anonymous_models
  end

  # <tt>Sequel::Model</tt> is an object relational mapper built on top of Sequel core.  Each
  # model class is backed by a dataset instance, and many dataset methods can be
  # called directly on the class.  Model datasets return rows as model instances,
  # which have fairly standard ORM instance behavior.
  #
  # <tt>Sequel::Model</tt> is built completely out of plugins.  Plugins can override any class,
  # instance, or dataset method defined by a previous plugin and call super to get the default
  # behavior.  By default, <tt>Sequel::Model</tt> loads two plugins, <tt>Sequel::Model</tt>
  # (which is itself a plugin) for the base support, and <tt>Sequel::Model::Associations</tt>
  # for the associations support.
  #
  # You can set the +SEQUEL_NO_ASSOCIATIONS+ constant or environment variable to
  # make Sequel not load the associations plugin by default.
  class Model
    OPTS = Sequel::OPTS

    # Map that stores model classes created with <tt>Sequel::Model()</tt>, to allow the reopening
    # of classes when dealing with code reloading.
    ANONYMOUS_MODEL_CLASSES = {}

    # Mutex protecting access to ANONYMOUS_MODEL_CLASSES
    ANONYMOUS_MODEL_CLASSES_MUTEX = Mutex.new

    # Class methods added to model that call the method of the same name on the dataset
    DATASET_METHODS = (Dataset::ACTION_METHODS + Dataset::QUERY_METHODS +
      [:each_server]) - [:and, :or, :[], :columns, :columns!, :delete, :update, :add_graph_aliases, :first, :first!]
    
    # Boolean settings that can be modified at the global, class, or instance level.
    BOOLEAN_SETTINGS = [:typecast_empty_string_to_nil, :typecast_on_assignment, :strict_param_setting, \
      :raise_on_save_failure, :raise_on_typecast_failure, :require_modification, :use_after_commit_rollback, :use_transactions]

    # Hooks that are called before an action.  Can return false to not do the action.  When
    # overriding these, it is recommended to call +super+ as the last line of your method,
    # so later hooks are called before earlier hooks.
    BEFORE_HOOKS = [:before_create, :before_update, :before_save, :before_destroy, :before_validation]

    # Hooks that are called after an action.  When overriding these, it is recommended to call
    # +super+ on the first line of your method, so later hooks are called after earlier hooks.
    AFTER_HOOKS = [:after_create, :after_update, :after_save, :after_destroy,
      :after_validation, :after_commit, :after_rollback, :after_destroy_commit, :after_destroy_rollback]

    # Hooks that are called around an action.  If overridden, these methods must call super
    # exactly once if the behavior they wrap is desired.  The can be used to rescue exceptions
    # raised by the code they wrap or ensure that some behavior is executed no matter what.
    AROUND_HOOKS = [:around_create, :around_update, :around_save, :around_destroy, :around_validation]

    # Empty instance methods to create that the user can override to get hook/callback behavior.
    # Just like any other method defined by Sequel, if you override one of these, you should
    # call +super+ to get the default behavior (while empty by default, they can also be defined
    # by plugins).  See the {"Model Hooks" guide}[rdoc-ref:doc/model_hooks.rdoc] for
    # more detail on hooks.
    HOOKS = BEFORE_HOOKS + AFTER_HOOKS

    # Class instance variables that are inherited in subclasses.  If the value is <tt>:dup</tt>, dup is called
    # on the superclass's instance variable when creating the instance variable in the subclass.
    # If the value is +nil+, the superclass's instance variable is used directly in the subclass.
    INHERITED_INSTANCE_VARIABLES = {:@allowed_columns=>:dup,
      :@dataset_method_modules=>:dup, :@primary_key=>nil, :@use_transactions=>nil,
      :@raise_on_save_failure=>nil, :@require_modification=>nil, 
      :@restricted_columns=>:dup, :@restrict_primary_key=>nil,
      :@simple_pk=>nil, :@simple_table=>nil, :@strict_param_setting=>nil,
      :@typecast_empty_string_to_nil=>nil, :@typecast_on_assignment=>nil,
      :@raise_on_typecast_failure=>nil, :@plugins=>:dup, :@setter_methods=>nil,
      :@use_after_commit_rollback=>nil, :@fast_pk_lookup_sql=>nil,
      :@fast_instance_delete_sql=>nil, :@finders=>:dup, :@finder_loaders=>:dup,
      :@db=>nil, :@default_set_fields_options=>:dup}

    # Regular expression that determines if a method name is normal in the sense that
    # it could be used literally in ruby code without using send.  Used to
    # avoid problems when using eval with a string to define methods.
    NORMAL_METHOD_NAME_REGEXP = /\A[A-Za-z_][A-Za-z0-9_]*\z/

    # Regular expression that determines if the method is a valid setter name
    # (i.e. it ends with =).
    SETTER_METHOD_REGEXP = /=\z/

    @allowed_columns = nil
    @db = nil
    @db_schema = nil
    @dataset = nil
    @dataset_method_modules = []
    @default_eager_limit_strategy = true
    @default_set_fields_options = {}
    @finders = {}
    @finder_loaders = {}
    @overridable_methods_module = nil
    @fast_pk_lookup_sql = nil
    @fast_instance_delete_sql = nil
    @plugins = []
    @primary_key = :id
    @raise_on_save_failure = true
    @raise_on_typecast_failure = false
    @require_modification = nil
    @restrict_primary_key = true
    @restricted_columns = nil
    @setter_methods = nil
    @simple_pk = nil
    @simple_table = nil
    @strict_param_setting = true
    @typecast_empty_string_to_nil = true
    @typecast_on_assignment = true
    @use_after_commit_rollback = true
    @use_transactions = true

    Sequel.require %w"default_inflections inflections plugins dataset_module base exceptions errors", "model"
    if !defined?(::SEQUEL_NO_ASSOCIATIONS) && !ENV.has_key?('SEQUEL_NO_ASSOCIATIONS')
      Sequel.require 'associations', 'model'
      plugin Model::Associations
    end

    # The setter methods (methods ending with =) that are never allowed
    # to be called automatically via +set+/+update+/+new+/etc..
    RESTRICTED_SETTER_METHODS = instance_methods.map{|x| x.to_s}.grep(SETTER_METHOD_REGEXP)
  end
end
