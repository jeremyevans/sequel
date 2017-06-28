# frozen-string-literal: true

require 'sequel/core'

module Sequel
  # Delegate to Sequel::Model, only for backwards compatibility.
  def self.cache_anonymous_models
    Sequel::Deprecation.deprecate("Sequel.cache_anonymous_models", "Use Sequel::Model.cache_anonymous_models")
    Model.cache_anonymous_models
  end

  # Delegate to Sequel::Model, only for backwards compatibility.
  def self.cache_anonymous_models=(v)
    Sequel::Deprecation.deprecate("Sequel.cache_anonymous_models=", "Use Sequel::Model.cache_anonymous_models=")
    Model.cache_anonymous_models = v
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

    DATASET_METHODS = (Dataset::ACTION_METHODS + Dataset::QUERY_METHODS + [:each_server, :where_all, :where_each, :where_single_value]) - [:and, :or, :[], :columns, :columns!, :delete, :update, :add_graph_aliases]
    Sequel::Deprecation.deprecate_constant(self, :DATASET_METHODS)
    BOOLEAN_SETTINGS = [:typecast_empty_string_to_nil, :typecast_on_assignment, :strict_param_setting, :raise_on_save_failure, :raise_on_typecast_failure, :require_modification, :use_transactions, :use_after_commit_rollback]
    Sequel::Deprecation.deprecate_constant(self, :BOOLEAN_SETTINGS)
    BEFORE_HOOKS = [:before_create, :before_update, :before_save, :before_destroy, :before_validation]
    Sequel::Deprecation.deprecate_constant(self, :BEFORE_HOOKS)
    AFTER_HOOKS = [:after_create, :after_update, :after_save, :after_destroy, :after_validation, :after_commit, :after_rollback, :after_destroy_commit, :after_destroy_rollback]
    Sequel::Deprecation.deprecate_constant(self, :AFTER_HOOKS)
    AROUND_HOOKS = [:around_create, :around_update, :around_save, :around_destroy, :around_validation]
    Sequel::Deprecation.deprecate_constant(self, :AROUND_HOOKS)
    NORMAL_METHOD_NAME_REGEXP = /\A[A-Za-z_][A-Za-z0-9_]*\z/
    Sequel::Deprecation.deprecate_constant(self, :NORMAL_METHOD_NAME_REGEXP)
    SETTER_METHOD_REGEXP = /=\z/
    Sequel::Deprecation.deprecate_constant(self, :SETTER_METHOD_REGEXP)
    ANONYMOUS_MODEL_CLASSES = @Model_cache = {}
    Sequel::Deprecation.deprecate_constant(self, :ANONYMOUS_MODEL_CLASSES)
    ANONYMOUS_MODEL_CLASSES_MUTEX = Mutex.new
    Sequel::Deprecation.deprecate_constant(self, :ANONYMOUS_MODEL_CLASSES_MUTEX)
    INHERITED_INSTANCE_VARIABLES = { :@allowed_columns=>:dup, :@dataset_method_modules=>:dup, :@primary_key=>nil, :@use_transactions=>nil, :@raise_on_save_failure=>nil, :@require_modification=>nil, :@restrict_primary_key=>nil, :@simple_pk=>nil, :@simple_table=>nil, :@strict_param_setting=>nil, :@typecast_empty_string_to_nil=>nil, :@typecast_on_assignment=>nil, :@raise_on_typecast_failure=>nil, :@plugins=>:dup, :@setter_methods=>nil, :@use_after_commit_rollback=>nil, :@fast_pk_lookup_sql=>nil, :@fast_instance_delete_sql=>nil, :@finders=>:dup, :@finder_loaders=>:dup, :@db=>nil, :@default_set_fields_options=>:dup, :@require_valid_table=>nil, :@cache_anonymous_models=>nil, :@dataset_module_class=>nil}
    Sequel::Deprecation.deprecate_constant(self, :INHERITED_INSTANCE_VARIABLES)

    # Empty instance methods to create that the user can override to get hook/callback behavior.
    # Just like any other method defined by Sequel, if you override one of these, you should
    # call +super+ to get the default behavior (while empty by default, they can also be defined
    # by plugins).  See the {"Model Hooks" guide}[rdoc-ref:doc/model_hooks.rdoc] for
    # more detail on hooks.
    HOOKS = [:before_create, :before_update, :before_save, :before_destroy, :before_validation,
      :after_create, :after_update, :after_save, :after_destroy, :after_validation,
      :after_commit, :after_rollback, :after_destroy_commit, :after_destroy_rollback # SEQUEL5: Remove commit/rollback hooks
    ]#.freeze # SEQUEL5

    @allowed_columns = nil # SEQUEL5: Remove
    @cache_anonymous_models = true
    @db = nil
    @db_schema = nil
    @dataset = nil
    @dataset_method_modules = []
    @default_eager_limit_strategy = true
    @default_set_fields_options = {}
    @finders = {} # SEQUEL5: Remove
    @finder_loaders = {} # SEQUEL5: Remove
    @overridable_methods_module = nil
    @fast_pk_lookup_sql = nil
    @fast_instance_delete_sql = nil
    @plugins = []
    @primary_key = :id
    @raise_on_save_failure = true
    @raise_on_typecast_failure = false
    @require_modification = nil
    @require_valid_table = nil
    @restrict_primary_key = true
    @setter_methods = nil
    @simple_pk = nil
    @simple_table = nil
    @strict_param_setting = true
    @typecast_empty_string_to_nil = true
    @typecast_on_assignment = true
    @use_after_commit_rollback = nil
    @use_transactions = true

    Sequel.require %w"default_inflections inflections plugins dataset_module base exceptions errors", "model"
    if !defined?(::SEQUEL_NO_ASSOCIATIONS) && !ENV.has_key?('SEQUEL_NO_ASSOCIATIONS')
      Sequel.require 'associations', 'model'
      plugin Model::Associations
    end

    def_Model(::Sequel)

    # The setter methods (methods ending with =) that are never allowed
    # to be called automatically via +set+/+update+/+new+/etc..
    RESTRICTED_SETTER_METHODS = instance_methods.map(&:to_s).select{|l| l.end_with?('=')}#.freeze # SEQUEL5

    # SEQUEL5: Remove
    class DeprecatedColumnsUpdated # :nodoc:
      def initialize(columns_updated)
        @columns_updated = columns_updated
      end

      def method_missing(*args, &block)
        Sequel::Deprecation.deprecate("Accessing @columns_updated directly", "Use the columns_updated plugin and switch to the columns_updated method")
        @columns_updated.send(*args, &block)
      end
    end
  end
end
