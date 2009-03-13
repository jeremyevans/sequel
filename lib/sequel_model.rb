require 'sequel/core'

module Sequel
  # Holds the nameless subclasses that are created with
  # Sequel::Model(), necessary for reopening subclasses with the
  # Sequel::Model() superclass specified.
  @models = {}

  # Lets you create a Model subclass with its dataset already set.
  # source can be an existing dataset or a symbol (in which case
  # it will create a dataset using the default database with 
  # source as the table name).
  #
  # Example:
  #   class Comment < Sequel::Model(:something)
  #     table_name # => :something
  #   end
  def self.Model(source)
    @models[source] ||= Class.new(Model).set_dataset(source)
  end

  # Model has some methods that are added via metaprogramming:
  #
  # * All of the methods in DATASET_METHODS have class methods created that call
  #   the Model's dataset with the method of the same name with the given
  #   arguments.
  # * All of the methods in HOOKS have class methods created that accept
  #   either a method name symbol or an optional tag and a block.  These
  #   methods run the code as a callback at the specified time.  For example:
  #
  #     Model.before_save :do_something
  #     Model.before_save(:do_something_else){ self.something_else = 42}
  #     object = Model.new
  #     object.save
  #
  #   Would run the object's :do_something method following by the code
  #   block related to :do_something_else.  Note that if you specify a
  #   block, a tag is optional.  If the tag is not nil, it will overwrite
  #   a previous block with the same tag.  This allows hooks to work with
  #   systems that reload code.
  # * All of the methods in HOOKS also create instance methods, but you
  #   should not override these instance methods.
  # * The following instance_methods all call the class method of the same
  #   name: columns, dataset, db, primary_key, db_schema.
  # * The following class level attr_readers are created: allowed_columns,
  #   dataset_methods, primary_key, and restricted_columns.
  #   You should not usually need to  access these directly.
  # * All validation methods also accept the options specified in #validates_each,
  #   in addition to the options specified in the RDoc for that method.
  # * The following class level attr_accessors are created: raise_on_typecast_failure,
  #   raise_on_save_failure, strict_param_setting, typecast_empty_string_to_nil,
  #   and typecast_on_assignment:
  #
  #     # Don't raise an error if a validation attempt fails in
  #     # save/create/save_changes/etc.
  #     Model.raise_on_save_failure = false
  #     Model.before_save{false}
  #     Model.new.save # => nil
  #     # Don't raise errors in new/set/update/etc. if an attempt to
  #     # access a missing/restricted method occurs (just silently
  #     # skip it)
  #     Model.strict_param_setting = false
  #     Model.new(:id=>1) # No Error
  #     # Don't typecast attribute values on assignment
  #     Model.typecast_on_assignment = false
  #     m = Model.new
  #     m.number = '10'
  #     m.number # => '10' instead of 10
  #     # Don't typecast empty string to nil for non-string, non-blob columns.
  #     Model.typecast_empty_string_to_nil = false
  #     m.number = ''
  #     m.number # => '' instead of nil
  #     # Don't raise if unable to typecast data for a column
  #     Model.typecast_empty_string_to_nil = true
  #     Model.raise_on_typecast_failure = false
  #     m.not_null_column = '' # => nil
  #     m.number = 'A' # => 'A'
  #
  # * The following class level method aliases are defined:
  #   * Model.dataset= => set_dataset
  #   * Model.is_a => is
  class Model
    # Dataset methods to proxy via metaprogramming
    DATASET_METHODS = %w'<< all avg count delete distinct eager eager_graph each each_page 
       empty? except exclude filter first from from_self full_outer_join get graph 
       group group_and_count group_by having inner_join insert 
       insert_multiple intersect interval join join_table last 
       left_outer_join limit map multi_insert naked order order_by order_more 
       paginate print query range reverse_order right_outer_join select 
       select_all select_more server set set_graph_aliases single_value to_csv to_hash
       transform union unfiltered unordered update where with_sql'.map{|x| x.to_sym}
  
    # Regular expression that much match for a public instance method of a plugin
    # dataset to have a model method created that calls it
    DATASET_METHOD_RE = /\A[A-Za-z_][A-Za-z0-9_]*\z/

    # Empty instance variables, for -w compliance
    EMPTY_INSTANCE_VARIABLES = [:@overridable_methods_module, :@transform, :@db, :@skip_superclass_validations]

    # Hooks that are safe for public use
    HOOKS = [:after_initialize, :before_create, :after_create, :before_update,
      :after_update, :before_save, :after_save, :before_destroy, :after_destroy,
      :before_validation, :after_validation]

    # Instance variables that are inherited in subclasses
    INHERITED_INSTANCE_VARIABLES = {:@allowed_columns=>:dup, :@dataset_methods=>:dup, 
      :@dataset_method_modules=>:dup, :@primary_key=>nil, :@use_transactions=>nil,
      :@raise_on_save_failure=>nil, :@restricted_columns=>:dup, :@restrict_primary_key=>nil,
      :@simple_pk=>nil, :@simple_table=>nil, :@strict_param_setting=>nil,
      :@typecast_empty_string_to_nil=>nil, :@typecast_on_assignment=>nil,
      :@raise_on_typecast_failure=>nil, :@association_reflections=>:dup}

    # The setter methods (methods ending with =) that are never allowed
    # to be called automatically via set.
    RESTRICTED_SETTER_METHODS = %w"== === []= taguri= typecast_empty_string_to_nil= typecast_on_assignment= strict_param_setting= raise_on_save_failure= raise_on_typecast_failure="

    @allowed_columns = nil
    @association_reflections = {}
    @cache_store = nil
    @cache_ttl = nil
    @db = nil
    @db_schema = nil
    @dataset_method_modules = []
    @dataset_methods = {}
    @overridable_methods_module = nil
    @primary_key = :id
    @raise_on_save_failure = true
    @raise_on_typecast_failure = true
    @restrict_primary_key = true
    @restricted_columns = nil
    @simple_pk = nil
    @simple_table = nil
    @skip_superclass_validations = nil
    @strict_param_setting = true
    @transform = nil
    @typecast_empty_string_to_nil = true
    @typecast_on_assignment = true
    @use_transactions = true
  end
end

%w"inflections plugins base association_reflection associations exceptions errors deprecated".each do |f|
  require "sequel_model/#{f}"
end

