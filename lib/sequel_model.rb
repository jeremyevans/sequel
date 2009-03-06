require 'sequel_core'
%w"inflector base hooks record schema association_reflection dataset_methods 
  associations caching plugins validations eager_loading exceptions deprecated".each do |f|
  require "sequel_model/#{f}"
end

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
  #   cache_store, cache_ttl, dataset_methods, primary_key, restricted_columns,
  #   sti_dataset, and sti_key.  You should not usually need to
  #   access these directly.
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
    extend Enumerable
    extend Associations
  end
end
