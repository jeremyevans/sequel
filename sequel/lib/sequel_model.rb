require 'sequel_core'
%w"inflector base hooks record schema association_reflection 
  associations caching plugins validations eager_loading".each do |f|
  require "sequel_model/#{f}"
end

module Sequel
  # Holds the nameless subclasses that are created with
  # Sequel::Model(), necessary for reopening subclasses with the
  # Sequel::Model() superclass specified.
  @models = {}

  # Lets you create a Model class with its table name already set or reopen
  # an existing Model.
  #
  # Makes given dataset inherited.
  #
  # === Example:
  #   class Comment < Sequel::Model(:something)
  #     table_name # => :something
  #
  #     # ...
  #
  #   end
  def self.Model(source)
    return @models[source] if @models[source]
    klass = Class.new(Model)
    klass.set_dataset(source.is_a?(Dataset) ? source : Model.db[source])
    @models[source] = klass
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
  #   name: columns, dataset, db, primary_key, str_columns.
  class Model
    extend Enumerable
    extend Associations
    include Validation
  end
end
