require 'sequel_core'
%w"inflector inflections base hooks record schema association_reflection 
  associations caching plugins validations eager_loading deprecated".each do |f|
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

  class Model
    extend Enumerable
    extend Associations
  end
end
