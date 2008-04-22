module Sequel
  class Model
    include Sequel::Deprecation
    extend Sequel::Deprecation

    # Check the Model.associate method to remove the :from option

    def self.is_dataset_magic_method?(m)
      method_name = m.to_s
      Sequel::Dataset::MAGIC_METHODS.each_key do |r|
        return true if method_name =~ r
      end
      false
    end

    def self.method_missing(m, *args, &block) #:nodoc:
      Thread.exclusive do
        if dataset.respond_to?(m) || is_dataset_magic_method?(m)
          instance_eval("def #{m}(*args, &block); deprecate('Sequel::Model.method_missing', 'Please define Sequel::Model.#{m} or use def_dataset_method :#{m}'); dataset.#{m}(*args, &block); end")
        end
      end
      respond_to?(m) ? send(m, *args, &block) : super(m, *args)
    end

    def self.create_with_params(params)
      deprecate("Sequel::Model.create_with_params", "Use .create")
      create(params)
    end

    def self.create_with(params)
      deprecate("Sequel::Model.create_with", "Use .create")
      create(params)
    end

    def update_with(params)
      deprecate("Sequel::Model#update_with", "Use #update_with_params")
      update_with_params(params)
    end

    def new_record?
      deprecate("Sequel::Model#new_record?", "Use #new?")
      new?
    end

    def set(values)
      deprecate("Sequel::Model#set", "Use #update_values")
      update_values(values)
    end

    def update(values)
      deprecate("Sequel::Model#update", "Use #update_values")
      update_values(values)
    end

    # deprecated, please use many_to_one instead
    def one_to_one(*args, &block)
      deprecate("Sequel::Model.one_to_one", "Use many_to_one")
      many_to_one(*args, &block)
    end
  end
end
