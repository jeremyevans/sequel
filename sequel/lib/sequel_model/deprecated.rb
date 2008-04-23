module Sequel
  class Model
    include Sequel::Deprecation
    extend Sequel::Deprecation

    # Check the Model.associate method to remove the :from option

    def self.is_dataset_magic_method?(m) #:nodoc:
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

    def method_missing(m, *args, &block) #:nodoc:
      if m.to_s =~ /=\z/
        attribute = m.to_s.chop
        values.keys.each do |k|
          next unless k.to_s == attribute
          deprecate("Sequel::Model#method_missing", "Use model[:#{attribute}] = ...")
          return self[attribute.to_sym] = args.first
        end
        super
      else
        attribute = m.to_s
        values.keys.each do |k|
          next unless k.to_s == attribute
          deprecate("Sequel::Model#method_missing", "Use model[:#{attribute}]")
          return self[attribute.to_sym]
        end
        super
      end
    end

    def self.create_with_params(params) #:nodoc:
      deprecate("Sequel::Model.create_with_params", "Use .create")
      create(params)
    end

    def self.create_with(params) #:nodoc:
      deprecate("Sequel::Model.create_with", "Use .create")
      create(params)
    end

    def update_with(params) #:nodoc:
      deprecate("Sequel::Model#update_with", "Use #update_with_params")
      update_with_params(params)
    end

    def new_record? #:nodoc:
      deprecate("Sequel::Model#new_record?", "Use #new?")
      new?
    end

    def set(values) #:nodoc:
      deprecate("Sequel::Model#set", "Use #update_values")
      update_values(values)
    end

    def update(values) #:nodoc:
      deprecate("Sequel::Model#update", "Use #update_values")
      update_values(values)
    end

    # deprecated, please use many_to_one instead
    def self.one_to_one(*args, &block) #:nodoc:
      deprecate("Sequel::Model.one_to_one", "Use many_to_one")
      many_to_one(*args, &block)
    end
  end
end
