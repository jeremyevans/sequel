module Sequel
  class Model
    include Sequel::Deprecation
    extend Sequel::Deprecation

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
          instance_eval("def #{m}(*args, &block); deprecate(#{m.inspect}, 'is not a defined method'); dataset.#{m}(*args, &block); end")
        end
      end
      respond_to?(m) ? send(m, *args, &block) : super(m, *args)
    end
  end
end
