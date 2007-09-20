module Sequel
  class Model
  end
end

require File.join(File.dirname(__FILE__), 'model/base')
require File.join(File.dirname(__FILE__), 'model/hooks')
require File.join(File.dirname(__FILE__), 'model/record')
require File.join(File.dirname(__FILE__), 'model/schema')
require File.join(File.dirname(__FILE__), 'model/relations')

module Sequel
  class Model
    def self.subset(name, *args, &block)
      meta_def(name) {filter(*args, &block)}
    end
    
    def self.find(cond)
      dataset[cond.is_a?(Hash) ? cond : {primary_key => cond}]
    end
    
    def self.find_or_create(cond)
      find(cond) || create(cond)
    end

    class << self; alias_method :[], :find; end
    
    ############################################################################
    
    def self.destroy_all
      has_hooks?(:before_destroy) ? dataset.destroy : dataset.delete
    end
    def self.delete_all; dataset.delete; end
    
    FIND_BY_REGEXP = /^find_by_(.*)/.freeze
    FILTER_BY_REGEXP = /^filter_by_(.*)/.freeze
    ALL_BY_REGEXP = /^all_by_(.*)/.freeze
    
    def self.method_missing(m, *args, &block)
      Thread.exclusive do
        method_name = m.to_s
        if method_name =~ FIND_BY_REGEXP
          c = $1.to_sym
          meta_def(method_name) {|arg| find(c => arg)}
        elsif method_name =~ FILTER_BY_REGEXP
          c = $1.to_sym
          meta_def(method_name) {|arg| filter(c => arg)}
        elsif method_name =~ ALL_BY_REGEXP
          c = $1.to_sym
          meta_def(method_name) {|arg| filter(c => arg).all}
        elsif dataset.respond_to?(m)
          instance_eval("def #{m}(*args, &block); dataset.#{m}(*args, &block); end")
        end
      end
      respond_to?(m) ? send(m, *args, &block) : super(m, *args)
    end
    
    def self.join(*args)
      table_name = dataset.opts[:from].first
      dataset.join(*args).select(table_name.to_sym.ALL)
    end
    
    def [](field); @values[field]; end
    
    def []=(field, value); @values[field] = value; end
    
    def each(&block); @values.each(&block); end
    def keys; @values.keys; end
    
    def id; @values[:id]; end
    
    def ==(obj)
      (obj.class == model) && (obj.values == @values)
    end
    
    SERIALIZE_GET_PROC = "proc {@unserialized_%s ||= YAML.load(@values[:%s]) }".freeze
    SERIALIZE_SET_PROC = "proc {|v| @values[:%s] = v.to_yaml; @unserialized_%s = v }".freeze
    
    def self.serialize(*fields)
      fields.each do |f|
        # define getter
        define_method f, &eval(SERIALIZE_GET_PROC % [f, f])
        # define setter
        define_method "#{f}=", &eval(SERIALIZE_SET_PROC % [f, f])
        # add before_create to serialize values before creation
        before_create do
          @values[f] = @values[f].to_yaml
        end
      end      
    end
  end
  
end
