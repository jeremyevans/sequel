module Sequel
  class Model
    {:import=>:multi_insert, :size=>:count, :uniq=>:distinct}.each do |o, n|
      instance_eval "def #{o}(*args, &block); Deprecation.deprecate('Sequel::Model.#{o}', 'Use Sequel::Model.dataset.#{n}'); dataset.#{n}(*args, &block); end"
    end

    def self.is(*args, &block)
      Deprecation.deprecate('Sequel::Model.is', 'Use Sequel::Model.plugin')
      plugin(*args, &block)
    end

    def self.is_a(*args, &block)
      Deprecation.deprecate('Sequel::Model.is_a', 'Use Sequel::Model.plugin')
      plugin(*args, &block)
    end

    def self.delete_all
      Deprecation.deprecate('Sequel::Model.delete_all', 'Use Sequel::Model.delete')
      dataset.delete
    end

    def self.destroy_all
      Deprecation.deprecate('Sequel::Model.destroy_all', 'Use Sequel::Model.destroy')
      dataset.destroy
    end

    def self.str_columns
      Deprecation.deprecate('Sequel::Model.str_columns', 'Use model.columns.map{|x| x.to_s}')
      @str_columns ||= columns.map{|c| c.to_s.freeze}
    end

    def dataset
      Deprecation.deprecate('Sequel::Model#dataset', 'Use model_object.model.dataset')
      model.dataset
    end

    def str_columns
      Deprecation.deprecate('Sequel::Model#str_columns', 'Use model_object.columns.map{|x| x.to_s}')
      model.str_columns
    end

    def set_values(values)
      Deprecation.deprecate('Sequel::Model#set_values', 'Use Sequel::Model#set')
      s = str_columns
      vals = values.inject({}) do |m, kv|
        k, v = kv
        k = case k
        when Symbol
          k
        when String
          raise(Error, "all string keys must be a valid columns") unless s.include?(k)
          k.to_sym
        else
          raise(Error, "Only symbols and strings allows as keys")
        end
        m[k] = v
        m
      end
      vals.each {|k, v| @values[k] = v}
      vals
    end

    def update_values(values)
      Deprecation.deprecate('Sequel::Model#update_values', 'Use Sequel::Model#update or model_object.this.update')
      before_update_values
      this.update(set_values(values))
    end

    module Associations
      def belongs_to(*args, &block)
        Deprecation.deprecate('Sequel::Model.belongs_to', 'Use Sequel::Model.many_to_one')
        many_to_one(*args, &block)
      end

      def has_many(*args, &block)
        Deprecation.deprecate('Sequel::Model.has_many', 'Use Sequel::Model.one_to_many')
        one_to_many(*args, &block)
      end

      def has_and_belongs_to_many(*args, &block)
        Deprecation.deprecate('Sequel::Model.has_and_belongs_to_many', 'Use Sequel::Model.many_to_many')
        many_to_many(*args, &block)
      end
    end
  end
end
