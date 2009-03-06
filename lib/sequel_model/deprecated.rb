module Sequel
  class Model
    {:import=>:multi_insert, :size=>:count, :uniq=>:distinct}.each do |o, n|
      instance_eval "def #{o}(*args, &block); Deprecation.deprecate('Sequel::Model.#{o}', 'Use Sequel::Model.dataset.#{n}'); dataset.#{n}(*args, &block); end"
    end

    def self.delete_all
      Deprecation.deprecate('Sequel::Model.delete_all', 'Use Sequel::Model.delete')
      dataset.delete
    end

    def self.destroy_all
      Deprecation.deprecate('Sequel::Model.destroy_all', 'Use Sequel::Model.destroy')
      dataset.destroy
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
