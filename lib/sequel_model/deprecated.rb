module Sequel
  class Model
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
