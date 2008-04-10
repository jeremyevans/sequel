gem "assistance", ">= 0.1.2" # because we need Validations

require "assistance"

# custom model validations
module Validation
  module ClassMethods

    def validates_uniqueness_of(*atts) # field value uniqueness validation
      opts = {
        :message => 'is already taken',
      }.merge!(atts.extract_options!)

      validates_each(*atts) do |o, a, v|
        o.errors[a] << opts[:message] unless v && !v.blank? && !o.class[a => v]
      end
    end

  end
end

module Sequel
  class Model
    include Validation
    
    alias_method :save!, :save
    def save(*args)
      return false unless valid?
      save!(*args)
    end
  end
end
