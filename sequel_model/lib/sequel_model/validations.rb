gem 'assistance', '>= 0.1.2' # because we need Validations

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
