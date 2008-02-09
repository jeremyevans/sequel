validations = File.
  join File.dirname(__FILE__), %w[validations *_validation.rb]

Dir.glob(validations).each { |validation_path| require validation_path }
