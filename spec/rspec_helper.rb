unless defined?(RSPEC_EXAMPLE_GROUP)
  if defined?(RSpec)
    require 'rspec/version'
    if RSpec::Version::STRING >= '2.11.0'
      RSpec.configure do |config|
        config.expect_with :rspec do |c|
          c.syntax = :should
        end
        config.mock_with :rspec do |c|
          c.syntax = :should
        end
      end
    end
    RSPEC_EXAMPLE_GROUP = RSpec::Core::ExampleGroup
  else
    RSPEC_EXAMPLE_GROUP = Spec::Example::ExampleGroup
  end
end
