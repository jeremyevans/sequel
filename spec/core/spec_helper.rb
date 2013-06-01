require 'rubygems'

if ENV['COVERAGE']
  require File.join(File.dirname(File.expand_path(__FILE__)), "../sequel_coverage")
  SimpleCov.sequel_coverage(:filter=>%r{lib/sequel/(\w+\.rb|(dataset|database|model|connection_pool)/\w+\.rb|adapters/mock\.rb)\z})
end

unless Object.const_defined?('Sequel')
  $:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../../lib/"))
  require 'sequel/core'
end
Sequel::Deprecation.backtrace_filter = lambda{|line, lineno| lineno < 4 || line =~ /_spec\.rb/}

(defined?(RSpec) ? RSpec::Core::ExampleGroup : Spec::Example::ExampleGroup).class_eval do
  def meta_def(obj, name, &block)
    (class << obj; self end).send(:define_method, name, &block)
  end

  if ENV['SEQUEL_DEPRECATION_WARNINGS']
    class << self
      alias qspecify specify
    end
  else
    def self.qspecify(*a, &block)
      specify(*a) do
        begin
          output = Sequel::Deprecation.output
          Sequel::Deprecation.output = false
          instance_exec(&block)
        ensure
          Sequel::Deprecation.output = output 
        end
      end
    end
  end
end

if ENV['SEQUEL_COLUMNS_INTROSPECTION']
  Sequel.extension :columns_introspection
  Sequel::Dataset.introspect_all_columns
end

Sequel.quote_identifiers = false
Sequel.identifier_input_method = nil
Sequel.identifier_output_method = nil
