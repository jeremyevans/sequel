require 'rubygems'

if ENV['COVERAGE']
  require File.join(File.dirname(File.expand_path(__FILE__)), "../sequel_coverage")
  SimpleCov.sequel_coverage(:filter=>%r{lib/sequel/(\w+\.rb|(dataset|database|model|connection_pool)/\w+\.rb|adapters/mock\.rb)\z})
end

unless Object.const_defined?('Sequel')
  $:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../../lib/"))
  SEQUEL_NO_CORE_EXTENSIONS = true
  require 'sequel/core'
end

Sequel.extension :meta_def

if ENV['SEQUEL_COLUMNS_INTROSPECTION']
  Sequel.extension :columns_introspection
  Sequel::Dataset.introspect_all_columns
end

Sequel.quote_identifiers = false
Sequel.identifier_input_method = nil
Sequel.identifier_output_method = nil
