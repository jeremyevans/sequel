require 'rubygems'

unless Object.const_defined?('Sequel')
  $:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../../lib/"))
  require 'sequel/core'
end

if ENV['SEQUEL_COLUMNS_INTROSPECTION']
  Sequel.extension :columns_introspection
  Sequel::Dataset.introspect_all_columns
end

Sequel.quote_identifiers = false
Sequel.identifier_input_method = nil
Sequel.identifier_output_method = nil
