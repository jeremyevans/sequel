require 'rubygems'
unless Object.const_defined?('Sequel')
  $:.unshift(File.join(File.dirname(__FILE__), "../../lib/"))
  require 'sequel_core'
  Sequel.quote_identifiers = false
end
begin
  require File.join(File.dirname(__FILE__), '../spec_config.rb')
rescue LoadError
end
