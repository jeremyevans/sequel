require 'rubygems'
require 'logger'

if ENV['COVERAGE']
  require File.join(File.dirname(File.expand_path(__FILE__)), "../sequel_coverage")
  SimpleCov.sequel_coverage(:group=>%r{lib/sequel/adapters})
end

unless Object.const_defined?('Sequel')
  $:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../../lib/"))
  require 'sequel'
end
begin
  require File.join(File.dirname(File.dirname(__FILE__)), 'spec_config.rb') unless defined?(DB)
rescue LoadError
end
Sequel::Deprecation.backtrace_filter = lambda{|line, lineno| lineno < 4 || line =~ /_(spec|test)\.rb/}

Sequel::Database.extension :columns_introspection if ENV['SEQUEL_COLUMNS_INTROSPECTION']
Sequel::Model.cache_associations = false if ENV['SEQUEL_NO_CACHE_ASSOCIATIONS']
Sequel::Model.use_transactions = false
Sequel.cache_anonymous_models = false

require './spec/guards_helper'

unless defined?(DB)
  DB = Sequel.connect(ENV['SEQUEL_INTEGRATION_URL'])
end

if DB.adapter_scheme == :ibmdb || (DB.adapter_scheme == :ado && DB.database_type == :access)
  def DB.drop_table(*tables)
    super
  rescue Sequel::DatabaseError
    disconnect
    super
  end
end

if ENV['SEQUEL_ERROR_SQL']
  DB.extension :error_sql
end

if ENV['SEQUEL_CONNECTION_VALIDATOR']
  ENV['SEQUEL_NO_CHECK_SQLS'] = '1'
  DB.extension(:connection_validator)
  DB.pool.connection_validation_timeout = -1
end

