require 'rubygems'
require 'logger'
require "#{File.dirname(File.dirname(__FILE__))}/sequel_warning.rb"

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

Sequel.split_symbols = false if ENV['SEQUEL_NO_SPLIT_SYMBOLS']
Sequel::Database.extension :columns_introspection if ENV['SEQUEL_COLUMNS_INTROSPECTION']
Sequel::Model.cache_associations = false if ENV['SEQUEL_NO_CACHE_ASSOCIATIONS']
if ENV['SEQUEL_MODEL_PREPARED_STATEMENTS']
  Sequel::Model.plugin :prepared_statements
  Sequel::Model.plugin :prepared_statements_associations
end
Sequel::Model.use_transactions = false
Sequel.cache_anonymous_models = false

require './spec/guards_helper'

IDENTIFIER_MANGLING = !ENV['SEQUEL_NO_MANGLE'] unless defined?(IDENTIFIER_MANGLING)

unless defined?(DB)
  opts = {}
  opts[:identifier_mangling] = false unless IDENTIFIER_MANGLING
  DB = Sequel.connect(ENV['SEQUEL_INTEGRATION_URL'], opts)
  DB.extension(:freeze_datasets) if ENV['SEQUEL_FREEZE_DATASETS']
end

if DB.adapter_scheme == :ibmdb || (DB.adapter_scheme == :ado && DB.database_type == :access)
  def DB.drop_table(*tables)
    super
  rescue Sequel::DatabaseError
    disconnect
    super
  end
end

if ENV['SEQUEL_NO_AUTO_LITERAL_STRINGS']
  DB.extension :no_auto_literal_strings
end

if ENV['SEQUEL_ERROR_SQL']
  DB.extension :error_sql
end

if ENV['SEQUEL_CONNECTION_VALIDATOR']
  ENV['SEQUEL_NO_CHECK_SQLS'] = '1'
  DB.extension(:connection_validator)
  DB.pool.connection_validation_timeout = -1
end

if dch = ENV['SEQUEL_DUPLICATE_COLUMNS_HANDLER']
  DB.extension :duplicate_columns_handler
  DB.opts[:on_duplicate_columns] = dch.to_sym unless dch.empty?
end

if ENV['SEQUEL_FREEZE_DATABASE']
  DB.extension(:constraint_validations, :string_agg, :date_arithmetic)
  DB.extension(:pg_array) if DB.database_type == :postgres
  DB.freeze
end
