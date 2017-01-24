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
  require File.join(File.dirname(File.dirname(File.expand_path(__FILE__))), 'spec_config.rb')
rescue LoadError
end

Sequel.split_symbols = false if ENV['SEQUEL_NO_SPLIT_SYMBOLS']
Sequel::Database.extension :duplicate_column_handler if ENV['SEQUEL_DUPLICATE_COLUMN_HANDLER']
Sequel::Database.extension :columns_introspection if ENV['SEQUEL_COLUMNS_INTROSPECTION']
Sequel::Model.cache_associations = false if ENV['SEQUEL_NO_CACHE_ASSOCIATIONS']
if ENV['SEQUEL_MODEL_PREPARED_STATEMENTS']
  Sequel::Model.plugin :prepared_statements
  Sequel::Model.plugin :prepared_statements_associations
end
Sequel.cache_anonymous_models = false

class Sequel::Database
  def log_duration(duration, message)
    log_info(message)
  end
end

require './spec/guards_helper'

class Minitest::HooksSpec
  def check_sqls
    yield unless ENV['SEQUEL_NO_CHECK_SQLS']
  end
  def self.check_sqls
    yield unless ENV['SEQUEL_NO_CHECK_SQLS']
  end
end

IDENTIFIER_MANGLING = !ENV['SEQUEL_NO_MANGLE'] unless defined?(IDENTIFIER_MANGLING)

unless defined?(DB)
  env_var = "SEQUEL_#{SEQUEL_ADAPTER_TEST.to_s.upcase}_URL"
  env_var = ENV.has_key?(env_var) ? env_var : 'SEQUEL_INTEGRATION_URL'
  opts = {}
  opts[:identifier_mangling] = false unless IDENTIFIER_MANGLING
  DB = Sequel.connect(ENV[env_var], opts)
  DB.extension(:freeze_datasets) if ENV['SEQUEL_FREEZE_DATASETS']
end

if dch = ENV['SEQUEL_DUPLICATE_COLUMNS_HANDLER']
  DB.extension :duplicate_columns_handler
  DB.opts[:on_duplicate_columns] = dch.to_sym unless dch.empty?
end
