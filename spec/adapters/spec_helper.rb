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
  require File.join(File.dirname(File.dirname(File.expand_path(__FILE__))), 'spec_config.rb')
rescue LoadError
end

Sequel::Database.extension :columns_introspection if ENV['SEQUEL_COLUMNS_INTROSPECTION']
Sequel::Model.cache_associations = false if ENV['SEQUEL_NO_CACHE_ASSOCIATIONS']
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

unless defined?(DB)
  env_var = "SEQUEL_#{SEQUEL_ADAPTER_TEST.to_s.upcase}_URL"
  env_var = ENV.has_key?(env_var) ? env_var : 'SEQUEL_INTEGRATION_URL'
  DB = Sequel.connect(ENV[env_var])
end
