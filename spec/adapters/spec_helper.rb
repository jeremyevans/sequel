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
Sequel.cache_anonymous_models = false

class Sequel::Database
  def log_duration(duration, message)
    log_info(message)
  end
end

(defined?(RSpec) ? RSpec::Core::ExampleGroup : Spec::Example::ExampleGroup).class_eval do
  def log 
    begin
      INTEGRATION_DB.loggers << Logger.new(STDOUT)
      yield
    ensure
     INTEGRATION_DB.loggers.pop
    end 
  end 

  def self.cspecify(message, *checked, &block)
    return specify(message, &block) if ENV['SEQUEL_NO_PENDING']
    pending = false
    checked.each do |c|
      case c
      when INTEGRATION_DB.adapter_scheme
        pending = c
      when Proc
        pending = c if c.first.call(INTEGRATION_DB)
      when Array
        pending = c if c.first == INTEGRATION_DB.adapter_scheme && c.last == INTEGRATION_DB.call(INTEGRATION_DB)
      end
    end
    if pending
      specify(message){pending("Not yet working on #{Array(pending).join(', ')}", &block)}
    else
      specify(message, &block)
    end
  end

  def check_sqls
    yield unless ENV['SEQUEL_NO_CHECK_SQLS']
  end
  def self.check_sqls
    yield unless ENV['SEQUEL_NO_CHECK_SQLS']
  end
end
