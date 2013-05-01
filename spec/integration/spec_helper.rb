require 'rubygems'
require 'logger'

if ENV['COVERAGE']
  require File.join(File.dirname(File.expand_path(__FILE__)), "../sequel_coverage")
  SimpleCov.sequel_coverage(:group=>%r{lib/sequel/adapters})
end

unless Object.const_defined?('Sequel')
  $:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../../lib/"))
  require 'sequel/no_core_ext'
end
begin
  require File.join(File.dirname(File.dirname(__FILE__)), 'spec_config.rb') unless defined?(INTEGRATION_DB)
rescue LoadError
end
Sequel::Deprecation.backtrace_filter = lambda{|line, lineno| lineno < 4 || line =~ /_(spec|test)\.rb/}

if ENV['SEQUEL_COLUMNS_INTROSPECTION']
  Sequel.extension :columns_introspection
  Sequel::Dataset.introspect_all_columns
end

Sequel::Model.use_transactions = false
Sequel::Model.cache_anonymous_models = false

unless defined?(RSpec)
  module Spec::Matchers
    class BeWithin
      include Spec::Matchers
      def initialize(delta); @delta = delta; end
      def of(expected); be_close(expected, @delta); end 
    end
    def be_within(delta)
      BeWithin.new(delta)
    end
  end
end

def Sequel.guarded?(*checked)
  unless ENV['SEQUEL_NO_PENDING']
    checked.each do |c|
      case c
      when INTEGRATION_DB.database_type
        return c
      when Array
        case c.length
        when 1
          return c if c.first == INTEGRATION_DB.adapter_scheme
        when 2
          if c.first.is_a?(Proc)
            return c if c.last == INTEGRATION_DB.database_type && c.first.call(INTEGRATION_DB)
          elsif c.last.is_a?(Proc)
            return c if c.first == INTEGRATION_DB.adapter_scheme && c.last.call(INTEGRATION_DB)
          else
            return c if c.first == INTEGRATION_DB.adapter_scheme && c.last == INTEGRATION_DB.database_type
          end
        when 3
          return c if c[0] == INTEGRATION_DB.adapter_scheme && c[1] == INTEGRATION_DB.database_type && c[2].call(INTEGRATION_DB)
        end          
      end
    end
  end
  false
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
    if pending = Sequel.guarded?(*checked)
      specify(message){pending("Not yet working on #{Array(pending).map{|x| x.is_a?(Proc) ? :proc : x}.join(', ')}", &block)}
    else
      specify(message, &block)
    end
  end
end

if defined?(INTEGRATION_DB) || defined?(INTEGRATION_URL) || ENV['SEQUEL_INTEGRATION_URL']
  unless defined?(INTEGRATION_DB)
    url = defined?(INTEGRATION_URL) ? INTEGRATION_URL : ENV['SEQUEL_INTEGRATION_URL']
    INTEGRATION_DB = Sequel.connect(url)
    #INTEGRATION_DB.instance_variable_set(:@server_version, 80100)
  end
else
  INTEGRATION_DB = Sequel.sqlite
end

if INTEGRATION_DB.adapter_scheme == :ibmdb || (INTEGRATION_DB.adapter_scheme == :ado && INTEGRATION_DB.database_type == :access)
  def INTEGRATION_DB.drop_table(*tables)
    super
  rescue Sequel::DatabaseError
    disconnect
    super
  end
end

if ENV['SEQUEL_CONNECTION_VALIDATOR']
  ENV['SEQUEL_NO_CHECK_SQLS'] = '1'
  INTEGRATION_DB.extension(:connection_validator)
  INTEGRATION_DB.pool.connection_validation_timeout = -1
end

