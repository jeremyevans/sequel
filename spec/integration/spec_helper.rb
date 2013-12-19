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
Sequel::Model.use_transactions = false
Sequel.cache_anonymous_models = false

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
      when DB.database_type
        return c
      when Array
        case c.length
        when 1
          return c if c.first == DB.adapter_scheme
        when 2
          if c.first.is_a?(Proc)
            return c if c.last == DB.database_type && c.first.call(DB)
          elsif c.last.is_a?(Proc)
            return c if c.first == DB.adapter_scheme && c.last.call(DB)
          else
            return c if c.first == DB.adapter_scheme && c.last == DB.database_type
          end
        when 3
          return c if c[0] == DB.adapter_scheme && c[1] == DB.database_type && c[2].call(DB)
        end          
      end
    end
  end
  false
end

require File.join(File.dirname(File.expand_path(__FILE__)), "../rspec_helper.rb")

RSPEC_EXAMPLE_GROUP.class_eval do
  def log
    begin
      DB.loggers << Logger.new(STDOUT)
      yield
    ensure
     DB.loggers.pop
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

