require 'rubygems'
require 'logger'
unless Object.const_defined?('Sequel')
  $:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../../lib/"))
  require 'sequel'
end
begin
  require File.join(File.dirname(File.dirname(__FILE__)), 'spec_config.rb') unless defined?(INTEGRATION_DB)
rescue LoadError
end

Sequel::Model.use_transactions = false

$sqls = []
def clear_sqls
  $sqls.clear
end 

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

(defined?(RSpec) ? RSpec::Core::ExampleGroup : Spec::Example::ExampleGroup).class_eval do
  def log
    begin
      INTEGRATION_DB.loggers << Logger.new(STDOUT)
      yield
    ensure
     INTEGRATION_DB.loggers.pop
    end
  end
  
  def self.log_specify(message, &block)
    specify(message){log{instance_eval(&block)}}
  end

  def self.cspecify(message, *checked, &block)
    pending = false
    checked.each do |c|
      case c
      when INTEGRATION_DB.database_type
        pending = c
      when Array
        case c.length
        when 1
          pending = c if c.first == INTEGRATION_DB.adapter_scheme
        when 2
          if c.first.is_a?(Proc)
            pending = c if c.first.call(INTEGRATION_DB) && c.last == INTEGRATION_DB.database_type
          elsif c.last.is_a?(Proc)
            pending = c if c.first == INTEGRATION_DB.adapter_scheme && c.last.call(INTEGRATION_DB)
          else
            pending = c if c.first == INTEGRATION_DB.adapter_scheme && c.last == INTEGRATION_DB.database_type
          end
        when 3
          pending = c if c[0] == INTEGRATION_DB.adapter_scheme && c[1] == INTEGRATION_DB.database_type && c[2].call(INTEGRATION_DB)
        end          
      end
      break if pending
    end
    if pending
      specify(message){pending("Not yet working on #{Array(pending).join(', ')}", &block)}
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
  INTEGRATION_DB = Sequel.sqlite('', :quote_identifiers=>false)
end
