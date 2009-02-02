require 'rubygems'
unless Object.const_defined?('Sequel')
  $:.unshift(File.join(File.dirname(__FILE__), "../../lib/"))
  require 'sequel'
end
begin
  require File.join(File.dirname(__FILE__), '../spec_config.rb')
rescue LoadError
end

$sqls = []
def clear_sqls
  $sqls.clear
end 

class Spec::Example::ExampleGroup
  def start_logging
    require 'logger'
    INTEGRATION_DB.loggers << Logger.new(STDOUT)
  end
  def stop_logging
     INTEGRATION_DB.loggers.clear
  end
end

if defined?(INTEGRATION_DB) || defined?(INTEGRATION_URL) || ENV['SEQUEL_INTEGRATION_URL']
  unless defined?(INTEGRATION_DB)
    url = defined?(INTEGRATION_URL) ? INTEGRATION_URL : ENV['SEQUEL_INTEGRATION_URL']
    INTEGRATION_DB = Sequel.connect(url)
  end
  class Spec::Example::ExampleGroup
    def sqls_should_be(*args)
    end 
  end
else
  sql_logger = Object.new
  def sql_logger.info(str)
    $sqls << str 
  end
  INTEGRATION_DB = Sequel.sqlite('', :loggers=>[sql_logger], :quote_identifiers=>false)
  class Spec::Example::ExampleGroup
    def sqls_should_be(*sqls)
      sqls.zip($sqls).each do |should_be, is|
        case should_be
        when String
          is.should == should_be
        when Regexp
          is.should =~ should_be
        else
          raise ArgumentError, "need String or RegExp"
        end
      end
      $sqls.length.should == sqls.length
      clear_sqls
    end 
  end
end
