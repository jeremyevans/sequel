dir = File.join(File.dirname(__FILE__), 'sequel')
require File.join(dir, 'core_ext')
require File.join(dir, 'error')
require File.join(dir, 'database')
require File.join(dir, 'connection_pool')
require File.join(dir, 'schema')
require File.join(dir, 'pretty_table')
require File.join(dir, 'expressions')
require File.join(dir, 'dataset')
require File.join(dir, 'model')

module Sequel #:nodoc:
  class << self
    def connect(url)
      Database.connect(url)
    end
    
    alias_method :open, :connect
  end
end
