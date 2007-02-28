dir = File.join(File.dirname(__FILE__), 'sequel')
require File.join(dir, 'database')
require File.join(dir, 'connection_pool')
require File.join(dir, 'schema')
require File.join(dir, 'dataset')
require File.join(dir, 'model')

module Sequel
  def self.connect(url)
    Database.connect(url)
  end
end
