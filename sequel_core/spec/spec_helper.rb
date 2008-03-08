require 'rubygems'
require File.join(File.dirname(__FILE__), "../lib/sequel_core") 

if File.exists?(File.join(File.dirname(__FILE__), 'spec_config.rb'))
  require File.join(File.dirname(__FILE__), 'spec_config.rb')
end

class MockDataset < Sequel::Dataset
  def insert(*args)
    @db.execute insert_sql(*args)
  end
  
  def update(*args)
    @db.execute update_sql(*args)
  end
  
  def fetch_rows(sql)
    @db.execute(sql)
    yield({:id => 1, :x => 1})
  end
end

class MockDatabase < Sequel::Database
  attr_reader :sqls
  
  def execute(sql)
    @sqls ||= []
    @sqls << sql
  end

  def reset
    @sqls = []
  end

  def transaction; yield; end
  
  def dataset; MockDataset.new(self); end
end

class SchemaDummyDatabase < Sequel::Database
  attr_reader :sqls
  
  def execute(sql)
    @sqls ||= []
    @sqls << sql
  end
end

