require 'rubygems'
unless Object.const_defined?('Sequel')
  require 'sequel_core'
end
require File.join(File.dirname(__FILE__), "../lib/sequel_model")

class MockDataset < Sequel::Dataset
  def insert(*args)
    @db.execute insert_sql(*args)
  end
  
  def update(*args)
    @db.execute update_sql(*args)
  end
  
  def delete(*args)
    @db.execute delete_sql(*args)
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

class << Sequel::Model
  alias orig_columns columns
  def columns(*cols)
    define_method(:columns){cols}
    define_method(:str_columns){cols.map{|x|x.to_s.freeze}}
    def_column_accessor(*cols)
  end
end

Sequel::Model.db = MODEL_DB = MockDatabase.new
