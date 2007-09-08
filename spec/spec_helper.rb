require File.join(File.dirname(__FILE__), '../lib/sequel')

class MockDataset < Sequel::Dataset
  def insert(*args)
    @db << insert_sql(*args)
  end
  
  def fetch_rows(sql)
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