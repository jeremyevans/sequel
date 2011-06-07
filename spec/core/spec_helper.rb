require 'rubygems'

unless Object.const_defined?('Sequel')
  $:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../../lib/"))
  require 'sequel/core'
end

if ENV['SEQUEL_COLUMNS_INTROSPECTION']
  Sequel.extension :columns_introspection
  Sequel::Dataset.introspect_all_columns
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

  def quoted_identifier(c)
    "\"#{c}\""
  end
end

class MockDatabase < Sequel::Database
  set_adapter_scheme :mock
  @@quote_identifiers = false
  self.identifier_input_method = nil
  self.identifier_output_method = nil
  attr_reader :sqls
  
  def execute(sql, opts={})
    @sqls ||= []
    @sqls << sql
  end

  def reset
    @sqls = []
  end

  def transaction(opts={}); yield; end
  
  def dataset; MockDataset.new(self); end
end

class SchemaDummyDatabase < Sequel::Database
  attr_reader :sqls
  self.identifier_input_method = nil
  self.identifier_output_method = nil
  
  def execute(sql, opts={})
    @sqls ||= []
    @sqls << sql
  end
end

class DummyDataset < Sequel::Dataset
  def first
    raise if @opts[:from] == [:a]
    true
  end
end

class DummyDatabase < Sequel::Database
  attr_reader :sqls
  
  def execute(sql, opts={})
    @sqls ||= []
    @sqls << sql
  end
  
  def transaction; yield; end

  def dataset
    DummyDataset.new(self)
  end
end

class Dummy2Database < Sequel::Database
  attr_reader :sql
  def execute(sql); @sql = sql; end
  def transaction; yield; end
end

