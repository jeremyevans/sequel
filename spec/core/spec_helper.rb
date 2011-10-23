require 'rubygems'

unless Object.const_defined?('Sequel')
  $:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../../lib/"))
  require 'sequel/core'
end

if ENV['SEQUEL_COLUMNS_INTROSPECTION']
  Sequel.extension :columns_introspection
  Sequel::Dataset.introspect_all_columns
end

Sequel.quote_identifiers = false

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

  VALUES = [
    {:a => 1, :b => 2},
    {:a => 3, :b => 4},
    {:a => 5, :b => 6}
  ]
  def fetch_rows(sql, &block)
    VALUES.each(&block)
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
