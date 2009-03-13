require 'rubygems'
unless Object.const_defined?('Sequel')
  $:.unshift(File.join(File.dirname(__FILE__), "../../lib/"))
  require 'sequel/core'
end

Sequel.virtual_row_instance_eval = true

module Spec::Example::ExampleMethods
  def deprec
    output = Sequel::Deprecation.output = nil 
    begin
      yield
    ensure
      Sequel::Deprecation.output = output
    end 
  end 
end

module Spec::Example::ExampleGroupMethods
  def deprec_specify(*args, &block)
    specify(*args) do
      output = Sequel::Deprecation.output
      Sequel::Deprecation.output = nil
      begin
        instance_eval(&block)
      ensure
        Sequel::Deprecation.output = output
      end
    end
  end
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
