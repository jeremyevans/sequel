require 'rubygems'
unless Object.const_defined?('Sequel')
  $:.unshift(File.join(File.dirname(__FILE__), "../../lib/"))
  require 'sequel_core'
end
unless Sequel.const_defined?('Model')
  $:.unshift(File.join(File.dirname(__FILE__), "../../lib/"))
  require 'sequel_model'
end

Sequel.virtual_row_instance_eval = true

extensions = %w'string_date_time inflector'
plugins = {:hook_class_methods=>[], :schema=>[], :validation_class_methods=>[]}

extensions.each{|e| require "sequel/extensions/#{e}"}
plugins.each{|p, opts| Sequel::Model.plugin(p, *opts)}

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
    return if sql =~ /information_schema/
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

  def schema(table_name, opts)
    if table_name
      [[:id, {:primary_key=>true}]]
    else
      {table_name=>[[:id, {:primary_key=>true}]]}
    end
  end

  def transaction(opts={}); yield; end
  
  def dataset(opts=nil); MockDataset.new(self, opts); end
end

class << Sequel::Model
  alias orig_columns columns
  alias orig_str_columns str_columns
  def columns(*cols)
    return if cols.empty?
    define_method(:columns){cols}
    @dataset.instance_variable_set(:@columns, cols) if @dataset
    define_method(:str_columns){cols.map{|x|x.to_s.freeze}}
    def_column_accessor(*cols)
    @columns = cols
    @db_schema = {}
    cols.each{|c| @db_schema[c] = {}}
  end
  def simple_table
    nil
  end
end

Sequel::Model.db = MODEL_DB = MockDatabase.new
Sequel::Model.use_transactions = false
