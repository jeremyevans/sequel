require 'rubygems'
unless Object.const_defined?('Sequel')
  $:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../../lib/"))
  require 'sequel/core'
end
unless Sequel.const_defined?('Model')
  $:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../../lib/"))
  require 'sequel/model'
end

Sequel.extension(*%w'string_date_time inflector pagination query pretty_table blank migration schema_dumper looser_typecasting sql_expr thread_local_timezones to_dot')
{:hook_class_methods=>[], :schema=>[], :validation_class_methods=>[]}.each{|p, opts| Sequel::Model.plugin(p, *opts)}

def skip_warn(s)
  warn "Skipping test of #{s}" if ENV["SKIPPED_TEST_WARN"]
end

class MockDataset < Sequel::Dataset
  def insert(*args)
    @db.execute insert_sql(*args)
  end
  
  def update(*args)
    @db.execute update_sql(*args)
    1
  end
  
  def delete(*args)
    @db.execute delete_sql(*args)
    1
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
  
  def new_sqls
    s = sqls
    reset
    s
  end

  def reset
    @sqls = []
  end

  def schema(table_name, opts)
    [[:id, {:primary_key=>true}]]
  end

  def transaction(opts={}); yield; end
  
  def dataset(opts=nil); MockDataset.new(self, opts); end
end

class << Sequel::Model
  alias orig_columns columns
  def columns(*cols)
    return @columns if cols.empty?
    define_method(:columns){cols}
    @dataset.instance_variable_set(:@columns, cols) if @dataset
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
