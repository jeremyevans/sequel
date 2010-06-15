require 'rubygems'
unless Object.const_defined?('Sequel')
  $:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../../lib/"))
  require 'sequel/core'
end
unless Sequel.const_defined?('Model')
  $:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../../lib/"))
  require 'sequel/model'
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

   def transaction(opts={})
    return yield if @transactions.include?(Thread.current)
    execute('BEGIN')
    begin
      @transactions << Thread.current
      yield
    rescue Exception => e
      execute('ROLLBACK')
      transaction_error(e)
    ensure
      unless e
        execute('COMMIT')
      end
      @transactions.delete(Thread.current)
    end
  end

  def dataset(opts=nil); MockDataset.new(self, opts); end
end

class << Sequel::Model
  alias orig_columns columns
  def columns(*cols)
    return if cols.empty?
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

Sequel::Model.use_transactions = false
Sequel::Model.db = MODEL_DB = MockDatabase.new
