require 'rubygems'

if defined?(RSpec)
  begin
    require 'rspec/expectations/syntax'
    if defined?(RSpec::Expectations::Syntax) && RSpec::Expectations::Syntax.respond_to?(:enable_should)
       RSpec::Expectations::Syntax.enable_should 
    end
  rescue LoadError
    begin
      require 'rspec/expectations/extensions/kernel'
    rescue LoadError
      nil
    end
  end
end

unless Object.const_defined?('Sequel')
  $:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../../lib/"))
  require 'sequel/core'
end
unless Sequel.const_defined?('Model')
  $:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../../lib/"))
  require 'sequel/model'
end

begin
  # Attempt to load ActiveSupport inflector first, so Sequel inflector
  # can override it.
  require 'active_support/inflector'
  require 'active_support/string/inflections'
rescue LoadError
end

Sequel.extension(*%w'string_date_time inflector pagination query pretty_table blank migration schema_dumper looser_typecasting sql_expr thread_local_timezones to_dot columns_introspection server_block arbitrary_servers pg_auto_parameterize pg_statement_cache pg_hstore pg_hstore_ops pg_inet schema_caching null_dataset select_remove query_literals')
{:hook_class_methods=>[], :schema=>[], :validation_class_methods=>[]}.each{|p, opts| Sequel::Model.plugin(p, *opts)}

Sequel::Dataset.introspect_all_columns if ENV['SEQUEL_COLUMNS_INTROSPECTION']

def skip_warn(s)
  warn "Skipping test of #{s}" if ENV["SKIPPED_TEST_WARN"]
end

Sequel.quote_identifiers = false
Sequel.identifier_input_method = nil
Sequel.identifier_output_method = nil

class << Sequel::Model
  attr_writer :db_schema
  alias orig_columns columns
  def columns(*cols)
    return super if cols.empty?
    define_method(:columns){cols}
    @dataset.instance_variable_set(:@columns, cols) if @dataset
    def_column_accessor(*cols)
    @columns = cols
    @db_schema = {}
    cols.each{|c| @db_schema[c] = {}}
  end
end

Sequel::Model.use_transactions = false
Sequel::Model.cache_anonymous_models = false

db = Sequel.mock(:fetch=>{:id => 1, :x => 1}, :numrows=>1, :autoid=>proc{|sql| 10})
def db.schema(*) [[:id, {:primary_key=>true}]] end
def db.reset() sqls end
Sequel::Model.db = MODEL_DB = db
