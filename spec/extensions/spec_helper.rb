require_relative "../sequel_warning"

if ENV['COVERAGE']
  require_relative "../sequel_coverage"
  SimpleCov.sequel_coverage(:filter=>%r{lib/sequel/(extensions|plugins)/\w+\.rb\z})
end

gem 'minitest'
require 'minitest/autorun'
require 'minitest/hooks/default'
require 'minitest/shared_description'

$:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../../lib/"))
require_relative "../../lib/sequel"

require_relative '../deprecation_helper'

begin
  # Attempt to load ActiveSupport blank extension and inflector first, so Sequel
  # can override them.
  require 'active_support/core_ext/object/blank'
  require 'active_support/inflector'
  require 'active_support/core_ext/string/inflections'
rescue LoadError
  nil
end

Sequel.extension :core_refinements if RUBY_VERSION >= '2.0.0' && RUBY_ENGINE == 'ruby'

class << Sequel::Model
  attr_writer :db_schema
  alias orig_columns columns
  def columns(*cols)
    return super if cols.empty?
    define_method(:columns){cols}
    @dataset.send(:columns=, cols) if @dataset
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
def db.supports_schema_parsing?() true end
Sequel::Model.db = DB = db

if ENV['SEQUEL_COLUMNS_INTROSPECTION']
  Sequel.extension :columns_introspection
  Sequel::Database.extension :columns_introspection
  Sequel::Mock::Dataset.send(:include, Sequel::ColumnsIntrospection)
end
if ENV['SEQUEL_NO_CACHE_ASSOCIATIONS']
  Sequel::Model.cache_associations = false
end
