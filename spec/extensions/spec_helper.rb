require 'rubygems'

if defined?(RSpec)
  begin
    require 'rspec/expectations'
  rescue LoadError
    nil
  end
end

if ENV['COVERAGE']
  require File.join(File.dirname(File.expand_path(__FILE__)), "../sequel_coverage")
  SimpleCov.sequel_coverage(:filter=>%r{lib/sequel/(extensions|plugins)/\w+\.rb\z})
end

unless Object.const_defined?('Sequel') && Sequel.const_defined?('Model')
  $:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../../lib/"))
  require 'sequel'
end
Sequel::Deprecation.backtrace_filter = lambda{|line, lineno| lineno < 4 || line =~ /_spec\.rb/}
SEQUEL_EXTENSIONS_NO_DEPRECATION_WARNING = true

begin
  # Attempt to load ActiveSupport blank extension and inflector first, so Sequel
  # can override them.
  require 'active_support/core_ext/object/blank'
  require 'active_support/inflector'
  require 'active_support/core_ext/string/inflections'
rescue LoadError
  nil
end

Sequel.extension :meta_def
Sequel.extension :core_refinements if RUBY_VERSION >= '2.0.0'

Sequel::Dataset.introspect_all_columns if ENV['SEQUEL_COLUMNS_INTROSPECTION']

def skip_warn(s)
  warn "Skipping test of #{s}" if ENV["SKIPPED_TEST_WARN"]
end

(defined?(RSpec) ? RSpec::Core::ExampleGroup : Spec::Example::ExampleGroup).class_eval do
  if ENV['SEQUEL_DEPRECATION_WARNINGS']
    class << self
      alias qspecify specify
    end
  else
    def self.qspecify(*a, &block)
      specify(*a) do
        begin
          output = Sequel::Deprecation.output
          Sequel::Deprecation.output = false
          instance_exec(&block)
        ensure
          Sequel::Deprecation.output = output 
        end
      end
    end
  end
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
Sequel.cache_anonymous_models = false

db = Sequel.mock(:fetch=>{:id => 1, :x => 1}, :numrows=>1, :autoid=>proc{|sql| 10})
def db.schema(*) [[:id, {:primary_key=>true}]] end
def db.reset() sqls end
def db.supports_schema_parsing?() true end
Sequel::Model.db = MODEL_DB = db
