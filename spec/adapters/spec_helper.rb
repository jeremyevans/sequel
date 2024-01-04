require 'logger'

if ENV['COVERAGE']
  require_relative "../sequel_coverage"
  SimpleCov.sequel_coverage(:group=>%r{lib/sequel/adapters})
end

$:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../../lib/"))
require_relative "../../lib/sequel"

begin
  require_relative "../spec_config" unless defined?(DB)
rescue LoadError
end
Sequel::Deprecation.backtrace_filter = lambda{|line, lineno| lineno < 4 || line =~ /_(spec|test)\.rb/}

Sequel.extension :fiber_concurrency if ENV['SEQUEL_FIBER_CONCURRENCY']

# Set so that internal use of DB constant inside Sequel code is caught by tests.
Sequel::DB = nil

Sequel::Database.extension :columns_introspection if ENV['SEQUEL_COLUMNS_INTROSPECTION']
Sequel::Model.cache_associations = false if ENV['SEQUEL_NO_CACHE_ASSOCIATIONS']
Sequel::Model.plugin :prepared_statements if ENV['SEQUEL_MODEL_PREPARED_STATEMENTS']
Sequel::Model.plugin :throw_failures if ENV['SEQUEL_MODEL_THROW_FAILURES']
Sequel::Model.plugin :primary_key_lookup_check_values if ENV['SEQUEL_PRIMARY_KEY_LOOKUP_CHECK_VALUES']
Sequel::Model.use_transactions = false
Sequel::Model.cache_anonymous_models = false

require_relative '../guards_helper'

unless defined?(DB)
  if defined?(SEQUEL_ADAPTER_TEST)
    env_var = "SEQUEL_#{SEQUEL_ADAPTER_TEST.to_s.upcase}_URL"
    env_var = nil unless ENV.has_key?(env_var)
    adapter_test_type = SEQUEL_ADAPTER_TEST
  end
  env_var ||= 'SEQUEL_INTEGRATION_URL'
  DB = Sequel.connect(ENV[env_var])
end

require_relative "../visibility_checking" if ENV['CHECK_METHOD_VISIBILITY']

IDENTIFIER_MANGLING = !!ENV['SEQUEL_IDENTIFIER_MANGLING'] unless defined?(IDENTIFIER_MANGLING)
DB.extension(:identifier_mangling) if IDENTIFIER_MANGLING

if DB.adapter_scheme == :ibmdb || (DB.adapter_scheme == :ado && DB.database_type == :access)
  def DB.drop_table(*tables)
    super
  rescue Sequel::DatabaseError
    disconnect
    super
  end
end

require_relative '../async_spec_helper'

if ENV['SEQUEL_TRANSACTION_CONNECTION_VALIDATOR']
  DB.extension(:transaction_connection_validator)
end

if ENV['SEQUEL_CONNECTION_VALIDATOR']
  DB.extension(:connection_validator)
  DB.pool.connection_validation_timeout = -1
end

DB.extension :pg_timestamptz if ENV['SEQUEL_PG_TIMESTAMPTZ']
DB.extension :integer64 if ENV['SEQUEL_INTEGER64']
DB.extension :error_sql if ENV['SEQUEL_ERROR_SQL']
DB.extension :index_caching if ENV['SEQUEL_INDEX_CACHING']
DB.extension :synchronize_sql if ENV['SEQUEL_SYNCHRONIZE_SQL']
DB.extension :auto_cast_date_and_time if ENV['SEQUEL_AUTO_CAST_DATE_TIME']

if dch = ENV['SEQUEL_DUPLICATE_COLUMNS_HANDLER']
  DB.extension :duplicate_columns_handler
  DB.opts[:on_duplicate_columns] = dch
end

if ENV['SEQUEL_FREEZE_DATABASE']
  raise "cannot freeze database when running specs for specific adapters" if adapter_test_type
  DB.extension(:constraint_validations, :string_agg, :date_arithmetic)
  DB.extension(:pg_array) if DB.database_type == :postgres
  DB.freeze
end

version = if DB.respond_to?(:server_version)
  DB.server_version
elsif DB.respond_to?(:sqlite_version)
  DB.sqlite_version
end

puts "running #{adapter_test_type || "integration (database type: #{DB.database_type})"} specs on #{RUBY_ENGINE} #{defined?(JRUBY_VERSION) ? JRUBY_VERSION : RUBY_VERSION} with #{DB.adapter_scheme} adapter#{" (database version: #{version})" if version}"
