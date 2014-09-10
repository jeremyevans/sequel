require 'rake'
require 'rake/clean'

require 'rspec/core/rake_task'
require 'sequel'

SEQUEL_PATH = Gem.loaded_specs['sequel'].full_gem_path

desc 'Default: run specs.'
task :default => :spec

desc 'Run our specs unique to fdbsql'
RSpec::Core::RakeTask.new('spec')

desc 'Run the integration specs from sequel main on fdbsql'
RSpec::Core::RakeTask.new('spec_integration') do |t|
  t.pattern = "#{SEQUEL_PATH}/spec/integration/*_test.rb"
  t.ruby_opts = "-Ilib -C#{SEQUEL_PATH}"
  raise 'ENV[SEQUEL_INTEGRATION_URL] must be set for spec_integration' unless ENV['SEQUEL_INTEGRATION_URL']
  uri = URI.parse(ENV['SEQUEL_INTEGRATION_URL'])
  if RUBY_ENGINE == 'jruby'
    unless (uri.scheme == 'jdbc' and URI.parse(uri.opaque).scheme == 'fdbsql')
      raise "SEQUEL_INTEGRATION_URL is not a jdbc:fdbsql uri, so these tests won't test against the sql-layer: #{uri}"
    end
  else
    unless uri.scheme == 'fdbsql'
      raise "SEQUEL_INTEGRATION_URL is not an fdbsql uri, so these tests won't test against the sql-layer: #{uri}"
    end
  end
end

