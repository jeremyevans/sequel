require 'rake'
require 'rake/clean'

require 'rspec/core/rake_task'
require 'sequel'

SEQUEL_PATH = Gem.loaded_specs['sequel'].full_gem_path

desc 'Default: run specs.'
task :default => :spec


desc 'Run specs'
RSpec::Core::RakeTask.new('spec_integration') do |t|
  t.pattern = "#{SEQUEL_PATH}/spec/integration/*_test.rb"
  t.ruby_opts = '-r sequel-fdbsql-adapter'
  raise 'ENV[SEQUEL_INTEGRATION_URL] must be set for spec_integration' unless ENV['SEQUEL_INTEGRATION_URL']
end

