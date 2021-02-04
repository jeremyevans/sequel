require "rake"
require "rake/clean"

NAME = 'sequel'
VERS = lambda do
  require File.expand_path("../lib/sequel/version", __FILE__)
  Sequel.version
end
CLEAN.include ["sequel-*.gem", "rdoc", "coverage", "www/public/*.html", "www/public/rdoc*", "spec/bin-sequel-*"]

# Gem Packaging

desc "Build sequel gem"
task :package=>[:clean] do |p|
  sh %{#{FileUtils::RUBY} -S gem build sequel.gemspec}
end

### Website

desc "Make local version of website"
task :website do
  sh %{#{FileUtils::RUBY} www/make_www.rb}
end

### RDoc

RDOC_DEFAULT_OPTS = ["--line-numbers", '--title', 'Sequel: The Database Toolkit for Ruby']

begin
  # Sequel uses hanna-nouveau for the website RDoc.
  gem 'hanna-nouveau'
  RDOC_DEFAULT_OPTS.concat(['-f', 'hanna'])
rescue Gem::LoadError
end

require "rdoc/task"

RDOC_OPTS = RDOC_DEFAULT_OPTS + ['--main', 'README.rdoc']

RDoc::Task.new do |rdoc|
  rdoc.rdoc_dir = "rdoc"
  rdoc.options += RDOC_OPTS
  rdoc.rdoc_files.add %w"README.rdoc CHANGELOG MIT-LICENSE lib/**/*.rb doc/*.rdoc doc/release_notes/*.txt"
end

desc "Make rdoc for website"
task :website_rdoc=>[:website_rdoc_main, :website_rdoc_adapters, :website_rdoc_plugins]

RDoc::Task.new(:website_rdoc_main) do |rdoc|
  rdoc.rdoc_dir = "www/public/rdoc"
  rdoc.options += RDOC_OPTS + %w'--no-ignore-invalid'
  rdoc.rdoc_files.add %w"README.rdoc CHANGELOG doc/CHANGELOG.old MIT-LICENSE lib/*.rb lib/sequel/*.rb lib/sequel/{connection_pool,dataset,database,model}/*.rb doc/*.rdoc doc/release_notes/*.txt lib/sequel/extensions/migration.rb"
end

RDoc::Task.new(:website_rdoc_adapters) do |rdoc|
  rdoc.rdoc_dir = "www/public/rdoc-adapters"
  rdoc.options += RDOC_DEFAULT_OPTS + %w'--main Sequel --no-ignore-invalid'
  rdoc.rdoc_files.add %w"lib/sequel/adapters/**/*.rb"
end

RDoc::Task.new(:website_rdoc_plugins) do |rdoc|
  rdoc.rdoc_dir = "www/public/rdoc-plugins"
  rdoc.options += RDOC_DEFAULT_OPTS + %w'--main Sequel --no-ignore-invalid'
  rdoc.rdoc_files.add %w"lib/sequel/{extensions,plugins}/**/*.rb doc/core_*"
end

### Specs

run_spec = proc do |file|
  lib_dir = File.join(File.dirname(File.expand_path(__FILE__)), 'lib')
  rubylib = ENV['RUBYLIB']
  ENV['RUBYLIB'] ? (ENV['RUBYLIB'] += ":#{lib_dir}") : (ENV['RUBYLIB'] = lib_dir)
  sh "#{FileUtils::RUBY} #{file}"
  ENV['RUBYLIB'] = rubylib
end

spec_task = proc do |description, name, file, coverage, visibility|
  desc description
  task name do
    run_spec.call(file)
  end

  desc "#{description} with warnings, some warnings filtered"
  task :"#{name}_w" do
    rubyopt = ENV['RUBYOPT']
    ENV['RUBYOPT'] = "#{rubyopt} -w"
    ENV['WARNING'] = '1'
    run_spec.call(file)
    ENV.delete('WARNING')
    ENV['RUBYOPT'] = rubyopt
  end

  if coverage
    desc "#{description} with coverage"
    task :"#{name}_cov" do
      ENV['COVERAGE'] = coverage == true ? '1' : coverage
      run_spec.call(file)
      ENV.delete('COVERAGE')
    end
  end

  if visibility
    desc "Run specs with method visibility checking"
    task :"#{name}_vis" do
      ENV['CHECK_METHOD_VISIBILITY'] = '1'
      run_spec.call(file)
      ENV.delete('CHECK_METHOD_VISIBILITY')
    end
  end
end

desc "Run the core, model, and extension/plugin specs"
task :default => :spec

desc "Run the core, model, and extension/plugin specs"
task :spec => [:spec_core, :spec_model, :spec_plugin]

desc "Run the core, model, and extension/plugin specs with warnings"
task :spec_w => [:spec_core_w, :spec_model_w, :spec_plugin_w]

spec_task.call("Run core and model specs together", :spec_core_model, 'spec/core_model_spec.rb', "core-model", false)
spec_task.call("Run core specs", :spec_core, 'spec/core_spec.rb', false, false)
spec_task.call("Run model specs", :spec_model, 'spec/model_spec.rb', false, false)
spec_task.call("Run plugin/extension specs", :spec_plugin, 'spec/plugin_spec.rb', "plugin-extension", true)
spec_task.call("Run bin/sequel specs", :spec_bin, 'spec/bin_spec.rb', false, false)
spec_task.call("Run core extensions specs", :spec_core_ext, 'spec/core_extensions_spec.rb', true, true)
spec_task.call("Run integration tests", :spec_integration, 'spec/adapter_spec.rb none', true, true)

%w'postgres sqlite mysql oracle mssql db2 sqlanywhere'.each do |adapter|
  spec_task.call("Run #{adapter} tests", :"spec_#{adapter}", "spec/adapter_spec.rb #{adapter}", true, true)
end

spec_task.call("Run model specs without the associations code", :_spec_model_no_assoc, 'spec/model_no_assoc_spec.rb', false)
desc "Run model specs without the associations code"
task :spec_model_no_assoc do
  ENV['SEQUEL_NO_ASSOCIATIONS'] = '1'
  Rake::Task['_spec_model_no_assoc'].invoke
end

desc "Run core/model/extension/plugin specs with coverage"
task :spec_cov do
  ENV['SEQUEL_MERGE_COVERAGE'] = '1'
  Rake::Task['spec_core_model_cov'].invoke
  Rake::Task['spec_plugin_cov'].invoke
end

task :spec_ci=>[:spec_core, :spec_model, :spec_plugin, :spec_core_ext] do
  mysql_host = "localhost"
  pg_database = "sequel_test" unless ENV["DEFAULT_DATABASE"]

  if ENV["MYSQL_ROOT_PASSWORD"]
    mysql_password = "&password=root"
    mysql_host= "127.0.0.1:3306"
  end

  if defined?(RUBY_ENGINE) && RUBY_ENGINE == 'jruby'
    ENV['SEQUEL_SQLITE_URL'] = "jdbc:sqlite::memory:"
    ENV['SEQUEL_POSTGRES_URL'] = "jdbc:postgresql://localhost/#{pg_database}?user=postgres"
    ENV['SEQUEL_MYSQL_URL'] = "jdbc:mysql://#{mysql_host}/sequel_test?user=root#{mysql_password}"
  else
    ENV['SEQUEL_SQLITE_URL'] = "sqlite:/"
    ENV['SEQUEL_POSTGRES_URL'] = "postgres://localhost/#{pg_database}?user=postgres"
    ENV['SEQUEL_MYSQL_URL'] = "mysql2://#{mysql_host}/sequel_test?user=root#{mysql_password}"
  end

  Rake::Task['spec_postgres'].invoke

  if RUBY_VERSION >= '2.4'
    Rake::Task['spec_sqlite'].invoke
    Rake::Task['spec_mysql'].invoke
  end
end

desc "Print Sequel version"
task :version do
  puts VERS.call
end

desc "Check syntax of all .rb files"
task :check_syntax do
  Dir['**/*.rb'].each{|file| print `#{FileUtils::RUBY} -c #{file} | fgrep -v "Syntax OK"`}
end
