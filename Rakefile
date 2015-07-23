require "rake"
require "rake/clean"

NAME = 'sequel'
VERS = lambda do
  require File.expand_path("../lib/sequel/version", __FILE__)
  Sequel.version
end
CLEAN.include ["**/.*.sw?", "sequel-*.gem", ".config", "rdoc", "coverage", "www/public/*.html", "www/public/rdoc*", '**/*.rbc']

# Gem Packaging and Release

desc "Build sequel gem"
task :package=>[:clean] do |p|
  sh %{#{FileUtils::RUBY} -S gem build sequel.gemspec}
end

desc "Publish sequel gem to rubygems.org"
task :release=>[:package] do
  sh %{#{FileUtils::RUBY} -S gem push ./#{NAME}-#{VERS.call}.gem}
end

### Website

desc "Make local version of website"
task :website do
  sh %{#{FileUtils::RUBY} www/make_www.rb}
end

### RDoc

RDOC_DEFAULT_OPTS = ["--line-numbers", "--inline-source", '--title', 'Sequel: The Database Toolkit for Ruby']

begin
  # Sequel uses hanna-nouveau for the website RDoc.
  gem 'hanna-nouveau'
  RDOC_DEFAULT_OPTS.concat(['-f', 'hanna'])
rescue Gem::LoadError
end

rdoc_task_class = begin
  require "rdoc/task"
  RDoc::Task
rescue LoadError
  begin
    require "rake/rdoctask"
    Rake::RDocTask
  rescue LoadError, StandardError
  end
end

if rdoc_task_class
  RDOC_OPTS = RDOC_DEFAULT_OPTS + ['--main', 'README.rdoc']

  rdoc_task_class.new do |rdoc|
    rdoc.rdoc_dir = "rdoc"
    rdoc.options += RDOC_OPTS
    rdoc.rdoc_files.add %w"README.rdoc CHANGELOG MIT-LICENSE lib/**/*.rb doc/*.rdoc doc/release_notes/*.txt"
  end

  desc "Make rdoc for website"
  task :website_rdoc=>[:website_rdoc_main, :website_rdoc_adapters, :website_rdoc_plugins]

  rdoc_task_class.new(:website_rdoc_main) do |rdoc|
    rdoc.rdoc_dir = "www/public/rdoc"
    rdoc.options += RDOC_OPTS + %w'--no-ignore-invalid'
    rdoc.rdoc_files.add %w"README.rdoc CHANGELOG MIT-LICENSE lib/*.rb lib/sequel/*.rb lib/sequel/{connection_pool,dataset,database,model}/*.rb doc/*.rdoc doc/release_notes/*.txt lib/sequel/extensions/migration.rb"
  end

  rdoc_task_class.new(:website_rdoc_adapters) do |rdoc|
    rdoc.rdoc_dir = "www/public/rdoc-adapters"
    rdoc.options += RDOC_DEFAULT_OPTS + %w'--main Sequel --no-ignore-invalid'
    rdoc.rdoc_files.add %w"lib/sequel/adapters/**/*.rb"
  end

  rdoc_task_class.new(:website_rdoc_plugins) do |rdoc|
    rdoc.rdoc_dir = "www/public/rdoc-plugins"
    rdoc.options += RDOC_DEFAULT_OPTS + %w'--main Sequel --no-ignore-invalid'
    rdoc.rdoc_files.add %w"lib/sequel/{extensions,plugins}/**/*.rb doc/core_*"
  end
end

### Specs

run_spec = proc do |patterns|
  lib_dir = File.join(File.dirname(File.expand_path(__FILE__)), 'lib')
  rubylib = ENV['RUBYLIB']
  ENV['RUBYLIB'] ? (ENV['RUBYLIB'] += ":#{lib_dir}") : (ENV['RUBYLIB'] = lib_dir)
  if RUBY_PLATFORM =~ /mingw32/ || RUBY_DESCRIPTION =~ /windows/i
    patterns = patterns.split.map{|pat| Dir[pat].to_a}.flatten.join(' ')
  end
  sh "#{FileUtils::RUBY} -e \"ARGV.each{|f| require f}\" #{patterns}"
  ENV['RUBYLIB'] = rubylib
end

spec_task = proc do |description, name, files|
  desc description
  task name do
    run_spec.call(files)
  end

  desc "#{description} with warnings, some warnings filtered"
  task :"#{name}_w" do
    ENV['RUBYOPT'] ? (ENV['RUBYOPT'] += " -w") : (ENV['RUBYOPT'] = '-w')
    rake = ENV['RAKE'] || "#{FileUtils::RUBY} -S rake"
    sh "#{rake} #{name} 2>&1 | egrep -v \"(: warning: instance variable @.* not initialized|: warning: method redefined; discarding old|: warning: previous definition of)\""
  end

  desc "#{description} with coverage"
  task :"#{name}_cov" do
    ENV['COVERAGE'] = '1'
    run_spec.call(files)
    ENV.delete('COVERAGE')
  end
end

desc "Run the core, model, and extension/plugin specs"
task :default => :spec
desc "Run the core, model, and extension/plugin specs"
task :spec => [:spec_core, :spec_model, :spec_plugin]

spec_task.call("Run core and model specs together", :spec_core_model, './spec/core/*_spec.rb ./spec/model/*_spec.rb')
spec_task.call("Run core specs", :spec_core, './spec/core/*_spec.rb')
spec_task.call("Run model specs", :spec_model, './spec/model/*_spec.rb')
spec_task.call("Run plugin/extension specs", :spec_plugin, './spec/extensions/*_spec.rb')
spec_task.call("Run bin/sequel specs", :spec_bin, './spec/bin_spec.rb')
spec_task.call("Run core extensions specs", :spec_core_ext, './spec/core_extensions_spec.rb')
spec_task.call("Run integration tests", :spec_integration, './spec/integration/*_test.rb')

%w'postgres sqlite mysql informix oracle firebird mssql db2 sqlanywhere'.each do |adapter|
  spec_task.call("Run #{adapter} tests", :"spec_#{adapter}", "./spec/adapters/#{adapter}_spec.rb ./spec/integration/*_test.rb")
end

spec_task.call("Run model specs without the associations code", :_spec_model_no_assoc, Dir["./spec/model/*_spec.rb"].delete_if{|f| f =~ /association|eager_loading/}.join(' '))
desc "Run model specs without the associations code"
task :spec_model_no_assoc do
  ENV['SEQUEL_NO_ASSOCIATIONS'] = '1'
  Rake::Task['_spec_model_no_assoc'].invoke
end

task :spec_travis=>[:spec_core, :spec_model, :spec_plugin, :spec_core_ext] do
  if defined?(RUBY_ENGINE) && RUBY_ENGINE == 'jruby'
    ENV['SEQUEL_SQLITE_URL'] = "jdbc:sqlite::memory:"
    ENV['SEQUEL_POSTGRES_URL'] = "jdbc:postgresql://localhost/sequel_test?user=postgres"
    ENV['SEQUEL_MYSQL_URL'] = "jdbc:mysql://localhost/sequel_test?user=root"
  else
    ENV['SEQUEL_SQLITE_URL'] = "sqlite:/"
    ENV['SEQUEL_POSTGRES_URL'] = "postgres://localhost/sequel_test?user=postgres"
    ENV['SEQUEL_MYSQL_URL'] = "mysql2://localhost/sequel_test?user=root"
  end

  Rake::Task['spec_sqlite'].invoke
  Rake::Task['spec_postgres'].invoke
  Rake::Task['spec_mysql'].invoke
end

desc "Print Sequel version"
task :version do
  puts VERS.call
end

desc "Check syntax of all .rb files"
task :check_syntax do
  Dir['**/*.rb'].each{|file| print `#{FileUtils::RUBY} -c #{file} | fgrep -v "Syntax OK"`}
end
