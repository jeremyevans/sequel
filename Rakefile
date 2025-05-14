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

def self.rdoc_task(rdoc_dir, main, files)
  rdoc_opts = ["--line-numbers", "--inline-source", '--title', 'Sequel: The Database Toolkit for Ruby']

  begin
    gem 'hanna'
    rdoc_opts.concat(['-f', 'hanna'])
  rescue Gem::LoadError
  end

  rdoc_opts.concat(['--main', main, "-o", rdoc_dir])

  FileUtils.rm_rf(rdoc_dir)

  require "rdoc"
  RDoc::RDoc.new.document(rdoc_opts + files)
end

desc "Generate rdoc"
task :rdoc do
  rdoc_task("rdoc", 'README.rdoc',
    %w"README.rdoc CHANGELOG doc/CHANGELOG.old MIT-LICENSE" +
    Dir["lib/**/*.rb"] +
    Dir["doc/*.rdoc"] +
    Dir['doc/release_notes/*.txt']
  )
end

desc "Generate all rdoc for Sequel website"
task :website_rdoc=>[:website_rdoc_main, :website_rdoc_adapters, :website_rdoc_plugins]

desc "Generate rdoc for core/model for Sequel website"
task :website_rdoc_main do
  rdoc_task("www/public/rdoc", 'README.rdoc',
    %w"README.rdoc CHANGELOG doc/CHANGELOG.old MIT-LICENSE" +
    Dir["lib/*.rb"] +
    Dir["lib/sequel/*.rb"] +
    Dir["lib/sequel/{connection_pool,dataset,database,model}/*.rb"] +
    ["lib/sequel/extensions/migration.rb"] +
    Dir["doc/*.rdoc"] + 
    Dir["doc/release_notes/*.txt"]
  )
end

desc "Generate rdoc for adapters for Sequel website"
task :website_rdoc_adapters do
  rdoc_task("www/public/rdoc-adapters", 'Sequel',
    Dir["lib/sequel/adapters/**/*.rb"]
  )
end

desc "Generate rdoc for plugins/extensions for Sequel website"
task :website_rdoc_plugins do
  rdoc_task("www/public/rdoc-plugins", 'Sequel',
    Dir["lib/sequel/{extensions,plugins}/**/*.rb"] +
    Dir["doc/core_*"]
  )
end

### Specs

run_spec = proc do |file|
  lib_dir = File.join(File.dirname(File.expand_path(__FILE__)), 'lib')
  rubylib = ENV['RUBYLIB']
  ENV['RUBYLIB'] ? (ENV['RUBYLIB'] += ":#{lib_dir}") : (ENV['RUBYLIB'] = lib_dir)
  sh "#{FileUtils::RUBY} #{"-w" if RUBY_VERSION >= '3'} #{'-W:strict_unused_block' if RUBY_VERSION >= '3.4'} #{file}"
  ENV['RUBYLIB'] = rubylib
end

spec_task = proc do |description, name, file, coverage, visibility|
  desc description
  task name do
    run_spec.call(file)
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

spec_task.call("Run core and model specs together", :spec_core_model, 'spec/core_model_spec.rb', "core-model", false)
spec_task.call("Run core specs", :spec_core, 'spec/core_spec.rb', false, false)
spec_task.call("Run model specs", :spec_model, 'spec/model_spec.rb', false, false)
spec_task.call("Run plugin/extension specs", :spec_plugin, 'spec/plugin_spec.rb', "plugin-extension", true)
spec_task.call("Run bin/sequel specs", :spec_bin, 'spec/bin_spec.rb', 'bin', false)
spec_task.call("Run core extensions specs", :spec_core_ext, 'spec/core_extensions_spec.rb', 'core-ext', true)
spec_task.call("Run integration tests", :spec_integration, 'spec/adapter_spec.rb none', '1', true)

%w'postgres sqlite mysql oracle mssql db2 sqlanywhere'.each do |adapter|
  spec_task.call("Run #{adapter} tests", :"spec_#{adapter}", "spec/adapter_spec.rb #{adapter}", adapter, true)
end

spec_task.call("Run model specs without the associations code", :_spec_model_no_assoc, 'spec/model_no_assoc_spec.rb', false, false)
desc "Run model specs without the associations code"
task :spec_model_no_assoc do
  ENV['SEQUEL_NO_ASSOCIATIONS'] = '1'
  Rake::Task['_spec_model_no_assoc'].invoke
end

desc "Run core/model/extension/plugin specs with coverage"
task :spec_cov do
  Rake::Cleaner.cleanup_files(::Rake::FileList["coverage"])
  ENV['SEQUEL_MERGE_COVERAGE'] = '1'
  Rake::Task['spec_bin_cov'].invoke
  Rake::Task['spec_core_model_cov'].invoke
  Rake::Task['spec_plugin_cov'].invoke
  Rake::Task['spec_core_ext_cov'].invoke
  ENV['NO_SEQUEL_PG'] = '1'
  Rake::Task['spec_postgres_cov'].invoke
end

task :spec_ci=>[:spec_core, :spec_model, :spec_plugin, :spec_core_ext] do
  mysql_host = "localhost"
  pg_database = "sequel_test" unless ENV["DEFAULT_DATABASE"]
  mysql_jdbc = "&allowPublicKeyRetrieval=true"

  if ENV["MYSQL_ROOT_PASSWORD"]
    mysql_password = "&password=root"
    mysql_host= "127.0.0.1:3306"
  end

  if ENV["MARIADB_ROOT_PASSWORD"]
    mysql_password = "&password=root"
    mysql_host = "127.0.0.1:3307"
    mysql_jdbc = ""
  end

  if defined?(RUBY_ENGINE) && RUBY_ENGINE == 'jruby'
    ENV['SEQUEL_SQLITE_URL'] = "jdbc:sqlite::memory:"
    ENV['SEQUEL_POSTGRES_URL'] = "jdbc:postgresql://localhost/#{pg_database}?user=postgres&password=postgres"
    ENV['SEQUEL_MYSQL_URL'] = "jdbc:mysql://#{mysql_host}/sequel_test?user=root#{mysql_password}&useSSL=false#{mysql_jdbc}"
  else
    ENV['SEQUEL_SQLITE_URL'] = "sqlite:/"
    ENV['SEQUEL_POSTGRES_URL'] = "postgres://localhost/#{pg_database}?user=postgres&password=postgres"
    ENV['SEQUEL_MYSQL_URL'] = "mysql2://#{mysql_host}/sequel_test?user=root#{mysql_password}&useSSL=false"
  end

  if RUBY_VERSION >= '2.4'
    Rake::Task['spec_postgres'].invoke
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

desc "Check documentation for plugin/extension files"
task :check_plugin_doc do
  text = File.binread('www/pages/plugins.html.erb')
  skip = %w'before_after_save freeze_datasets from_block no_auto_literal_strings auto_validations_constraint_validations_presence_message optimistic_locking_base'
  Dir['lib/sequel/{plugins,extensions}/*.rb'].map{|f| File.basename(f).sub('.rb', '') if File.size(f)}.sort.each do |f|
    puts f if !f.start_with?('_') && !skip.include?(f) && !text.include?(f)
  end
end
