require "rake"
require "rake/clean"
require "rake/gempackagetask"
begin
  require "hanna/rdoctask"
rescue LoadError
  require "rake/rdoctask"
end

NAME = 'sequel'
VERS = lambda do
  require File.expand_path("../lib/sequel/version", __FILE__)
  Sequel.version
end
CLEAN.include ["**/.*.sw?", "sequel-*.gem", ".config", "rdoc", "coverage", "www/public/*.html", "www/public/rdoc*"]
RDOC_DEFAULT_OPTS = ["--quiet", "--line-numbers", "--inline-source", '--title', 'Sequel: The Database Toolkit for Ruby']
RDOC_OPTS = RDOC_DEFAULT_OPTS + ['--main', 'README.rdoc']

# Gem Packaging and Release

desc "Packages sequel"
task :package=>[:clean] do |p|
  sh %{gem build sequel.gemspec}
end

desc "Install sequel gem"
task :install=>[:package] do
  sh %{sudo gem install ./#{NAME}-#{VERS.call} --local}
end

desc "Uninstall sequel gem"
task :uninstall=>[:clean] do
  sh %{sudo gem uninstall #{NAME}}
end

desc "Upload sequel gem to gemcutter"
task :release=>[:package] do
  sh %{gem push ./#{NAME}-#{VERS.call}.gem} 
end

### RDoc

Rake::RDocTask.new do |rdoc|
  rdoc.rdoc_dir = "rdoc"
  rdoc.options += RDOC_OPTS
  rdoc.rdoc_files.add %w"README.rdoc CHANGELOG COPYING lib/**/*.rb doc/*.rdoc doc/release_notes/*.txt"
end

### Website

desc "Make local version of website"
task :website do
  sh %{www/make_www.rb}
end

desc "Make rdoc for website"
task :website_rdoc=>[:website_rdoc_main, :website_rdoc_adapters, :website_rdoc_plugins]

Rake::RDocTask.new(:website_rdoc_main) do |rdoc|
  rdoc.rdoc_dir = "www/public/rdoc"
  rdoc.options += RDOC_OPTS
  rdoc.rdoc_files.add %w"README.rdoc CHANGELOG COPYING lib/*.rb lib/sequel/*.rb lib/sequel/{connection_pool,dataset,database,model}/*.rb doc/*.rdoc doc/release_notes/*.txt lib/sequel/extensions/migration.rb"
end

Rake::RDocTask.new(:website_rdoc_adapters) do |rdoc|
  rdoc.rdoc_dir = "www/public/rdoc-adapters"
  rdoc.options += RDOC_DEFAULT_OPTS + %w'--main Sequel'
  rdoc.rdoc_files.add %w"lib/sequel/adapters/**/*.rb"
end

Rake::RDocTask.new(:website_rdoc_plugins) do |rdoc|
  rdoc.rdoc_dir = "www/public/rdoc-plugins"
  rdoc.options += RDOC_DEFAULT_OPTS + %w'--main Sequel'
  rdoc.rdoc_files.add %w"lib/sequel/{extensions,plugins}/**/*.rb"
end

desc "Update Non-RDoc section of sequel.rubyforge.org"
task :website_rf_base=>[:website] do
  sh %{rsync -rt www/public/*.html rubyforge.org:/var/www/gforge-projects/sequel/}
end

desc "Update sequel.rubyforge.org"
task :website_rf=>[:website, :website_rdoc] do
  sh %{rsync -rvt www/public/* rubyforge.org:/var/www/gforge-projects/sequel/}
end

### Specs

begin
  require "spec/rake/spectask"

  spec = lambda do |name, files, d|
    lib_dir = File.join(File.dirname(File.expand_path(__FILE__)), 'lib')
    ENV['RUBYLIB'] ? (ENV['RUBYLIB'] += ":#{lib_dir}") : (ENV['RUBYLIB'] = lib_dir)
    desc d
    Spec::Rake::SpecTask.new(name) do |t|
      t.spec_files = files
      t.spec_opts = ENV['SEQUEL_SPEC_OPTS'].split if ENV['SEQUEL_SPEC_OPTS']
    end
  end

  spec_with_cov = lambda do |name, files, d|
    spec.call(name, files, d)
    t = spec.call("#{name}_cov", files, "#{d} with coverage")
    t.rcov = true
    t.rcov_opts = File.read("spec/rcov.opts").split("\n")
  end
  
  task :default => [:spec]
  spec_with_cov.call("spec", Dir["spec/{core,model}/*_spec.rb"], "Run core and model specs")
  spec.call("spec_core", Dir["spec/core/*_spec.rb"], "Run core specs")
  spec.call("spec_model", Dir["spec/model/*_spec.rb"], "Run model specs")
  spec_with_cov.call("spec_plugin", Dir["spec/extensions/*_spec.rb"], "Run extension/plugin specs")
  spec_with_cov.call("spec_integration", Dir["spec/integration/*_test.rb"], "Run integration tests")
  
  %w'postgres sqlite mysql mysql2 informix oracle firebird mssql'.each do |adapter|
    spec_with_cov.call("spec_#{adapter}", ["spec/adapters/#{adapter}_spec.rb"] + Dir["spec/integration/*_test.rb"], "Run #{adapter} specs")
  end
rescue LoadError
  task :default do
    puts "Must install rspec to run the default task (which runs specs)"
  end
end

desc "check documentation coverage"
task :dcov do
  sh %{find lib -name '*.rb' | xargs dcov}
end

### Statistics

desc "Report code statistics (KLOCs, etc) from the application"
task :stats do
  STATS_DIRECTORIES = [%w(Code lib/), %w(Spec spec)].map{|name, dir| [ name, "./#{dir}" ] }.select { |name, dir| File.directory?(dir)}
  require "extra/stats"
  verbose = true
  CodeStatistics.new(*STATS_DIRECTORIES).to_s
end

desc "Print Sequel version"
task :version do
  puts VERS.call
end

desc "Check syntax of all .rb files"
task :check_syntax do
  Dir['**/*.rb'].each{|file| print `#{ENV['RUBY'] || :ruby} -c #{file} | fgrep -v "Syntax OK"`}
end
