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
CLEAN.include ["**/.*.sw?", "pkg", ".config", "rdoc", "coverage", "www/public/*.html", "www/public/rdoc*"]
RDOC_DEFAULT_OPTS = ["--quiet", "--line-numbers", "--inline-source", '--title', 'Sequel: The Database Toolkit for Ruby']
RDOC_OPTS = RDOC_DEFAULT_OPTS + ['--main', 'README.rdoc']

# Gem Packaging and Release

spec = Gem::Specification.new do |s|
  s.name = NAME
  s.rubyforge_project = 'sequel'
  s.version = VERS.call
  s.platform = Gem::Platform::RUBY
  s.has_rdoc = true
  s.extra_rdoc_files = ["README.rdoc", "CHANGELOG", "COPYING"] + Dir["doc/*.rdoc"] + Dir['doc/release_notes/*.txt']
  s.rdoc_options += RDOC_OPTS 
  s.summary = "The Database Toolkit for Ruby"
  s.description = s.summary
  s.author = "Jeremy Evans"
  s.email = "code@jeremyevans.net"
  s.homepage = "http://sequel.rubyforge.org"
  s.required_ruby_version = ">= 1.8.4"
  s.files = %w(COPYING CHANGELOG README.rdoc Rakefile) + Dir["{bin,doc,spec,lib}/**/*"]
  s.require_path = "lib"
  s.bindir = 'bin'
  s.executables << 'sequel'
end

desc "Packages sequel"
task :package=>[:clean]
Rake::GemPackageTask.new(spec) do |p|
  p.need_tar = true
  p.gem_spec = spec
end

desc "Install sequel gem"
task :install=>[:package] do
  sh %{sudo gem install pkg/#{NAME}-#{VERS.call} --local}
end

desc "Uninstall sequel gem"
task :uninstall=>[:clean] do
  sh %{sudo gem uninstall #{NAME}}
end

desc "Upload sequel gem to gemcutter"
task :release=>[:package] do
  sh %{gem push pkg/#{NAME}-#{VERS.call}.gem} 
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
  rdoc.rdoc_files.add %w"README.rdoc CHANGELOG COPYING lib/*.rb lib/sequel/*.rb lib/sequel/{dataset,database,model}/*.rb doc/*.rdoc doc/release_notes/*.txt"
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

  spec_opts = lambda do
    lib_dir = File.join(File.dirname(__FILE__), 'lib')
    ENV['RUBYLIB'] ? (ENV['RUBYLIB'] += ":#{lib_dir}") : (ENV['RUBYLIB'] = lib_dir)
  end

  rcov_opts = lambda do
    [true, File.read("spec/rcov.opts").split("\n")]
  end
  
  desc "Run core and model specs with coverage"
  Spec::Rake::SpecTask.new("spec_coverage") do |t|
    t.spec_files = Dir["spec/{core,model}/*_spec.rb"]
    spec_opts.call
    t.rcov, t.rcov_opts = rcov_opts.call
  end
  
  desc "Run core and model specs"
  task :default => [:spec]
  Spec::Rake::SpecTask.new("spec") do |t|
    t.spec_files = Dir["spec/{core,model}/*_spec.rb"]
    spec_opts.call
  end
  
  desc "Run core specs"
  Spec::Rake::SpecTask.new("spec_core") do |t|
    t.spec_files = Dir["spec/core/*_spec.rb"]
    spec_opts.call
  end
  
  desc "Run model specs"
  Spec::Rake::SpecTask.new("spec_model") do |t|
    t.spec_files = Dir["spec/model/*_spec.rb"]
    spec_opts.call
  end
  
  desc "Run extension/plugin specs"
  Spec::Rake::SpecTask.new("spec_plugin") do |t|
    t.spec_files = Dir["spec/extensions/*_spec.rb"]
    spec_opts.call
  end
  
  desc "Run extention/plugin specs with coverage"
  Spec::Rake::SpecTask.new("spec_plugin_cov") do |t|
    t.spec_files = Dir["spec/extensions/*_spec.rb"]
    spec_opts.call
    t.rcov, t.rcov_opts = rcov_opts.call
  end
  
  desc "Run integration tests"
  Spec::Rake::SpecTask.new("integration") do |t|
    t.spec_files = Dir["spec/integration/*_test.rb"]
    spec_opts.call
  end
  
  desc "Run integration tests with coverage"
  Spec::Rake::SpecTask.new("integration_cov") do |t|
    t.spec_files = Dir["spec/integration/*_test.rb"]
    spec_opts.call
    t.rcov, t.rcov_opts = rcov_opts.call
  end
  
  %w'postgres sqlite mysql informix oracle firebird mssql'.each do |adapter|
    desc "Run #{adapter} specs"
    Spec::Rake::SpecTask.new("spec_#{adapter}") do |t|
      t.spec_files = ["spec/adapters/#{adapter}_spec.rb"] + Dir["spec/integration/*_test.rb"]
      spec_opts.call
    end

    desc "Run #{adapter} specs with coverage"
    Spec::Rake::SpecTask.new("spec_#{adapter}_cov") do |t|
      t.spec_files = ["spec/adapters/#{adapter}_spec.rb"] + Dir["spec/integration/*_test.rb"]
      spec_opts.call
      t.rcov, t.rcov_opts = rcov_opts.call
    end
  end
rescue LoadError
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
