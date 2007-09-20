require 'rake'
require 'rake/clean'
require 'rake/gempackagetask'
require 'rake/rdoctask'
require 'fileutils'
include FileUtils

NAME = "sequel"
VERS = "0.2.0.2"
CLEAN.include ['**/.*.sw?', 'pkg/*', '.config', 'doc/*', 'coverage/*']
RDOC_OPTS = ['--quiet', '--title', "Sequel: Concise ORM for Ruby",
  "--opname", "index.html",
  "--line-numbers", 
  "--main", "README",
  "--inline-source"]

desc "Packages up Sequel."
task :default => [:package]
task :package => [:clean]

task :doc => [:rdoc]

Rake::RDocTask.new do |rdoc|
  rdoc.rdoc_dir = 'doc/rdoc'
  rdoc.options += RDOC_OPTS
  rdoc.main = "README"
  rdoc.title = "Sequel: Lightweight ORM library for Ruby"
  rdoc.rdoc_files.add ['README', 'COPYING', 'lib/sequel.rb', 'lib/sequel/**/*.rb']
end

spec = Gem::Specification.new do |s|
  s.name = NAME
  s.version = VERS
  s.platform = Gem::Platform::RUBY
  s.has_rdoc = true
  s.extra_rdoc_files = ["README", "CHANGELOG", "COPYING"]
  s.rdoc_options += RDOC_OPTS + 
    ['--exclude', '^(examples|extras)\/', '--exclude', 'lib/sequel.rb']
  s.summary = "Lightweight ORM library for Ruby"
  s.description = s.summary
  s.author = "Sharon Rosner"
  s.email = 'ciconia@gmail.com'
  s.homepage = 'http://sequel.rubyforge.org'
  s.executables = ['sequel']

  s.add_dependency('metaid')
  s.add_dependency('ParseTree')
  s.add_dependency('ruby2ruby')
  
  s.required_ruby_version = '>= 1.8.4'

  s.files = %w(COPYING README Rakefile) + Dir.glob("{bin,doc,spec,lib}/**/*")
      
  s.require_path = "lib"
  s.bindir = "bin"
end

win_spec = Gem::Specification.new do |s|
  s.name = NAME
  s.version = VERS
  s.platform = Gem::Platform::WIN32
  s.has_rdoc = true
  s.extra_rdoc_files = ["README", "CHANGELOG", "COPYING"]
  s.rdoc_options += RDOC_OPTS + 
    ['--exclude', '^(examples|extras)\/', '--exclude', 'lib/sequel.rb']
  s.summary = "Lightweight ORM library for Ruby"
  s.description = s.summary
  s.author = "Sharon Rosner"
  s.email = 'ciconia@gmail.com'
  s.homepage = 'http://sequel.rubyforge.org'
  s.executables = ['sequel']

  s.add_dependency('metaid')
  
  s.required_ruby_version = '>= 1.8.4'

  s.files = %w(COPYING README Rakefile) + Dir.glob("{bin,doc,spec,lib}/**/*")
      
  s.require_path = "lib"
  s.bindir = "bin"
end

Rake::GemPackageTask.new(spec) do |p|
  p.need_tar = true
  p.gem_spec = spec
end

Rake::GemPackageTask.new(win_spec) do |p|
  p.need_tar = true
  p.gem_spec = win_spec
end

task :install do
  sh %{rake package}
  sh %{sudo gem install pkg/#{NAME}-#{VERS}}
end

task :install_no_docs do
  sh %{rake package}
  sh %{sudo gem install pkg/#{NAME}-#{VERS} --no-rdoc --no-ri}
end

task :uninstall => [:clean] do
  sh %{sudo gem uninstall #{NAME}}
end

desc 'Update docs and upload to rubyforge.org'
task :doc_rforge do
  sh %{rake doc}
  sh %{scp -r doc/rdoc/* ciconia@rubyforge.org:/var/www/gforge-projects/sequel}
end

require 'spec/rake/spectask'

desc "Run specs with coverage"
Spec::Rake::SpecTask.new('spec') do |t|
  t.spec_files = FileList['spec/*_spec.rb']
  t.rcov = true
end

desc "Run specs without coverage"
Spec::Rake::SpecTask.new('spec_no_cov') do |t|
  t.spec_files = FileList['spec/*_spec.rb']
end

desc "Run adapter specs without coverage"
Spec::Rake::SpecTask.new('spec_adapters') do |t|
  t.spec_files = FileList['spec/adapters/*_spec.rb']
end

desc "Run all specs with coverage"
Spec::Rake::SpecTask.new('spec_all') do |t|
  t.spec_files = FileList['spec/*_spec.rb', 'spec/adapters/*_spec.rb']
  t.rcov = true
end

##############################################################################
# Statistics
##############################################################################

STATS_DIRECTORIES = [
  %w(Code   lib/),
  %w(Spec   spec/)
].collect { |name, dir| [ name, "./#{dir}" ] }.select { |name, dir| File.directory?(dir) }

desc "Report code statistics (KLOCs, etc) from the application"
task :stats do
  require 'extra/stats'
  verbose = true
  CodeStatistics.new(*STATS_DIRECTORIES).to_s
end

