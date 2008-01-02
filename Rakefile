require "rake"
require "rake/clean"
require "rake/gempackagetask"
require "rake/rdoctask"
require "fileutils"

include FileUtils

PROJECTS = %w{sequel_core sequel_model}

def with_each_project
  PROJECTS.each do |p|
    FileUtils.cd(p)
    begin
      yield p
    ensure
      FileUtils.cd('..')
    end
  end
end

def sh_with_each_project(cmd)
  with_each_project {sh cmd rescue nil}
end

##############################################################################
# gem packaging and release
##############################################################################
desc "Packages up Sequel and Sequel Model."
task :default => [:package]
task :package => [:clean]
task :doc => [:rdoc]

task :package do
  sh_with_each_project "rake package"
end

task :install do
  sh_with_each_project "rake install"
end

task :install_no_docs do
  sh_with_each_project "rake install_no_docs"
end

task :uninstall => [:clean] do
  sh_with_each_project "rake uninstall"
end

task :tag do
  sh_with_each_project "rake tag"
end

##############################################################################
# rspec
##############################################################################
# task :spec do 
#   sh_with_each_project "rake spec"
# end
# 
# task :spec_no_cov do
#   sh_with_each_project "rake spec_no_cov"
# end
# 
##############################################################################
# rdoc
##############################################################################
RDOC_OPTS = [
  "--quiet", 
  "--title", "Sequel Model: Lightweight ORM for Ruby",
  "--opname", "index.html",
  "--line-numbers", 
  "--main", "sequel/README",
  "--inline-source"
]

Rake::RDocTask.new do |rdoc|
  rdoc.rdoc_dir = "sequel/doc/rdoc"
  rdoc.options += RDOC_OPTS
  rdoc.main = "sequel/README"
  rdoc.title = "Sequel: Lightweight ORM for Ruby"
  rdoc.rdoc_files.add ["sequel/README", "sequel/COPYING", 
    "sequel_core/lib/sequel_core.rb", "sequel_core/lib/**/*.rb",
    "sequel_model/lib/sequel_model.rb", "sequel_model/lib/**/*.rb",
  ]
end

task :doc_rforge => [:doc]

desc "Update docs and upload to rubyforge.org"
task :doc_rforge do
  # sh %{rake doc}
  sh %{scp -r sequel/doc/rdoc/* ciconia@rubyforge.org:/var/www/gforge-projects/sequel}
end

##############################################################################
# specs
##############################################################################
require "spec/rake/spectask"

desc "Run specs with coverage"
Spec::Rake::SpecTask.new("spec") do |t|
  t.spec_files = FileList["sequel_core/spec/*_spec.rb", "sequel_model/spec/*_spec.rb"]
  t.spec_opts  = File.read("sequel_core/spec/spec.opts").split("\n")
  t.rcov_opts  = File.read("sequel_core/spec/rcov.opts").split("\n")
  t.rcov = true
end

desc "Run specs without coverage"
Spec::Rake::SpecTask.new("spec_no_cov") do |t|
  t.spec_files = FileList["sequel_core/spec/*_spec.rb", "sequel_model/spec/*_spec.rb"]
  t.spec_opts  = File.read("sequel_core/spec/spec.opts").split("\n")
end

##############################################################################
# Statistics
##############################################################################

STATS_DIRECTORIES = [
  %w(core_code   sequel_core/lib/),
  %w(core_spec   sequel_core/spec/),
  %w(model_code  sequel_model/lib/),
  %w(model_spec  sequel_model/spec/)
].collect { |name, dir| [ name, "./#{dir}" ] }.select { |name, dir| File.directory?(dir) }

desc "Report code statistics (KLOCs, etc) from the application"
task :stats do
  require "sequel_core/extra/stats"
  verbose = true
  CodeStatistics.new(*STATS_DIRECTORIES).to_s
end

