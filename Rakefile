require "rake"
require "rake/clean"
require "rake/gempackagetask"
require "rake/rdoctask"
require "fileutils"

include FileUtils

PROJECTS = %w{core model}

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
task :spec do
  sh_with_each_project "rake spec"
end

task :spec_no_cov do
  sh_with_each_project "rake spec_no_cov"
end

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
  rdoc.main = "core/README"
  rdoc.title = "Sequel: Lightweight ORM for Ruby"
  rdoc.rdoc_files.add ["sequel/README", "sequel/COPYING", 
    "core/lib/sequel_core.rb", "core/lib/**/*.rb",
    "model/lib/sequel_model.rb", "model/lib/**/*.rb",
  ]
end

task :doc_rforge => [:doc]

desc "Update docs and upload to rubyforge.org"
task :doc_rforge do
  # sh %{rake doc}
  sh %{scp -r doc/rdoc/* ciconia@rubyforge.org:/var/www/gforge-projects/sequel}
end

