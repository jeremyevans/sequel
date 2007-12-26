require "rake"
require "rake/clean"
require "rake/gempackagetask"
require "rake/rdoctask"
require "fileutils"

include FileUtils

PROJECTS = FileList["*"].reject {|f| !File.directory?(f) || f =~ /model_plugins/}

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

desc "Packages up Sequel and Sequel Model."
task :default => [:package]
task :package => [:clean]
task :doc => [:rdoc]

task :package do
  with_each_project {sh "rake package"}
end

task :install do
  with_each_project {sh "rake install"}
end

task :install_no_docs do
  with_each_project {sh "rake install_no_docs"}
end

task :uninstall => [:clean] do
  with_each_project {sh "rake uninstall"}
end

task :spec do
  with_each_project {sh "rake spec"}
end

task :spec_no_cov do
  with_each_project {sh "rake spec_no_cov"}
end
