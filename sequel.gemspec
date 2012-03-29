require File.expand_path("../lib/sequel/version", __FILE__)
SEQUEL_GEMSPEC = Gem::Specification.new do |s|
  s.name = 'sequel'
  s.rubyforge_project = 'sequel'
  s.version = Sequel.version
  s.platform = Gem::Platform::RUBY
  s.has_rdoc = true
  s.extra_rdoc_files = ["README.rdoc", "CHANGELOG", "MIT-LICENSE"] + Dir["doc/*.rdoc"] + Dir['doc/release_notes/*.txt']
  s.rdoc_options += ["--quiet", "--line-numbers", "--inline-source", '--title', 'Sequel: The Database Toolkit for Ruby', '--main', 'README.rdoc']
  s.summary = "The Database Toolkit for Ruby"
  s.description = s.summary
  s.author = "Jeremy Evans"
  s.email = "code@jeremyevans.net"
  s.homepage = "http://sequel.rubyforge.org"
  s.required_ruby_version = ">= 1.8.7"
  s.files = %w(MIT-LICENSE CHANGELOG README.rdoc Rakefile) + Dir["{bin,doc,spec,lib}/**/*"]
  s.require_path = "lib"
  s.bindir = 'bin'
  s.executables << 'sequel'
end
