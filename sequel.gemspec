require File.expand_path("../lib/sequel/version", __FILE__)
SEQUEL_GEMSPEC = Gem::Specification.new do |s|
  s.name = 'sequel'
  s.version = Sequel.version
  s.platform = Gem::Platform::RUBY
  s.has_rdoc = true
  s.extra_rdoc_files = ["README.rdoc", "CHANGELOG", "MIT-LICENSE"] + Dir["doc/*.rdoc"] + Dir['doc/release_notes/*.txt']
  s.rdoc_options += ["--quiet", "--line-numbers", "--inline-source", '--title', 'Sequel: The Database Toolkit for Ruby', '--main', 'README.rdoc']
  s.summary = "The Database Toolkit for Ruby"
  s.description = s.summary
  s.author = "Jeremy Evans"
  s.email = "code@jeremyevans.net"
  s.homepage = "http://sequel.jeremyevans.net"
  s.license = 'MIT'
  s.required_ruby_version = ">= 1.8.7"
  s.files = %w(MIT-LICENSE CHANGELOG README.rdoc Rakefile bin/sequel) + Dir["doc/**/*.{rdoc,txt}"] + Dir["{spec,lib}/**/*.{rb,RB}"]
  s.require_path = "lib"
  s.bindir = 'bin'
  s.executables << 'sequel'
  s.add_development_dependency "minitest", '>=5.7.0'
  s.add_development_dependency "minitest-hooks"
  s.add_development_dependency "minitest-shared_description"
  s.add_development_dependency "tzinfo"
  s.add_development_dependency "activemodel"
  s.add_development_dependency "nokogiri"
end
