#
# FoundationDB SQL Layer Sequel Adapter
# Copyright (c) 2013-2014 FoundationDB, LLC
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#

version = File.read(File.expand_path('../VERSION', __FILE__)).strip

FDBSQL_GEMSPEC = Gem::Specification.new do |s|
  s.name         = 'sequel-fdbsql-adapter'
  s.version      = version
  s.date         = Time.new.strftime '%Y-%m-%d'
  s.summary      = "Sequel Adapter for the FoundationDB SQL Layer"
  s.description  = <<-EOF
Sequel Adapter for the FoundationDB SQL Layer.

Complete documentation of the FoundationDB SQL Layer can be found at:
https://foundationdb.com/layers/sql/
EOF
  s.authors      = ["FoundationDB"]
  s.email        = 'distribution@foundationdb.com'
  s.files        = Dir['LICENSE', 'README.md', 'VERSION', 'lib/**/*']
  s.homepage     = 'https://github.com/FoundationDB/sql-layer-adapter-sequel'
  s.license      = 'MIT'
  s.platform     = Gem::Platform::RUBY

  s.requirements = 'FoundationDB SQL Layer version 1.9.6'
  s.required_ruby_version = '>= 1.9.3'

  s.add_dependency 'sequel', '~> 4.12'
  s.add_dependency 'pg', '~> 0.17'

  s.add_development_dependency "rake", ">= 10"
  s.add_development_dependency 'rspec', '~> 2.14', '<2.99.0'
end
