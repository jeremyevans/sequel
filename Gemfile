source 'https://rubygems.org'

# Specify your gem's dependencies in sequel-fdbsql-adapter.gemspec
gemspec

# When doing bundle install on this repository it will use the fork
# requiring the adapter gem will include the sequel gem
# even if I had put this under group :test it would install
# you can point this to fdbsql-4.xx.0 for the different versions
# the fdbsql branch points to master + the changes
gem 'sequel', :git => 'https://github.com/ScottDugas/sequel', :branch => 'fdbsql-jdbc'
