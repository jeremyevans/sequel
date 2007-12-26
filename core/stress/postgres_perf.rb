require 'benchmark'
require 'rubygems'
# require 'fastthread'
require File.join(File.dirname(__FILE__), '../lib/sequel/postgres')

DB = Sequel('postgres://postgres:postgres@localhost:5432/reality_development')

N = 100

Benchmark::bmbm(20) do |x|
  x.report('postgres read') do
    N.times do
      DB[:timeline].each {|t| [t[:stamp], t[:kind], t[:node_id]]}
    end
  end
end
