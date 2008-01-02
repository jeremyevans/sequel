require 'benchmark'
require 'rubygems'
require File.join(File.dirname(__FILE__), '../lib/sequel_core/adapters/sqlite')

DB = Sequel.sqlite

N = 10_000

Benchmark::bm(20) do |x|
  x.report('from') do
    t = DB[:test]
    N.times { t.sql }
  end

  x.report('order') do
    t = DB[:test].order(:stamp)
    N.times { t.sql }
  end

  x.report('filter') do
    t = DB[:test].filter {:stamp < (Time.now - 3600)}
    N.times { t.sql }
  end

  x.report('filter.order') do
    t = DB[:test].filter {:stamp < (Time.now - 3600)}.order(:stamp)
    N.times { t.sql }
  end
end