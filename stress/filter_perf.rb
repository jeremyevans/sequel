require 'benchmark'
require 'rubygems'
require File.join(File.dirname(__FILE__), '../lib/sequel/sqlite')

DB = Sequel.sqlite
DS = DB[:t]

N = 10_000

Benchmark::bm(24) do |x|
  x.report('hash filter') do
    N.times {DS.filter(:x => 100).sql}
  end
  
  x.report('parameterized filter') do
    N.times {DS.filter('x = ?', 100).sql}
  end
  
  x.report('string filter') do
    N.times {DS.filter('x = 100').sql}
  end

  x.report('proc filter') do
    N.times {DS.filter {:x == 100}.sql}
  end
end