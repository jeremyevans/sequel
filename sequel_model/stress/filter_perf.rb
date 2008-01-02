  require 'benchmark'
  require 'rubygems'
  require File.join(File.dirname(__FILE__), '../lib/sequel/sqlite')

  DB = Sequel.sqlite
  DS = DB[:t]

  N = 10_000

  Benchmark::bm(6) do |x|
    x.report('hash') do
      N.times {DS.filter(:x => 100, :y => 200).sql}
    end
  
    x.report('param') do
      N.times {DS.filter('(x = ?) AND (y = ?)', 200).sql}
    end
  
    x.report('string') do
      N.times {DS.filter('(x = 100) AND (y = 200)').sql}
    end

    x.report('proc') do
      N.times {DS.filter {:x == 100 && :y == 200}.sql}
    end
  end