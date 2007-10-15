require File.join(File.dirname(__FILE__), '../lib/sequel/array_keys')
require 'benchmark'

N = 100_000

puts "pid = #{Process.pid}"

Benchmark::bm(25) do |x|
  keys = [:a, :b, :c, :d, :e]

  rows = []

  x.report('create hash') do
    N.times do
      values = [rand] * 5
      rows << keys.inject({}) {|m, k| m[k] = values.shift; m}
    end
  end
  
  GC.start; sleep 5;

  rows = []

  x.report('create array with keys') do
    N.times do
      values = [rand] * 5
      values.keys = keys
      rows << values
    end
  end

  GC.start; sleep 5;

  rows = []

  hashes = [{:a => 1, :b => 2, :c => 3, :d => 4, :e => 5}] * N
  values = [rand] * 5
  values.keys = keys
  arrays = [values] * N
  
  x.report('access hash') do
    hashes.each {|h| h[:a]}
  end

  x.report('access array with keys') do
    arrays.each {|a| a[:a]}
  end
end

