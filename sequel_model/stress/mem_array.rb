require File.join(File.dirname(__FILE__), '../lib/sequel/array_keys')

N = 100_000

puts "pid = #{Process.pid}"

keys = [:a, :b, :c, :d, :e]

rows = []

N.times do
  values = [rand] * 5
  rows << values
end

loop {sleep 1}
