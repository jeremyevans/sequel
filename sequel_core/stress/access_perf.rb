require 'benchmark'
require 'rubygems'
require File.join(File.dirname(__FILE__), '../lib/sequel_core/adapters/sqlite')

DB = Sequel.sqlite

DB.create_table :items do
  text :name
  integer :price
end

N = 10_000

N.times {DB[:items] << {:name => rand(10000).to_s, :price => rand(10000)}}

# DB[:items].print

Benchmark::bmbm(20) do |x|
  x.report('access') do
    DB[:items].each {|r| r[:name]; r[:price]}
  end
end
