require 'rubygems'
require File.join(File.dirname(__FILE__), '../lib/sequel/sqlite')

DB = Sequel.sqlite
DS = DB[:t]

N = 10_000
require 'profile'

N.times {DS.filter {:x == 100}.sql}