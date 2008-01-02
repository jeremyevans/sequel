require 'rubygems'
require File.join(File.dirname(__FILE__), '../lib/sequel_core')

db = Sequel.open("sqlite:/:memory:")
db << "create table k1 (id integer primary key autoincrement, f1 text)"
db << "create table k2 (id integer primary key autoincrement, f2 text)"
db << "create table k3 (id integer primary key autoincrement, f3 text)"
db << "create table records (id integer primary key autoincrement,
	k1_id integer, k2_id integer, k3_id integer, value text)"
db << "create unique index records_key_unique on records(k1_id,k2_id,k3_id)"
db << "create view data as select records.id as id, k1.f1 as f1, k2.f2 as f2,
	k3.f3 as f3, records.value as value
	from records inner join k1 on records.k1_id = k1.id
	inner join k2 on records.k2_id = k2.id
	inner join k3 on records.k3_id = k3.id
	order by k1.f1, k2.f2, k3.f3"
k1 = db[:k1]
k1 << [1, 'Alfred']
k1 << [2,'Barry']
k1 << [3, 'Charles']
k1 << [4,'Dave']
k1 << [5,'Douglas']
k2 = db[:k2]
k2 << [1,'USA']
k2 << [2,'Japan']
k2 << [3,'Brazil']
k3 = db[:k3]
k3 << [1,'APL']
k3 << [2,'BASIC']
k3 << [3,'COBOL']
k3 << [4,'Ruby']
records = db[:records]
records << [1,1,1,1,'Red']
records << [2,2,2,2,'Yellow']
records << [3,3,3,3,'Green']
records << [4,4,1,4,'Magenta']
records << [5,5,2,4,'Blue']
data = db[:data].filter(:f1 => ['Dave','Douglas'])
puts data.sql
data.print(:id, :f1, :f2, :f3, :value)
