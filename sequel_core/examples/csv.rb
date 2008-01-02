require 'rubygems'
require 'faster_csv'
require File.join(File.dirname(__FILE__), '../lib/sequel_core')

DB = Sequel.open 'sqlite:///test.db'
DB.create_table :countries do
  column :name, :text
  column :population, :integer
end unless DB.table_exists?(:countries)

FCSV.foreach('/home/sharon/reality/server/trunk/test.csv', 
  :headers => true, :header_converters => :symbol) do |l|
  DB[:countries] << l.to_hash
end

DB[:countries].print(:name, :population)