require 'rubygems'
require 'sequel/sqlite' # gem install sequel (and sqlite3 as well)

DB = Sequel.open 'sqlite:/' # memory DB

DB.create_table :items do
  text :name
  decimal :price
end

items = DB[:items]

1000.times {|i| items << {:name => "item#{i}", :price => rand * 100}}

puts "#{items.count} total items"

puts "Average price: #{items.avg(:price)}"

puts "3 most expensive items:"
items.order(:price.DESC).limit(3).print(:name, :price)

puts "#{items.filter {price < items.avg(:price)}.count} below the average"
#{}

puts "Changing price for expensive items"
items.filter {price > items.avg(:price)}.update(:price => 'price + 10'.expr)

puts "Highest price: #{items.max(:price)}"

puts "Updated average price: #{items.avg(:price)}"