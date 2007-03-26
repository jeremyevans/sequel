require File.join(File.dirname(__FILE__), '../lib/sequel/sqlite')

# Let's open an in-memory database
DB = Sequel.open 'sqlite:/:memory:'

# Create a new table
DB.create_table :items do
  primary_key :id, :integer, :auto_increment => true
  column :name, :text
  column :price, :float
end

# Create a dataset
items = DB[:items]

# Populate the table
items << {:name => 'abc', :price => rand * 100}
items << {:name => 'def', :price => rand * 100}
items << {:name => 'ghi', :price => rand * 100}

# Print out the number of records
puts "Item count: #{items.count}"

# Print out the records
items.each {|i| puts "#{i[:name]} costs #{i[:price]}"}

# Print out the average price
puts "The average price is: #{items.avg(:price)}"