require 'sequel/sqlite'

DB = Sequel.open 'sqlite:///countries.db'
countries = DB[:countries]

# select name, region and popuplation
countries.select(:name, :region, :population).all

# show the name for the countries that have a population of 
# at least 200 million.
large_populations = countries.filter {population >= 200_000_000}
large_populations.map(:name)

# Give the name and the per capita GDP for those countries 
# with a population of at least 200 million.
large_populations.hash_map(:name, :gdp)

# Show the name and population in millions for the countries of Asia
countries.filter(:region => 'Asia').select(:name, 'population/1000000').all

# Show the name and population for France, Germany, Italy
countries.filter(:name => ['France', 'Germany', 'Italy']).hash_map(:name, :population)

