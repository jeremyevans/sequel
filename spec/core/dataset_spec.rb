require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Dataset" do
  before do
    @dataset = Sequel.mock.dataset
  end
  
  it "should accept database in initialize" do
    db = "db"
    d = Sequel::Dataset.new(db)
    d.db.must_be_same_as(db)
    d.opts.must_equal({})
  end
  
  it "should provide clone for chainability" do
    d1 = @dataset.clone(:from => [:test])
    d1.class.must_equal @dataset.class
    d1.wont_equal @dataset
    d1.db.must_be_same_as(@dataset.db)
    d1.opts[:from].must_equal [:test]
    @dataset.opts[:from].must_equal nil
    
    d2 = d1.clone(:order => [:name])
    d2.class.must_equal @dataset.class
    d2.wont_equal d1
    d2.wont_equal @dataset
    d2.db.must_be_same_as(@dataset.db)
    d2.opts[:from].must_equal [:test]
    d2.opts[:order].must_equal [:name]
    d1.opts[:order].must_equal nil
  end
  
  it "should include Enumerable" do
    Sequel::Dataset.included_modules.must_include(Enumerable)
  end
  
  it "should yield rows to each" do
    ds = Sequel.mock[:t]
    ds._fetch = {:x=>1}
    called = false
    ds.each{|a| called = true; a.must_equal(:x=>1)}
    called.must_equal true
  end
  
  it "should get quote_identifiers default from database" do
    db = Sequel::Database.new(:quote_identifiers=>true)
    db[:a].quote_identifiers?.must_equal true
    db = Sequel::Database.new(:quote_identifiers=>false)
    db[:a].quote_identifiers?.must_equal false
  end

  it "should get identifier_input_method default from database" do
    db = Sequel::Database.new(:identifier_input_method=>:upcase)
    db[:a].identifier_input_method.must_equal :upcase
    db = Sequel::Database.new(:identifier_input_method=>:downcase)
    db[:a].identifier_input_method.must_equal :downcase
  end

  it "should get identifier_output_method default from database" do
    db = Sequel::Database.new(:identifier_output_method=>:upcase)
    db[:a].identifier_output_method.must_equal :upcase
    db = Sequel::Database.new(:identifier_output_method=>:downcase)
    db[:a].identifier_output_method.must_equal :downcase
  end
  
  it "should have quote_identifiers= method which changes literalization of identifiers" do
    @dataset.quote_identifiers = true
    @dataset.literal(:a).must_equal '"a"'
    @dataset.quote_identifiers = false
    @dataset.literal(:a).must_equal 'a'
  end
  
  it "should have identifier_input_method= method which changes literalization of identifiers" do
    @dataset.identifier_input_method = :upcase
    @dataset.literal(:a).must_equal 'A'
    @dataset.identifier_input_method = :downcase
    @dataset.literal(:A).must_equal 'a'
    @dataset.identifier_input_method = :reverse
    @dataset.literal(:at_b).must_equal 'b_ta'
  end
  
  it "should have identifier_output_method= method which changes identifiers returned from the database" do
    @dataset.send(:output_identifier, "at_b_C").must_equal :at_b_C
    @dataset.identifier_output_method = :upcase
    @dataset.send(:output_identifier, "at_b_C").must_equal :AT_B_C
    @dataset.identifier_output_method = :downcase
    @dataset.send(:output_identifier, "at_b_C").must_equal :at_b_c
    @dataset.identifier_output_method = :reverse
    @dataset.send(:output_identifier, "at_b_C").must_equal :C_b_ta
  end
  
  it "should have output_identifier handle empty identifiers" do
    @dataset.send(:output_identifier, "").must_equal :untitled
    @dataset.identifier_output_method = :upcase
    @dataset.send(:output_identifier, "").must_equal :UNTITLED
    @dataset.identifier_output_method = :downcase
    @dataset.send(:output_identifier, "").must_equal :untitled
    @dataset.identifier_output_method = :reverse
    @dataset.send(:output_identifier, "").must_equal :deltitnu
  end
end

describe "Dataset#clone" do
  before do
    @dataset = Sequel.mock.dataset.from(:items)
  end
  
  it "should create an exact copy of the dataset" do
    @dataset.row_proc = Proc.new{|r| r}
    clone = @dataset.clone

    clone.object_id.wont_equal @dataset.object_id
    clone.class.must_equal @dataset.class
    clone.db.must_equal @dataset.db
    clone.opts.must_equal @dataset.opts
    clone.row_proc.must_equal @dataset.row_proc
  end
  
  it "should copy the dataset opts" do
    clone = @dataset.clone

    clone.opts.must_equal @dataset.opts
    @dataset.filter!(:a => 'b')
    clone.opts[:filter].must_equal nil

    clone = @dataset.clone(:from => [:other])
    @dataset.opts[:from].must_equal [:items]
    clone.opts[:from].must_equal [:other]
  end
  
  it "should merge the specified options" do
    clone = @dataset.clone(1 => 2)
    clone.opts.must_equal(1 => 2, :from => [:items])
  end
  
  it "should overwrite existing options" do
    clone = @dataset.clone(:from => [:other])
    clone.opts.must_equal(:from => [:other])
  end
  
  it "should return an object with the same modules included" do
    m = Module.new do
      def __xyz__; "xyz"; end
    end
    @dataset.extend(m)
    @dataset.clone({}).must_respond_to(:__xyz__)
  end
end

describe "Dataset#==" do
  before do
    @db = Sequel.mock
    @h = {}
  end
  
  it "should be the true for dataset with the same db, opts, and SQL" do
    @db[:t].must_equal @db[:t]
  end

  it "should be different for datasets with different dbs" do
    @db[:t].wont_equal Sequel.mock[:t]
  end
  
  it "should be different for datasets with different opts" do
    @db[:t].wont_equal @db[:t].clone(:blah=>1)
  end
  
  it "should be different for datasets with different SQL" do
    ds = @db[:t]
    ds.quote_identifiers = true
    ds.wont_equal @db[:t]
  end
end

describe "Dataset#hash" do
  before do
    @db = Sequel.mock
    @h = {}
  end
  
  it "should be the same for dataset with the same db, opts, and SQL" do
    @db[:t].hash.must_equal @db[:t].hash
    @h[@db[:t]] = 1
    @h[@db[:t]].must_equal 1
  end

  it "should be different for datasets with different dbs" do
    @db[:t].hash.wont_equal Sequel.mock[:t].hash
  end
  
  it "should be different for datasets with different opts" do
    @db[:t].hash.wont_equal @db[:t].clone(:blah=>1).hash
  end
  
  it "should be different for datasets with different SQL" do
    ds = @db[:t]
    ds.quote_identifiers = true
    ds.hash.wont_equal @db[:t].hash
  end
end

describe "A simple dataset" do
  before do
    @dataset = Sequel.mock.dataset.from(:test)
  end
  
  it "should format a select statement" do
    @dataset.select_sql.must_equal 'SELECT * FROM test'
  end
  
  it "should format a delete statement" do
    @dataset.delete_sql.must_equal 'DELETE FROM test'
  end
  
  it "should format a truncate statement" do
    @dataset.truncate_sql.must_equal 'TRUNCATE TABLE test'
  end
  
  it "should format a truncate statement with multiple tables if supported" do
    meta_def(@dataset, :check_truncation_allowed!){}
    @dataset.from(:test, :test2).truncate_sql.must_equal 'TRUNCATE TABLE test, test2'
  end
  
  it "should format an insert statement with default values" do
    @dataset.insert_sql.must_equal 'INSERT INTO test DEFAULT VALUES'
  end
  
  it "should use a single column with a default value when the dataset doesn't support using insert statement with default values" do
    meta_def(@dataset, :insert_supports_empty_values?){false}
    meta_def(@dataset, :columns){[:a, :b]}
    @dataset.insert_sql.must_equal 'INSERT INTO test (b) VALUES (DEFAULT)'
  end
  
  it "should format an insert statement with hash" do
    @dataset.insert_sql(:name => 'wxyz', :price => 342).
      must_match(/INSERT INTO test \(name, price\) VALUES \('wxyz', 342\)|INSERT INTO test \(price, name\) VALUES \(342, 'wxyz'\)/)

      @dataset.insert_sql({}).must_equal "INSERT INTO test DEFAULT VALUES"
  end

  it "should format an insert statement with string keys" do
    @dataset.insert_sql('name' => 'wxyz', 'price' => 342).
      must_match(/INSERT INTO test \(name, price\) VALUES \('wxyz', 342\)|INSERT INTO test \(price, name\) VALUES \(342, 'wxyz'\)/)
  end
  
  it "should format an insert statement with an arbitrary value" do
    @dataset.insert_sql(123).must_equal "INSERT INTO test VALUES (123)"
  end
  
  it "should format an insert statement with sub-query" do
    @dataset.insert_sql(@dataset.from(:something).filter(:x => 2)).must_equal "INSERT INTO test SELECT * FROM something WHERE (x = 2)"
  end
  
  it "should format an insert statement with array" do
    @dataset.insert_sql('a', 2, 6.5).must_equal "INSERT INTO test VALUES ('a', 2, 6.5)"
  end
  
  it "should format an update statement" do
    @dataset.update_sql(:name => 'abc').must_equal "UPDATE test SET name = 'abc'"
  end

  it "should be able to return rows for arbitrary SQL" do
    @dataset.clone(:sql => 'xxx yyy zzz').select_sql.must_equal "xxx yyy zzz"
  end

  it "should use the :sql option for all sql methods" do
    sql = "X"
    ds = @dataset.clone(:sql=>sql)
    ds.sql.must_equal sql
    ds.select_sql.must_equal sql
    ds.insert_sql.must_equal sql
    ds.delete_sql.must_equal sql
    ds.update_sql.must_equal sql
    ds.truncate_sql.must_equal sql
  end
end

describe "A dataset with multiple tables in its FROM clause" do
  before do
    @dataset = Sequel.mock.dataset.from(:t1, :t2)
  end

  it "should raise on #update_sql" do
    proc {@dataset.update_sql(:a=>1)}.must_raise(Sequel::InvalidOperation)
  end

  it "should raise on #delete_sql" do
    proc {@dataset.delete_sql}.must_raise(Sequel::InvalidOperation)
  end
  
  it "should raise on #truncate_sql" do
    proc {@dataset.truncate_sql}.must_raise(Sequel::InvalidOperation)
  end

  it "should raise on #insert_sql" do
    proc {@dataset.insert_sql}.must_raise(Sequel::InvalidOperation)
  end

  it "should generate a select query FROM all specified tables" do
    @dataset.select_sql.must_equal "SELECT * FROM t1, t2"
  end
end

describe "Dataset#unused_table_alias" do
  before do
    @ds = Sequel.mock.dataset.from(:test)
  end
  
  it "should return given symbol if it hasn't already been used" do
    @ds.unused_table_alias(:blah).must_equal :blah
  end

  it "should return a symbol specifying an alias that hasn't already been used if it has already been used" do
    @ds.unused_table_alias(:test).must_equal :test_0
    @ds.from(:test, :test_0).unused_table_alias(:test).must_equal :test_1
    @ds.from(:test, :test_0).cross_join(:test_1).unused_table_alias(:test).must_equal :test_2
  end

  it "should return an appropriate symbol if given other forms of identifiers" do
    @ds.unused_table_alias('test').must_equal :test_0
    @ds.unused_table_alias(:b__t___test).must_equal :test_0
    @ds.unused_table_alias(:b__test).must_equal :test_0
    @ds.unused_table_alias(Sequel.qualify(:b, :test)).must_equal :test_0
    @ds.unused_table_alias(Sequel.expr(:b).as(:test)).must_equal :test_0
    @ds.unused_table_alias(Sequel.expr(:b).as(Sequel.identifier(:test))).must_equal :test_0
    @ds.unused_table_alias(Sequel.expr(:b).as('test')).must_equal :test_0
    @ds.unused_table_alias(Sequel.identifier(:test)).must_equal :test_0
  end
end

describe "Dataset#exists" do
  before do
    @ds1 = Sequel.mock[:test]
    @ds2 = @ds1.filter(Sequel.expr(:price) < 100)
    @ds3 = @ds1.filter(Sequel.expr(:price) > 50)
  end
  
  it "should work in filters" do
    @ds1.filter(@ds2.exists).sql.
      must_equal 'SELECT * FROM test WHERE (EXISTS (SELECT * FROM test WHERE (price < 100)))'
    @ds1.filter(@ds2.exists & @ds3.exists).sql.
      must_equal 'SELECT * FROM test WHERE ((EXISTS (SELECT * FROM test WHERE (price < 100))) AND (EXISTS (SELECT * FROM test WHERE (price > 50))))'
  end

  it "should work in select" do
    @ds1.select(@ds2.exists.as(:a), @ds3.exists.as(:b)).sql.
      must_equal 'SELECT (EXISTS (SELECT * FROM test WHERE (price < 100))) AS a, (EXISTS (SELECT * FROM test WHERE (price > 50))) AS b FROM test'
  end
end

describe "Dataset#where" do
  before do
    @dataset = Sequel.mock[:test]
    @d1 = @dataset.where(:region => 'Asia')
    @d2 = @dataset.where('region = ?', 'Asia')
    @d3 = @dataset.where("a = 1")
  end
  
  it "should just clone if given an empty argument" do
    @dataset.where({}).sql.must_equal @dataset.sql
    @dataset.where([]).sql.must_equal @dataset.sql
    @dataset.where('').sql.must_equal @dataset.sql

    @dataset.filter({}).sql.must_equal @dataset.sql
    @dataset.filter([]).sql.must_equal @dataset.sql
    @dataset.filter('').sql.must_equal @dataset.sql
  end
  
  it "should work with hashes" do
    @dataset.where(:name => 'xyz', :price => 342).select_sql.
      must_match(/WHERE \(\(name = 'xyz'\) AND \(price = 342\)\)|WHERE \(\(price = 342\) AND \(name = 'xyz'\)\)/)
  end
  
  it "should work with a string with placeholders and arguments for those placeholders" do
    @dataset.where('price < ? AND id in ?', 100, [1, 2, 3]).select_sql.must_equal "SELECT * FROM test WHERE (price < 100 AND id in (1, 2, 3))"
  end
  
  it "should not modify passed array with placeholders" do
    a = ['price < ? AND id in ?', 100, 1, 2, 3]
    b = a.dup
    @dataset.where(a)
    b.must_equal a
  end

  it "should work with strings (custom SQL expressions)" do
    @dataset.where('(a = 1 AND b = 2)').select_sql.
      must_equal "SELECT * FROM test WHERE ((a = 1 AND b = 2))"
  end
    
  it "should work with a string with named placeholders and a hash of placeholder value arguments" do
    @dataset.where('price < :price AND id in :ids', :price=>100, :ids=>[1, 2, 3]).select_sql.
      must_equal "SELECT * FROM test WHERE (price < 100 AND id in (1, 2, 3))"
  end
    
  it "should not modify passed array with named placeholders" do
    a = ['price < :price AND id in :ids', {:price=>100}]
    b = a.dup
    @dataset.where(a)
    b.must_equal a
  end

  it "should not replace named placeholders that don't exist in the hash" do
    @dataset.where('price < :price AND id in :ids', :price=>100).select_sql.must_equal "SELECT * FROM test WHERE (price < 100 AND id in :ids)"
  end
  
  it "should raise an error for a mismatched number of placeholders" do
    proc{@dataset.where('price < ? AND id in ?', 100).select_sql}.must_raise(Sequel::Error)
    proc{@dataset.where('price < ? AND id in ?', 100, [1, 2, 3], 4).select_sql}.must_raise(Sequel::Error)
  end

  it "should handle placeholders when using an array" do
    @dataset.where(Sequel.lit(['price < ', ' AND id in '], 100, [1, 2, 3])).select_sql.must_equal "SELECT * FROM test WHERE price < 100 AND id in (1, 2, 3)"
    @dataset.where(Sequel.lit(['price < ', ' AND id in '], 100)).select_sql.must_equal "SELECT * FROM test WHERE price < 100 AND id in "
  end

  it "should handle a mismatched number of placeholders when using an array" do
    proc{@dataset.where(Sequel.lit(['a = ', ' AND price < ', ' AND id in '], 100)).select_sql}.must_raise(Sequel::Error)
    proc{@dataset.where(Sequel.lit(['price < ', ' AND id in '], 100, [1, 2, 3], 4)).select_sql}.must_raise(Sequel::Error)
  end
  
  it "should handle partial names" do
    @dataset.where('price < :price AND id = :p', :p=>2, :price=>100).select_sql.must_equal "SELECT * FROM test WHERE (price < 100 AND id = 2)"
  end

  it "should handle ::cast syntax when no parameters are supplied" do
    @dataset.where('price::float = 10', {}).select_sql.must_equal "SELECT * FROM test WHERE (price::float = 10)"
    @dataset.where('price::float ? 10', {}).select_sql.must_equal "SELECT * FROM test WHERE (price::float ? 10)"
  end

  it "should affect select, delete and update statements" do
    @d1.select_sql.must_equal "SELECT * FROM test WHERE (region = 'Asia')"
    @d1.delete_sql.must_equal "DELETE FROM test WHERE (region = 'Asia')"
    @d1.update_sql(:GDP => 0).must_equal "UPDATE test SET GDP = 0 WHERE (region = 'Asia')"
    
    @d2.select_sql.must_equal "SELECT * FROM test WHERE (region = 'Asia')"
    @d2.delete_sql.must_equal "DELETE FROM test WHERE (region = 'Asia')"
    @d2.update_sql(:GDP => 0).must_equal "UPDATE test SET GDP = 0 WHERE (region = 'Asia')"
    
    @d3.select_sql.must_equal "SELECT * FROM test WHERE (a = 1)"
    @d3.delete_sql.must_equal "DELETE FROM test WHERE (a = 1)"
    @d3.update_sql(:GDP => 0).must_equal "UPDATE test SET GDP = 0 WHERE (a = 1)"
  end
  
  it "should be composable using AND operator (for scoping)" do
    @d1.where(:size => 'big').select_sql.must_equal "SELECT * FROM test WHERE ((region = 'Asia') AND (size = 'big'))"
    @d1.where('population > 1000').select_sql.must_equal "SELECT * FROM test WHERE ((region = 'Asia') AND (population > 1000))"
    @d1.where('(a > 1) OR (b < 2)').select_sql.must_equal "SELECT * FROM test WHERE ((region = 'Asia') AND ((a > 1) OR (b < 2)))"
    @d1.where('GDP > ?', 1000).select_sql.must_equal "SELECT * FROM test WHERE ((region = 'Asia') AND (GDP > 1000))"
    @d2.where('GDP > ?', 1000).select_sql.must_equal "SELECT * FROM test WHERE ((region = 'Asia') AND (GDP > 1000))"
    @d2.where(:name => ['Japan', 'China']).select_sql.must_equal "SELECT * FROM test WHERE ((region = 'Asia') AND (name IN ('Japan', 'China')))"
    @d2.where('GDP > ?').select_sql.must_equal "SELECT * FROM test WHERE ((region = 'Asia') AND (GDP > ?))"
    @d3.where('b = 2').select_sql.must_equal "SELECT * FROM test WHERE ((a = 1) AND (b = 2))"
    @d3.where(:c => 3).select_sql.must_equal "SELECT * FROM test WHERE ((a = 1) AND (c = 3))"
    @d3.where('d = ?', 4).select_sql.must_equal "SELECT * FROM test WHERE ((a = 1) AND (d = 4))"
  end
      
  it "should be composable using AND operator (for scoping) with block" do
    @d3.where{e < 5}.select_sql.must_equal "SELECT * FROM test WHERE ((a = 1) AND (e < 5))"
  end
  
  it "should accept ranges" do
    @dataset.filter(:id => 4..7).sql.must_equal 'SELECT * FROM test WHERE ((id >= 4) AND (id <= 7))'
    @dataset.filter(:id => 4...7).sql.must_equal 'SELECT * FROM test WHERE ((id >= 4) AND (id < 7))'

    @dataset.filter(:table__id => 4..7).sql.must_equal 'SELECT * FROM test WHERE ((table.id >= 4) AND (table.id <= 7))'
    @dataset.filter(:table__id => 4...7).sql.must_equal 'SELECT * FROM test WHERE ((table.id >= 4) AND (table.id < 7))'
  end

  it "should accept nil" do
    @dataset.filter(:owner_id => nil).sql.must_equal 'SELECT * FROM test WHERE (owner_id IS NULL)'
  end

  it "should accept a subquery" do
    @dataset.filter('gdp > ?', @d1.select(Sequel.function(:avg, :gdp))).sql.must_equal "SELECT * FROM test WHERE (gdp > (SELECT avg(gdp) FROM test WHERE (region = 'Asia')))"
  end
  
  it "should handle all types of IN/NOT IN queries with empty arrays" do
    @dataset.filter(:id => []).sql.must_equal "SELECT * FROM test WHERE (1 = 0)"
    @dataset.filter([:id1, :id2] => []).sql.must_equal "SELECT * FROM test WHERE (1 = 0)"
    @dataset.exclude(:id => []).sql.must_equal "SELECT * FROM test WHERE (1 = 1)"
    @dataset.exclude([:id1, :id2] => []).sql.must_equal "SELECT * FROM test WHERE (1 = 1)"
  end

  it "should handle all types of IN/NOT IN queries" do
    @dataset.filter(:id => @d1.select(:id)).sql.must_equal "SELECT * FROM test WHERE (id IN (SELECT id FROM test WHERE (region = 'Asia')))"
    @dataset.filter(:id => [1, 2]).sql.must_equal "SELECT * FROM test WHERE (id IN (1, 2))"
    @dataset.filter([:id1, :id2] => @d1.select(:id1, :id2)).sql.must_equal "SELECT * FROM test WHERE ((id1, id2) IN (SELECT id1, id2 FROM test WHERE (region = 'Asia')))"
    @dataset.filter([:id1, :id2] => Sequel.value_list([[1, 2], [3,4]])).sql.must_equal "SELECT * FROM test WHERE ((id1, id2) IN ((1, 2), (3, 4)))"
    @dataset.filter([:id1, :id2] => [[1, 2], [3,4]]).sql.must_equal "SELECT * FROM test WHERE ((id1, id2) IN ((1, 2), (3, 4)))"

    @dataset.exclude(:id => @d1.select(:id)).sql.must_equal "SELECT * FROM test WHERE (id NOT IN (SELECT id FROM test WHERE (region = 'Asia')))"
    @dataset.exclude(:id => [1, 2]).sql.must_equal "SELECT * FROM test WHERE (id NOT IN (1, 2))"
    @dataset.exclude([:id1, :id2] => @d1.select(:id1, :id2)).sql.must_equal "SELECT * FROM test WHERE ((id1, id2) NOT IN (SELECT id1, id2 FROM test WHERE (region = 'Asia')))"
    @dataset.exclude([:id1, :id2] => Sequel.value_list([[1, 2], [3,4]])).sql.must_equal "SELECT * FROM test WHERE ((id1, id2) NOT IN ((1, 2), (3, 4)))"
    @dataset.exclude([:id1, :id2] => [[1, 2], [3,4]]).sql.must_equal "SELECT * FROM test WHERE ((id1, id2) NOT IN ((1, 2), (3, 4)))"
  end

  it "should handle IN/NOT IN queries with multiple columns and an array where the database doesn't support it" do
    meta_def(@dataset, :supports_multiple_column_in?){false}
    @dataset.filter([:id1, :id2] => [[1, 2], [3,4]]).sql.must_equal "SELECT * FROM test WHERE (((id1 = 1) AND (id2 = 2)) OR ((id1 = 3) AND (id2 = 4)))"
    @dataset.exclude([:id1, :id2] => [[1, 2], [3,4]]).sql.must_equal "SELECT * FROM test WHERE (((id1 != 1) OR (id2 != 2)) AND ((id1 != 3) OR (id2 != 4)))"
    @dataset.filter([:id1, :id2] => Sequel.value_list([[1, 2], [3,4]])).sql.must_equal "SELECT * FROM test WHERE (((id1 = 1) AND (id2 = 2)) OR ((id1 = 3) AND (id2 = 4)))"
    @dataset.exclude([:id1, :id2] => Sequel.value_list([[1, 2], [3,4]])).sql.must_equal "SELECT * FROM test WHERE (((id1 != 1) OR (id2 != 2)) AND ((id1 != 3) OR (id2 != 4)))"
  end

  it "should handle IN/NOT IN queries with multiple columns and a dataset where the database doesn't support it" do
    meta_def(@dataset, :supports_multiple_column_in?){false}
    db = Sequel.mock(:fetch=>[{:id1=>1, :id2=>2}, {:id1=>3, :id2=>4}])
    d1 = db[:test].select(:id1, :id2).filter(:region=>'Asia').columns(:id1, :id2)
    @dataset.filter([:id1, :id2] => d1).sql.must_equal "SELECT * FROM test WHERE (((id1 = 1) AND (id2 = 2)) OR ((id1 = 3) AND (id2 = 4)))"
    db.sqls.must_equal ["SELECT id1, id2 FROM test WHERE (region = 'Asia')"]
    @dataset.exclude([:id1, :id2] => d1).sql.must_equal "SELECT * FROM test WHERE (((id1 != 1) OR (id2 != 2)) AND ((id1 != 3) OR (id2 != 4)))"
    db.sqls.must_equal ["SELECT id1, id2 FROM test WHERE (region = 'Asia')"]
  end
  
  it "should handle IN/NOT IN queries with multiple columns and an empty dataset where the database doesn't support it" do
    meta_def(@dataset, :supports_multiple_column_in?){false}
    db = Sequel.mock
    d1 = db[:test].select(:id1, :id2).filter(:region=>'Asia').columns(:id1, :id2)
    @dataset.filter([:id1, :id2] => d1).sql.must_equal "SELECT * FROM test WHERE (1 = 0)"
    db.sqls.must_equal ["SELECT id1, id2 FROM test WHERE (region = 'Asia')"]
    @dataset.exclude([:id1, :id2] => d1).sql.must_equal "SELECT * FROM test WHERE (1 = 1)"
    db.sqls.must_equal ["SELECT id1, id2 FROM test WHERE (region = 'Asia')"]
  end
  
  it "should handle IN/NOT IN queries for datasets with row_procs" do
    meta_def(@dataset, :supports_multiple_column_in?){false}
    db = Sequel.mock(:fetch=>[{:id1=>1, :id2=>2}, {:id1=>3, :id2=>4}])
    d1 = db[:test].select(:id1, :id2).filter(:region=>'Asia').columns(:id1, :id2)
    d1.row_proc = proc{|h| Object.new}
    @dataset.filter([:id1, :id2] => d1).sql.must_equal "SELECT * FROM test WHERE (((id1 = 1) AND (id2 = 2)) OR ((id1 = 3) AND (id2 = 4)))"
    db.sqls.must_equal ["SELECT id1, id2 FROM test WHERE (region = 'Asia')"]
    @dataset.exclude([:id1, :id2] => d1).sql.must_equal "SELECT * FROM test WHERE (((id1 != 1) OR (id2 != 2)) AND ((id1 != 3) OR (id2 != 4)))"
    db.sqls.must_equal ["SELECT id1, id2 FROM test WHERE (region = 'Asia')"]
  end
  
  it "should accept a subquery for an EXISTS clause" do
    a = @dataset.filter(Sequel.expr(:price) < 100)
    @dataset.filter(a.exists).sql.must_equal 'SELECT * FROM test WHERE (EXISTS (SELECT * FROM test WHERE (price < 100)))'
  end
  
  it "should accept proc expressions" do
    d = @d1.select(Sequel.function(:avg, :gdp))
    @dataset.filter{gdp > d}.sql.must_equal "SELECT * FROM test WHERE (gdp > (SELECT avg(gdp) FROM test WHERE (region = 'Asia')))"
    @dataset.filter{a < 1}.sql.must_equal 'SELECT * FROM test WHERE (a < 1)'
    @dataset.filter{(a >= 1) & (b <= 2)}.sql.must_equal 'SELECT * FROM test WHERE ((a >= 1) AND (b <= 2))'
    @dataset.filter{c.like 'ABC%'}.sql.must_equal "SELECT * FROM test WHERE (c LIKE 'ABC%' ESCAPE '\\')"
    @dataset.filter{c.like 'ABC%', '%XYZ'}.sql.must_equal "SELECT * FROM test WHERE ((c LIKE 'ABC%' ESCAPE '\\') OR (c LIKE '%XYZ' ESCAPE '\\'))"
  end
  
  it "should work for grouped datasets" do
    @dataset.group(:a).filter(:b => 1).sql.must_equal 'SELECT * FROM test WHERE (b = 1) GROUP BY a'
  end

  it "should accept true and false as arguments" do
    @dataset.filter(true).sql.must_equal "SELECT * FROM test WHERE 't'"
    @dataset.filter(Sequel::SQLTRUE).sql.must_equal "SELECT * FROM test WHERE 't'"
    @dataset.filter(false).sql.must_equal "SELECT * FROM test WHERE 'f'"
    @dataset.filter(Sequel::SQLFALSE).sql.must_equal "SELECT * FROM test WHERE 'f'"
  end

  it "should use boolean expression if dataset does not support where true/false" do
    def @dataset.supports_where_true?() false end
    @dataset.filter(true).sql.must_equal "SELECT * FROM test WHERE (1 = 1)"
    @dataset.filter(Sequel::SQLTRUE).sql.must_equal "SELECT * FROM test WHERE (1 = 1)"
    @dataset.filter(false).sql.must_equal "SELECT * FROM test WHERE (1 = 0)"
    @dataset.filter(Sequel::SQLFALSE).sql.must_equal "SELECT * FROM test WHERE (1 = 0)"
  end

  it "should allow the use of multiple arguments" do
    @dataset.filter(:a, :b).sql.must_equal 'SELECT * FROM test WHERE (a AND b)'
    @dataset.filter(:a, :b=>1).sql.must_equal 'SELECT * FROM test WHERE (a AND (b = 1))'
    @dataset.filter(:a, Sequel.expr(:c) > 3, :b=>1).sql.must_equal 'SELECT * FROM test WHERE (a AND (c > 3) AND (b = 1))'
  end

  it "should allow the use of blocks and arguments simultaneously" do
    @dataset.filter(Sequel.expr(:zz) < 3){yy > 3}.sql.must_equal 'SELECT * FROM test WHERE ((zz < 3) AND (yy > 3))'
  end

  it "should yield a VirtualRow to the block" do
    x = nil
    @dataset.filter{|r| x = r; false}
    x.must_be_kind_of(Sequel::SQL::VirtualRow)
    @dataset.filter{|r| ((r.name < 'b') & {r.table__id => 1}) | r.is_active(r.blah, r.xx, r.x__y_z)}.sql.
      must_equal "SELECT * FROM test WHERE (((name < 'b') AND (table.id = 1)) OR is_active(blah, xx, x.y_z))"
  end

  it "should instance_eval the block in the context of a VirtualRow if the block doesn't request an argument" do
    x = nil
    @dataset.filter{x = self; false}
    x.must_be_kind_of(Sequel::SQL::VirtualRow)
    @dataset.filter{((name < 'b') & {table__id => 1}) | is_active(blah, xx, x__y_z)}.sql.
      must_equal "SELECT * FROM test WHERE (((name < 'b') AND (table.id = 1)) OR is_active(blah, xx, x.y_z))"
  end

  it "should handle arbitrary objects" do
    o = Object.new
    def o.sql_literal(ds)
      "foo"
    end
    @dataset.filter(o).sql.must_equal 'SELECT * FROM test WHERE foo'
  end

  it "should raise an error if an numeric is used" do
    proc{@dataset.filter(1)}.must_raise(Sequel::Error)
    proc{@dataset.filter(1.0)}.must_raise(Sequel::Error)
    proc{@dataset.filter(BigDecimal.new('1.0'))}.must_raise(Sequel::Error)
  end

  it "should raise an error if a NumericExpression or StringExpression is used" do
    proc{@dataset.filter(Sequel.expr(:x) + 1)}.must_raise(Sequel::Error)
    proc{@dataset.filter(Sequel.expr(:x).sql_string)}.must_raise(Sequel::Error)
  end
end

describe "Dataset#or" do
  before do
    @dataset = Sequel.mock.dataset.from(:test)
    @d1 = @dataset.where(:x => 1)
  end
  
  it "should just clone if no where clause exists" do
    @dataset.or(:a => 1).sql.must_equal 'SELECT * FROM test'
  end
  
  it "should just clone if given an empty argument" do
    @d1.or({}).sql.must_equal @d1.sql
    @d1.or([]).sql.must_equal @d1.sql
    @d1.or('').sql.must_equal @d1.sql
  end
  
  it "should add an alternative expression to the where clause" do
    @d1.or(:y => 2).sql.must_equal 'SELECT * FROM test WHERE ((x = 1) OR (y = 2))'
  end
  
  it "should accept all forms of filters" do
    @d1.or('y > ?', 2).sql.must_equal 'SELECT * FROM test WHERE ((x = 1) OR (y > 2))'
    @d1.or(Sequel.expr(:yy) > 3).sql.must_equal 'SELECT * FROM test WHERE ((x = 1) OR (yy > 3))'
  end    

  it "should accept blocks passed to filter" do
    @d1.or{yy > 3}.sql.must_equal 'SELECT * FROM test WHERE ((x = 1) OR (yy > 3))'
  end
  
  it "should correctly add parens to give predictable results" do
    @d1.filter(:y => 2).or(:z => 3).sql.must_equal 'SELECT * FROM test WHERE (((x = 1) AND (y = 2)) OR (z = 3))'
    @d1.or(:y => 2).filter(:z => 3).sql.must_equal 'SELECT * FROM test WHERE (((x = 1) OR (y = 2)) AND (z = 3))'
  end

  it "should allow the use of blocks and arguments simultaneously" do
    @d1.or(Sequel.expr(:zz) < 3){yy > 3}.sql.must_equal 'SELECT * FROM test WHERE ((x = 1) OR ((zz < 3) AND (yy > 3)))'
  end
end

describe "Dataset#and" do
  before do
    @dataset = Sequel.mock.dataset.from(:test)
    @d1 = @dataset.where(:x => 1)
  end
  
  it "should add a WHERE filter if none exists" do
    @dataset.and(:a => 1).sql.must_equal 'SELECT * FROM test WHERE (a = 1)'
  end
  
  it "should add an expression to the where clause" do
    @d1.and(:y => 2).sql.must_equal 'SELECT * FROM test WHERE ((x = 1) AND (y = 2))'
  end
  
  it "should accept different types of filters" do
    @d1.and('y > ?', 2).sql.must_equal 'SELECT * FROM test WHERE ((x = 1) AND (y > 2))'
    @d1.and(Sequel.expr(:yy) > 3).sql.must_equal 'SELECT * FROM test WHERE ((x = 1) AND (yy > 3))'
  end
      
  it "should accept blocks passed to filter" do
    @d1.and{yy > 3}.sql.must_equal 'SELECT * FROM test WHERE ((x = 1) AND (yy > 3))'
  end
  
  it "should correctly add parens to give predictable results" do
    @d1.or(:y => 2).and(:z => 3).sql.must_equal 'SELECT * FROM test WHERE (((x = 1) OR (y = 2)) AND (z = 3))'
    @d1.and(:y => 2).or(:z => 3).sql.must_equal 'SELECT * FROM test WHERE (((x = 1) AND (y = 2)) OR (z = 3))'
  end
end

describe "Dataset#exclude" do
  before do
    @dataset = Sequel.mock.dataset.from(:test)
  end

  it "should correctly negate the expression when one condition is given" do
    @dataset.exclude(:region=>'Asia').select_sql.must_equal "SELECT * FROM test WHERE (region != 'Asia')"
  end

  it "should take multiple conditions as a hash and express the logic correctly in SQL" do
    @dataset.exclude(:region => 'Asia', :name => 'Japan').select_sql.
      must_match(Regexp.union(/WHERE \(\(region != 'Asia'\) OR \(name != 'Japan'\)\)/,
                                /WHERE \(\(name != 'Japan'\) OR \(region != 'Asia'\)\)/))
  end

  it "should parenthesize a single string condition correctly" do
    @dataset.exclude("region = 'Asia' AND name = 'Japan'").select_sql.must_equal "SELECT * FROM test WHERE NOT (region = 'Asia' AND name = 'Japan')"
  end

  it "should parenthesize an array condition correctly" do
    @dataset.exclude('region = ? AND name = ?', 'Asia', 'Japan').select_sql.must_equal "SELECT * FROM test WHERE NOT (region = 'Asia' AND name = 'Japan')"
  end

  it "should correctly parenthesize when it is used twice" do
    @dataset.exclude(:region => 'Asia').exclude(:name => 'Japan').select_sql.must_equal "SELECT * FROM test WHERE ((region != 'Asia') AND (name != 'Japan'))"
  end
  
  it "should support proc expressions" do
    @dataset.exclude{id < 6}.sql.must_equal 'SELECT * FROM test WHERE (id >= 6)'
  end
  
  it "should allow the use of blocks and arguments simultaneously" do
    @dataset.exclude(:id => (7..11)){id < 6}.sql.must_equal 'SELECT * FROM test WHERE ((id < 7) OR (id > 11) OR (id >= 6))'
    @dataset.exclude([:id, 1], [:x, 3]){id < 6}.sql.must_equal 'SELECT * FROM test WHERE ((id != 1) OR (x != 3) OR (id >= 6))'
  end
end

describe "Dataset#exclude_where" do
  before do
    @dataset = Sequel.mock.dataset.from(:test)
  end

  it "should correctly negate the expression and add it to the where clause" do
    @dataset.exclude_where(:region=>'Asia').sql.must_equal "SELECT * FROM test WHERE (region != 'Asia')"
    @dataset.exclude_where(:region=>'Asia').exclude_where(:region=>'NA').sql.must_equal "SELECT * FROM test WHERE ((region != 'Asia') AND (region != 'NA'))"
  end

  it "should affect the where clause even if having clause is already used" do
    @dataset.group_and_count(:name).having{count > 2}.exclude_where(:region=>'Asia').sql.
      must_equal "SELECT name, count(*) AS count FROM test WHERE (region != 'Asia') GROUP BY name HAVING (count > 2)"
  end
end

describe "Dataset#exclude_having" do
  it "should correctly negate the expression and add it to the having clause" do
    Sequel.mock.dataset.from(:test).exclude_having{count > 2}.exclude_having{count < 0}.sql.must_equal "SELECT * FROM test HAVING ((count <= 2) AND (count >= 0))"
  end
end

describe "Dataset#invert" do
  before do
    @d = Sequel.mock.dataset.from(:test)
  end

  it "should return a dataset that selects no rows if dataset is not filtered" do
    @d.invert.sql.must_equal "SELECT * FROM test WHERE 'f'"
  end

  it "should invert current filter if dataset is filtered" do
    @d.filter(:x).invert.sql.must_equal 'SELECT * FROM test WHERE NOT x'
  end

  it "should invert both having and where if both are preset" do
    @d.filter(:x).group(:x).having(:x).invert.sql.must_equal 'SELECT * FROM test WHERE NOT x GROUP BY x HAVING NOT x'
  end
end

describe "Dataset#having" do
  before do
    @dataset = Sequel.mock.dataset.from(:test)
    @grouped = @dataset.group(:region).select(:region, Sequel.function(:sum, :population), Sequel.function(:avg, :gdp))
  end

  it "should just clone if given an empty argument" do
    @dataset.having({}).sql.must_equal @dataset.sql
    @dataset.having([]).sql.must_equal @dataset.sql
    @dataset.having('').sql.must_equal @dataset.sql
  end
  
  it "should affect select statements" do
    @grouped.having('sum(population) > 10').select_sql.must_equal "SELECT region, sum(population), avg(gdp) FROM test GROUP BY region HAVING (sum(population) > 10)"
  end

  it "should support proc expressions" do
    @grouped.having{Sequel.function(:sum, :population) > 10}.sql.must_equal "SELECT region, sum(population), avg(gdp) FROM test GROUP BY region HAVING (sum(population) > 10)"
  end
end

describe "a grouped dataset" do
  before do
    @dataset = Sequel.mock.dataset.from(:test).group(:type_id)
  end

  it "should raise when trying to generate an update statement" do
    proc {@dataset.update_sql(:id => 0)}.must_raise Sequel::InvalidOperation
  end

  it "should raise when trying to generate a delete statement" do
    proc {@dataset.delete_sql}.must_raise Sequel::InvalidOperation
  end
  
  it "should raise when trying to generate a truncate statement" do
    proc {@dataset.truncate_sql}.must_raise Sequel::InvalidOperation
  end

  it "should raise when trying to generate an insert statement" do
    proc {@dataset.insert_sql}.must_raise Sequel::InvalidOperation
  end

  it "should specify the grouping in generated select statement" do
    @dataset.select_sql.must_equal "SELECT * FROM test GROUP BY type_id"
  end
  
  it "should format the right statement for counting (as a subquery)" do
    db = Sequel.mock
    db[:test].select(:name).group(:name).count
    db.sqls.must_equal ["SELECT count(*) AS count FROM (SELECT name FROM test GROUP BY name) AS t1 LIMIT 1"]
  end
end

describe "Dataset#group_by" do
  before do
    @dataset = Sequel.mock[:test].group_by(:type_id)
  end

  it "should raise when trying to generate an update statement" do
    proc {@dataset.update_sql(:id => 0)}.must_raise Sequel::InvalidOperation
  end

  it "should raise when trying to generate a delete statement" do
    proc {@dataset.delete_sql}.must_raise Sequel::InvalidOperation
  end

  it "should specify the grouping in generated select statement" do
    @dataset.select_sql.must_equal "SELECT * FROM test GROUP BY type_id"
    @dataset.group_by(:a, :b).select_sql.must_equal "SELECT * FROM test GROUP BY a, b"
    @dataset.group_by(:type_id=>nil).select_sql.must_equal "SELECT * FROM test GROUP BY (type_id IS NULL)"
  end

  it "should ungroup when passed nil or no arguments" do
    @dataset.group_by.select_sql.must_equal "SELECT * FROM test"
    @dataset.group_by(nil).select_sql.must_equal "SELECT * FROM test"
  end

  it "should undo previous grouping" do
    @dataset.group_by(:a).group_by(:b).select_sql.must_equal "SELECT * FROM test GROUP BY b"
    @dataset.group_by(:a, :b).group_by.select_sql.must_equal "SELECT * FROM test"
  end

  it "should be aliased as #group" do
    @dataset.group(:type_id=>nil).select_sql.must_equal "SELECT * FROM test GROUP BY (type_id IS NULL)"
  end

  it "should take a virtual row block" do
    @dataset.group{type_id > 1}.sql.must_equal "SELECT * FROM test GROUP BY (type_id > 1)"
    @dataset.group_by{type_id > 1}.sql.must_equal "SELECT * FROM test GROUP BY (type_id > 1)"
    @dataset.group{[type_id > 1, type_id < 2]}.sql.must_equal "SELECT * FROM test GROUP BY (type_id > 1), (type_id < 2)"
    @dataset.group(:foo){type_id > 1}.sql.must_equal "SELECT * FROM test GROUP BY foo, (type_id > 1)"
  end

  it "should support a #group_rollup method if the database supports it" do
    meta_def(@dataset, :supports_group_rollup?){true}
    @dataset.group(:type_id).group_rollup.select_sql.must_equal "SELECT * FROM test GROUP BY ROLLUP(type_id)"
    @dataset.group(:type_id, :b).group_rollup.select_sql.must_equal "SELECT * FROM test GROUP BY ROLLUP(type_id, b)"
    meta_def(@dataset, :uses_with_rollup?){true}
    @dataset.group(:type_id).group_rollup.select_sql.must_equal "SELECT * FROM test GROUP BY type_id WITH ROLLUP"
    @dataset.group(:type_id, :b).group_rollup.select_sql.must_equal "SELECT * FROM test GROUP BY type_id, b WITH ROLLUP"
  end

  it "should support a #group_cube method if the database supports it" do
    meta_def(@dataset, :supports_group_cube?){true}
    @dataset.group(:type_id).group_cube.select_sql.must_equal "SELECT * FROM test GROUP BY CUBE(type_id)"
    @dataset.group(:type_id, :b).group_cube.select_sql.must_equal "SELECT * FROM test GROUP BY CUBE(type_id, b)"
    meta_def(@dataset, :uses_with_rollup?){true}
    @dataset.group(:type_id).group_cube.select_sql.must_equal "SELECT * FROM test GROUP BY type_id WITH CUBE"
    @dataset.group(:type_id, :b).group_cube.select_sql.must_equal "SELECT * FROM test GROUP BY type_id, b WITH CUBE"
  end

  it "should support a #grouping_sets method if the database supports it" do
    meta_def(@dataset, :supports_grouping_sets?){true}
    @dataset.group(:type_id).grouping_sets.select_sql.must_equal "SELECT * FROM test GROUP BY GROUPING SETS((type_id))"
    @dataset.group([:type_id, :b], :type_id, []).grouping_sets.select_sql.must_equal "SELECT * FROM test GROUP BY GROUPING SETS((type_id, b), (type_id), ())"
  end

  it "should have #group_* methods raise an Error if not supported it" do
    proc{@dataset.group(:type_id).group_rollup}.must_raise(Sequel::Error)
    proc{@dataset.group(:type_id).group_cube}.must_raise(Sequel::Error)
    proc{@dataset.group(:type_id).grouping_sets}.must_raise(Sequel::Error)
  end
end

describe "Dataset#group_append" do
  before do
    @d = Sequel.mock.dataset.from(:test)
  end

  it "should group by the given columns if no existing columns are present" do
    @d.group_append(:a).sql.must_equal 'SELECT * FROM test GROUP BY a'
  end

  it "should add to the currently grouped columns" do
    @d.group(:a).group_append(:b).sql.must_equal 'SELECT * FROM test GROUP BY a, b'
  end

  it "should accept a block that yields a virtual row" do
    @d.group(:a).group_append{:b}.sql.must_equal 'SELECT * FROM test GROUP BY a, b'
    @d.group(:a).group_append(:c){b}.sql.must_equal 'SELECT * FROM test GROUP BY a, c, b'
  end
end

describe "Dataset#as" do
  it "should set up an alias" do
    dataset = Sequel.mock.dataset.from(:test)
    dataset.select(dataset.limit(1).select(:name).as(:n)).sql.must_equal 'SELECT (SELECT name FROM test LIMIT 1) AS n FROM test'
    dataset.select(dataset.limit(1).select(:name).as(:n, [:nm])).sql.must_equal 'SELECT (SELECT name FROM test LIMIT 1) AS n(nm) FROM test'
  end
end

describe "Dataset#literal" do
  before do
    @ds = Sequel::Database.new.dataset
  end
  
  it "should convert qualified symbol notation into dot notation" do
    @ds.literal(:abc__def).must_equal 'abc.def'
  end
  
  it "should convert AS symbol notation into SQL AS notation" do
    @ds.literal(:xyz___x).must_equal 'xyz AS x'
    @ds.literal(:abc__def___x).must_equal 'abc.def AS x'
  end
  
  it "should support names with digits" do
    @ds.literal(:abc2).must_equal 'abc2'
    @ds.literal(:xx__yy3).must_equal 'xx.yy3'
    @ds.literal(:ab34__temp3_4ax).must_equal 'ab34.temp3_4ax'
    @ds.literal(:x1___y2).must_equal 'x1 AS y2'
    @ds.literal(:abc2__def3___ggg4).must_equal 'abc2.def3 AS ggg4'
  end
  
  it "should support upper case and lower case" do
    @ds.literal(:ABC).must_equal 'ABC'
    @ds.literal(:Zvashtoy__aBcD).must_equal 'Zvashtoy.aBcD'
  end

  it "should support spaces inside column names" do
    @ds.quote_identifiers = true
    @ds.literal(:"AB C").must_equal '"AB C"'
    @ds.literal(:"Zvas htoy__aB cD").must_equal '"Zvas htoy"."aB cD"'
    @ds.literal(:"aB cD___XX XX").must_equal '"aB cD" AS "XX XX"'
    @ds.literal(:"Zva shtoy__aB cD___XX XX").must_equal '"Zva shtoy"."aB cD" AS "XX XX"'
  end
end

describe "Dataset#literal" do
  before do
    @dataset = Sequel::Database.new.from(:test)
  end
  
  it "should escape strings properly" do
    @dataset.literal('abc').must_equal "'abc'"
    @dataset.literal('a"x"bc').must_equal "'a\"x\"bc'"
    @dataset.literal("a'bc").must_equal "'a''bc'"
    @dataset.literal("a''bc").must_equal "'a''''bc'"
    @dataset.literal("a\\bc").must_equal "'a\\bc'"
    @dataset.literal("a\\\\bc").must_equal "'a\\\\bc'"
    @dataset.literal("a\\'bc").must_equal "'a\\''bc'"
  end
  
  it "should escape blobs as strings by default" do
    @dataset.literal(Sequel.blob('abc')).must_equal "'abc'"
  end

  it "should literalize numbers properly" do
    @dataset.literal(1).must_equal "1"
    @dataset.literal(1.5).must_equal "1.5"
  end

  it "should literalize nil as NULL" do
    @dataset.literal(nil).must_equal "NULL"
  end

  it "should literalize an array properly" do
    @dataset.literal([]).must_equal "(NULL)"
    @dataset.literal([1, 'abc', 3]).must_equal "(1, 'abc', 3)"
    @dataset.literal([1, "a'b''c", 3]).must_equal "(1, 'a''b''''c', 3)"
  end

  it "should literalize symbols as column references" do
    @dataset.literal(:name).must_equal "name"
    @dataset.literal(:items__name).must_equal "items.name"
    @dataset.literal(:"items__na#m$e").must_equal "items.na#m$e"
  end

  it "should call sql_literal_append with dataset and sql on type if not natively supported and the object responds to it" do
    @a = Class.new do
      def sql_literal_append(ds, sql)
        sql << "called #{ds.blah}"
      end
      def sql_literal(ds)
        "not called #{ds.blah}"
      end
    end
    def @dataset.blah
      "ds"
    end
    @dataset.literal(@a.new).must_equal "called ds"
  end
  
  it "should call sql_literal with dataset on type if not natively supported and the object responds to it" do
    @a = Class.new do
      def sql_literal(ds)
        "called #{ds.blah}"
      end
    end
    def @dataset.blah
      "ds"
    end
    @dataset.literal(@a.new).must_equal "called ds"
  end
  
  it "should literalize datasets as subqueries" do
    d = @dataset.from(:test)
    d.literal(d).must_equal "(#{d.sql})"
  end
  
  it "should literalize times properly" do
    @dataset.literal(Sequel::SQLTime.create(1, 2, 3, 500000)).must_equal "'01:02:03.500000'"
    @dataset.literal(Time.local(2010, 1, 2, 3, 4, 5, 500000)).must_equal "'2010-01-02 03:04:05.500000'"
    @dataset.literal(DateTime.new(2010, 1, 2, 3, 4, Rational(55, 10))).must_equal "'2010-01-02 03:04:05.500000'"
  end
  
  it "should literalize times properly for databases supporting millisecond precision" do
    meta_def(@dataset, :timestamp_precision){3}
    @dataset.literal(Sequel::SQLTime.create(1, 2, 3, 500000)).must_equal "'01:02:03.500'"
    @dataset.literal(Time.local(2010, 1, 2, 3, 4, 5, 500000)).must_equal "'2010-01-02 03:04:05.500'"
    @dataset.literal(DateTime.new(2010, 1, 2, 3, 4, Rational(55, 10))).must_equal "'2010-01-02 03:04:05.500'"
  end
  
  it "should literalize Date properly" do
    d = Date.today
    s = d.strftime("'%Y-%m-%d'")
    @dataset.literal(d).must_equal s
  end

  it "should literalize Date properly, even if to_s is overridden" do
    d = Date.today
    def d.to_s; "adsf" end
    s = d.strftime("'%Y-%m-%d'")
    @dataset.literal(d).must_equal s
  end

  it "should literalize Time, DateTime, Date properly if SQL standard format is required" do
    meta_def(@dataset, :requires_sql_standard_datetimes?){true}
    @dataset.literal(Time.local(2010, 1, 2, 3, 4, 5, 500000)).must_equal "TIMESTAMP '2010-01-02 03:04:05.500000'"
    @dataset.literal(DateTime.new(2010, 1, 2, 3, 4, Rational(55, 10))).must_equal "TIMESTAMP '2010-01-02 03:04:05.500000'"
    @dataset.literal(Date.new(2010, 1, 2)).must_equal "DATE '2010-01-02'"
  end
  
  it "should literalize Time and DateTime properly if the database support timezones in timestamps" do
    meta_def(@dataset, :supports_timestamp_timezones?){true}
    @dataset.literal(Time.utc(2010, 1, 2, 3, 4, 5, 500000)).must_equal "'2010-01-02 03:04:05.500000+0000'"
    @dataset.literal(DateTime.new(2010, 1, 2, 3, 4, Rational(55, 10))).must_equal "'2010-01-02 03:04:05.500000+0000'"

    meta_def(@dataset, :supports_timestamp_usecs?){false}
    @dataset.literal(Time.utc(2010, 1, 2, 3, 4, 5)).must_equal "'2010-01-02 03:04:05+0000'"
    @dataset.literal(DateTime.new(2010, 1, 2, 3, 4, 5)).must_equal "'2010-01-02 03:04:05+0000'"
  end
  
  it "should not modify literal strings" do
    @dataset.quote_identifiers = true
    @dataset.literal(Sequel.lit('col1 + 2')).must_equal 'col1 + 2'
    @dataset.update_sql(Sequel::SQL::Identifier.new(Sequel.lit('a')) => Sequel.lit('a + 2')).must_equal 'UPDATE "test" SET a = a + 2'
  end

  it "should literalize BigDecimal instances correctly" do
    @dataset.literal(BigDecimal.new("80")).must_equal "80.0"
    @dataset.literal(BigDecimal.new("NaN")).must_equal "'NaN'"
    @dataset.literal(BigDecimal.new("Infinity")).must_equal "'Infinity'"
    @dataset.literal(BigDecimal.new("-Infinity")).must_equal "'-Infinity'"
  end

  it "should literalize PlaceholderLiteralStrings correctly" do
    @dataset.literal(Sequel::SQL::PlaceholderLiteralString.new('? = ?', [1, 2])).must_equal '1 = 2'
    @dataset.literal(Sequel::SQL::PlaceholderLiteralString.new('? = ?', [1, 2], true)).must_equal '(1 = 2)'
    @dataset.literal(Sequel::SQL::PlaceholderLiteralString.new(':a = :b', :a=>1, :b=>2)).must_equal '1 = 2'
    @dataset.literal(Sequel::SQL::PlaceholderLiteralString.new(':a = :b', {:a=>1, :b=>2}, true)).must_equal '(1 = 2)'
    @dataset.literal(Sequel::SQL::PlaceholderLiteralString.new(['', ' = ', ''], [1, 2])).must_equal '1 = 2'
    @dataset.literal(Sequel::SQL::PlaceholderLiteralString.new(['', ' = ', ''], [1, 2], true)).must_equal '(1 = 2)'
    @dataset.literal(Sequel::SQL::PlaceholderLiteralString.new(['', ' = '], [1, 2])).must_equal '1 = 2'
    @dataset.literal(Sequel::SQL::PlaceholderLiteralString.new(['', ' = '], [1, 2], true)).must_equal '(1 = 2)'
  end

  it "should raise an Error if the object can't be literalized" do
    proc{@dataset.literal(Object.new)}.must_raise(Sequel::Error)
  end
end

describe "Dataset#from" do
  before do
    @dataset = Sequel.mock.dataset
  end

  it "should accept a Dataset" do
    @dataset.from(@dataset)
  end

  it "should format a Dataset as a subquery if it has had options set" do
    @dataset.from(@dataset.from(:a).where(:a=>1)).select_sql.must_equal "SELECT * FROM (SELECT * FROM a WHERE (a = 1)) AS t1"
  end
  
  it "should automatically alias sub-queries" do
    @dataset.from(@dataset.from(:a).group(:b)).select_sql.must_equal "SELECT * FROM (SELECT * FROM a GROUP BY b) AS t1"
      
    d1 = @dataset.from(:a).group(:b)
    d2 = @dataset.from(:c).group(:d)
    @dataset.from(d1, d2).sql.must_equal "SELECT * FROM (SELECT * FROM a GROUP BY b) AS t1, (SELECT * FROM c GROUP BY d) AS t2"
  end
  
  it "should always use a subquery if given a dataset" do
    @dataset.from(@dataset.from(:a)).select_sql.must_equal "SELECT * FROM (SELECT * FROM a) AS t1"
  end
  
  it "should treat string arguments as identifiers" do
    @dataset.quote_identifiers = true
    @dataset.from('a').select_sql.must_equal "SELECT * FROM \"a\""
  end
  
  it "should not treat literal strings or blobs as identifiers" do
    @dataset.quote_identifiers = true
    @dataset.from(Sequel.lit('a')).select_sql.must_equal "SELECT * FROM a"
    @dataset.from(Sequel.blob('a')).select_sql.must_equal "SELECT * FROM 'a'"
  end
  
  it "should remove all FROM tables if called with no arguments" do
    @dataset.from.sql.must_equal 'SELECT *'
  end
  
  it "should accept sql functions" do
    @dataset.from(Sequel.function(:abc, :def)).select_sql.must_equal "SELECT * FROM abc(def)"
    @dataset.from(Sequel.function(:a, :i)).select_sql.must_equal "SELECT * FROM a(i)"
  end

  it "should accept virtual row blocks" do
    @dataset.from{abc(de)}.select_sql.must_equal "SELECT * FROM abc(de)"
    @dataset.from{[i, abc(de)]}.select_sql.must_equal "SELECT * FROM i, abc(de)"
    @dataset.from(:a){i}.select_sql.must_equal "SELECT * FROM a, i"
    @dataset.from(:a, :b){i}.select_sql.must_equal "SELECT * FROM a, b, i"
    @dataset.from(:a, :b){[i, abc(de)]}.select_sql.must_equal "SELECT * FROM a, b, i, abc(de)"
  end

  it "should handle LATERAL subqueries" do
    @dataset.from(:a, @dataset.from(:b).lateral).select_sql.must_equal "SELECT * FROM a, LATERAL (SELECT * FROM b) AS t1"
  end

  it "should automatically use a default from table if no from table is present" do
    def @dataset.empty_from_sql; ' FROM DEFFROM'; end
    @dataset.select_sql.must_equal "SELECT * FROM DEFFROM"
  end

  it "should accept :schema__table___alias symbol format" do
    @dataset.from(:abc__def).select_sql.must_equal "SELECT * FROM abc.def"
    @dataset.from(:a_b__c).select_sql.must_equal "SELECT * FROM a_b.c"
    @dataset.from(:'#__#').select_sql.must_equal 'SELECT * FROM #.#'
    @dataset.from(:abc__def___d).select_sql.must_equal "SELECT * FROM abc.def AS d"
    @dataset.from(:a_b__d_e___f_g).select_sql.must_equal "SELECT * FROM a_b.d_e AS f_g"
    @dataset.from(:'#__#___#').select_sql.must_equal 'SELECT * FROM #.# AS #'
    @dataset.from(:abc___def).select_sql.must_equal "SELECT * FROM abc AS def"
    @dataset.from(:a_b___c_d).select_sql.must_equal "SELECT * FROM a_b AS c_d"
    @dataset.from(:'#___#').select_sql.must_equal 'SELECT * FROM # AS #'
  end

  it "should not handle :foo__schema__table___alias specially" do
    @dataset.from(:foo__schema__table___alias).select_sql.must_equal "SELECT * FROM foo.schema__table AS alias"
  end

  it "should hoist WITH clauses from subqueries if the dataset doesn't support CTEs in subselects" do
    meta_def(@dataset, :supports_cte?){true}
    meta_def(@dataset, :supports_cte_in_subselect?){false}
    @dataset.from(@dataset.from(:a).with(:a, @dataset.from(:b))).sql.must_equal 'WITH a AS (SELECT * FROM b) SELECT * FROM (SELECT * FROM a) AS t1'
    @dataset.from(@dataset.from(:a).with(:a, @dataset.from(:b)), @dataset.from(:c).with(:c, @dataset.from(:d))).sql.must_equal 'WITH a AS (SELECT * FROM b), c AS (SELECT * FROM d) SELECT * FROM (SELECT * FROM a) AS t1, (SELECT * FROM c) AS t2'
  end
end

describe "Dataset#select" do
  before do
    @d = Sequel.mock.dataset.from(:test)
  end

  it "should accept variable arity" do
    @d.select(:name).sql.must_equal 'SELECT name FROM test'
    @d.select(:a, :b, :test__c).sql.must_equal 'SELECT a, b, test.c FROM test'
  end
  
  it "should accept symbols and literal strings" do
    @d.select(Sequel.lit('aaa')).sql.must_equal 'SELECT aaa FROM test'
    @d.select(:a, Sequel.lit('b')).sql.must_equal 'SELECT a, b FROM test'
    @d.select(:test__cc, Sequel.lit('test.d AS e')).sql.must_equal 'SELECT test.cc, test.d AS e FROM test'
    @d.select(Sequel.lit('test.d AS e'), :test__cc).sql.must_equal 'SELECT test.d AS e, test.cc FROM test'
    @d.select(:test__name___n).sql.must_equal 'SELECT test.name AS n FROM test'
  end
  
  it "should accept ColumnAlls" do
    @d.select(Sequel::SQL::ColumnAll.new(:test)).sql.must_equal 'SELECT test.* FROM test'
  end
  
  it "should accept QualifiedIdentifiers" do
    @d.select(Sequel.expr(:test__name).as(:n)).sql.must_equal 'SELECT test.name AS n FROM test'
  end

  it "should use the wildcard if no arguments are given" do
    @d.select.sql.must_equal 'SELECT * FROM test'
  end
  
  it "should handle array condition specifiers that are aliased" do
    @d.select(Sequel.as([[:b, :c]], :n)).sql.must_equal 'SELECT (b = c) AS n FROM test'
  end

  it "should handle hashes returned from virtual row blocks" do
    @d.select{{:b=>:c}}.sql.must_equal 'SELECT (b = c) FROM test'
  end

  it "should override the previous select option" do
    @d.select!(:a, :b, :c).select.sql.must_equal 'SELECT * FROM test'
    @d.select!(:price).select(:name).sql.must_equal 'SELECT name FROM test'
  end
  
  it "should accept arbitrary objects and literalize them correctly" do
    @d.select(1, :a, 't').sql.must_equal "SELECT 1, a, 't' FROM test"
    @d.select(nil, Sequel.function(:sum, :t), :x___y).sql.must_equal "SELECT NULL, sum(t), x AS y FROM test"
    @d.select(nil, 1, Sequel.as(:x, :y)).sql.must_equal "SELECT NULL, 1, x AS y FROM test"
  end

  it "should accept a block that yields a virtual row" do
    @d.select{|o| o.a}.sql.must_equal 'SELECT a FROM test'
    @d.select{a(1)}.sql.must_equal 'SELECT a(1) FROM test'
    @d.select{|o| o.a(1, 2)}.sql.must_equal 'SELECT a(1, 2) FROM test'
    @d.select{[a, a(1, 2)]}.sql.must_equal 'SELECT a, a(1, 2) FROM test'
  end

  it "should merge regular arguments with argument returned from block" do
    @d.select(:b){a}.sql.must_equal 'SELECT b, a FROM test'
    @d.select(:b, :c){|o| o.a(1)}.sql.must_equal 'SELECT b, c, a(1) FROM test'
    @d.select(:b){[a, a(1, 2)]}.sql.must_equal 'SELECT b, a, a(1, 2) FROM test'
    @d.select(:b, :c){|o| [o.a, o.a(1, 2)]}.sql.must_equal 'SELECT b, c, a, a(1, 2) FROM test'
  end
end

describe "Dataset#select_group" do
  before do
    @d = Sequel.mock.dataset.from(:test)
  end

  it "should set both SELECT and GROUP" do
    @d.select_group(:name).sql.must_equal 'SELECT name FROM test GROUP BY name'
    @d.select_group(:a, :b__c, :d___e).sql.must_equal 'SELECT a, b.c, d AS e FROM test GROUP BY a, b.c, d'
  end

  it "should remove from both SELECT and GROUP if no arguments" do
    @d.select_group(:name).select_group.sql.must_equal 'SELECT * FROM test'
  end

  it "should accept virtual row blocks" do
    @d.select_group{name}.sql.must_equal 'SELECT name FROM test GROUP BY name'
    @d.select_group{[name, f(v).as(a)]}.sql.must_equal 'SELECT name, f(v) AS a FROM test GROUP BY name, f(v)'
    @d.select_group(:name){f(v).as(a)}.sql.must_equal 'SELECT name, f(v) AS a FROM test GROUP BY name, f(v)'
  end
end
  
describe "Dataset#select_all" do
  before do
    @d = Sequel.mock.dataset.from(:test)
  end

  it "should select the wildcard" do
    @d.select_all.sql.must_equal 'SELECT * FROM test'
  end
  
  it "should override the previous select option" do
    @d.select!(:a, :b, :c).select_all.sql.must_equal 'SELECT * FROM test'
  end

  it "should select all columns in a table if given an argument" do
    @d.select_all(:test).sql.must_equal 'SELECT test.* FROM test'
  end
  
  it "should select all columns all tables if given a multiple arguments" do
    @d.select_all(:test, :foo).sql.must_equal 'SELECT test.*, foo.* FROM test'
  end
  
  it "should work correctly with qualified symbols" do
    @d.select_all(:sch__test).sql.must_equal 'SELECT sch.test.* FROM test'
  end
  
  it "should work correctly with aliased symbols" do
    @d.select_all(:test___al).sql.must_equal 'SELECT al.* FROM test'
    @d.select_all(:sch__test___al).sql.must_equal 'SELECT al.* FROM test'
  end
  
  it "should work correctly with SQL::Identifiers" do
    @d.select_all(Sequel.identifier(:test)).sql.must_equal 'SELECT test.* FROM test'
  end
  
  it "should work correctly with SQL::QualifiedIdentifier" do
    @d.select_all(Sequel.qualify(:sch, :test)).sql.must_equal 'SELECT sch.test.* FROM test'
  end
  
  it "should work correctly with SQL::AliasedExpressions" do
    @d.select_all(Sequel.expr(:test).as(:al)).sql.must_equal 'SELECT al.* FROM test'
  end
  
  it "should work correctly with SQL::JoinClauses" do
    d = @d.cross_join(:foo).cross_join(:test___al)
    @d.select_all(*d.opts[:join]).sql.must_equal 'SELECT foo.*, al.* FROM test'
  end
end

describe "Dataset#select_more" do
  before do
    @d = Sequel.mock.dataset.from(:test)
  end
  
  it "should act like #select_append for datasets with no selection" do
    @d.select_more(:a, :b).sql.must_equal 'SELECT *, a, b FROM test'
    @d.select_all.select_more(:a, :b).sql.must_equal 'SELECT *, a, b FROM test'
    @d.select(:blah).select_all.select_more(:a, :b).sql.must_equal 'SELECT *, a, b FROM test'
  end

  it "should add to the currently selected columns" do
    @d.select(:a).select_more(:b).sql.must_equal 'SELECT a, b FROM test'
    @d.select(Sequel::SQL::ColumnAll.new(:a)).select_more(Sequel::SQL::ColumnAll.new(:b)).sql.must_equal 'SELECT a.*, b.* FROM test'
  end

  it "should accept a block that yields a virtual row" do
    @d.select(:a).select_more{|o| o.b}.sql.must_equal 'SELECT a, b FROM test'
    @d.select(Sequel::SQL::ColumnAll.new(:a)).select_more(Sequel::SQL::ColumnAll.new(:b)){b(1)}.sql.must_equal 'SELECT a.*, b.*, b(1) FROM test'
  end
end

describe "Dataset#select_append" do
  before do
    @d = Sequel.mock.dataset.from(:test)
  end
  
  it "should select * in addition to columns if no columns selected" do
    @d.select_append(:a, :b).sql.must_equal 'SELECT *, a, b FROM test'
    @d.select_all.select_append(:a, :b).sql.must_equal 'SELECT *, a, b FROM test'
    @d.select(:blah).select_all.select_append(:a, :b).sql.must_equal 'SELECT *, a, b FROM test'
  end

  it "should add to the currently selected columns" do
    @d.select(:a).select_append(:b).sql.must_equal 'SELECT a, b FROM test'
    @d.select(Sequel::SQL::ColumnAll.new(:a)).select_append(Sequel::SQL::ColumnAll.new(:b)).sql.must_equal 'SELECT a.*, b.* FROM test'
  end

  it "should accept a block that yields a virtual row" do
    @d.select(:a).select_append{|o| o.b}.sql.must_equal 'SELECT a, b FROM test'
    @d.select(Sequel::SQL::ColumnAll.new(:a)).select_append(Sequel::SQL::ColumnAll.new(:b)){b(1)}.sql.must_equal 'SELECT a.*, b.*, b(1) FROM test'
  end

  it "should select from all from and join tables if SELECT *, column not supported" do
    meta_def(@d, :supports_select_all_and_column?){false}
    @d.select_append(:b).sql.must_equal 'SELECT test.*, b FROM test'
    @d.from(:test, :c).select_append(:b).sql.must_equal 'SELECT test.*, c.*, b FROM test, c'
    @d.cross_join(:c).select_append(:b).sql.must_equal 'SELECT test.*, c.*, b FROM test CROSS JOIN c'
    @d.cross_join(:c).cross_join(:d).select_append(:b).sql.must_equal 'SELECT test.*, c.*, d.*, b FROM test CROSS JOIN c CROSS JOIN d'
  end
end

describe "Dataset#order" do
  before do
    @dataset = Sequel.mock.dataset.from(:test)
  end
  
  it "should include an ORDER BY clause in the select statement" do
    @dataset.order(:name).sql.must_equal 'SELECT * FROM test ORDER BY name'
  end
  
  it "should accept multiple arguments" do
    @dataset.order(:name, Sequel.desc(:price)).sql.must_equal 'SELECT * FROM test ORDER BY name, price DESC'
  end
  
  it "should accept :nulls options for asc and desc" do
    @dataset.order(Sequel.asc(:name, :nulls=>:last), Sequel.desc(:price, :nulls=>:first)).sql.must_equal 'SELECT * FROM test ORDER BY name ASC NULLS LAST, price DESC NULLS FIRST'
  end
  
  it "should override a previous ordering" do
    @dataset.order(:name).order(:stamp).sql.must_equal 'SELECT * FROM test ORDER BY stamp'
  end
  
  it "should accept a literal string" do
    @dataset.order(Sequel.lit('dada ASC')).sql.must_equal 'SELECT * FROM test ORDER BY dada ASC'
  end
  
  it "should accept a hash as an expression" do
    @dataset.order(:name=>nil).sql.must_equal 'SELECT * FROM test ORDER BY (name IS NULL)'
  end
  
  it "should accept a nil to remove ordering" do
    @dataset.order(:bah).order(nil).sql.must_equal 'SELECT * FROM test'
  end

  it "should accept a block that yields a virtual row" do
    @dataset.order{|o| o.a}.sql.must_equal 'SELECT * FROM test ORDER BY a'
    @dataset.order{a(1)}.sql.must_equal 'SELECT * FROM test ORDER BY a(1)'
    @dataset.order{|o| o.a(1, 2)}.sql.must_equal 'SELECT * FROM test ORDER BY a(1, 2)'
    @dataset.order{[a, a(1, 2)]}.sql.must_equal 'SELECT * FROM test ORDER BY a, a(1, 2)'
  end

  it "should merge regular arguments with argument returned from block" do
    @dataset.order(:b){a}.sql.must_equal 'SELECT * FROM test ORDER BY b, a'
    @dataset.order(:b, :c){|o| o.a(1)}.sql.must_equal 'SELECT * FROM test ORDER BY b, c, a(1)'
    @dataset.order(:b){[a, a(1, 2)]}.sql.must_equal 'SELECT * FROM test ORDER BY b, a, a(1, 2)'
    @dataset.order(:b, :c){|o| [o.a, o.a(1, 2)]}.sql.must_equal 'SELECT * FROM test ORDER BY b, c, a, a(1, 2)'
  end
end

describe "Dataset#unfiltered" do
  it "should remove filtering from the dataset" do
    Sequel.mock.dataset.from(:test).filter(:score=>1).unfiltered.sql.must_equal 'SELECT * FROM test'
  end
end

describe "Dataset#unlimited" do
  it "should remove limit and offset from the dataset" do
    Sequel.mock.dataset.from(:test).limit(1, 2).unlimited.sql.must_equal 'SELECT * FROM test'
  end
end

describe "Dataset#ungrouped" do
  it "should remove group and having clauses from the dataset" do
    Sequel.mock.dataset.from(:test).group(:a).having(:b).ungrouped.sql.must_equal 'SELECT * FROM test'
  end
end

describe "Dataset#unordered" do
  it "should remove ordering from the dataset" do
    Sequel.mock.dataset.from(:test).order(:name).unordered.sql.must_equal 'SELECT * FROM test'
  end
end

describe "Dataset#with_sql" do
  before do
    @dataset = Sequel.mock.dataset.from(:test)
  end
  
  it "should use static sql" do
    @dataset.with_sql('SELECT 1 FROM test').sql.must_equal 'SELECT 1 FROM test'
  end
  
  it "should work with placeholders" do
    @dataset.with_sql('SELECT ? FROM test', 1).sql.must_equal 'SELECT 1 FROM test'
  end

  it "should work with named placeholders" do
    @dataset.with_sql('SELECT :x FROM test', :x=>1).sql.must_equal 'SELECT 1 FROM test'
  end

  it "should keep row_proc" do
    @dataset.with_sql('SELECT 1 FROM test').row_proc.must_equal @dataset.row_proc
  end

  it "should work with method symbols and arguments" do
    @dataset.with_sql(:delete_sql).sql.must_equal 'DELETE FROM test'
    @dataset.with_sql(:insert_sql, :b=>1).sql.must_equal 'INSERT INTO test (b) VALUES (1)'
    @dataset.with_sql(:update_sql, :b=>1).sql.must_equal 'UPDATE test SET b = 1'
  end
  
end

describe "Dataset#order_by" do
  before do
    @dataset = Sequel.mock.dataset.from(:test)
  end
  
  it "should include an ORDER BY clause in the select statement" do
    @dataset.order_by(:name).sql.must_equal 'SELECT * FROM test ORDER BY name'
  end
  
  it "should accept multiple arguments" do
    @dataset.order_by(:name, Sequel.desc(:price)).sql.must_equal 'SELECT * FROM test ORDER BY name, price DESC'
  end
  
  it "should override a previous ordering" do
    @dataset.order_by(:name).order(:stamp).sql.must_equal 'SELECT * FROM test ORDER BY stamp'
  end
  
  it "should accept a string" do
    @dataset.order_by(Sequel.lit('dada ASC')).sql.must_equal 'SELECT * FROM test ORDER BY dada ASC'
  end

  it "should accept a nil to remove ordering" do
    @dataset.order_by(:bah).order_by(nil).sql.must_equal 'SELECT * FROM test'
  end
end

describe "Dataset#order_more and order_append" do
  before do
    @dataset = Sequel.mock.dataset.from(:test)
  end
  
  it "should include an ORDER BY clause in the select statement" do
    @dataset.order_more(:name).sql.must_equal 'SELECT * FROM test ORDER BY name'
    @dataset.order_append(:name).sql.must_equal 'SELECT * FROM test ORDER BY name'
  end
  
  it "should add to the end of a previous ordering" do
    @dataset.order(:name).order_more(Sequel.desc(:stamp)).sql.must_equal 'SELECT * FROM test ORDER BY name, stamp DESC'
    @dataset.order(:name).order_append(Sequel.desc(:stamp)).sql.must_equal 'SELECT * FROM test ORDER BY name, stamp DESC'
  end

  it "should accept a block that yields a virtual row" do
    @dataset.order(:a).order_more{|o| o.b}.sql.must_equal 'SELECT * FROM test ORDER BY a, b'
    @dataset.order(:a, :b).order_more(:c, :d){[e, f(1, 2)]}.sql.must_equal 'SELECT * FROM test ORDER BY a, b, c, d, e, f(1, 2)'
    @dataset.order(:a).order_append{|o| o.b}.sql.must_equal 'SELECT * FROM test ORDER BY a, b'
    @dataset.order(:a, :b).order_append(:c, :d){[e, f(1, 2)]}.sql.must_equal 'SELECT * FROM test ORDER BY a, b, c, d, e, f(1, 2)'
  end
end

describe "Dataset#order_prepend" do
  before do
    @dataset = Sequel.mock.dataset.from(:test)
  end
  
  it "should include an ORDER BY clause in the select statement" do
    @dataset.order_prepend(:name).sql.must_equal 'SELECT * FROM test ORDER BY name'
  end
  
  it "should add to the beginning of a previous ordering" do
    @dataset.order(:name).order_prepend(Sequel.desc(:stamp)).sql.must_equal 'SELECT * FROM test ORDER BY stamp DESC, name'
  end

  it "should accept a block that yields a virtual row" do
    @dataset.order(:a).order_prepend{|o| o.b}.sql.must_equal 'SELECT * FROM test ORDER BY b, a'
    @dataset.order(:a, :b).order_prepend(:c, :d){[e, f(1, 2)]}.sql.must_equal 'SELECT * FROM test ORDER BY c, d, e, f(1, 2), a, b'
  end
end

describe "Dataset#reverse_order" do
  before do
    @dataset = Sequel.mock.dataset.from(:test)
  end
  
  it "should use DESC as default order" do
    @dataset.reverse_order(:name).sql.must_equal 'SELECT * FROM test ORDER BY name DESC'
  end
  
  it "should invert the order given" do
    @dataset.reverse_order(Sequel.desc(:name)).sql.must_equal 'SELECT * FROM test ORDER BY name ASC'
  end
  
  it "should invert the order for ASC expressions" do
    @dataset.reverse_order(Sequel.asc(:name)).sql.must_equal 'SELECT * FROM test ORDER BY name DESC'
  end
  
  it "should accept multiple arguments" do
    @dataset.reverse_order(:name, Sequel.desc(:price)).sql.must_equal 'SELECT * FROM test ORDER BY name DESC, price ASC'
  end

  it "should handles NULLS ordering correctly when reversing" do
    @dataset.reverse_order(Sequel.asc(:name, :nulls=>:first), Sequel.desc(:price, :nulls=>:last)).sql.must_equal 'SELECT * FROM test ORDER BY name DESC NULLS LAST, price ASC NULLS FIRST'
  end

  it "should reverse a previous ordering if no arguments are given" do
    @dataset.order(:name).reverse_order.sql.must_equal 'SELECT * FROM test ORDER BY name DESC'
    @dataset.order(Sequel.desc(:clumsy), :fool).reverse_order.sql.must_equal 'SELECT * FROM test ORDER BY clumsy ASC, fool DESC'
  end
  
  it "should return an unordered dataset for a dataset with no order" do
    @dataset.unordered.reverse_order.sql.must_equal 'SELECT * FROM test'
  end
  
  it "should have #reverse alias" do
    @dataset.order(:name).reverse.sql.must_equal 'SELECT * FROM test ORDER BY name DESC'
  end

  it "should accept a block" do
    @dataset.reverse{name}.sql.must_equal 'SELECT * FROM test ORDER BY name DESC'
    @dataset.reverse_order{name}.sql.must_equal 'SELECT * FROM test ORDER BY name DESC'
    @dataset.reverse(:foo){name}.sql.must_equal 'SELECT * FROM test ORDER BY foo DESC, name DESC'
    @dataset.reverse_order(:foo){name}.sql.must_equal 'SELECT * FROM test ORDER BY foo DESC, name DESC'
    @dataset.reverse(Sequel.desc(:foo)){name}.sql.must_equal 'SELECT * FROM test ORDER BY foo ASC, name DESC'
    @dataset.reverse_order(Sequel.desc(:foo)){name}.sql.must_equal 'SELECT * FROM test ORDER BY foo ASC, name DESC'
  end
end

describe "Dataset#limit" do
  before do
    @dataset = Sequel.mock.dataset.from(:test)
  end
  
  it "should include a LIMIT clause in the select statement" do
    @dataset.limit(10).sql.must_equal 'SELECT * FROM test LIMIT 10'
  end
  
  it "should accept ranges" do
    @dataset.limit(3..7).sql.must_equal 'SELECT * FROM test LIMIT 5 OFFSET 3'
    @dataset.limit(3...7).sql.must_equal 'SELECT * FROM test LIMIT 4 OFFSET 3'
  end
  
  it "should include an offset if a second argument is given" do
    @dataset.limit(6, 10).sql.must_equal 'SELECT * FROM test LIMIT 6 OFFSET 10'
  end
    
  it "should convert regular strings to integers" do
    @dataset.limit('6', 'a() - 1').sql.must_equal 'SELECT * FROM test LIMIT 6 OFFSET 0'
  end
  
  it "should not convert literal strings to integers" do
    @dataset.limit(Sequel.lit('6'), Sequel.lit('a() - 1')).sql.must_equal 'SELECT * FROM test LIMIT 6 OFFSET a() - 1'
  end
    
  it "should not convert other objects" do
    @dataset.limit(6, Sequel.function(:a) - 1).sql.must_equal 'SELECT * FROM test LIMIT 6 OFFSET (a() - 1)'
  end
  
  it "should be able to reset limit and offset with nil values" do
    @dataset.limit(6).limit(nil).sql.must_equal 'SELECT * FROM test'
    @dataset.limit(6, 1).limit(nil).sql.must_equal 'SELECT * FROM test OFFSET 1'
    @dataset.limit(6, 1).limit(nil, nil).sql.must_equal 'SELECT * FROM test'
  end
  
  it "should work with fixed sql datasets" do
    @dataset.opts[:sql] = 'select * from cccc'
    @dataset.limit(6, 10).sql.must_equal 'SELECT * FROM (select * from cccc) AS t1 LIMIT 6 OFFSET 10'
  end
  
  it "should raise an error if an invalid limit or offset is used" do
    proc{@dataset.limit(-1)}.must_raise(Sequel::Error)
    proc{@dataset.limit(0)}.must_raise(Sequel::Error)
    @dataset.limit(1)
    proc{@dataset.limit(1, -1)}.must_raise(Sequel::Error)
    @dataset.limit(1, 0)
    @dataset.limit(1, 1)
  end
end

describe "Dataset#offset" do
  before do
    @dataset = Sequel.mock.dataset.from(:test)
  end

  it "should include an OFFSET clause in the select statement" do
    @dataset.offset(10).sql.must_equal 'SELECT * FROM test OFFSET 10'
  end

  it "should convert regular strings to integers" do
    @dataset.offset('a() - 1').sql.must_equal 'SELECT * FROM test OFFSET 0'
  end

  it "should raise an error if a negative offset is used" do
    proc{@dataset.offset(-1)}.must_raise(Sequel::Error)
  end

  it "should be able to reset offset with nil values" do
    @dataset.offset(6).offset(nil).sql.must_equal 'SELECT * FROM test'
  end

  it "should not convert literal strings to integers" do
    @dataset.offset(Sequel.lit('a() - 1')).sql.must_equal 'SELECT * FROM test OFFSET a() - 1'
  end

  it "should not convert other objects" do
    @dataset.offset(Sequel.function(:a) - 1).sql.must_equal 'SELECT * FROM test OFFSET (a() - 1)'
  end

  it "should override offset given to limit" do
    @dataset.limit(nil, 5).offset(6).sql.must_equal 'SELECT * FROM test OFFSET 6'
  end

  it "should not be overridable by limit if limit is not given an offset" do
    @dataset.offset(6).limit(nil).sql.must_equal 'SELECT * FROM test OFFSET 6'
  end

  it "should be overridable by limit if limit is given an offset" do
    @dataset.offset(6).limit(nil, nil).sql.must_equal 'SELECT * FROM test'
    @dataset.offset(6).limit(nil, 5).sql.must_equal 'SELECT * FROM test OFFSET 5'
  end
end

describe "Dataset#naked" do
  it "should returned clone dataset without row_proc" do
    d = Sequel.mock.dataset
    d.row_proc = Proc.new{|r| r}
    d.naked.row_proc.must_equal nil
    refute_equal nil, d.row_proc
  end
end

describe "Dataset#naked!" do
  it "should remove any existing row_proc" do
    d = Sequel.mock.dataset
    d.row_proc = Proc.new{|r| r}
    d.naked!.row_proc.must_equal nil
    d.row_proc.must_equal nil
  end
end

describe "Dataset#qualified_column_name" do
  before do
    @dataset = Sequel.mock.dataset.from(:test)
  end
  
  it "should return the literal value if not given a symbol" do
    @dataset.literal(@dataset.send(:qualified_column_name, 'ccc__b', :items)).must_equal "'ccc__b'"
    @dataset.literal(@dataset.send(:qualified_column_name, 3, :items)).must_equal '3'
    @dataset.literal(@dataset.send(:qualified_column_name, Sequel.lit('a'), :items)).must_equal 'a'
  end
  
  it "should qualify the column with the supplied table name if given an unqualified symbol" do
    @dataset.literal(@dataset.send(:qualified_column_name, :b1, :items)).must_equal 'items.b1'
  end

  it "should not changed the qualifed column's table if given a qualified symbol" do
    @dataset.literal(@dataset.send(:qualified_column_name, :ccc__b, :items)).must_equal 'ccc.b'
  end

  it "should handle an aliased identifier" do
    @dataset.literal(@dataset.send(:qualified_column_name, :ccc, Sequel.expr(:items).as(:i))).must_equal 'i.ccc'
  end
end

describe "Dataset#map" do
  before do
    @d = Sequel.mock(:fetch=>[{:a => 1, :b => 2}, {:a => 3, :b => 4}, {:a => 5, :b => 6}])[:items]
  end
  
  it "should provide the usual functionality if no argument is given" do
    @d.map{|n| n[:a] + n[:b]}.must_equal [3, 7, 11]
  end
  
  it "should map using #[column name] if column name is given" do
    @d.map(:a).must_equal [1, 3, 5]
  end
  
  it "should support multiple column names if an array of column names is given" do
    @d.map([:a, :b]).must_equal [[1, 2], [3, 4], [5, 6]]
  end
  
  it "should not call the row_proc if an argument is given" do
    @d.row_proc = proc{|r| h = {}; r.keys.each{|k| h[k] = r[k] * 2}; h}
    @d.map(:a).must_equal [1, 3, 5]
    @d.map([:a, :b]).must_equal [[1, 2], [3, 4], [5, 6]]
  end

  it "should call the row_proc if no argument is given" do
    @d.row_proc = proc{|r| h = {}; r.keys.each{|k| h[k] = r[k] * 2}; h}
    @d.map{|n| n[:a] + n[:b]}.must_equal [6, 14, 22]
  end
  
  it "should return the complete dataset values if nothing is given" do
    @d.map.to_a.must_equal [{:a => 1, :b => 2}, {:a => 3, :b => 4}, {:a => 5, :b => 6}]
  end
end

describe "Dataset#to_hash" do
  before do
    @d = Sequel.mock(:fetch=>[{:a => 1, :b => 2}, {:a => 3, :b => 4}, {:a => 5, :b => 6}])[:items]
  end
  
  it "should provide a hash with the first column as key and the second as value" do
    @d.to_hash(:a, :b).must_equal(1 => 2, 3 => 4, 5 => 6)
    @d.to_hash(:b, :a).must_equal(2 => 1, 4 => 3, 6 => 5)
  end
  
  it "should provide a hash with the first column as key and the entire hash as value if the value column is blank or nil" do
    @d.to_hash(:a).must_equal(1 => {:a => 1, :b => 2}, 3 => {:a => 3, :b => 4}, 5 => {:a => 5, :b => 6})
    @d.to_hash(:b).must_equal(2 => {:a => 1, :b => 2}, 4 => {:a => 3, :b => 4}, 6 => {:a => 5, :b => 6})
  end

  it "should support using an array of columns as either the key or the value" do
    @d.to_hash([:a, :b], :b).must_equal([1, 2] => 2, [3, 4] => 4, [5, 6] => 6)
    @d.to_hash(:b, [:a, :b]).must_equal(2 => [1, 2], 4 => [3, 4], 6 => [5, 6])
    @d.to_hash([:b, :a], [:a, :b]).must_equal([2, 1] => [1, 2], [4, 3] => [3, 4], [6, 5] => [5, 6])
    @d.to_hash([:a, :b]).must_equal([1, 2] => {:a => 1, :b => 2}, [3, 4] => {:a => 3, :b => 4}, [5, 6] => {:a => 5, :b => 6})
  end

  it "should not call the row_proc if two arguments are given" do
    @d.row_proc = proc{|r| h = {}; r.keys.each{|k| h[k] = r[k] * 2}; h}
    @d.to_hash(:a, :b).must_equal(1 => 2, 3 => 4, 5 => 6)
    @d.to_hash(:b, :a).must_equal(2 => 1, 4 => 3, 6 => 5)
    @d.to_hash([:a, :b], :b).must_equal([1, 2] => 2, [3, 4] => 4, [5, 6] => 6)
    @d.to_hash(:b, [:a, :b]).must_equal(2 => [1, 2], 4 => [3, 4], 6 => [5, 6])
    @d.to_hash([:b, :a], [:a, :b]).must_equal([2, 1] => [1, 2], [4, 3] => [3, 4], [6, 5] => [5, 6])
  end

  it "should call the row_proc if only a single argument is given" do
    @d.row_proc = proc{|r| h = {}; r.keys.each{|k| h[k] = r[k] * 2}; h}
    @d.to_hash(:a).must_equal(2 => {:a => 2, :b => 4}, 6 => {:a => 6, :b => 8}, 10 => {:a => 10, :b => 12})
    @d.to_hash(:b).must_equal(4 => {:a => 2, :b => 4}, 8 => {:a => 6, :b => 8}, 12 => {:a => 10, :b => 12})
    @d.to_hash([:a, :b]).must_equal([2, 4] => {:a => 2, :b => 4}, [6, 8] => {:a => 6, :b => 8}, [10, 12] => {:a => 10, :b => 12})
  end

  it "should handle a single composite key when using a row_proc" do
    c = @d.row_proc = Class.new do
      def self.call(h); new(h); end
      def initialize(h); @h = h; end
      def [](k) @h[k]; end
      def h; @h; end
      def ==(o) @h == o.h; end
    end
    @d.to_hash([:a, :b]).must_equal([1, 2] => c.call(:a => 1, :b => 2), [3, 4] => c.call(:a => 3, :b => 4), [5, 6] => c.call(:a => 5, :b => 6))
  end
end

describe "Dataset#to_hash_groups" do
  before do
    @d = Sequel.mock(:fetch=>[{:a => 1, :b => 2}, {:a => 3, :b => 4}, {:a => 1, :b => 6}, {:a => 7, :b => 4}])[:items]
  end
  
  it "should provide a hash with the first column as key and the second as arrays of matching values" do
    @d.to_hash_groups(:a, :b).must_equal(1 => [2, 6], 3 => [4], 7 => [4])
    @d.to_hash_groups(:b, :a).must_equal(2 => [1], 4=>[3, 7], 6=>[1])
  end
  
  it "should provide a hash with the first column as key and the entire hash as value if the value column is blank or nil" do
    @d.to_hash_groups(:a).must_equal(1 => [{:a => 1, :b => 2}, {:a => 1, :b => 6}], 3 => [{:a => 3, :b => 4}], 7 => [{:a => 7, :b => 4}])
    @d.to_hash_groups(:b).must_equal(2 => [{:a => 1, :b => 2}], 4 => [{:a => 3, :b => 4}, {:a => 7, :b => 4}], 6 => [{:a => 1, :b => 6}])
  end

  it "should support using an array of columns as either the key or the value" do
    @d.to_hash_groups([:a, :b], :b).must_equal([1, 2] => [2], [3, 4] => [4], [1, 6] => [6], [7, 4]=>[4])
    @d.to_hash_groups(:b, [:a, :b]).must_equal(2 => [[1, 2]], 4 => [[3, 4], [7, 4]], 6 => [[1, 6]])
    @d.to_hash_groups([:b, :a], [:a, :b]).must_equal([2, 1] => [[1, 2]], [4, 3] => [[3, 4]], [6, 1] => [[1, 6]], [4, 7]=>[[7, 4]])
    @d.to_hash_groups([:a, :b]).must_equal([1, 2] => [{:a => 1, :b => 2}], [3, 4] => [{:a => 3, :b => 4}], [1, 6] => [{:a => 1, :b => 6}], [7, 4] => [{:a => 7, :b => 4}])
  end

  it "should not call the row_proc if two arguments are given" do
    @d.row_proc = proc{|r| h = {}; r.keys.each{|k| h[k] = r[k] * 2}; h}
    @d.to_hash_groups(:a, :b).must_equal(1 => [2, 6], 3 => [4], 7 => [4])
    @d.to_hash_groups(:b, :a).must_equal(2 => [1], 4=>[3, 7], 6=>[1])
    @d.to_hash_groups([:a, :b], :b).must_equal([1, 2] => [2], [3, 4] => [4], [1, 6] => [6], [7, 4]=>[4])
    @d.to_hash_groups(:b, [:a, :b]).must_equal(2 => [[1, 2]], 4 => [[3, 4], [7, 4]], 6 => [[1, 6]])
    @d.to_hash_groups([:b, :a], [:a, :b]).must_equal([2, 1] => [[1, 2]], [4, 3] => [[3, 4]], [6, 1] => [[1, 6]], [4, 7]=>[[7, 4]])
  end

  it "should call the row_proc if only a single argument is given" do
    @d.row_proc = proc{|r| h = {}; r.keys.each{|k| h[k] = r[k] * 2}; h}
    @d.to_hash_groups(:a).must_equal(2 => [{:a => 2, :b => 4}, {:a => 2, :b => 12}], 6 => [{:a => 6, :b => 8}], 14 => [{:a => 14, :b => 8}])
    @d.to_hash_groups(:b).must_equal(4 => [{:a => 2, :b => 4}], 8 => [{:a => 6, :b => 8}, {:a => 14, :b => 8}], 12 => [{:a => 2, :b => 12}])
    @d.to_hash_groups([:a, :b]).must_equal([2, 4] => [{:a => 2, :b => 4}], [6, 8] => [{:a => 6, :b => 8}], [2, 12] => [{:a => 2, :b => 12}], [14, 8] => [{:a => 14, :b => 8}])
  end

  it "should handle a single composite key when using a row_proc" do
    c = @d.row_proc = Class.new do
      def self.call(h); new(h); end
      def initialize(h); @h = h; end
      def [](k) @h[k]; end
      def h; @h; end
      def ==(o) @h == o.h; end
    end
    @d.to_hash_groups([:a, :b]).must_equal([1, 2] => [c.call(:a => 1, :b => 2)], [3, 4] => [c.call(:a => 3, :b => 4)], [1, 6] => [c.call(:a => 1, :b => 6)], [7, 4] => [c.call(:a => 7, :b => 4)])
  end
end

describe "Dataset#distinct" do
  before do
    @db = Sequel.mock
    @dataset = @db[:test].select(:name)
  end
  
  it "should include DISTINCT clause in statement" do
    @dataset.distinct.sql.must_equal 'SELECT DISTINCT name FROM test'
  end
  
  it "should raise an error if columns given and DISTINCT ON is not supported" do
    @dataset.distinct
    proc{@dataset.distinct(:a)}.must_raise(Sequel::InvalidOperation)
  end
  
  it "should use DISTINCT ON if columns are given and DISTINCT ON is supported" do
    meta_def(@dataset, :supports_distinct_on?){true}
    @dataset.distinct(:a, :b).sql.must_equal 'SELECT DISTINCT ON (a, b) name FROM test'
    @dataset.distinct(Sequel.cast(:stamp, :integer), :node_id=>nil).sql.must_equal 'SELECT DISTINCT ON (CAST(stamp AS integer), (node_id IS NULL)) name FROM test'
  end

  it "should use DISTINCT ON if columns are given in a virtual row block and DISTINCT ON is supported" do
    meta_def(@dataset, :supports_distinct_on?){true}
    @dataset.distinct{func(:id)}.sql.must_equal 'SELECT DISTINCT ON (func(id)) name FROM test'
  end

  it "should do a subselect for count" do
    @dataset.distinct.count
    @db.sqls.must_equal ['SELECT count(*) AS count FROM (SELECT DISTINCT name FROM test) AS t1 LIMIT 1']
  end
end

describe "Dataset#count" do
  before do
    @db = Sequel.mock(:fetch=>{:count=>1})
    @dataset = @db.from(:test).columns(:count)
  end
  
  it "should format SQL properly" do
    @dataset.count.must_equal 1
    @db.sqls.must_equal ['SELECT count(*) AS count FROM test LIMIT 1']
  end
  
  it "should accept an argument" do
    @dataset.count(:foo).must_equal 1
    @db.sqls.must_equal ['SELECT count(foo) AS count FROM test LIMIT 1']
  end
  
  it "should work with a nil argument" do
    @dataset.count(nil).must_equal 1
    @db.sqls.must_equal ['SELECT count(NULL) AS count FROM test LIMIT 1']
  end
  
  it "should accept a virtual row block" do
    @dataset.count{foo(bar)}.must_equal 1
    @db.sqls.must_equal ['SELECT count(foo(bar)) AS count FROM test LIMIT 1']
  end
  
  it "should raise an Error if given an argument and a block" do
    proc{@dataset.count(:foo){foo(bar)}}.must_raise(Sequel::Error)
  end
  
  it "should include the where clause if it's there" do
    @dataset.filter(Sequel.expr(:abc) < 30).count.must_equal 1
    @db.sqls.must_equal ['SELECT count(*) AS count FROM test WHERE (abc < 30) LIMIT 1']
  end
  
  it "should count properly for datasets with fixed sql" do
    @dataset.opts[:sql] = "select abc from xyz"
    @dataset.count.must_equal 1
    @db.sqls.must_equal ["SELECT count(*) AS count FROM (select abc from xyz) AS t1 LIMIT 1"]
  end

  it "should count properly when using UNION, INTERSECT, or EXCEPT" do
    @dataset.union(@dataset).count.must_equal 1
    @db.sqls.must_equal ["SELECT count(*) AS count FROM (SELECT * FROM test UNION SELECT * FROM test) AS t1 LIMIT 1"]
    @dataset.intersect(@dataset).count.must_equal 1
    @db.sqls.must_equal ["SELECT count(*) AS count FROM (SELECT * FROM test INTERSECT SELECT * FROM test) AS t1 LIMIT 1"]
    @dataset.except(@dataset).count.must_equal 1
    @db.sqls.must_equal ["SELECT count(*) AS count FROM (SELECT * FROM test EXCEPT SELECT * FROM test) AS t1 LIMIT 1"]
  end

  it "should return limit if count is greater than it" do
    @dataset.limit(5).count.must_equal 1
    @db.sqls.must_equal ["SELECT count(*) AS count FROM (SELECT * FROM test LIMIT 5) AS t1 LIMIT 1"]
  end
  
  it "should work correctly with offsets" do
    @dataset.limit(nil, 5).count.must_equal 1
    @db.sqls.must_equal ["SELECT count(*) AS count FROM (SELECT * FROM test OFFSET 5) AS t1 LIMIT 1"]
  end
  
  it "should work on a graphed_dataset" do
    def @dataset.columns
      [:a]
    end
    @dataset.graph(@dataset, [:a], :table_alias=>:test2).count.must_equal 1
    @db.sqls.must_equal ['SELECT count(*) AS count FROM test LEFT OUTER JOIN test AS test2 USING (a) LIMIT 1']
  end

  it "should not cache the columns value" do
    ds = @dataset.from(:blah).columns(:a)
    ds.columns.must_equal [:a]
    ds.count.must_equal 1
    @db.sqls.must_equal ['SELECT count(*) AS count FROM blah LIMIT 1']
    ds.columns.must_equal [:a]
  end
end

describe "Dataset#group_and_count" do
  before do
    @ds = Sequel.mock.dataset.from(:test)
  end
  
  it "should format SQL properly" do
    @ds.group_and_count(:name).sql.must_equal "SELECT name, count(*) AS count FROM test GROUP BY name"
  end

  it "should accept multiple columns for grouping" do
    @ds.group_and_count(:a, :b).sql.must_equal "SELECT a, b, count(*) AS count FROM test GROUP BY a, b"
  end

  it "should format column aliases in the select clause but not in the group clause" do
    @ds.group_and_count(:name___n).sql.must_equal "SELECT name AS n, count(*) AS count FROM test GROUP BY name"
    @ds.group_and_count(:name__n).sql.must_equal "SELECT name.n, count(*) AS count FROM test GROUP BY name.n"
  end

  it "should handle identifiers" do
    @ds.group_and_count(Sequel.identifier(:name___n)).sql.must_equal "SELECT name___n, count(*) AS count FROM test GROUP BY name___n"
  end

  it "should handle literal strings" do
    @ds.group_and_count(Sequel.lit("name")).sql.must_equal "SELECT name, count(*) AS count FROM test GROUP BY name"
  end

  it "should handle aliased expressions" do
    @ds.group_and_count(Sequel.expr(:name).as(:n)).sql.must_equal "SELECT name AS n, count(*) AS count FROM test GROUP BY name"
    @ds.group_and_count(Sequel.identifier(:name).as(:n)).sql.must_equal "SELECT name AS n, count(*) AS count FROM test GROUP BY name"
  end

  it "should take a virtual row block" do
    @ds.group_and_count{(type_id > 1).as(t)}.sql.must_equal "SELECT (type_id > 1) AS t, count(*) AS count FROM test GROUP BY (type_id > 1)"
    @ds.group_and_count{[(type_id > 1).as(t), type_id < 2]}.sql.must_equal "SELECT (type_id > 1) AS t, (type_id < 2), count(*) AS count FROM test GROUP BY (type_id > 1), (type_id < 2)"
    @ds.group_and_count(:foo){type_id > 1}.sql.must_equal "SELECT foo, (type_id > 1), count(*) AS count FROM test GROUP BY foo, (type_id > 1)"
  end
end

describe "Dataset#empty?" do
  it "should return true if no records exist in the dataset" do
    db = Sequel.mock(:fetch=>proc{|sql| {1=>1} unless sql =~ /WHERE 'f'/})
    db.from(:test).wont_be :empty?
    db.sqls.must_equal ['SELECT 1 AS one FROM test LIMIT 1']
    db.from(:test).filter(false).must_be :empty?
    db.sqls.must_equal ["SELECT 1 AS one FROM test WHERE 'f' LIMIT 1"]
  end

  it "should ignore order" do
    db = Sequel.mock(:fetch=>proc{|sql| {1=>1}})
    db.from(:test).wont_be :empty?
    without_order = db.sqls
    db.from(:test).order(:the_order_column).wont_be :empty?
    with_order = db.sqls
    without_order.must_equal with_order
  end
end

describe "Dataset#first_source_alias" do
  before do
    @ds = Sequel.mock.dataset
  end
  
  it "should be the entire first source if not aliased" do
    @ds.from(:t).first_source_alias.must_equal :t
    @ds.from(Sequel.identifier(:t__a)).first_source_alias.must_equal Sequel.identifier(:t__a)
    @ds.from(:s__t).first_source_alias.must_equal :s__t
    @ds.from(Sequel.qualify(:s, :t)).first_source_alias.must_equal Sequel.qualify(:s, :t)
  end
  
  it "should be the alias if aliased" do
    @ds.from(:t___a).first_source_alias.must_equal :a
    @ds.from(:s__t___a).first_source_alias.must_equal :a
    @ds.from(Sequel.expr(:t).as(:a)).first_source_alias.must_equal :a
  end
  
  it "should be aliased as first_source" do
    @ds.from(:t).first_source.must_equal :t
    @ds.from(Sequel.identifier(:t__a)).first_source.must_equal Sequel.identifier(:t__a)
    @ds.from(:s__t___a).first_source.must_equal :a
    @ds.from(Sequel.expr(:t).as(:a)).first_source.must_equal :a
  end
  
  it "should raise exception if table doesn't have a source" do
    proc{@ds.first_source_alias}.must_raise(Sequel::Error)
  end
end

describe "Dataset#first_source_table" do
  before do
    @ds = Sequel.mock.dataset
  end
  
  it "should be the entire first source if not aliased" do
    @ds.from(:t).first_source_table.must_equal :t
    @ds.from(Sequel.identifier(:t__a)).first_source_table.must_equal Sequel.identifier(:t__a)
    @ds.from(:s__t).first_source_table.must_equal :s__t
    @ds.from(Sequel.qualify(:s, :t)).first_source_table.must_equal Sequel.qualify(:s, :t)
  end
  
  it "should be the unaliased part if aliased" do
    @ds.literal(@ds.from(:t___a).first_source_table).must_equal "t"
    @ds.literal(@ds.from(:s__t___a).first_source_table).must_equal "s.t"
    @ds.literal(@ds.from(Sequel.expr(:t).as(:a)).first_source_table).must_equal "t"
  end
  
  it "should raise exception if table doesn't have a source" do
    proc{@ds.first_source_table}.must_raise(Sequel::Error)
  end
end

describe "Dataset#from_self" do
  before do
    @ds = Sequel.mock.dataset.from(:test).select(:name).limit(1)
  end
  
  it "should set up a default alias" do
    @ds.from_self.sql.must_equal 'SELECT * FROM (SELECT name FROM test LIMIT 1) AS t1'
  end
  
  it "should modify only the new dataset" do
    @ds.from_self.select(:bogus).sql.must_equal 'SELECT bogus FROM (SELECT name FROM test LIMIT 1) AS t1'
  end
  
  it "should use the user-specified alias" do
    @ds.from_self(:alias=>:some_name).sql.must_equal 'SELECT * FROM (SELECT name FROM test LIMIT 1) AS some_name'
  end
  
  it "should use the user-specified column aliases" do
    @ds.from_self(:alias=>:some_name, :column_aliases=>[:c1, :c2]).sql.must_equal 'SELECT * FROM (SELECT name FROM test LIMIT 1) AS some_name(c1, c2)'
  end
  
  it "should use the user-specified alias for joins" do
    @ds.from_self(:alias=>:some_name).inner_join(:posts, :alias=>:name).sql.must_equal \
      'SELECT * FROM (SELECT name FROM test LIMIT 1) AS some_name INNER JOIN posts ON (posts.alias = some_name.name)'
  end
  
  it "should not remove non-SQL options such as :server" do
    @ds.server(:blah).from_self(:alias=>:some_name).opts[:server].must_equal :blah
  end

  it "should hoist WITH clauses in current dataset if dataset doesn't support WITH in subselect" do
    ds = Sequel.mock.dataset
    meta_def(ds, :supports_cte?){true}
    meta_def(ds, :supports_cte_in_subselect?){false}
    ds.from(:a).with(:a, ds.from(:b)).from_self.sql.must_equal 'WITH a AS (SELECT * FROM b) SELECT * FROM (SELECT * FROM a) AS t1'
    ds.from(:a, :c).with(:a, ds.from(:b)).with(:c, ds.from(:d)).from_self.sql.must_equal 'WITH a AS (SELECT * FROM b), c AS (SELECT * FROM d) SELECT * FROM (SELECT * FROM a, c) AS t1'
  end

  it "should have working mutation method" do
    @ds.from_self!
    @ds.sql.must_equal 'SELECT * FROM (SELECT name FROM test LIMIT 1) AS t1'
  end
end

describe "Dataset#join_table" do
  before do
    @d = Sequel.mock.dataset.from(:items)
    @d.quote_identifiers = true
  end
  
  it "should format the JOIN clause properly" do
    @d.join_table(:left_outer, :categories, :category_id => :id).sql.must_equal 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."category_id" = "items"."id")'
  end
  
  it "should handle multiple conditions on the same join table column" do
    @d.join_table(:left_outer, :categories, [[:category_id, :id], [:category_id, 0..100]]).sql.
      must_equal 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON (("categories"."category_id" = "items"."id") AND ("categories"."category_id" >= 0) AND ("categories"."category_id" <= 100))'
  end
  
  it "should include WHERE clause if applicable" do
    @d.filter(Sequel.expr(:price) < 100).join_table(:right_outer, :categories, :category_id => :id).sql.
      must_equal 'SELECT * FROM "items" RIGHT OUTER JOIN "categories" ON ("categories"."category_id" = "items"."id") WHERE ("price" < 100)'
  end
  
  it "should include ORDER BY clause if applicable" do
    @d.order(:stamp).join_table(:full_outer, :categories, :category_id => :id).sql.must_equal 'SELECT * FROM "items" FULL OUTER JOIN "categories" ON ("categories"."category_id" = "items"."id") ORDER BY "stamp"'
  end
  
  it "should support multiple joins" do
    @d.join_table(:inner, :b, :items_id=>:id).join_table(:left_outer, :c, :b_id => :b__id).sql.must_equal 'SELECT * FROM "items" INNER JOIN "b" ON ("b"."items_id" = "items"."id") LEFT OUTER JOIN "c" ON ("c"."b_id" = "b"."id")'
  end

  it "should handle LATERAL subqueries" do
    @d.join(@d.lateral, :a=>:b).select_sql.must_equal 'SELECT * FROM "items" INNER JOIN LATERAL (SELECT * FROM "items") AS "t1" ON ("t1"."a" = "items"."b")'
    @d.left_join(@d.lateral, :a=>:b).select_sql.must_equal 'SELECT * FROM "items" LEFT JOIN LATERAL (SELECT * FROM "items") AS "t1" ON ("t1"."a" = "items"."b")'
    @d.cross_join(@d.lateral).select_sql.must_equal 'SELECT * FROM "items" CROSS JOIN LATERAL (SELECT * FROM "items") AS "t1"'
  end
  
  it "should support arbitrary join types" do
    @d.join_table(:magic, :categories, :category_id=>:id).sql.must_equal 'SELECT * FROM "items" MAGIC JOIN "categories" ON ("categories"."category_id" = "items"."id")'
  end

  it "should support many join methods" do
    @d.left_outer_join(:categories, :category_id=>:id).sql.must_equal 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."category_id" = "items"."id")'
    @d.right_outer_join(:categories, :category_id=>:id).sql.must_equal 'SELECT * FROM "items" RIGHT OUTER JOIN "categories" ON ("categories"."category_id" = "items"."id")'
    @d.full_outer_join(:categories, :category_id=>:id).sql.must_equal 'SELECT * FROM "items" FULL OUTER JOIN "categories" ON ("categories"."category_id" = "items"."id")'
    @d.inner_join(:categories, :category_id=>:id).sql.must_equal 'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."category_id" = "items"."id")'
    @d.left_join(:categories, :category_id=>:id).sql.must_equal 'SELECT * FROM "items" LEFT JOIN "categories" ON ("categories"."category_id" = "items"."id")'
    @d.right_join(:categories, :category_id=>:id).sql.must_equal 'SELECT * FROM "items" RIGHT JOIN "categories" ON ("categories"."category_id" = "items"."id")'
    @d.full_join(:categories, :category_id=>:id).sql.must_equal 'SELECT * FROM "items" FULL JOIN "categories" ON ("categories"."category_id" = "items"."id")'
    @d.natural_join(:categories).sql.must_equal 'SELECT * FROM "items" NATURAL JOIN "categories"'
    @d.natural_left_join(:categories).sql.must_equal 'SELECT * FROM "items" NATURAL LEFT JOIN "categories"'
    @d.natural_right_join(:categories).sql.must_equal 'SELECT * FROM "items" NATURAL RIGHT JOIN "categories"'
    @d.natural_full_join(:categories).sql.must_equal 'SELECT * FROM "items" NATURAL FULL JOIN "categories"'
    @d.cross_join(:categories).sql.must_equal 'SELECT * FROM "items" CROSS JOIN "categories"'
  end
  
  it "should support options hashes for join methods that don't take conditions" do
    @d.natural_join(:categories, :table_alias=>:a).sql.must_equal 'SELECT * FROM "items" NATURAL JOIN "categories" AS "a"'
    @d.natural_left_join(:categories, :table_alias=>:a).sql.must_equal 'SELECT * FROM "items" NATURAL LEFT JOIN "categories" AS "a"'
    @d.natural_right_join(:categories, :table_alias=>:a).sql.must_equal 'SELECT * FROM "items" NATURAL RIGHT JOIN "categories" AS "a"'
    @d.natural_full_join(:categories, :table_alias=>:a).sql.must_equal 'SELECT * FROM "items" NATURAL FULL JOIN "categories" AS "a"'
    @d.cross_join(:categories, :table_alias=>:a).sql.must_equal 'SELECT * FROM "items" CROSS JOIN "categories" AS "a"'
  end

  it "should raise an error if non-hash arguments are provided to join methods that don't take conditions" do
    proc{@d.natural_join(:categories, nil)}.must_raise(Sequel::Error)
    proc{@d.natural_left_join(:categories, nil)}.must_raise(Sequel::Error)
    proc{@d.natural_right_join(:categories, nil)}.must_raise(Sequel::Error)
    proc{@d.natural_full_join(:categories, nil)}.must_raise(Sequel::Error)
    proc{@d.cross_join(:categories, nil)}.must_raise(Sequel::Error)
  end

  it "should raise an error if blocks are provided to join methods that don't pass them" do
    proc{@d.natural_join(:categories){}}.must_raise(Sequel::Error)
    proc{@d.natural_left_join(:categories){}}.must_raise(Sequel::Error)
    proc{@d.natural_right_join(:categories){}}.must_raise(Sequel::Error)
    proc{@d.natural_full_join(:categories){}}.must_raise(Sequel::Error)
    proc{@d.cross_join(:categories){}}.must_raise(Sequel::Error)
  end

  it "should default to a plain join if nil is used for the type" do
    @d.join_table(nil, :categories, :category_id=>:id).sql.must_equal 'SELECT * FROM "items"  JOIN "categories" ON ("categories"."category_id" = "items"."id")'
  end

  it "should use an inner join for Dataset#join" do
    @d.join(:categories, :category_id=>:id).sql.must_equal 'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."category_id" = "items"."id")'
  end
  
  it "should support aliased tables using the :table_alias option" do
    @d.from('stats').join('players', {:id => :player_id}, :table_alias=>:p).sql.must_equal 'SELECT * FROM "stats" INNER JOIN "players" AS "p" ON ("p"."id" = "stats"."player_id")'
  end
  
  it "should support aliased tables using an implicit alias" do
    @d.from('stats').join(Sequel.expr(:players).as(:p), {:id => :player_id}).sql.must_equal 'SELECT * FROM "stats" INNER JOIN "players" AS "p" ON ("p"."id" = "stats"."player_id")'
  end
  
  it "should support aliased tables with an implicit column aliases" do
    @d.from('stats').join(Sequel.expr(:players).as(:p, [:c1, :c2]), {:id => :player_id}).sql.must_equal 'SELECT * FROM "stats" INNER JOIN "players" AS "p"("c1", "c2") ON ("p"."id" = "stats"."player_id")'
  end
  
  it "should support using an alias for the FROM when doing the first join with unqualified condition columns" do
    @d.from(Sequel.as(:foo, :f)).join_table(:inner, :bar, :id => :bar_id).sql.must_equal 'SELECT * FROM "foo" AS "f" INNER JOIN "bar" ON ("bar"."id" = "f"."bar_id")'
  end
  
  it "should support implicit schemas in from table symbols" do
    @d.from(:s__t).join(:u__v, {:id => :player_id}).sql.must_equal 'SELECT * FROM "s"."t" INNER JOIN "u"."v" ON ("u"."v"."id" = "s"."t"."player_id")'
  end

  it "should support implicit aliases in from table symbols" do
    @d.from(:t___z).join(:v___y, {:id => :player_id}).sql.must_equal 'SELECT * FROM "t" AS "z" INNER JOIN "v" AS "y" ON ("y"."id" = "z"."player_id")'
    @d.from(:s__t___z).join(:u__v___y, {:id => :player_id}).sql.must_equal 'SELECT * FROM "s"."t" AS "z" INNER JOIN "u"."v" AS "y" ON ("y"."id" = "z"."player_id")'
  end
  
  it "should support AliasedExpressions" do
    @d.from(Sequel.expr(:s).as(:t)).join(Sequel.expr(:u).as(:v), {:id => :player_id}).sql.must_equal 'SELECT * FROM "s" AS "t" INNER JOIN "u" AS "v" ON ("v"."id" = "t"."player_id")'
  end

  it "should support the :implicit_qualifier option" do
    @d.from('stats').join('players', {:id => :player_id}, :implicit_qualifier=>:p).sql.must_equal 'SELECT * FROM "stats" INNER JOIN "players" ON ("players"."id" = "p"."player_id")'
  end
  
  it "should support the :reset_implicit_qualifier option" do
    @d.from(:stats).join(:a, [:b], :reset_implicit_qualifier=>false).join(:players, {:id => :player_id}).sql.must_equal 'SELECT * FROM "stats" INNER JOIN "a" USING ("b") INNER JOIN "players" ON ("players"."id" = "stats"."player_id")'
  end
  
  it "should default :qualify option to default_join_table_qualification" do
    def @d.default_join_table_qualification; false; end
    @d.from('stats').join(:players, :id => :player_id).sql.must_equal 'SELECT * FROM "stats" INNER JOIN "players" ON ("id" = "player_id")'
  end
  
  it "should not qualify if :qualify=>false option is given" do
    @d.from('stats').join(:players, {:id => :player_id}, :qualify=>false).sql.must_equal 'SELECT * FROM "stats" INNER JOIN "players" ON ("id" = "player_id")'
  end
  
  it "should do deep qualification if :qualify=>:deep option is given" do
    @d.from('stats').join(:players, {Sequel.function(:f, :id) => Sequel.subscript(:player_id, 0)}, :qualify=>:deep).sql.must_equal 'SELECT * FROM "stats" INNER JOIN "players" ON (f("players"."id") = "stats"."player_id"[0])'
  end
  
  it "should do only qualification if :qualify=>:symbol option is given" do
    @d.from('stats').join(:players, {Sequel.function(:f, :id) => :player_id}, :qualify=>:symbol).sql.must_equal 'SELECT * FROM "stats" INNER JOIN "players" ON (f("id") = "stats"."player_id")'
  end
  
  it "should allow for arbitrary conditions in the JOIN clause" do
    @d.join_table(:left_outer, :categories, :status => 0).sql.must_equal 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."status" = 0)'
    @d.join_table(:left_outer, :categories, :categorizable_type => "Post").sql.must_equal 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."categorizable_type" = \'Post\')'
    @d.join_table(:left_outer, :categories, :timestamp => Sequel::CURRENT_TIMESTAMP).sql.must_equal 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."timestamp" = CURRENT_TIMESTAMP)'
    @d.join_table(:left_outer, :categories, :status => [1, 2, 3]).sql.must_equal 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."status" IN (1, 2, 3))'
  end
  
  it "should raise error for a table without a source" do
    proc {Sequel.mock.dataset.join('players', :id => :player_id)}.must_raise(Sequel::Error)
  end

  it "should support joining datasets" do
    ds = Sequel.mock.dataset.from(:categories)
    @d.join_table(:left_outer, ds, :item_id => :id).sql.must_equal 'SELECT * FROM "items" LEFT OUTER JOIN (SELECT * FROM categories) AS "t1" ON ("t1"."item_id" = "items"."id")'
    ds.filter!(:active => true)
    @d.join_table(:left_outer, ds, :item_id => :id).sql.must_equal 'SELECT * FROM "items" LEFT OUTER JOIN (SELECT * FROM categories WHERE (active IS TRUE)) AS "t1" ON ("t1"."item_id" = "items"."id")'
    @d.from_self.join_table(:left_outer, ds, :item_id => :id).sql.must_equal 'SELECT * FROM (SELECT * FROM "items") AS "t1" LEFT OUTER JOIN (SELECT * FROM categories WHERE (active IS TRUE)) AS "t2" ON ("t2"."item_id" = "t1"."id")'
  end
  
  it "should support joining datasets and aliasing the join" do
    ds = Sequel.mock.dataset.from(:categories)
    @d.join_table(:left_outer, ds, {:ds__item_id => :id}, :table_alias=>:ds).sql.must_equal 'SELECT * FROM "items" LEFT OUTER JOIN (SELECT * FROM categories) AS "ds" ON ("ds"."item_id" = "items"."id")'      
  end
  
  it "should support joining multiple datasets" do
    ds = Sequel.mock.dataset.from(:categories)
    ds2 = Sequel.mock.dataset.from(:nodes).select(:name)
    ds3 = Sequel.mock.dataset.from(:attributes).filter("name = 'blah'")

    @d.join_table(:left_outer, ds, :item_id => :id).join_table(:inner, ds2, :node_id=>:id).join_table(:right_outer, ds3, :attribute_id=>:id).sql.
      must_equal 'SELECT * FROM "items" LEFT OUTER JOIN (SELECT * FROM categories) AS "t1" ON ("t1"."item_id" = "items"."id") ' \
      'INNER JOIN (SELECT name FROM nodes) AS "t2" ON ("t2"."node_id" = "t1"."id") ' \
      'RIGHT OUTER JOIN (SELECT * FROM attributes WHERE (name = \'blah\')) AS "t3" ON ("t3"."attribute_id" = "t2"."id")'
  end

  it "should support using an SQL String as the join condition" do
    @d.join(:categories, "c.item_id = items.id", :table_alias=>:c).sql.must_equal 'SELECT * FROM "items" INNER JOIN "categories" AS "c" ON (c.item_id = items.id)'
  end
  
  it "should support using a boolean column as the join condition" do
    @d.join(:categories, :active).sql.must_equal 'SELECT * FROM "items" INNER JOIN "categories" ON "active"'
  end

  it "should support using an expression as the join condition" do
    @d.join(:categories, Sequel.expr(:number) > 10).sql.must_equal 'SELECT * FROM "items" INNER JOIN "categories" ON ("number" > 10)'
  end

  it "should support natural and cross joins" do
    @d.join_table(:natural, :categories).sql.must_equal 'SELECT * FROM "items" NATURAL JOIN "categories"'
    @d.join_table(:cross, :categories, nil).sql.must_equal 'SELECT * FROM "items" CROSS JOIN "categories"'
    @d.join_table(:natural, :categories, nil, :table_alias=>:c).sql.must_equal 'SELECT * FROM "items" NATURAL JOIN "categories" AS "c"'
  end

  it "should support joins with a USING clause if an array of symbols is used" do
    @d.join(:categories, [:id]).sql.must_equal 'SELECT * FROM "items" INNER JOIN "categories" USING ("id")'
    @d.join(:categories, [:id1, :id2]).sql.must_equal 'SELECT * FROM "items" INNER JOIN "categories" USING ("id1", "id2")'
  end

  it "should emulate JOIN USING (poorly) if the dataset doesn't support it" do
    meta_def(@d, :supports_join_using?){false}
    @d.join(:categories, [:id]).sql.must_equal 'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."id" = "items"."id")'
  end

  it "should hoist WITH clauses from subqueries if the dataset doesn't support CTEs in subselects" do
    meta_def(@d, :supports_cte?){true}
    meta_def(@d, :supports_cte_in_subselect?){false}
    ds = Sequel.mock.dataset.from(:categories)
    meta_def(ds, :supports_cte?){true}
    @d.join(ds.with(:a, Sequel.mock.dataset.from(:b)), [:id]).sql.must_equal 'WITH "a" AS (SELECT * FROM b) SELECT * FROM "items" INNER JOIN (SELECT * FROM categories) AS "t1" USING ("id")'
  end

  it "should raise an error if using an array of symbols with a block" do
    proc{@d.join(:categories, [:id]){|j,lj,js|}}.must_raise(Sequel::Error)
  end

  it "should support using a block that receieves the join table/alias, last join table/alias, and array of previous joins" do
    @d.join(:categories) do |join_alias, last_join_alias, joins| 
      join_alias.must_equal :categories
      last_join_alias.must_equal :items
      joins.must_equal []
    end

    @d.from(Sequel.as(:items, :i)).join(:categories, nil, :table_alias=>:c) do |join_alias, last_join_alias, joins| 
      join_alias.must_equal :c
      last_join_alias.must_equal :i
      joins.must_equal []
    end

    @d.from(:items___i).join(:categories, nil, :table_alias=>:c) do |join_alias, last_join_alias, joins| 
      join_alias.must_equal :c
      last_join_alias.must_equal :i
      joins.must_equal []
    end

    @d.join(:blah).join(:categories, nil, :table_alias=>:c) do |join_alias, last_join_alias, joins| 
      join_alias.must_equal :c
      last_join_alias.must_equal :blah
      joins.must_be_kind_of(Array)
      joins.length.must_equal 1
      joins.first.must_be_kind_of(Sequel::SQL::JoinClause)
      joins.first.join_type.must_equal :inner
    end

    @d.join_table(:natural, :blah, nil, :table_alias=>:b).join(:categories, nil, :table_alias=>:c) do |join_alias, last_join_alias, joins| 
      join_alias.must_equal :c
      last_join_alias.must_equal :b
      joins.must_be_kind_of(Array)
      joins.length.must_equal 1
      joins.first.must_be_kind_of(Sequel::SQL::JoinClause)
      joins.first.join_type.must_equal :natural
    end

    @d.join(:blah).join(:categories).join(:blah2) do |join_alias, last_join_alias, joins| 
      join_alias.must_equal :blah2
      last_join_alias.must_equal :categories
      joins.must_be_kind_of(Array)
      joins.length.must_equal 2
      joins.first.must_be_kind_of(Sequel::SQL::JoinClause)
      joins.first.table.must_equal :blah
      joins.last.must_be_kind_of(Sequel::SQL::JoinClause)
      joins.last.table.must_equal :categories
    end
  end

  it "should use the block result as the only condition if no condition is given" do
    @d.join(:categories){|j,lj,js| {Sequel.qualify(j, :b)=>Sequel.qualify(lj, :c)}}.sql.must_equal 'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."b" = "items"."c")'
    @d.join(:categories){|j,lj,js| Sequel.qualify(j, :b) > Sequel.qualify(lj, :c)}.sql.must_equal 'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."b" > "items"."c")'
  end

  it "should combine the block conditions and argument conditions if both given" do
    @d.join(:categories, :a=>:d){|j,lj,js| {Sequel.qualify(j, :b)=>Sequel.qualify(lj, :c)}}.sql.must_equal 'SELECT * FROM "items" INNER JOIN "categories" ON (("categories"."a" = "items"."d") AND ("categories"."b" = "items"."c"))'
    @d.join(:categories, :a=>:d){|j,lj,js| Sequel.qualify(j, :b) > Sequel.qualify(lj, :c)}.sql.must_equal 'SELECT * FROM "items" INNER JOIN "categories" ON (("categories"."a" = "items"."d") AND ("categories"."b" > "items"."c"))'
  end

  it "should prefer explicit aliases over implicit" do
    @d.from(:items___i).join(:categories___c, {:category_id => :id}, {:table_alias=>:c2, :implicit_qualifier=>:i2}).sql.must_equal 'SELECT * FROM "items" AS "i" INNER JOIN "categories" AS "c2" ON ("c2"."category_id" = "i2"."id")'
    @d.from(Sequel.expr(:items).as(:i)).join(Sequel.expr(:categories).as(:c), {:category_id => :id}, {:table_alias=>:c2, :implicit_qualifier=>:i2}).sql.
      must_equal 'SELECT * FROM "items" AS "i" INNER JOIN "categories" AS "c2" ON ("c2"."category_id" = "i2"."id")'
  end
  
  it "should not allow insert, update, delete, or truncate" do
    proc{@d.join(:categories, :a=>:d).insert_sql}.must_raise(Sequel::InvalidOperation)
    proc{@d.join(:categories, :a=>:d).update_sql(:a=>1)}.must_raise(Sequel::InvalidOperation)
    proc{@d.join(:categories, :a=>:d).delete_sql}.must_raise(Sequel::InvalidOperation)
    proc{@d.join(:categories, :a=>:d).truncate_sql}.must_raise(Sequel::InvalidOperation)
  end
end

describe "Dataset aggregate methods" do
  before do
    @d = Sequel.mock(:fetch=>proc{|s| {1=>s}})[:test]
  end
  
  it "should include min" do
    @d.min(:a).must_equal 'SELECT min(a) AS min FROM test LIMIT 1'
  end
  
  it "should include max" do
    @d.max(:b).must_equal 'SELECT max(b) AS max FROM test LIMIT 1'
  end
  
  it "should include sum" do
    @d.sum(:c).must_equal 'SELECT sum(c) AS sum FROM test LIMIT 1'
  end
  
  it "should include avg" do
    @d.avg(:d).must_equal 'SELECT avg(d) AS avg FROM test LIMIT 1'
  end
  
  it "should accept qualified columns" do
    @d.avg(:test__bc).must_equal 'SELECT avg(test.bc) AS avg FROM test LIMIT 1'
  end
  
  it "should use a subselect for the same conditions as count" do
    d = @d.order(:a).limit(5)
    d.avg(:a).must_equal 'SELECT avg(a) AS avg FROM (SELECT * FROM test ORDER BY a LIMIT 5) AS t1 LIMIT 1'
    d.sum(:a).must_equal 'SELECT sum(a) AS sum FROM (SELECT * FROM test ORDER BY a LIMIT 5) AS t1 LIMIT 1'
    d.min(:a).must_equal 'SELECT min(a) AS min FROM (SELECT * FROM test ORDER BY a LIMIT 5) AS t1 LIMIT 1'
    d.max(:a).must_equal 'SELECT max(a) AS max FROM (SELECT * FROM test ORDER BY a LIMIT 5) AS t1 LIMIT 1'
  end
  
  it "should accept virtual row blocks" do
    @d.avg{a(b)}.must_equal 'SELECT avg(a(b)) AS avg FROM test LIMIT 1'
    @d.sum{a(b)}.must_equal 'SELECT sum(a(b)) AS sum FROM test LIMIT 1'
    @d.min{a(b)}.must_equal 'SELECT min(a(b)) AS min FROM test LIMIT 1'
    @d.max{a(b)}.must_equal 'SELECT max(a(b)) AS max FROM test LIMIT 1'
  end
end

describe "Dataset#range" do
  before do
    @db = Sequel.mock(:fetch=>{:v1 => 1, :v2 => 10})
    @ds = @db[:test]
  end
  
  it "should generate a correct SQL statement" do
    @ds.range(:stamp)
    @db.sqls.must_equal ["SELECT min(stamp) AS v1, max(stamp) AS v2 FROM test LIMIT 1"]

    @ds.filter(Sequel.expr(:price) > 100).range(:stamp)
    @db.sqls.must_equal ["SELECT min(stamp) AS v1, max(stamp) AS v2 FROM test WHERE (price > 100) LIMIT 1"]
  end
  
  it "should return a range object" do
    @ds.range(:tryme).must_equal(1..10)
  end
  
  it "should use a subselect for the same conditions as count" do
    @ds.order(:stamp).limit(5).range(:stamp).must_equal(1..10)
    @db.sqls.must_equal ['SELECT min(stamp) AS v1, max(stamp) AS v2 FROM (SELECT * FROM test ORDER BY stamp LIMIT 5) AS t1 LIMIT 1']
  end
  
  it "should accept virtual row blocks" do
    @ds.range{a(b)}
    @db.sqls.must_equal ["SELECT min(a(b)) AS v1, max(a(b)) AS v2 FROM test LIMIT 1"]
  end
end

describe "Dataset#interval" do
  before do
    @db = Sequel.mock(:fetch=>{:v => 1234})
    @ds = @db[:test]
  end
  
  it "should generate the correct SQL statement" do
    @ds.interval(:stamp)
    @db.sqls.must_equal ["SELECT (max(stamp) - min(stamp)) AS interval FROM test LIMIT 1"]

    @ds.filter(Sequel.expr(:price) > 100).interval(:stamp)
    @db.sqls.must_equal ["SELECT (max(stamp) - min(stamp)) AS interval FROM test WHERE (price > 100) LIMIT 1"]
  end
  
  it "should use a subselect for the same conditions as count" do
    @ds.order(:stamp).limit(5).interval(:stamp).must_equal 1234
    @db.sqls.must_equal ['SELECT (max(stamp) - min(stamp)) AS interval FROM (SELECT * FROM test ORDER BY stamp LIMIT 5) AS t1 LIMIT 1']
  end

  it "should accept virtual row blocks" do
    @ds.interval{a(b)}
    @db.sqls.must_equal ["SELECT (max(a(b)) - min(a(b))) AS interval FROM test LIMIT 1"]
  end
end

describe "Dataset #first and #last" do
  before do
    @d = Sequel.mock(:fetch=>proc{|s| {:s=>s}})[:test]
  end
  
  it "should return a single record if no argument is given" do
    @d.order(:a).first.must_equal(:s=>'SELECT * FROM test ORDER BY a LIMIT 1')
    @d.order(:a).last.must_equal(:s=>'SELECT * FROM test ORDER BY a DESC LIMIT 1')
  end

  it "should return the first/last matching record if argument is not an Integer" do
    @d.order(:a).first(:z => 26).must_equal(:s=>'SELECT * FROM test WHERE (z = 26) ORDER BY a LIMIT 1')
    @d.order(:a).first('z = ?', 15).must_equal(:s=>'SELECT * FROM test WHERE (z = 15) ORDER BY a LIMIT 1')
    @d.order(:a).last(:z => 26).must_equal(:s=>'SELECT * FROM test WHERE (z = 26) ORDER BY a DESC LIMIT 1')
    @d.order(:a).last('z = ?', 15).must_equal(:s=>'SELECT * FROM test WHERE (z = 15) ORDER BY a DESC LIMIT 1')
  end
  
  it "should set the limit and return an array of records if the given number is > 1" do
    i = rand(10) + 10
    r = @d.order(:a).first(i).must_equal [{:s=>"SELECT * FROM test ORDER BY a LIMIT #{i}"}]
    i = rand(10) + 10
    r = @d.order(:a).last(i).must_equal [{:s=>"SELECT * FROM test ORDER BY a DESC LIMIT #{i}"}]
  end
  
  it "should return the first matching record if a block is given without an argument" do
    @d.first{z > 26}.must_equal(:s=>'SELECT * FROM test WHERE (z > 26) LIMIT 1')
    @d.order(:name).last{z > 26}.must_equal(:s=>'SELECT * FROM test WHERE (z > 26) ORDER BY name DESC LIMIT 1')
  end
  
  it "should combine block and standard argument filters if argument is not an Integer" do
    @d.first(:y=>25){z > 26}.must_equal(:s=>'SELECT * FROM test WHERE ((z > 26) AND (y = 25)) LIMIT 1')
    @d.order(:name).last('y = ?', 16){z > 26}.must_equal(:s=>'SELECT * FROM test WHERE ((z > 26) AND (y = 16)) ORDER BY name DESC LIMIT 1')
  end
  
  it "should filter and return an array of records if an Integer argument is provided and a block is given" do
    i = rand(10) + 10
    r = @d.order(:a).first(i){z > 26}.must_equal [{:s=>"SELECT * FROM test WHERE (z > 26) ORDER BY a LIMIT #{i}"}]
    i = rand(10) + 10
    r = @d.order(:a).last(i){z > 26}.must_equal [{:s=>"SELECT * FROM test WHERE (z > 26) ORDER BY a DESC LIMIT #{i}"}]
  end

  it "should return nil if no records match" do
    Sequel.mock[:t].first.must_equal nil
  end
  
  it "#last should raise if no order is given" do
    proc {@d.last}.must_raise(Sequel::Error)
    proc {@d.last(2)}.must_raise(Sequel::Error)
    @d.order(:a).last
    @d.order(:a).last(2)
  end
  
  it "#last should invert the order" do
    @d.order(:a).last.must_equal(:s=>'SELECT * FROM test ORDER BY a DESC LIMIT 1')
    @d.order(Sequel.desc(:b)).last.must_equal(:s=>'SELECT * FROM test ORDER BY b ASC LIMIT 1')
    @d.order(:c, :d).last.must_equal(:s=>'SELECT * FROM test ORDER BY c DESC, d DESC LIMIT 1')
    @d.order(Sequel.desc(:e), :f).last.must_equal(:s=>'SELECT * FROM test ORDER BY e ASC, f DESC LIMIT 1')
  end
end

describe "Dataset #first!" do
  before do
    @db = Sequel.mock(:fetch=>proc{|s| {:s=>s}})
    @d = @db[:test]
  end
  
  it "should return a single record if no argument is given" do
    @d.order(:a).first!.must_equal(:s=>'SELECT * FROM test ORDER BY a LIMIT 1')
  end

  it "should return the first! matching record if argument is not an Integer" do
    @d.order(:a).first!(:z => 26).must_equal(:s=>'SELECT * FROM test WHERE (z = 26) ORDER BY a LIMIT 1')
    @d.order(:a).first!('z = ?', 15).must_equal(:s=>'SELECT * FROM test WHERE (z = 15) ORDER BY a LIMIT 1')
  end
  
  it "should set the limit and return an array of records if the given number is > 1" do
    i = rand(10) + 10
    @d.order(:a).first!(i).must_equal [{:s=>"SELECT * FROM test ORDER BY a LIMIT #{i}"}]
  end
  
  it "should return the first! matching record if a block is given without an argument" do
    @d.first!{z > 26}.must_equal(:s=>'SELECT * FROM test WHERE (z > 26) LIMIT 1')
  end
  
  it "should combine block and standard argument filters if argument is not an Integer" do
    @d.first!(:y=>25){z > 26}.must_equal(:s=>'SELECT * FROM test WHERE ((z > 26) AND (y = 25)) LIMIT 1')
  end
  
  it "should filter and return an array of records if an Integer argument is provided and a block is given" do
    i = rand(10) + 10
    @d.order(:a).first!(i){z > 26}.must_equal [{:s=>"SELECT * FROM test WHERE (z > 26) ORDER BY a LIMIT #{i}"}]
  end

  it "should raise NoMatchingRow exception if no rows match" do
    proc{Sequel.mock[:t].first!}.must_raise(Sequel::NoMatchingRow)
  end

  it "saves a reference to the dataset with the exception to allow further processing" do
    dataset = Sequel.mock[:t]
    begin
      dataset.first!
    rescue Sequel::NoMatchingRow => e
      e.dataset.must_equal(dataset)
    end
    proc{raise Sequel::NoMatchingRow, 'test'}.must_raise Sequel::NoMatchingRow
    proc{raise Sequel::NoMatchingRow.new('test')}.must_raise Sequel::NoMatchingRow
  end
end
  
describe "Dataset compound operations" do
  before do
    @a = Sequel.mock.dataset.from(:a).filter(:z => 1)
    @b = Sequel.mock.dataset.from(:b).filter(:z => 2)
  end
  
  it "should support UNION and UNION ALL" do
    @a.union(@b).sql.must_equal "SELECT * FROM (SELECT * FROM a WHERE (z = 1) UNION SELECT * FROM b WHERE (z = 2)) AS t1"
    @b.union(@a, :all=>true).sql.must_equal "SELECT * FROM (SELECT * FROM b WHERE (z = 2) UNION ALL SELECT * FROM a WHERE (z = 1)) AS t1"
  end

  it "should support INTERSECT and INTERSECT ALL" do
    @a.intersect(@b).sql.must_equal "SELECT * FROM (SELECT * FROM a WHERE (z = 1) INTERSECT SELECT * FROM b WHERE (z = 2)) AS t1"
    @b.intersect(@a, :all=>true).sql.must_equal "SELECT * FROM (SELECT * FROM b WHERE (z = 2) INTERSECT ALL SELECT * FROM a WHERE (z = 1)) AS t1"
  end

  it "should support EXCEPT and EXCEPT ALL" do
    @a.except(@b).sql.must_equal "SELECT * FROM (SELECT * FROM a WHERE (z = 1) EXCEPT SELECT * FROM b WHERE (z = 2)) AS t1"
    @b.except(@a, :all=>true).sql.must_equal "SELECT * FROM (SELECT * FROM b WHERE (z = 2) EXCEPT ALL SELECT * FROM a WHERE (z = 1)) AS t1"
  end
    
  it "should support :alias option for specifying identifier" do
    @a.union(@b, :alias=>:xx).sql.must_equal "SELECT * FROM (SELECT * FROM a WHERE (z = 1) UNION SELECT * FROM b WHERE (z = 2)) AS xx"
    @a.intersect(@b, :alias=>:xx).sql.must_equal "SELECT * FROM (SELECT * FROM a WHERE (z = 1) INTERSECT SELECT * FROM b WHERE (z = 2)) AS xx"
    @a.except(@b, :alias=>:xx).sql.must_equal "SELECT * FROM (SELECT * FROM a WHERE (z = 1) EXCEPT SELECT * FROM b WHERE (z = 2)) AS xx"
  end

  it "should support :from_self=>false option to not wrap the compound in a SELECT * FROM (...)" do
    @b.union(@a, :from_self=>false).sql.must_equal "SELECT * FROM b WHERE (z = 2) UNION SELECT * FROM a WHERE (z = 1)"
    @b.intersect(@a, :from_self=>false).sql.must_equal "SELECT * FROM b WHERE (z = 2) INTERSECT SELECT * FROM a WHERE (z = 1)"
    @b.except(@a, :from_self=>false).sql.must_equal "SELECT * FROM b WHERE (z = 2) EXCEPT SELECT * FROM a WHERE (z = 1)"
      
    @b.union(@a, :from_self=>false, :all=>true).sql.must_equal "SELECT * FROM b WHERE (z = 2) UNION ALL SELECT * FROM a WHERE (z = 1)"
    @b.intersect(@a, :from_self=>false, :all=>true).sql.must_equal "SELECT * FROM b WHERE (z = 2) INTERSECT ALL SELECT * FROM a WHERE (z = 1)"
    @b.except(@a, :from_self=>false, :all=>true).sql.must_equal "SELECT * FROM b WHERE (z = 2) EXCEPT ALL SELECT * FROM a WHERE (z = 1)"
  end

  it "should raise an InvalidOperation if INTERSECT or EXCEPT is used and they are not supported" do
    meta_def(@a, :supports_intersect_except?){false}
    proc{@a.intersect(@b)}.must_raise(Sequel::InvalidOperation)
    proc{@a.intersect(@b,:all=> true)}.must_raise(Sequel::InvalidOperation)
    proc{@a.except(@b)}.must_raise(Sequel::InvalidOperation)
    proc{@a.except(@b, :all=>true)}.must_raise(Sequel::InvalidOperation)
  end
    
  it "should raise an InvalidOperation if INTERSECT ALL or EXCEPT ALL is used and they are not supported" do
    meta_def(@a, :supports_intersect_except_all?){false}
    @a.intersect(@b)
    proc{@a.intersect(@b, :all=>true)}.must_raise(Sequel::InvalidOperation)
    @a.except(@b)
    proc{@a.except(@b, :all=>true)}.must_raise(Sequel::InvalidOperation)
  end
    
  it "should handle chained compound operations" do
    @a.union(@b).union(@a, :all=>true).sql.must_equal "SELECT * FROM (SELECT * FROM (SELECT * FROM a WHERE (z = 1) UNION SELECT * FROM b WHERE (z = 2)) AS t1 UNION ALL SELECT * FROM a WHERE (z = 1)) AS t1"
    @a.intersect(@b, :all=>true).intersect(@a).sql.must_equal "SELECT * FROM (SELECT * FROM (SELECT * FROM a WHERE (z = 1) INTERSECT ALL SELECT * FROM b WHERE (z = 2)) AS t1 INTERSECT SELECT * FROM a WHERE (z = 1)) AS t1"
    @a.except(@b).except(@a, :all=>true).sql.must_equal "SELECT * FROM (SELECT * FROM (SELECT * FROM a WHERE (z = 1) EXCEPT SELECT * FROM b WHERE (z = 2)) AS t1 EXCEPT ALL SELECT * FROM a WHERE (z = 1)) AS t1"
  end
  
  it "should use a subselect when using a compound operation with a dataset that already has a compound operation" do
    @a.union(@b.union(@a, :all=>true)).sql.must_equal "SELECT * FROM (SELECT * FROM a WHERE (z = 1) UNION SELECT * FROM (SELECT * FROM b WHERE (z = 2) UNION ALL SELECT * FROM a WHERE (z = 1)) AS t1) AS t1"
    @a.intersect(@b.intersect(@a), :all=>true).sql.must_equal "SELECT * FROM (SELECT * FROM a WHERE (z = 1) INTERSECT ALL SELECT * FROM (SELECT * FROM b WHERE (z = 2) INTERSECT SELECT * FROM a WHERE (z = 1)) AS t1) AS t1"
    @a.except(@b.except(@a, :all=>true)).sql.must_equal "SELECT * FROM (SELECT * FROM a WHERE (z = 1) EXCEPT SELECT * FROM (SELECT * FROM b WHERE (z = 2) EXCEPT ALL SELECT * FROM a WHERE (z = 1)) AS t1) AS t1"
  end

  it "should order and limit properly when using UNION, INTERSECT, or EXCEPT" do
    @dataset = Sequel.mock.dataset.from(:test)
    @dataset.union(@dataset).limit(2).sql.must_equal "SELECT * FROM (SELECT * FROM test UNION SELECT * FROM test) AS t1 LIMIT 2"
    @dataset.limit(2).intersect(@dataset).sql.must_equal "SELECT * FROM (SELECT * FROM (SELECT * FROM test LIMIT 2) AS t1 INTERSECT SELECT * FROM test) AS t1"
    @dataset.except(@dataset.limit(2)).sql.must_equal "SELECT * FROM (SELECT * FROM test EXCEPT SELECT * FROM (SELECT * FROM test LIMIT 2) AS t1) AS t1"

    @dataset.union(@dataset).order(:num).sql.must_equal "SELECT * FROM (SELECT * FROM test UNION SELECT * FROM test) AS t1 ORDER BY num"
    @dataset.order(:num).intersect(@dataset).sql.must_equal "SELECT * FROM (SELECT * FROM (SELECT * FROM test ORDER BY num) AS t1 INTERSECT SELECT * FROM test) AS t1"
    @dataset.except(@dataset.order(:num)).sql.must_equal "SELECT * FROM (SELECT * FROM test EXCEPT SELECT * FROM (SELECT * FROM test ORDER BY num) AS t1) AS t1"

    @dataset.limit(2).order(:a).union(@dataset.limit(3).order(:b)).order(:c).limit(4).sql.
      must_equal "SELECT * FROM (SELECT * FROM (SELECT * FROM test ORDER BY a LIMIT 2) AS t1 UNION SELECT * FROM (SELECT * FROM test ORDER BY b LIMIT 3) AS t1) AS t1 ORDER BY c LIMIT 4"
  end

  it "should handle raw SQL datasets properly when using UNION, INTERSECT, or EXCEPT" do
    @dataset = Sequel.mock['SELECT 1']
    @dataset.union(@dataset).sql.must_equal "SELECT * FROM (SELECT * FROM (SELECT 1) AS t1 UNION SELECT * FROM (SELECT 1) AS t1) AS t1"
    @dataset.intersect(@dataset).sql.must_equal "SELECT * FROM (SELECT * FROM (SELECT 1) AS t1 INTERSECT SELECT * FROM (SELECT 1) AS t1) AS t1"
    @dataset.except(@dataset).sql.must_equal "SELECT * FROM (SELECT * FROM (SELECT 1) AS t1 EXCEPT SELECT * FROM (SELECT 1) AS t1) AS t1"
  end

  it "should hoist WITH clauses in given dataset if dataset doesn't support WITH in subselect" do
    ds = Sequel.mock.dataset
    meta_def(ds, :supports_cte?){true}
    meta_def(ds, :supports_cte_in_subselect?){false}
    ds.from(:a).union(ds.from(:c).with(:c, ds.from(:d)), :from_self=>false).sql.must_equal 'WITH c AS (SELECT * FROM d) SELECT * FROM a UNION SELECT * FROM c'
    ds.from(:a).except(ds.from(:c).with(:c, ds.from(:d))).sql.must_equal 'WITH c AS (SELECT * FROM d) SELECT * FROM (SELECT * FROM a EXCEPT SELECT * FROM c) AS t1'
    ds.from(:a).with(:a, ds.from(:b)).intersect(ds.from(:c).with(:c, ds.from(:d)), :from_self=>false).sql.must_equal 'WITH a AS (SELECT * FROM b), c AS (SELECT * FROM d) SELECT * FROM a INTERSECT SELECT * FROM c'
  end
end

describe "Dataset#[]" do
  before do
    @db = Sequel.mock(:fetch=>{1 => 2, 3 => 4})
    @d = @db[:items]
  end
  
  it "should return a single record filtered according to the given conditions" do
    @d[:name => 'didi'].must_equal(1 => 2, 3 => 4)
    @db.sqls.must_equal ["SELECT * FROM items WHERE (name = 'didi') LIMIT 1"]

    @d[:id => 5..45].must_equal(1 => 2, 3 => 4)
    @db.sqls.must_equal ["SELECT * FROM items WHERE ((id >= 5) AND (id <= 45)) LIMIT 1"]
  end
end

describe "Dataset#single_record" do
  before do
    @db = Sequel.mock
  end
  
  it "should call each with a limit of 1 and return the record" do
    @db.fetch = {:a=>1}
    @db[:test].single_record.must_equal(:a=>1)
    @db.sqls.must_equal ['SELECT * FROM test LIMIT 1']
  end
  
  it "should return nil if no record is present" do
    @db[:test].single_record.must_equal nil
    @db.sqls.must_equal ['SELECT * FROM test LIMIT 1']
  end
end

describe "Dataset#single_record!" do
  before do
    @db = Sequel.mock
  end
  
  it "should call each and return the first record" do
    @db.fetch = [{:a=>1}, {:a=>2}]
    @db[:test].single_record!.must_equal(:a=>1)
    @db.sqls.must_equal ['SELECT * FROM test']
  end
  
  it "should return nil if no record is present" do
    @db[:test].single_record!.must_equal nil
    @db.sqls.must_equal ['SELECT * FROM test']
  end
end

describe "Dataset#single_value" do
  before do
    @db = Sequel.mock
  end
  
  it "should call each and return the first value of the first record" do
    @db.fetch = {:a=>1}
    @db[:test].single_value.must_equal 1
    @db.sqls.must_equal ['SELECT * FROM test LIMIT 1']
  end
  
  it "should return nil if no records" do
    @db[:test].single_value.must_equal nil
    @db.sqls.must_equal ['SELECT * FROM test LIMIT 1']
  end
  
  it "should work on a graphed_dataset" do
    @db.fetch = {:a=>1}
    ds = @db[:test].columns(:a)
    ds.graph(ds, [:a], :table_alias=>:test2).single_value.must_equal 1
    @db.sqls.must_equal ['SELECT test.a, test2.a AS test2_a FROM test LEFT OUTER JOIN test AS test2 USING (a) LIMIT 1']
  end
end

describe "Dataset#single_value!" do
  before do
    @db = Sequel.mock
  end
  
  it "should call each and return the first value of the first record" do
    @db.fetch = [{:a=>1, :b=>2}, {:a=>3, :b=>4}]
    @db[:test].single_value!.to_s.must_match /\A(1|2)\z/
    @db.sqls.must_equal ['SELECT * FROM test']
  end
  
  it "should return nil if no records" do
    @db[:test].single_value!.must_equal nil
    @db.sqls.must_equal ['SELECT * FROM test']
  end
end

describe "Dataset#get" do
  before do
    @d = Sequel.mock(:fetch=>proc{|s| {:name=>s}})[:test]
  end
  
  it "should select the specified column and fetch its value" do
    @d.get(:name).must_equal "SELECT name FROM test LIMIT 1"
    @d.get(:abc).must_equal "SELECT abc FROM test LIMIT 1"
  end
  
  it "should work with filters" do
    @d.filter(:id => 1).get(:name).must_equal "SELECT name FROM test WHERE (id = 1) LIMIT 1"
  end
  
  it "should work with aliased fields" do
    @d.get(Sequel.expr(:x__b).as(:name)).must_equal "SELECT x.b AS name FROM test LIMIT 1"
  end
  
  it "should accept a block that yields a virtual row" do
    @d.get{|o| o.x__b.as(:name)}.must_equal "SELECT x.b AS name FROM test LIMIT 1"
    @d.get{x(1).as(:name)}.must_equal "SELECT x(1) AS name FROM test LIMIT 1"
  end
  
  it "should raise an error if both a regular argument and block argument are used" do
    proc{@d.get(:name){|o| o.x__b.as(:name)}}.must_raise(Sequel::Error)
  end
  
  it "should support false and nil values" do
    @d.get(false).must_equal "SELECT 'f' AS v FROM test LIMIT 1"
    @d.get(nil).must_equal "SELECT NULL AS v FROM test LIMIT 1"
  end

  it "should support an array of expressions to get an array of results" do
    @d._fetch = {:name=>1, :abc=>2}
    @d.get([:name, :abc]).must_equal [1, 2]
    @d.db.sqls.must_equal ['SELECT name, abc FROM test LIMIT 1']
  end
  
  it "should support an array with a single expression" do
    @d.get([:name]).must_equal ['SELECT name FROM test LIMIT 1']
  end
  
  it "should handle an array with aliases" do
    @d._fetch = {:name=>1, :abc=>2}
    @d.get([:n___name, Sequel.as(:a, :abc)]).must_equal [1, 2]
    @d.db.sqls.must_equal ['SELECT n AS name, a AS abc FROM test LIMIT 1']
  end
  
  it "should raise an Error if an alias cannot be determined" do
    @d._fetch = {:name=>1, :abc=>2}
    proc{@d.get([Sequel.+(:a, 1), :a])}.must_raise(Sequel::Error)
  end
  
  it "should support an array of expressions in a virtual row" do
    @d._fetch = {:name=>1, :abc=>2}
    @d.get{[name, n__abc]}.must_equal [1, 2]
    @d.db.sqls.must_equal ['SELECT name, n.abc FROM test LIMIT 1']
  end
  
  it "should work with static SQL" do
    @d.with_sql('SELECT foo').get(:name).must_equal "SELECT foo"
    @d._fetch = {:name=>1, :abc=>2}
    @d.with_sql('SELECT foo').get{[name, n__abc]}.must_equal [1, 2]
    @d.db.sqls.must_equal ['SELECT foo'] * 2
  end

  it "should handle cases where no rows are returned" do
    @d._fetch = []
    @d.get(:n).must_equal nil
    @d.get([:n, :a]).must_equal nil
    @d.db.sqls.must_equal ['SELECT n FROM test LIMIT 1', 'SELECT n, a FROM test LIMIT 1']
  end
end

describe "Dataset#set_row_proc" do
  before do
    @db = Sequel.mock(:fetch=>[{:a=>1}, {:a=>2}])
    @dataset = @db[:items]
    @dataset.row_proc = proc{|h| h[:der] = h[:a] + 2; h}
  end
  
  it "should cause dataset to pass all rows through the filter" do
    rows = @dataset.all
    rows.map{|h| h[:der]}.must_equal [3, 4]
    @db.sqls.must_equal ['SELECT * FROM items']
  end
  
  it "should be copied over when dataset is cloned" do
    @dataset.filter(:a => 1).all.must_equal [{:a=>1, :der=>3}, {:a=>2, :der=>4}]
  end
end

describe "Dataset#<<" do
  before do
    @db = Sequel.mock
  end

  it "should call #insert" do
    @db[:items] << {:name => 1}
    @db.sqls.must_equal ['INSERT INTO items (name) VALUES (1)']
  end

  it "should be chainable" do
    @db[:items] << {:name => 1} << @db[:old_items].select(:name)
    @db.sqls.must_equal ['INSERT INTO items (name) VALUES (1)', 'INSERT INTO items SELECT name FROM old_items']
  end
end

describe "Dataset#columns" do
  before do
    @dataset = Sequel.mock[:items]
  end
  
  it "should return the value of @columns if @columns is not nil" do
    @dataset.columns(:a, :b, :c).columns.must_equal [:a, :b, :c]
    @dataset.db.sqls.must_equal []
  end
  
  it "should attempt to get a single record and return @columns if @columns is nil" do
    @dataset.db.columns = [:a]
    @dataset.columns.must_equal [:a]
    @dataset.db.sqls.must_equal ['SELECT * FROM items LIMIT 1']
  end
  
  it "should be cleared if you change the selected columns" do
    @dataset.db.columns = [[:a], [:b]]
    @dataset.columns.must_equal [:a]
    @dataset.db.sqls.must_equal ['SELECT * FROM items LIMIT 1']
    @dataset.columns.must_equal [:a]
    @dataset.db.sqls.must_equal []
    ds = @dataset.select{foo{}}
    ds.columns.must_equal [:b]
    @dataset.db.sqls.must_equal ['SELECT foo() FROM items LIMIT 1']
  end
  
  it "should be cleared if you change the FROM table" do
    @dataset.db.columns = [[:a], [:b]]
    @dataset.columns.must_equal [:a]
    @dataset.db.sqls.must_equal ['SELECT * FROM items LIMIT 1']
    ds = @dataset.from(:foo)
    ds.columns.must_equal [:b]
    @dataset.db.sqls.must_equal ['SELECT * FROM foo LIMIT 1']
  end
  
  it "should be cleared if you join a table to the dataset" do
    @dataset.db.columns = [[:a], [:a, :b]]
    @dataset.columns.must_equal [:a]
    @dataset.db.sqls.must_equal ['SELECT * FROM items LIMIT 1']
    ds = @dataset.cross_join(:foo)
    ds.columns.must_equal [:a, :b]
    @dataset.db.sqls.must_equal ['SELECT * FROM items CROSS JOIN foo LIMIT 1']
  end
  
  it "should be cleared if you set custom SQL for the dataset" do
    @dataset.db.columns = [[:a], [:b]]
    @dataset.columns.must_equal [:a]
    @dataset.db.sqls.must_equal ['SELECT * FROM items LIMIT 1']
    ds = @dataset.with_sql('SELECT b FROM foo')
    ds.columns.must_equal [:b]
    @dataset.db.sqls.must_equal ['SELECT b FROM foo']
  end
  
  it "should ignore any filters, orders, or DISTINCT clauses" do
    @dataset.db.columns = [:a]
    @dataset.filter!(:b=>100).order!(:b).distinct!
    @dataset.columns.must_equal [:a]
    @dataset.db.sqls.must_equal ['SELECT * FROM items LIMIT 1']
  end
end

describe "Dataset#columns!" do
  it "should always attempt to get a record and return @columns" do
    ds = Sequel.mock(:columns=>[[:a, :b, :c], [:d, :e, :f]])[:items]
    ds.columns!.must_equal [:a, :b, :c]
    ds.db.sqls.must_equal ['SELECT * FROM items LIMIT 1']
    ds.columns!.must_equal [:d, :e, :f]
    ds.db.sqls.must_equal ['SELECT * FROM items LIMIT 1']
  end
end

describe "Dataset#import" do
  before do
    @db = Sequel.mock
    @ds = @db[:items]
  end
  
  it "should return nil without a query if no values" do
    @ds.import(['x', 'y'], []).must_equal nil
    @db.sqls.must_equal []
  end

  it "should accept string keys as column names" do
    @ds.import(['x', 'y'], [[1, 2], [3, 4]])
    @db.sqls.must_equal ['BEGIN',
      "INSERT INTO items (x, y) VALUES (1, 2)",
      "INSERT INTO items (x, y) VALUES (3, 4)",
      'COMMIT']
  end

  it "should accept a columns array and a values array" do
    @ds.import([:x, :y], [[1, 2], [3, 4]])
    @db.sqls.must_equal ['BEGIN',
      "INSERT INTO items (x, y) VALUES (1, 2)",
      "INSERT INTO items (x, y) VALUES (3, 4)",
      'COMMIT']
  end

  it "should accept a columns array and a dataset" do
    @ds2 = @ds.from(:cats).filter(:purr => true).select(:a, :b)
    
    @ds.import([:x, :y], @ds2)
    @db.sqls.must_equal ['BEGIN',
      "INSERT INTO items (x, y) SELECT a, b FROM cats WHERE (purr IS TRUE)",
      'COMMIT']
  end

  it "should slice based on the default_import_slice option" do
    def @ds.default_import_slice; 2 end
    @ds.import([:x, :y], [[1, 2], [3, 4], [5, 6]])
    @db.sqls.must_equal ['BEGIN',
      "INSERT INTO items (x, y) VALUES (1, 2)",
      "INSERT INTO items (x, y) VALUES (3, 4)",
      'COMMIT',
      'BEGIN',
      "INSERT INTO items (x, y) VALUES (5, 6)",
      'COMMIT']

    @ds.import([:x, :y], [[1, 2], [3, 4], [5, 6]], :slice=>nil)
    @db.sqls.must_equal ['BEGIN',
      "INSERT INTO items (x, y) VALUES (1, 2)",
      "INSERT INTO items (x, y) VALUES (3, 4)",
      "INSERT INTO items (x, y) VALUES (5, 6)",
      'COMMIT']
  end

  it "should accept a columns array and a values array with :commit_every option" do
    @ds.import([:x, :y], [[1, 2], [3, 4], [5, 6]], :commit_every => 2)
    @db.sqls.must_equal ['BEGIN',
      "INSERT INTO items (x, y) VALUES (1, 2)",
      "INSERT INTO items (x, y) VALUES (3, 4)",
      'COMMIT',
      'BEGIN',
      "INSERT INTO items (x, y) VALUES (5, 6)",
      'COMMIT']
  end

  it "should accept a columns array and a values array with :slice option" do
    @ds.import([:x, :y], [[1, 2], [3, 4], [5, 6]], :slice => 2)
    @db.sqls.must_equal ['BEGIN',
      "INSERT INTO items (x, y) VALUES (1, 2)",
      "INSERT INTO items (x, y) VALUES (3, 4)",
      'COMMIT',
      'BEGIN',
      "INSERT INTO items (x, y) VALUES (5, 6)",
      'COMMIT']
  end

  it "should use correct sql for :values strategy" do
    def @ds.multi_insert_sql_strategy; :values end
    @ds.import([:x, :y], [[1, 2], [3, 4], [5, 6]])
    @db.sqls.must_equal ['BEGIN',
      "INSERT INTO items (x, y) VALUES (1, 2), (3, 4), (5, 6)",
      'COMMIT']

    @ds.import([:x, :y], [[1, 2], [3, 4], [5, 6]], :slice=>2)
    @db.sqls.must_equal ['BEGIN',
      "INSERT INTO items (x, y) VALUES (1, 2), (3, 4)",
      'COMMIT',
      'BEGIN',
      "INSERT INTO items (x, y) VALUES (5, 6)",
      'COMMIT']
  end

  it "should use correct sql for :union strategy" do
    def @ds.multi_insert_sql_strategy; :union end
    @ds.import([:x, :y], [[1, 2], [3, 4], [5, 6]])
    @db.sqls.must_equal ['BEGIN',
      "INSERT INTO items (x, y) SELECT 1, 2 UNION ALL SELECT 3, 4 UNION ALL SELECT 5, 6",
      'COMMIT']

    @ds.import([:x, :y], [[1, 2], [3, 4], [5, 6]], :slice=>2)
    @db.sqls.must_equal ['BEGIN',
      "INSERT INTO items (x, y) SELECT 1, 2 UNION ALL SELECT 3, 4",
      'COMMIT',
      'BEGIN',
      "INSERT INTO items (x, y) SELECT 5, 6",
      'COMMIT']
  end

  it "should use correct sql for :union strategy when FROM is required" do
    def @ds.empty_from_sql; ' FROM foo' end
    def @ds.multi_insert_sql_strategy; :union end
    @ds.import([:x, :y], [[1, 2], [3, 4], [5, 6]])
    @db.sqls.must_equal ['BEGIN',
      "INSERT INTO items (x, y) SELECT 1, 2 FROM foo UNION ALL SELECT 3, 4 FROM foo UNION ALL SELECT 5, 6 FROM foo",
      'COMMIT']

    @ds.import([:x, :y], [[1, 2], [3, 4], [5, 6]], :slice=>2)
    @db.sqls.must_equal ['BEGIN',
      "INSERT INTO items (x, y) SELECT 1, 2 FROM foo UNION ALL SELECT 3, 4 FROM foo",
      'COMMIT',
      'BEGIN',
      "INSERT INTO items (x, y) SELECT 5, 6 FROM foo",
      'COMMIT']
  end
end

describe "Dataset#multi_insert" do
  before do
    @db = Sequel.mock(:servers=>{:s1=>{}})
    @ds = @db[:items]
    @list = [{:name => 'abc'}, {:name => 'def'}, {:name => 'ghi'}]
  end
  
  it "should return nil without a query if no values" do
    @ds.multi_insert([]).must_equal nil
    @db.sqls.must_equal []
  end

  it "should issue multiple insert statements inside a transaction" do
    @ds.multi_insert(@list)
    @db.sqls.must_equal ['BEGIN',
      "INSERT INTO items (name) VALUES ('abc')",
      "INSERT INTO items (name) VALUES ('def')",
      "INSERT INTO items (name) VALUES ('ghi')",
      'COMMIT']
  end
  
  it "should respect :server option" do
    @ds.multi_insert(@list, :server=>:s1)
    @db.sqls.must_equal ['BEGIN -- s1',
      "INSERT INTO items (name) VALUES ('abc') -- s1",
      "INSERT INTO items (name) VALUES ('def') -- s1",
      "INSERT INTO items (name) VALUES ('ghi') -- s1",
      'COMMIT -- s1']
  end
  
  it "should respect existing :server option on dataset" do
    @ds.server(:s1).multi_insert(@list)
    @db.sqls.must_equal ['BEGIN -- s1',
      "INSERT INTO items (name) VALUES ('abc') -- s1",
      "INSERT INTO items (name) VALUES ('def') -- s1",
      "INSERT INTO items (name) VALUES ('ghi') -- s1",
      'COMMIT -- s1']
  end
  
  it "should respect :return=>:primary_key option" do
    @db.autoid = 1
    @ds.multi_insert(@list, :return=>:primary_key).must_equal [1, 2, 3]
    @db.sqls.must_equal ['BEGIN',
      "INSERT INTO items (name) VALUES ('abc')",
      "INSERT INTO items (name) VALUES ('def')",
      "INSERT INTO items (name) VALUES ('ghi')",
      'COMMIT']
  end
  
  it "should handle different formats for tables" do
    @ds = @ds.from(:sch__tab)
    @ds.multi_insert(@list)
    @db.sqls.must_equal ['BEGIN',
      "INSERT INTO sch.tab (name) VALUES ('abc')",
      "INSERT INTO sch.tab (name) VALUES ('def')",
      "INSERT INTO sch.tab (name) VALUES ('ghi')",
      'COMMIT']

    @ds = @ds.from(Sequel.qualify(:sch, :tab))
    @ds.multi_insert(@list)
    @db.sqls.must_equal ['BEGIN',
      "INSERT INTO sch.tab (name) VALUES ('abc')",
      "INSERT INTO sch.tab (name) VALUES ('def')",
      "INSERT INTO sch.tab (name) VALUES ('ghi')",
      'COMMIT']

    @ds = @ds.from(Sequel.identifier(:sch__tab))
    @ds.multi_insert(@list)
    @db.sqls.must_equal ['BEGIN',
      "INSERT INTO sch__tab (name) VALUES ('abc')",
      "INSERT INTO sch__tab (name) VALUES ('def')",
      "INSERT INTO sch__tab (name) VALUES ('ghi')",
      'COMMIT']
  end
  
  it "should accept the :commit_every option for committing every x records" do
    @ds.multi_insert(@list, :commit_every => 1)
    @db.sqls.must_equal ['BEGIN',
      "INSERT INTO items (name) VALUES ('abc')",
      'COMMIT',
      'BEGIN',
      "INSERT INTO items (name) VALUES ('def')",
      'COMMIT',
      'BEGIN',
      "INSERT INTO items (name) VALUES ('ghi')",
      'COMMIT']
  end

  it "should accept the :slice option for committing every x records" do
    @ds.multi_insert(@list, :slice => 2)
    @db.sqls.must_equal ['BEGIN',
      "INSERT INTO items (name) VALUES ('abc')",
      "INSERT INTO items (name) VALUES ('def')",
      'COMMIT',
      'BEGIN',
      "INSERT INTO items (name) VALUES ('ghi')",
      'COMMIT']
  end
  
  it "should accept string keys as column names" do
    @ds.multi_insert([{'x'=>1, 'y'=>2}, {'x'=>3, 'y'=>4}])
    sqls = @db.sqls
    ["INSERT INTO items (x, y) VALUES (1, 2)", "INSERT INTO items (y, x) VALUES (2, 1)"].must_include(sqls.slice!(1))
    ["INSERT INTO items (x, y) VALUES (3, 4)", "INSERT INTO items (y, x) VALUES (4, 3)"].must_include(sqls.slice!(1))
    sqls.must_equal ['BEGIN', 'COMMIT']
  end

  it "should not do anything if no hashes are provided" do
    @ds.multi_insert([])
    @db.sqls.must_equal []
  end
end

describe "Dataset" do
  before do
    @d = Sequel.mock.dataset.from(:x)
  end

  it "should support self-changing select!" do
    @d.select!(:y)
    @d.sql.must_equal "SELECT y FROM x"
  end
  
  it "should support self-changing from!" do
    @d.from!(:y)
    @d.sql.must_equal "SELECT * FROM y"
  end

  it "should support self-changing order!" do
    @d.order!(:y)
    @d.sql.must_equal "SELECT * FROM x ORDER BY y"
  end
  
  it "should support self-changing filter!" do
    @d.filter!(:y => 1)
    @d.sql.must_equal "SELECT * FROM x WHERE (y = 1)"
  end

  it "should support self-changing filter! with block" do
    @d.filter!{y < 2}
    @d.sql.must_equal "SELECT * FROM x WHERE (y < 2)"
  end
  
  it "should raise for ! methods that don't return a dataset" do
    proc {@d.opts!}.must_raise(NoMethodError)
  end
  
  it "should raise for missing methods" do
    proc {@d.xuyz}.must_raise(NoMethodError)
    proc {@d.xyz!}.must_raise(NoMethodError)
    proc {@d.xyz?}.must_raise(NoMethodError)
  end
  
  it "should support chaining of bang methods" do
      @d.order!(:y).filter!(:y => 1).sql.must_equal "SELECT * FROM x WHERE (y = 1) ORDER BY y"
  end
end

describe "Dataset#update_sql" do
  before do
    @ds = Sequel.mock.dataset.from(:items)
  end
  
  it "should accept strings" do
    @ds.update_sql("a = b").must_equal "UPDATE items SET a = b"
  end
  
  it "should handle implicitly qualified symbols" do
    @ds.update_sql(:items__a=>:b).must_equal "UPDATE items SET items.a = b"
  end
  
  it "should accept hash with string keys" do
    @ds.update_sql('c' => 'd').must_equal "UPDATE items SET c = 'd'"
  end

  it "should accept array subscript references" do
    @ds.update_sql((Sequel.subscript(:day, 1)) => 'd').must_equal "UPDATE items SET day[1] = 'd'"
  end
end

describe "Dataset#insert_sql" do
  before do
    @ds = Sequel.mock.dataset.from(:items)
  end
  
  it "should accept hash with symbol keys" do
    @ds.insert_sql(:c => 'd').must_equal "INSERT INTO items (c) VALUES ('d')"
  end

  it "should accept hash with string keys" do
    @ds.insert_sql('c' => 'd').must_equal "INSERT INTO items (c) VALUES ('d')"
  end

  it "should quote string keys" do
    @ds.quote_identifiers = true
    @ds.insert_sql('c' => 'd').must_equal "INSERT INTO \"items\" (\"c\") VALUES ('d')"
  end

  it "should accept array subscript references" do
    @ds.insert_sql((Sequel.subscript(:day, 1)) => 'd').must_equal "INSERT INTO items (day[1]) VALUES ('d')"
  end

  it "should raise an Error if the dataset has no sources" do
    proc{Sequel::Database.new.dataset.insert_sql}.must_raise(Sequel::Error)
  end
  
  it "should accept datasets" do
    @ds.insert_sql(@ds).must_equal "INSERT INTO items SELECT * FROM items"
  end
  
  it "should accept datasets with columns" do
    @ds.insert_sql([:a, :b], @ds).must_equal "INSERT INTO items (a, b) SELECT * FROM items"
  end
  
  it "should raise if given bad values" do
    proc{@ds.clone(:values=>'a').send(:_insert_sql)}.must_raise(Sequel::Error)
  end
  
  it "should accept separate values" do
    @ds.insert_sql(1).must_equal "INSERT INTO items VALUES (1)"
    @ds.insert_sql(1, 2).must_equal "INSERT INTO items VALUES (1, 2)"
    @ds.insert_sql(1, 2, 3).must_equal "INSERT INTO items VALUES (1, 2, 3)"
  end
  
  it "should accept a single array of values" do
    @ds.insert_sql([1, 2, 3]).must_equal "INSERT INTO items VALUES (1, 2, 3)"
  end
  
  it "should accept an array of columns and an array of values" do
    @ds.insert_sql([:a, :b, :c], [1, 2, 3]).must_equal "INSERT INTO items (a, b, c) VALUES (1, 2, 3)"
  end
  
  it "should raise an array if the columns and values differ in size" do
    proc{@ds.insert_sql([:a, :b], [1, 2, 3])}.must_raise(Sequel::Error)
  end
  
  it "should accept a single LiteralString" do
    @ds.insert_sql(Sequel.lit('VALUES (1, 2, 3)')).must_equal "INSERT INTO items VALUES (1, 2, 3)"
  end
  
  it "should accept an array of columns and an LiteralString" do
    @ds.insert_sql([:a, :b, :c], Sequel.lit('VALUES (1, 2, 3)')).must_equal "INSERT INTO items (a, b, c) VALUES (1, 2, 3)"
  end

  it "should use unaliased table name" do
    @ds.from(:items___i).insert_sql(1).must_equal "INSERT INTO items VALUES (1)"
    @ds.from(Sequel.as(:items, :i)).insert_sql(1).must_equal "INSERT INTO items VALUES (1)"
  end
end

describe "Dataset#inspect" do
  before do
    class ::InspectDataset < Sequel::Dataset; end
  end
  after do
    Object.send(:remove_const, :InspectDataset) if defined?(::InspectDataset)
  end

  it "should include the class name and the corresponding SQL statement" do
    Sequel::Dataset.new(Sequel.mock).from(:blah).inspect.must_equal '#<Sequel::Dataset: "SELECT * FROM blah">'
    InspectDataset.new(Sequel.mock).from(:blah).inspect.must_equal '#<InspectDataset: "SELECT * FROM blah">'
  end

  it "should skip anonymous classes" do
    Class.new(Class.new(Sequel::Dataset)).new(Sequel.mock).from(:blah).inspect.must_equal '#<Sequel::Dataset: "SELECT * FROM blah">'
    Class.new(InspectDataset).new(Sequel.mock).from(:blah).inspect.must_equal '#<InspectDataset: "SELECT * FROM blah">'
  end
end

describe "Dataset#all" do
  before do
    @dataset = Sequel.mock(:fetch=>[{:x => 1, :y => 2}, {:x => 3, :y => 4}])[:items]
  end

  it "should return an array with all records" do
    @dataset.all.must_equal [{:x => 1, :y => 2}, {:x => 3, :y => 4}]
    @dataset.db.sqls.must_equal ["SELECT * FROM items"]
  end
  
  it "should iterate over the array if a block is given" do
    a = []
    @dataset.all{|r| a << r.values_at(:x, :y)}.must_equal [{:x => 1, :y => 2}, {:x => 3, :y => 4}]
    a.must_equal [[1, 2], [3, 4]]
    @dataset.db.sqls.must_equal ["SELECT * FROM items"]
  end
end

describe "Dataset#grep" do
  before do
    @ds = Sequel.mock[:posts]
  end
  
  it "should format a filter correctly" do
    @ds.grep(:title, 'ruby').sql.must_equal "SELECT * FROM posts WHERE ((title LIKE 'ruby' ESCAPE '\\'))"
  end

  it "should support multiple columns" do
    @ds.grep([:title, :body], 'ruby').sql.must_equal "SELECT * FROM posts WHERE ((title LIKE 'ruby' ESCAPE '\\') OR (body LIKE 'ruby' ESCAPE '\\'))"
  end
  
  it "should support multiple search terms" do
    @ds.grep(:title, ['abc', 'def']).sql.must_equal "SELECT * FROM posts WHERE ((title LIKE 'abc' ESCAPE '\\') OR (title LIKE 'def' ESCAPE '\\'))"
  end
  
  it "should support multiple columns and search terms" do
    @ds.grep([:title, :body], ['abc', 'def']).sql.must_equal "SELECT * FROM posts WHERE ((title LIKE 'abc' ESCAPE '\\') OR (title LIKE 'def' ESCAPE '\\') OR (body LIKE 'abc' ESCAPE '\\') OR (body LIKE 'def' ESCAPE '\\'))"
  end
  
  it "should support the :all_patterns option" do
    @ds.grep([:title, :body], ['abc', 'def'], :all_patterns=>true).sql.must_equal "SELECT * FROM posts WHERE (((title LIKE 'abc' ESCAPE '\\') OR (body LIKE 'abc' ESCAPE '\\')) AND ((title LIKE 'def' ESCAPE '\\') OR (body LIKE 'def' ESCAPE '\\')))"
  end
  
  it "should support the :all_columns option" do
    @ds.grep([:title, :body], ['abc', 'def'], :all_columns=>true).sql.must_equal "SELECT * FROM posts WHERE (((title LIKE 'abc' ESCAPE '\\') OR (title LIKE 'def' ESCAPE '\\')) AND ((body LIKE 'abc' ESCAPE '\\') OR (body LIKE 'def' ESCAPE '\\')))"
  end
  
  it "should support the :case_insensitive option" do
    @ds.grep([:title, :body], ['abc', 'def'], :case_insensitive=>true).sql.must_equal "SELECT * FROM posts WHERE ((UPPER(title) LIKE UPPER('abc') ESCAPE '\\') OR (UPPER(title) LIKE UPPER('def') ESCAPE '\\') OR (UPPER(body) LIKE UPPER('abc') ESCAPE '\\') OR (UPPER(body) LIKE UPPER('def') ESCAPE '\\'))"
  end
  
  it "should support the :all_patterns and :all_columns options together" do
    @ds.grep([:title, :body], ['abc', 'def'], :all_patterns=>true, :all_columns=>true).sql.must_equal "SELECT * FROM posts WHERE ((title LIKE 'abc' ESCAPE '\\') AND (body LIKE 'abc' ESCAPE '\\') AND (title LIKE 'def' ESCAPE '\\') AND (body LIKE 'def' ESCAPE '\\'))"
  end
  
  it "should support the :all_patterns and :case_insensitive options together" do
    @ds.grep([:title, :body], ['abc', 'def'], :all_patterns=>true, :case_insensitive=>true).sql.must_equal "SELECT * FROM posts WHERE (((UPPER(title) LIKE UPPER('abc') ESCAPE '\\') OR (UPPER(body) LIKE UPPER('abc') ESCAPE '\\')) AND ((UPPER(title) LIKE UPPER('def') ESCAPE '\\') OR (UPPER(body) LIKE UPPER('def') ESCAPE '\\')))"
  end
  
  it "should support the :all_columns and :case_insensitive options together" do
    @ds.grep([:title, :body], ['abc', 'def'], :all_columns=>true, :case_insensitive=>true).sql.must_equal "SELECT * FROM posts WHERE (((UPPER(title) LIKE UPPER('abc') ESCAPE '\\') OR (UPPER(title) LIKE UPPER('def') ESCAPE '\\')) AND ((UPPER(body) LIKE UPPER('abc') ESCAPE '\\') OR (UPPER(body) LIKE UPPER('def') ESCAPE '\\')))"
  end
  
  it "should support the :all_patterns, :all_columns, and :case_insensitive options together" do
    @ds.grep([:title, :body], ['abc', 'def'], :all_patterns=>true, :all_columns=>true, :case_insensitive=>true).sql.must_equal "SELECT * FROM posts WHERE ((UPPER(title) LIKE UPPER('abc') ESCAPE '\\') AND (UPPER(body) LIKE UPPER('abc') ESCAPE '\\') AND (UPPER(title) LIKE UPPER('def') ESCAPE '\\') AND (UPPER(body) LIKE UPPER('def') ESCAPE '\\'))"
  end

  it "should not support regexps if the database doesn't supports it" do
    proc{@ds.grep(:title, /ruby/).sql}.must_raise(Sequel::InvalidOperation)
    proc{@ds.grep(:title, [/^ruby/, 'ruby']).sql}.must_raise(Sequel::InvalidOperation)
  end

  it "should support regexps if the database supports it" do
    def @ds.supports_regexp?; true end
    @ds.grep(:title, /ruby/).sql.must_equal "SELECT * FROM posts WHERE ((title ~ 'ruby'))"
    @ds.grep(:title, [/^ruby/, 'ruby']).sql.must_equal "SELECT * FROM posts WHERE ((title ~ '^ruby') OR (title LIKE 'ruby' ESCAPE '\\'))"
  end

  it "should support searching against other columns" do
    @ds.grep(:title, :body).sql.must_equal "SELECT * FROM posts WHERE ((title LIKE body ESCAPE '\\'))"
  end
end

describe "Dataset default #fetch_rows, #insert, #update, #delete, #truncate, #execute" do
  before do
    @db = Sequel.mock(:servers=>{:read_only=>{}}, :autoid=>1)
    @ds = @db[:items]
  end

  it "#delete should execute delete SQL" do
    @ds.delete.must_equal 0
    @db.sqls.must_equal ["DELETE FROM items"]
  end

  it "#insert should execute insert SQL" do
    @ds.insert([]).must_equal 1
    @db.sqls.must_equal ["INSERT INTO items DEFAULT VALUES"]
  end

  it "#update should execute update SQL" do
    @ds.update(:number=>1).must_equal 0
    @db.sqls.must_equal ["UPDATE items SET number = 1"]
  end
  
  it "#truncate should execute truncate SQL" do
    @ds.truncate.must_equal nil
    @db.sqls.must_equal ["TRUNCATE TABLE items"]
  end
  
  it "#truncate should raise an InvalidOperation exception if the dataset is filtered" do
    proc{@ds.filter(:a=>1).truncate}.must_raise(Sequel::InvalidOperation)
    proc{@ds.having(:a=>1).truncate}.must_raise(Sequel::InvalidOperation)
  end
  
  it "#execute should execute the SQL on the read_only database" do
    @ds.send(:execute, 'SELECT 1')
    @db.sqls.must_equal ["SELECT 1 -- read_only"]
  end
  
  it "#execute should execute the SQL on the default database if locking is used" do
    @ds.for_update.send(:execute, 'SELECT 1')
    @db.sqls.must_equal ["SELECT 1"]
  end
end

describe "Dataset#with_sql_*" do
  before do
    @db = Sequel.mock(:servers=>{:read_only=>{}}, :autoid=>1, :fetch=>{:id=>1})
    @ds = @db[:items]
  end

  it "#with_sql_insert should execute given insert SQL" do
    @ds.with_sql_insert('INSERT INTO foo (1)').must_equal 1
    @db.sqls.must_equal ["INSERT INTO foo (1)"]
  end

  it "#with_sql_delete should execute given delete SQL" do
    @ds.with_sql_delete('DELETE FROM foo').must_equal 0
    @db.sqls.must_equal ["DELETE FROM foo"]
  end

  it "#with_sql_update should execute given update SQL" do
    @ds.with_sql_update('UPDATE foo SET a = 1').must_equal 0
    @db.sqls.must_equal ["UPDATE foo SET a = 1"]
  end

  it "#with_sql_all should return all rows from running the SQL" do
    @ds.with_sql_all('SELECT * FROM foo').must_equal [{:id=>1}]
    @db.sqls.must_equal ["SELECT * FROM foo -- read_only"]
  end

  it "#with_sql_all should yield each row to the block" do
    a = []
    @ds.with_sql_all('SELECT * FROM foo'){|r| a << r}
    a.must_equal [{:id=>1}]
    @db.sqls.must_equal ["SELECT * FROM foo -- read_only"]
  end

  it "#with_sql_each should yield each row to the block" do
    a = []
    @ds.with_sql_each('SELECT * FROM foo'){|r| a << r}
    a.must_equal [{:id=>1}]
    @db.sqls.must_equal ["SELECT * FROM foo -- read_only"]
  end

  it "#with_sql_first should return first row" do
    @ds.with_sql_first('SELECT * FROM foo').must_equal(:id=>1)
    @db.sqls.must_equal ["SELECT * FROM foo -- read_only"]
  end

  it "#with_sql_first should return nil if no rows returned" do
    @db.fetch = []
    @ds.with_sql_first('SELECT * FROM foo').must_equal nil
    @db.sqls.must_equal ["SELECT * FROM foo -- read_only"]
  end

  it "#with_sql_single_value should return first value from first row" do
    @ds.with_sql_single_value('SELECT * FROM foo').must_equal 1
    @db.sqls.must_equal ["SELECT * FROM foo -- read_only"]
  end

  it "#with_sql_single_value should return nil if no rows returned" do
    @db.fetch = []
    @ds.with_sql_single_value('SELECT * FROM foo').must_equal nil
    @db.sqls.must_equal ["SELECT * FROM foo -- read_only"]
  end
end

describe "Dataset prepared statements and bound variables " do
  before do
    @db = Sequel.mock
    @ds = @db[:items]
    meta_def(@ds, :insert_select_sql){|*v| "#{insert_sql(*v)} RETURNING *" }
  end
  
  it "#call should take a type and bind hash and interpolate it" do
    @ds.filter(:num=>:$n).call(:each, :n=>1)
    @ds.filter(:num=>:$n).call(:select, :n=>1)
    @ds.filter(:num=>:$n).call([:map, :a], :n=>1)
    @ds.filter(:num=>:$n).call([:to_hash, :a, :b], :n=>1)
    @ds.filter(:num=>:$n).call([:to_hash_groups, :a, :b], :n=>1)
    @ds.filter(:num=>:$n).call(:first, :n=>1)
    @ds.filter(:num=>:$n).call(:delete, :n=>1)
    @ds.filter(:num=>:$n).call(:update, {:n=>1, :n2=>2}, :num=>:$n2)
    @ds.call(:insert, {:n=>1}, :num=>:$n)
    @ds.call(:insert_select, {:n=>1}, :num=>:$n)
    @db.sqls.must_equal [
      'SELECT * FROM items WHERE (num = 1)',
      'SELECT * FROM items WHERE (num = 1)',
      'SELECT * FROM items WHERE (num = 1)',
      'SELECT * FROM items WHERE (num = 1)',
      'SELECT * FROM items WHERE (num = 1)',
      'SELECT * FROM items WHERE (num = 1) LIMIT 1',
      'DELETE FROM items WHERE (num = 1)',
      'UPDATE items SET num = 2 WHERE (num = 1)',
      'INSERT INTO items (num) VALUES (1)',
      'INSERT INTO items (num) VALUES (1) RETURNING *']
  end
    
  it "#prepare should take a type and name and store it in the database for later use with call" do
    pss = []
    pss << @ds.filter(:num=>:$n).prepare(:each, :en)
    pss << @ds.filter(:num=>:$n).prepare(:select, :sn)
    pss << @ds.filter(:num=>:$n).prepare([:map, :a], :sm)
    pss << @ds.filter(:num=>:$n).prepare([:to_hash, :a, :b], :sh)
    pss << @ds.filter(:num=>:$n).prepare([:to_hash_groups, :a, :b], :shg)
    pss << @ds.filter(:num=>:$n).prepare(:first, :fn)
    pss << @ds.filter(:num=>:$n).prepare(:delete, :dn)
    pss << @ds.filter(:num=>:$n).prepare(:update, :un, :num=>:$n2)
    pss << @ds.prepare(:insert, :in, :num=>:$n)
    pss << @ds.prepare(:insert_select, :ins, :num=>:$n)
    @db.prepared_statements.keys.sort_by{|k| k.to_s}.must_equal [:dn, :en, :fn, :in, :ins, :sh, :shg, :sm, :sn, :un]
    [:en, :sn, :sm, :sh, :shg, :fn, :dn, :un, :in, :ins].each_with_index{|x, i| @db.prepared_statements[x].must_equal pss[i]}
    @db.call(:en, :n=>1){}
    @db.call(:sn, :n=>1)
    @db.call(:sm, :n=>1)
    @db.call(:sh, :n=>1)
    @db.call(:shg, :n=>1)
    @db.call(:fn, :n=>1)
    @db.call(:dn, :n=>1)
    @db.call(:un, :n=>1, :n2=>2)
    @db.call(:in, :n=>1)
    @db.call(:ins, :n=>1)
    @db.sqls.must_equal [
      'SELECT * FROM items WHERE (num = 1)',
      'SELECT * FROM items WHERE (num = 1)',
      'SELECT * FROM items WHERE (num = 1)',
      'SELECT * FROM items WHERE (num = 1)',
      'SELECT * FROM items WHERE (num = 1)',
      'SELECT * FROM items WHERE (num = 1) LIMIT 1',
      'DELETE FROM items WHERE (num = 1)',
      'UPDATE items SET num = 2 WHERE (num = 1)',
      'INSERT INTO items (num) VALUES (1)',
      'INSERT INTO items (num) VALUES (1) RETURNING *']
  end
    
  it "#call and #prepare should handle returning" do
    meta_def(@ds, :supports_returning?){|_| true}
    meta_def(@ds, :insert_sql){|*v| "#{super(*v)} RETURNING *" }
    meta_def(@ds, :update_sql){|*v| "#{super(*v)} RETURNING *" }
    meta_def(@ds, :delete_sql){"#{super()} RETURNING *" }
    @ds = @ds.returning
    @ds.call(:insert, {:n=>1}, :num=>:$n)
    @ds.filter(:num=>:$n).call(:update, {:n=>1, :n2=>2}, :num=>:$n2)
    @ds.filter(:num=>:$n).call(:delete, :n=>1)
    @ds.prepare(:insert, :insert_rn, :num=>:$n).call(:n=>1)
    @ds.filter(:num=>:$n).prepare(:update, :update_rn, :num=>:$n2).call(:n=>1, :n2=>2)
    @ds.filter(:num=>:$n).prepare(:delete, :delete_rn).call(:n=>1)
    @db.sqls.must_equal([
      'INSERT INTO items (num) VALUES (1) RETURNING *',
      'UPDATE items SET num = 2 WHERE (num = 1) RETURNING *',
      'DELETE FROM items WHERE (num = 1) RETURNING *',
    ]*2)
  end

  it "PreparedStatement#prepare should raise an error" do
    ps = @ds.prepare(:select, :select_n)
    proc{ps.prepare(:select, :select_n2)}.must_raise Sequel::Error
  end

  it "#call should default to using :all if an invalid type is given" do
    @ds.filter(:num=>:$n).call(:select_all, :n=>1)
    @db.sqls.must_equal ['SELECT * FROM items WHERE (num = 1)']
  end

  it "#inspect should indicate it is a prepared statement with the prepared SQL" do
    @ds.filter(:num=>:$n).prepare(:select, :sn).inspect.must_equal \
      '<Sequel::Mock::Dataset/PreparedStatement "SELECT * FROM items WHERE (num = $n)">'
  end
    
  it "should handle literal strings" do
    @ds.filter("num = ?", :$n).call(:select, :n=>1)
    @db.sqls.must_equal ['SELECT * FROM items WHERE (num = 1)']
  end
    
  it "should handle columns on prepared statements correctly" do
    @db.columns = [:num]
    meta_def(@ds, :select_where_sql){|sql| super(sql); sql << " OR #{columns.first} = 1" if opts[:where]}
    @ds.filter(:num=>:$n).prepare(:select, :sn).sql.must_equal 'SELECT * FROM items WHERE (num = $n) OR num = 1'
    @db.sqls.must_equal ['SELECT * FROM items LIMIT 1']
  end
    
  it "should handle datasets using static sql and placeholders" do
    @db["SELECT * FROM items WHERE (num = ?)", :$n].call(:select, :n=>1)
    @db.sqls.must_equal ['SELECT * FROM items WHERE (num = 1)']
  end
    
  it "should handle subselects" do
    @ds.filter(:$b).filter(:num=>@ds.select(:num).filter(:num=>:$n)).filter(:$c).call(:select, :n=>1, :b=>0, :c=>2)
    @db.sqls.must_equal ['SELECT * FROM items WHERE (0 AND (num IN (SELECT num FROM items WHERE (num = 1))) AND 2)']
  end
    
  it "should handle subselects in subselects" do
    @ds.filter(:$b).filter(:num=>@ds.select(:num).filter(:num=>@ds.select(:num).filter(:num=>:$n))).call(:select, :n=>1, :b=>0)
    @db.sqls.must_equal ['SELECT * FROM items WHERE (0 AND (num IN (SELECT num FROM items WHERE (num IN (SELECT num FROM items WHERE (num = 1))))))']
  end
    
  it "should handle subselects with literal strings" do
    @ds.filter(:$b).filter(:num=>@ds.select(:num).filter("num = ?", :$n)).call(:select, :n=>1, :b=>0)
    @db.sqls.must_equal ['SELECT * FROM items WHERE (0 AND (num IN (SELECT num FROM items WHERE (num = 1))))']
  end
    
  it "should handle subselects with static sql and placeholders" do
    @ds.filter(:$b).filter(:num=>@db["SELECT num FROM items WHERE (num = ?)", :$n]).call(:select, :n=>1, :b=>0)
    @db.sqls.must_equal ['SELECT * FROM items WHERE (0 AND (num IN (SELECT num FROM items WHERE (num = 1))))']
  end

  it "should handle usage with Dataset.prepared_statements_module" do
    m = Module.new
    @ds.extend(Sequel::Dataset.send(:prepared_statements_module, :prepare_bind, [Sequel::Dataset::ArgumentMapper, Sequel::Dataset::PreparedStatementMethods]){def foo; :bar; end})
    @ds.foo.must_equal :bar
    @ds.prepared_statement_name = 'foo'
    @ds.call(:a=>1)
    @db.sqls.must_equal ["foo"]
  end
end

describe Sequel::Dataset::UnnumberedArgumentMapper do
  before do
    @db = Sequel.mock
    @ds = @db[:items].filter(:num=>:$n)
    def @ds.execute(sql, opts={}, &block)
      super(sql, opts.merge({:arguments=>bind_arguments}), &block)
    end
    def @ds.execute_dui(sql, opts={}, &block)
      super(sql, opts.merge({:arguments=>bind_arguments}), &block)
    end
    def @ds.execute_insert(sql, opts={}, &block)
      super(sql, opts.merge({:arguments=>bind_arguments}), &block)
    end
    @ps = []
    @ps << @ds.prepare(:select, :s)
    @ps << @ds.prepare(:all, :a)
    @ps << @ds.prepare(:first, :f)
    @ps << @ds.prepare(:delete, :d)
    @ps << @ds.prepare(:insert, :i, :num=>:$n)
    @ps << @ds.prepare(:update, :u, :num=>:$n)
    @ps.each do |p|
      p.extend(Sequel::Dataset::ArgumentMapper) # Work around for old rbx
      p.extend(Sequel::Dataset::UnnumberedArgumentMapper)
    end
  end

  it "#inspect should show the actual SQL submitted to the database" do
    @ps.first.inspect.must_equal '<Sequel::Mock::Dataset/PreparedStatement "SELECT * FROM items WHERE (num = ?)">'
  end
  
  it "should submit the SQL to the database with placeholders and bind variables" do
    @ps.each{|p| p.prepared_sql; p.call(:n=>1)}
    @db.sqls.must_equal ["SELECT * FROM items WHERE (num = ?) -- args: [1]",
      "SELECT * FROM items WHERE (num = ?) -- args: [1]",
      "SELECT * FROM items WHERE (num = ?) LIMIT 1 -- args: [1]",
      "DELETE FROM items WHERE (num = ?) -- args: [1]",
      "INSERT INTO items (num) VALUES (?) -- args: [1]",
      "UPDATE items SET num = ? WHERE (num = ?) -- args: [1, 1]"]
  end

  it "should handle unrecognized statement types as :all" do
    ps = @ds.prepare(:select_all, :s)
    ps.extend(Sequel::Dataset::ArgumentMapper)  # Work around for old rbx
    ps.extend(Sequel::Dataset::UnnumberedArgumentMapper)
    ps.prepared_sql
    ps.call(:n=>1)
    @db.sqls.must_equal ["SELECT * FROM items WHERE (num = ?) -- args: [1]"]
  end
end

describe "Sequel::Dataset#server" do
  it "should set the server to use for the dataset" do
    @db = Sequel.mock(:servers=>{:s=>{}, :i=>{}, :d=>{}, :u=>{}})
    @ds = @db[:items].server(:s)
    @ds.all
    @ds.server(:i).insert(:a=>1)
    @ds.server(:d).delete
    @ds.server(:u).update(:a=>Sequel.expr(:a)+1)
    @db.sqls.must_equal ['SELECT * FROM items -- s', 'INSERT INTO items (a) VALUES (1) -- i', 'DELETE FROM items -- d', 'UPDATE items SET a = (a + 1) -- u']
  end
end

describe "Sequel::Dataset#each_server" do
  it "should yield a dataset for each server" do
    @db = Sequel.mock(:servers=>{:s=>{}, :i=>{}})
    @ds = @db[:items]
    @ds.each_server do |ds|
      ds.must_be_kind_of(Sequel::Dataset)
      ds.wont_equal @ds
      ds.sql.must_equal @ds.sql
      ds.all
    end
    @db.sqls.sort.must_equal ['SELECT * FROM items', 'SELECT * FROM items -- i', 'SELECT * FROM items -- s']
  end
end

describe "Sequel::Dataset#qualify" do
  before do
    @ds = Sequel::Database.new[:t]
  end

  it "should qualify to the table if one is given" do
    @ds.filter{a<b}.qualify(:e).sql.must_equal 'SELECT e.* FROM t WHERE (e.a < e.b)'
  end

  it "should handle the select, order, where, having, and group options/clauses" do
    @ds.select(:a).filter(:a=>1).order(:a).group(:a).having(:a).qualify.sql.must_equal 'SELECT t.a FROM t WHERE (t.a = 1) GROUP BY t.a HAVING t.a ORDER BY t.a'
  end

  it "should handle the select using a table.* if all columns are currently selected" do
    @ds.filter(:a=>1).order(:a).group(:a).having(:a).qualify.sql.must_equal 'SELECT t.* FROM t WHERE (t.a = 1) GROUP BY t.a HAVING t.a ORDER BY t.a'
  end

  it "should handle hashes in select option" do
    @ds.select(:a=>:b).qualify.sql.must_equal 'SELECT (t.a = t.b) FROM t'
  end

  it "should handle symbols" do
    @ds.select(:a, :b__c, :d___e, :f__g___h).qualify.sql.must_equal 'SELECT t.a, b.c, t.d AS e, f.g AS h FROM t'
  end

  it "should handle arrays" do
    @ds.filter(:a=>[:b, :c]).qualify.sql.must_equal 'SELECT t.* FROM t WHERE (t.a IN (t.b, t.c))'
  end

  it "should handle hashes" do
    @ds.select(Sequel.case({:b=>{:c=>1}}, false)).qualify.sql.must_equal "SELECT (CASE WHEN t.b THEN (t.c = 1) ELSE 'f' END) FROM t"
  end

  it "should handle SQL::Identifiers" do
    @ds.select{a}.qualify.sql.must_equal 'SELECT t.a FROM t'
  end

  it "should handle SQL::OrderedExpressions" do
    @ds.order(Sequel.desc(:a), Sequel.asc(:b)).qualify.sql.must_equal 'SELECT t.* FROM t ORDER BY t.a DESC, t.b ASC'
  end

  it "should handle SQL::AliasedExpressions" do
    @ds.select(Sequel.expr(:a).as(:b)).qualify.sql.must_equal 'SELECT t.a AS b FROM t'
    @ds.select(Sequel.expr(:a).as(:b, [:c, :d])).qualify.sql.must_equal 'SELECT t.a AS b(c, d) FROM t'
  end

  it "should handle SQL::CaseExpressions" do
    @ds.filter{Sequel.case({a=>b}, c, d)}.qualify.sql.must_equal 'SELECT t.* FROM t WHERE (CASE t.d WHEN t.a THEN t.b ELSE t.c END)'
  end

  it "should handle SQL:Casts" do
    @ds.filter{a.cast(:boolean)}.qualify.sql.must_equal 'SELECT t.* FROM t WHERE CAST(t.a AS boolean)'
  end

  it "should handle SQL::Functions" do
    @ds.filter{a(b, 1)}.qualify.sql.must_equal 'SELECT t.* FROM t WHERE a(t.b, 1)'
  end

  it "should handle SQL::ComplexExpressions" do
    @ds.filter{(a+b)<(c-3)}.qualify.sql.must_equal 'SELECT t.* FROM t WHERE ((t.a + t.b) < (t.c - 3))'
  end

  it "should handle SQL::ValueLists" do
    @ds.filter(:a=>Sequel.value_list([:b, :c])).qualify.sql.must_equal 'SELECT t.* FROM t WHERE (t.a IN (t.b, t.c))'
  end

  it "should handle SQL::Subscripts" do
    @ds.filter{a.sql_subscript(b,3)}.qualify.sql.must_equal 'SELECT t.* FROM t WHERE t.a[t.b, 3]'
  end

  it "should handle SQL::PlaceholderLiteralStrings" do
    @ds.filter('? > ?', :a, 1).qualify.sql.must_equal 'SELECT t.* FROM t WHERE (t.a > 1)'
  end

  it "should handle SQL::PlaceholderLiteralStrings with named placeholders" do
    @ds.filter(':a > :b', :a=>:c, :b=>1).qualify.sql.must_equal 'SELECT t.* FROM t WHERE (t.c > 1)'
  end

  it "should handle SQL::Wrappers" do
    @ds.filter(Sequel::SQL::Wrapper.new(:a)).qualify.sql.must_equal 'SELECT t.* FROM t WHERE t.a'
  end

  it "should handle SQL::Functions with windows" do
    meta_def(@ds, :supports_window_functions?){true}
    @ds.select{sum(:a).over(:partition=>:b, :order=>:c)}.qualify.sql.must_equal 'SELECT sum(t.a) OVER (PARTITION BY t.b ORDER BY t.c) FROM t'
  end

  it "should handle SQL::DelayedEvaluation" do
    t = :a
    ds = @ds.filter(Sequel.delay{t}).qualify
    ds.sql.must_equal 'SELECT t.* FROM t WHERE t.a'
    t = :b
    ds.sql.must_equal 'SELECT t.* FROM t WHERE t.b'
  end

  it "should handle SQL::DelayedEvaluations that take dataset arguments" do
    ds = @ds.filter(Sequel.delay{|x| x.first_source}).qualify
    ds.sql.must_equal 'SELECT t.* FROM t WHERE t.t'
  end

  it "should handle all other objects by returning them unchanged" do
    @ds.select("a").filter{a(3)}.filter('blah').order(Sequel.lit('true')).group(Sequel.lit('a > ?', 1)).having(false).qualify.sql.must_equal "SELECT 'a' FROM t WHERE (a(3) AND (blah)) GROUP BY a > 1 HAVING 'f' ORDER BY true"
  end
end

describe "Sequel::Dataset#unbind" do
  before do
    @ds = Sequel::Database.new[:t]
    @u = proc{|ds| ds, bv = ds.unbind; [ds.sql, bv]}
  end

  it "should unbind values assigned to equality and inequality statements" do
    @ds.filter(:foo=>1).unbind.first.sql.must_equal "SELECT * FROM t WHERE (foo = $foo)"
    @ds.exclude(:foo=>1).unbind.first.sql.must_equal "SELECT * FROM t WHERE (foo != $foo)"
    @ds.filter{foo > 1}.unbind.first.sql.must_equal "SELECT * FROM t WHERE (foo > $foo)"
    @ds.filter{foo >= 1}.unbind.first.sql.must_equal "SELECT * FROM t WHERE (foo >= $foo)"
    @ds.filter{foo < 1}.unbind.first.sql.must_equal "SELECT * FROM t WHERE (foo < $foo)"
    @ds.filter{foo <= 1}.unbind.first.sql.must_equal "SELECT * FROM t WHERE (foo <= $foo)"
  end

  it "should return variables that could be used bound to recreate the previous query" do
    @ds.filter(:foo=>1).unbind.last.must_equal(:foo=>1)
    @ds.exclude(:foo=>1).unbind.last.must_equal(:foo=>1)
  end

  it "should return variables as symbols" do
    @ds.filter(Sequel.expr(:foo)=>1).unbind.last.must_equal(:foo=>1)
    @ds.exclude(Sequel.expr(:foo__bar)=>1).unbind.last.must_equal(:"foo.bar"=>1)
  end

  it "should handle numerics, strings, dates, times, and datetimes" do
    @u[@ds.filter(:foo=>1)].must_equal ["SELECT * FROM t WHERE (foo = $foo)", {:foo=>1}]
    @u[@ds.filter(:foo=>1.0)].must_equal ["SELECT * FROM t WHERE (foo = $foo)", {:foo=>1.0}]
    @u[@ds.filter(:foo=>BigDecimal.new('1.0'))].must_equal ["SELECT * FROM t WHERE (foo = $foo)", {:foo=>BigDecimal.new('1.0')}]
    @u[@ds.filter(:foo=>'a')].must_equal ["SELECT * FROM t WHERE (foo = $foo)", {:foo=>'a'}]
    @u[@ds.filter(:foo=>Date.today)].must_equal ["SELECT * FROM t WHERE (foo = $foo)", {:foo=>Date.today}]
    t = Time.now
    @u[@ds.filter(:foo=>t)].must_equal ["SELECT * FROM t WHERE (foo = $foo)", {:foo=>t}]
    dt = DateTime.now
    @u[@ds.filter(:foo=>dt)].must_equal ["SELECT * FROM t WHERE (foo = $foo)", {:foo=>dt}]
  end

  it "should not unbind literal strings" do
    @u[@ds.filter(:foo=>Sequel.lit('a'))].must_equal ["SELECT * FROM t WHERE (foo = a)", {}]
  end

  it "should not unbind Identifiers, QualifiedIdentifiers, or Symbols used as booleans" do
    @u[@ds.filter(:foo).filter{bar}.filter{foo__bar}].must_equal ["SELECT * FROM t WHERE (foo AND bar AND foo.bar)", {}]
  end

  it "should not unbind for values it doesn't understand" do
    @u[@ds.filter(:foo=>Class.new{def sql_literal(ds) 'bar' end}.new)].must_equal ["SELECT * FROM t WHERE (foo = bar)", {}]
  end

  it "should handle QualifiedIdentifiers" do
    @u[@ds.filter{foo__bar > 1}].must_equal ["SELECT * FROM t WHERE (foo.bar > $foo.bar)", {:"foo.bar"=>1}]
  end

  it "should handle wrapped objects" do
    @u[@ds.filter{Sequel::SQL::Wrapper.new(foo__bar) > Sequel::SQL::Wrapper.new(1)}].must_equal ["SELECT * FROM t WHERE (foo.bar > $foo.bar)", {:"foo.bar"=>1}]
  end

  it "should handle deep nesting" do
    @u[@ds.filter{foo > 1}.and{bar < 2}.or(:baz=>3).and(Sequel.case({~Sequel.expr(:x=>4)=>true}, false))].must_equal ["SELECT * FROM t WHERE ((((foo > $foo) AND (bar < $bar)) OR (baz = $baz)) AND (CASE WHEN (x != $x) THEN 't' ELSE 'f' END))", {:foo=>1, :bar=>2, :baz=>3, :x=>4}]
  end

  it "should handle JOIN ON" do
    @u[@ds.cross_join(:x).join(:a, [:u]).join(:b, [[:c, :d], [:e,1]])].must_equal ["SELECT * FROM t CROSS JOIN x INNER JOIN a USING (u) INNER JOIN b ON ((b.c = a.d) AND (b.e = $b.e))", {:"b.e"=>1}]
  end

  it "should raise an UnbindDuplicate exception if same variable is used with multiple different values" do
    proc{@ds.filter(:foo=>1).or(:foo=>2).unbind}.must_raise(Sequel::UnbindDuplicate)
  end

  it "should handle case where the same variable has the same value in multiple places " do
    @u[@ds.filter(:foo=>1).or(:foo=>1)].must_equal ["SELECT * FROM t WHERE ((foo = $foo) OR (foo = $foo))", {:foo=>1}]
  end

  it "should raise Error for unhandled objects inside Identifiers and QualifiedIndentifiers" do
    proc{@ds.filter(Sequel::SQL::Identifier.new([]) > 1).unbind}.must_raise(Sequel::Error)
    proc{@ds.filter{foo.qualify({}) > 1}.unbind}.must_raise(Sequel::Error)
  end
end

describe "Sequel::Dataset #with and #with_recursive" do
  before do
    @db = Sequel::Database.new
    @ds = @db[:t]
    def @ds.supports_cte?(*) true end
  end
  
  it "#with should take a name and dataset and use a WITH clause" do
    @ds.with(:t, @db[:x]).sql.must_equal 'WITH t AS (SELECT * FROM x) SELECT * FROM t'
  end

  it "#with_recursive should take a name, nonrecursive dataset, and recursive dataset, and use a WITH clause" do
    @ds.with_recursive(:t, @db[:x], @db[:t]).sql.must_equal 'WITH t AS (SELECT * FROM x UNION ALL SELECT * FROM t) SELECT * FROM t'
  end
  
  it "#with and #with_recursive should add to existing WITH clause if called multiple times" do
    @ds.with(:t, @db[:x]).with(:j, @db[:y]).sql.must_equal 'WITH t AS (SELECT * FROM x), j AS (SELECT * FROM y) SELECT * FROM t'
    @ds.with_recursive(:t, @db[:x], @db[:t]).with_recursive(:j, @db[:y], @db[:j]).sql.must_equal 'WITH t AS (SELECT * FROM x UNION ALL SELECT * FROM t), j AS (SELECT * FROM y UNION ALL SELECT * FROM j) SELECT * FROM t'
    @ds.with(:t, @db[:x]).with_recursive(:j, @db[:y], @db[:j]).sql.must_equal 'WITH t AS (SELECT * FROM x), j AS (SELECT * FROM y UNION ALL SELECT * FROM j) SELECT * FROM t'
  end
  
  it "#with and #with_recursive should take an :args option" do
    @ds.with(:t, @db[:x], :args=>[:b]).sql.must_equal 'WITH t(b) AS (SELECT * FROM x) SELECT * FROM t'
    @ds.with_recursive(:t, @db[:x], @db[:t], :args=>[:b, :c]).sql.must_equal 'WITH t(b, c) AS (SELECT * FROM x UNION ALL SELECT * FROM t) SELECT * FROM t'
  end
  
  it "#with and #with_recursive should quote the columns in the :args option" do
    @ds.quote_identifiers = true
    @ds.with(:t, @db[:x], :args=>[:b]).sql.must_equal 'WITH "t"("b") AS (SELECT * FROM x) SELECT * FROM "t"'
    @ds.with_recursive(:t, @db[:x], @db[:t], :args=>[:b, :c]).sql.must_equal 'WITH "t"("b", "c") AS (SELECT * FROM x UNION ALL SELECT * FROM t) SELECT * FROM "t"'
  end
  
  it "#with_recursive should take an :union_all=>false option" do
    @ds.with_recursive(:t, @db[:x], @db[:t], :union_all=>false).sql.must_equal 'WITH t AS (SELECT * FROM x UNION SELECT * FROM t) SELECT * FROM t'
  end

  it "#with and #with_recursive should raise an error unless the dataset supports CTEs" do
    meta_def(@ds, :supports_cte?){false}
    proc{@ds.with(:t, @db[:x], :args=>[:b])}.must_raise(Sequel::Error)
    proc{@ds.with_recursive(:t, @db[:x], @db[:t], :args=>[:b, :c])}.must_raise(Sequel::Error)
  end

  it "#with should work on insert, update, and delete statements if they support it" do
    sc = class << @ds; self; end
    Sequel::Dataset.def_sql_method(sc, :delete, %w'with delete from where')
    Sequel::Dataset.def_sql_method(sc, :insert, %w'with insert into columns values')
    Sequel::Dataset.def_sql_method(sc, :update, %w'with update table set where')
    @ds.with(:t, @db[:x]).insert_sql(1).must_equal 'WITH t AS (SELECT * FROM x) INSERT INTO t VALUES (1)'
    @ds.with(:t, @db[:x]).update_sql(:foo=>1).must_equal 'WITH t AS (SELECT * FROM x) UPDATE t SET foo = 1'
    @ds.with(:t, @db[:x]).delete_sql.must_equal 'WITH t AS (SELECT * FROM x) DELETE FROM t'
  end

  it "should hoist WITH clauses in given dataset(s) if dataset doesn't support WITH in subselect" do
    meta_def(@ds, :supports_cte?){true}
    meta_def(@ds, :supports_cte_in_subselect?){false}
    @ds.with(:t, @ds.from(:s).with(:s, @ds.from(:r))).sql.must_equal 'WITH s AS (SELECT * FROM r), t AS (SELECT * FROM s) SELECT * FROM t'
    @ds.with_recursive(:t, @ds.from(:s).with(:s, @ds.from(:r)), @ds.from(:q).with(:q, @ds.from(:p))).sql.must_equal 'WITH s AS (SELECT * FROM r), q AS (SELECT * FROM p), t AS (SELECT * FROM s UNION ALL SELECT * FROM q) SELECT * FROM t'
  end
end

describe Sequel::SQL::Constants do
  before do
    @db = Sequel::Database.new
  end
  
  it "should have CURRENT_DATE" do
    @db.literal(Sequel::SQL::Constants::CURRENT_DATE).must_equal 'CURRENT_DATE'
    @db.literal(Sequel::CURRENT_DATE).must_equal 'CURRENT_DATE'
  end

  it "should have CURRENT_TIME" do
    @db.literal(Sequel::SQL::Constants::CURRENT_TIME).must_equal 'CURRENT_TIME'
    @db.literal(Sequel::CURRENT_TIME).must_equal 'CURRENT_TIME'
  end

  it "should have CURRENT_TIMESTAMP" do
    @db.literal(Sequel::SQL::Constants::CURRENT_TIMESTAMP).must_equal 'CURRENT_TIMESTAMP'
    @db.literal(Sequel::CURRENT_TIMESTAMP).must_equal 'CURRENT_TIMESTAMP'
  end

  it "should have NULL" do
    @db.literal(Sequel::SQL::Constants::NULL).must_equal 'NULL'
    @db.literal(Sequel::NULL).must_equal 'NULL'
  end

  it "should have NOTNULL" do
    @db.literal(Sequel::SQL::Constants::NOTNULL).must_equal 'NOT NULL'
    @db.literal(Sequel::NOTNULL).must_equal 'NOT NULL'
  end

  it "should have TRUE and SQLTRUE" do
    @db.literal(Sequel::SQL::Constants::TRUE).must_equal "'t'"
    @db.literal(Sequel::TRUE).must_equal "'t'"
    @db.literal(Sequel::SQL::Constants::SQLTRUE).must_equal "'t'"
    @db.literal(Sequel::SQLTRUE).must_equal "'t'"
  end

  it "should have FALSE and SQLFALSE" do
    @db.literal(Sequel::SQL::Constants::FALSE).must_equal "'f'"
    @db.literal(Sequel::FALSE).must_equal "'f'"
    @db.literal(Sequel::SQL::Constants::SQLFALSE).must_equal "'f'"
    @db.literal(Sequel::SQLFALSE).must_equal "'f'"
  end
end

describe "Sequel timezone support" do
  before do
    @db = Sequel::Database.new
    @dataset = @db.dataset
    meta_def(@dataset, :supports_timestamp_timezones?){true}
    meta_def(@dataset, :supports_timestamp_usecs?){false}
    @utc_time = Time.utc(2010, 1, 2, 3, 4, 5)
    @local_time = Time.local(2010, 1, 2, 3, 4, 5)
    @offset = sprintf("%+03i%02i", *(@local_time.utc_offset/60).divmod(60))
    @dt_offset = @local_time.utc_offset/Rational(86400, 1)
    @utc_datetime = DateTime.new(2010, 1, 2, 3, 4, 5)
    @local_datetime = DateTime.new(2010, 1, 2, 3, 4, 5, @dt_offset)
  end
  after do
    Sequel.default_timezone = nil
    Sequel.datetime_class = Time
  end
  
  it "should handle an database timezone of :utc when literalizing values" do
    Sequel.database_timezone = :utc
    @dataset.literal(Time.utc(2010, 1, 2, 3, 4, 5)).must_equal "'2010-01-02 03:04:05+0000'"
    @dataset.literal(DateTime.new(2010, 1, 2, 3, 4, 5)).must_equal "'2010-01-02 03:04:05+0000'"
  end
  
  it "should handle an database timezone of :local when literalizing values" do
    Sequel.database_timezone = :local
    @dataset.literal(Time.local(2010, 1, 2, 3, 4, 5)).must_equal "'2010-01-02 03:04:05#{@offset}'"
    @dataset.literal(DateTime.new(2010, 1, 2, 3, 4, 5, @dt_offset)).must_equal "'2010-01-02 03:04:05#{@offset}'"
  end
  
  it "should have Database#timezone override Sequel.database_timezone" do
    Sequel.database_timezone = :local
    @db.timezone = :utc
    @dataset.literal(Time.utc(2010, 1, 2, 3, 4, 5)).must_equal "'2010-01-02 03:04:05+0000'"
    @dataset.literal(DateTime.new(2010, 1, 2, 3, 4, 5)).must_equal "'2010-01-02 03:04:05+0000'"

    Sequel.database_timezone = :utc
    @db.timezone = :local
    @dataset.literal(Time.local(2010, 1, 2, 3, 4, 5)).must_equal "'2010-01-02 03:04:05#{@offset}'"
    @dataset.literal(DateTime.new(2010, 1, 2, 3, 4, 5, @dt_offset)).must_equal "'2010-01-02 03:04:05#{@offset}'"
  end
  
  it "should handle converting database timestamps into application timestamps" do
    Sequel.database_timezone = :utc
    Sequel.application_timezone = :local
    t = Time.now.utc
    Sequel.database_to_application_timestamp(t).to_s.must_equal t.getlocal.to_s
    Sequel.database_to_application_timestamp(t.to_s).to_s.must_equal t.getlocal.to_s
    Sequel.database_to_application_timestamp(t.strftime('%Y-%m-%d %H:%M:%S')).to_s.must_equal t.getlocal.to_s
    
    Sequel.datetime_class = DateTime
    dt = DateTime.now
    dt2 = dt.new_offset(0)
    Sequel.database_to_application_timestamp(dt2).to_s.must_equal dt.to_s
    Sequel.database_to_application_timestamp(dt2.to_s).to_s.must_equal dt.to_s
    Sequel.database_to_application_timestamp(dt2.strftime('%Y-%m-%d %H:%M:%S')).to_s.must_equal dt.to_s
    
    Sequel.datetime_class = Time
    Sequel.database_timezone = :local
    Sequel.application_timezone = :utc
    Sequel.database_to_application_timestamp(t.getlocal).to_s.must_equal t.to_s
    Sequel.database_to_application_timestamp(t.getlocal.to_s).to_s.must_equal t.to_s
    Sequel.database_to_application_timestamp(t.getlocal.strftime('%Y-%m-%d %H:%M:%S')).to_s.must_equal t.to_s
    
    Sequel.datetime_class = DateTime
    Sequel.database_to_application_timestamp(dt).to_s.must_equal dt2.to_s
    Sequel.database_to_application_timestamp(dt.to_s).to_s.must_equal dt2.to_s
    Sequel.database_to_application_timestamp(dt.strftime('%Y-%m-%d %H:%M:%S')).to_s.must_equal dt2.to_s
  end
  
  it "should handle typecasting timestamp columns" do
    Sequel.typecast_timezone = :utc
    Sequel.application_timezone = :local
    t = Time.now.utc
    @db.typecast_value(:datetime, t).to_s.must_equal t.getlocal.to_s
    @db.typecast_value(:datetime, t.to_s).to_s.must_equal t.getlocal.to_s
    @db.typecast_value(:datetime, t.strftime('%Y-%m-%d %H:%M:%S')).to_s.must_equal t.getlocal.to_s
    
    Sequel.datetime_class = DateTime
    dt = DateTime.now
    dt2 = dt.new_offset(0)
    @db.typecast_value(:datetime, dt2).to_s.must_equal dt.to_s
    @db.typecast_value(:datetime, dt2.to_s).to_s.must_equal dt.to_s
    @db.typecast_value(:datetime, dt2.strftime('%Y-%m-%d %H:%M:%S')).to_s.must_equal dt.to_s
    
    Sequel.datetime_class = Time
    Sequel.typecast_timezone = :local
    Sequel.application_timezone = :utc
    @db.typecast_value(:datetime, t.getlocal).to_s.must_equal t.to_s
    @db.typecast_value(:datetime, t.getlocal.to_s).to_s.must_equal t.to_s
    @db.typecast_value(:datetime, t.getlocal.strftime('%Y-%m-%d %H:%M:%S')).to_s.must_equal t.to_s
    
    Sequel.datetime_class = DateTime
    @db.typecast_value(:datetime, dt).to_s.must_equal dt2.to_s
    @db.typecast_value(:datetime, dt.to_s).to_s.must_equal dt2.to_s
    @db.typecast_value(:datetime, dt.strftime('%Y-%m-%d %H:%M:%S')).to_s.must_equal dt2.to_s
  end
  
  it "should handle converting database timestamp columns from an array of values" do
    Sequel.database_timezone = :utc
    Sequel.application_timezone = :local
    t = Time.now.utc
    Sequel.database_to_application_timestamp([t.year, t.mon, t.day, t.hour, t.min, t.sec]).to_s.must_equal t.getlocal.to_s
    
    Sequel.datetime_class = DateTime
    dt = DateTime.now
    dt2 = dt.new_offset(0)
    Sequel.database_to_application_timestamp([dt2.year, dt2.mon, dt2.day, dt2.hour, dt2.min, dt2.sec]).to_s.must_equal dt.to_s
    
    Sequel.datetime_class = Time
    Sequel.database_timezone = :local
    Sequel.application_timezone = :utc
    t = t.getlocal
    Sequel.database_to_application_timestamp([t.year, t.mon, t.day, t.hour, t.min, t.sec]).to_s.must_equal t.getutc.to_s
    
    Sequel.datetime_class = DateTime
    Sequel.database_to_application_timestamp([dt.year, dt.mon, dt.day, dt.hour, dt.min, dt.sec]).to_s.must_equal dt2.to_s
  end
  
  it "should raise an InvalidValue error when an error occurs while converting a timestamp" do
    proc{Sequel.database_to_application_timestamp([0, 0, 0, 0, 0, 0])}.must_raise(Sequel::InvalidValue)
  end
  
  it "should raise an error when attempting to typecast to a timestamp from an unsupported type" do
    proc{Sequel.database_to_application_timestamp(Object.new)}.must_raise(Sequel::InvalidValue)
  end

  it "should raise an InvalidValue error when the DateTime class is used and when a bad application timezone is used when attempting to convert timestamps" do
    Sequel.application_timezone = :blah
    Sequel.datetime_class = DateTime
    proc{Sequel.database_to_application_timestamp('2009-06-01 10:20:30')}.must_raise(Sequel::InvalidValue)
  end
  
  it "should raise an InvalidValue error when the DateTime class is used and when a bad database timezone is used when attempting to convert timestamps" do
    Sequel.database_timezone = :blah
    Sequel.datetime_class = DateTime
    proc{Sequel.database_to_application_timestamp('2009-06-01 10:20:30')}.must_raise(Sequel::InvalidValue)
  end

  it "should have Sequel.default_timezone= should set all other timezones" do
    Sequel.database_timezone.must_equal nil
    Sequel.application_timezone.must_equal nil
    Sequel.typecast_timezone.must_equal nil
    Sequel.default_timezone = :utc
    Sequel.database_timezone.must_equal :utc
    Sequel.application_timezone.must_equal :utc
    Sequel.typecast_timezone.must_equal :utc
  end
end

describe "Sequel::Dataset#select_map" do
  before do
    @ds = Sequel.mock(:fetch=>[{:c=>1}, {:c=>2}])[:t]
  end

  it "should do select and map in one step" do
    @ds.select_map(:a).must_equal [1, 2]
    @ds.db.sqls.must_equal ['SELECT a FROM t']
  end

  it "should handle implicit qualifiers in arguments" do
    @ds.select_map(:a__b).must_equal [1, 2]
    @ds.db.sqls.must_equal ['SELECT a.b FROM t']
  end

  it "should raise if multiple arguments and can't determine alias" do
    proc{@ds.select_map([Sequel.function(:a), :b])}.must_raise(Sequel::Error)
    proc{@ds.select_map(Sequel.function(:a)){b}}.must_raise(Sequel::Error)
    proc{@ds.select_map{[a{}, b]}}.must_raise(Sequel::Error)
  end

  it "should handle implicit aliases in arguments" do
    @ds.select_map(:a___b).must_equal [1, 2]
    @ds.db.sqls.must_equal ['SELECT a AS b FROM t']
  end

  it "should handle other objects" do
    @ds.select_map(Sequel.lit("a").as(:b)).must_equal [1, 2]
    @ds.db.sqls.must_equal ['SELECT a AS b FROM t']
  end
  
  it "should handle identifiers with strings" do
    @ds.select_map([Sequel::SQL::Identifier.new('c'), :c]).must_equal [[1, 1], [2, 2]]
    @ds.db.sqls.must_equal ['SELECT c, c FROM t']
  end
  
  it "should raise an error for plain strings" do
    proc{@ds.select_map(['c', :c])}.must_raise(Sequel::Error)
    @ds.db.sqls.must_equal []
  end
  
  it "should handle an expression without a determinable alias" do
    @ds.select_map{a(t__c)}.must_equal [1, 2]
    @ds.db.sqls.must_equal ['SELECT a(t.c) AS v FROM t']
  end

  it "should accept a block" do
    @ds.select_map{a(t__c).as(b)}.must_equal [1, 2]
    @ds.db.sqls.must_equal ['SELECT a(t.c) AS b FROM t']
  end

  it "should accept a block with an array of columns" do
    @ds.select_map{[a(t__c).as(c), a(t__c).as(c)]}.must_equal [[1, 1], [2, 2]]
    @ds.db.sqls.must_equal ['SELECT a(t.c) AS c, a(t.c) AS c FROM t']
  end

  it "should accept a block with a column" do
    @ds.select_map(:c){a(t__c).as(c)}.must_equal [[1, 1], [2, 2]]
    @ds.db.sqls.must_equal ['SELECT c, a(t.c) AS c FROM t']
  end

  it "should accept a block and array of arguments" do
    @ds.select_map([:c, :c]){[a(t__c).as(c), a(t__c).as(c)]}.must_equal [[1, 1, 1, 1], [2, 2, 2, 2]]
    @ds.db.sqls.must_equal ['SELECT c, c, a(t.c) AS c, a(t.c) AS c FROM t']
  end

  it "should handle an array of columns" do
    @ds.select_map([:c, :c]).must_equal [[1, 1], [2, 2]]
    @ds.db.sqls.must_equal ['SELECT c, c FROM t']
    @ds.select_map([Sequel.expr(:d).as(:c), Sequel.qualify(:b, :c), Sequel.identifier(:c), Sequel.identifier(:c).qualify(:b), :a__c, :a__d___c]).must_equal [[1, 1, 1, 1, 1, 1], [2, 2, 2, 2, 2, 2]]
    @ds.db.sqls.must_equal ['SELECT d AS c, b.c, c, b.c, a.c, a.d AS c FROM t']
  end

  it "should handle an array with a single element" do
    @ds.select_map([:c]).must_equal [[1], [2]]
    @ds.db.sqls.must_equal ['SELECT c FROM t']
  end
end

describe "Sequel::Dataset#select_order_map" do
  before do
    @ds = Sequel.mock(:fetch=>[{:c=>1}, {:c=>2}])[:t]
  end

  it "should do select and map in one step" do
    @ds.select_order_map(:a).must_equal [1, 2]
    @ds.db.sqls.must_equal ['SELECT a FROM t ORDER BY a']
  end

  it "should handle implicit qualifiers in arguments" do
    @ds.select_order_map(:a__b).must_equal [1, 2]
    @ds.db.sqls.must_equal ['SELECT a.b FROM t ORDER BY a.b']
  end

  it "should raise if multiple arguments and can't determine alias" do
    proc{@ds.select_order_map([Sequel.function(:a), :b])}.must_raise(Sequel::Error)
    proc{@ds.select_order_map(Sequel.function(:a)){b}}.must_raise(Sequel::Error)
    proc{@ds.select_order_map{[a{}, b]}}.must_raise(Sequel::Error)
  end

  it "should handle implicit aliases in arguments" do
    @ds.select_order_map(:a___b).must_equal [1, 2]
    @ds.db.sqls.must_equal ['SELECT a AS b FROM t ORDER BY a']
  end

  it "should handle implicit qualifiers and aliases in arguments" do
    @ds.select_order_map(:t__a___b).must_equal [1, 2]
    @ds.db.sqls.must_equal ['SELECT t.a AS b FROM t ORDER BY t.a']
  end

  it "should handle AliasedExpressions" do
    @ds.select_order_map(Sequel.lit("a").as(:b)).must_equal [1, 2]
    @ds.db.sqls.must_equal ['SELECT a AS b FROM t ORDER BY a']
  end
  
  it "should handle OrderedExpressions" do
    @ds.select_order_map(Sequel.desc(:a)).must_equal [1, 2]
    @ds.db.sqls.must_equal ['SELECT a FROM t ORDER BY a DESC']
  end
  
  it "should handle an expression without a determinable alias" do
    @ds.select_order_map{a(t__c)}.must_equal [1, 2]
    @ds.db.sqls.must_equal ['SELECT a(t.c) AS v FROM t ORDER BY a(t.c)']
  end

  it "should accept a block" do
    @ds.select_order_map{a(t__c).as(b)}.must_equal [1, 2]
    @ds.db.sqls.must_equal ['SELECT a(t.c) AS b FROM t ORDER BY a(t.c)']
  end

  it "should accept a block with an array of columns" do
    @ds.select_order_map{[c.desc, a(t__c).as(c)]}.must_equal [[1, 1], [2, 2]]
    @ds.db.sqls.must_equal ['SELECT c, a(t.c) AS c FROM t ORDER BY c DESC, a(t.c)']
  end

  it "should accept a block with a column" do
    @ds.select_order_map(:c){a(t__c).as(c)}.must_equal [[1, 1], [2, 2]]
    @ds.db.sqls.must_equal ['SELECT c, a(t.c) AS c FROM t ORDER BY c, a(t.c)']
  end

  it "should accept a block and array of arguments" do
    @ds.select_order_map([:c, :c]){[a(t__c).as(c), c.desc]}.must_equal [[1, 1, 1, 1], [2, 2, 2, 2]]
    @ds.db.sqls.must_equal ['SELECT c, c, a(t.c) AS c, c FROM t ORDER BY c, c, a(t.c), c DESC']
  end

  it "should handle an array of columns" do
    @ds.select_order_map([:c, :c]).must_equal [[1, 1], [2, 2]]
    @ds.db.sqls.must_equal ['SELECT c, c FROM t ORDER BY c, c']
    @ds.select_order_map([Sequel.expr(:d).as(:c), Sequel.qualify(:b, :c), Sequel.identifier(:c), Sequel.identifier(:c).qualify(:b), Sequel.identifier(:c).qualify(:b).desc, :a__c, Sequel.desc(:a__d___c), Sequel.desc(Sequel.expr(:a__d___c))]).must_equal [[1, 1, 1, 1, 1, 1, 1, 1], [2, 2, 2, 2, 2, 2, 2, 2]]
    @ds.db.sqls.must_equal ['SELECT d AS c, b.c, c, b.c, b.c, a.c, a.d AS c, a.d AS c FROM t ORDER BY d, b.c, c, b.c, b.c DESC, a.c, a.d DESC, a.d DESC']
  end

  it "should handle an array with a single element" do
    @ds.select_order_map([:c]).must_equal [[1], [2]]
    @ds.db.sqls.must_equal ['SELECT c FROM t ORDER BY c']
  end
end

describe "Sequel::Dataset#select_hash" do
  before do
    @db = Sequel.mock(:fetch=>[{:a=>1, :b=>2}, {:a=>3, :b=>4}])
    @ds = @db[:t]
  end

  it "should do select and to_hash in one step" do
    @ds.select_hash(:a, :b).must_equal(1=>2, 3=>4)
    @ds.db.sqls.must_equal ['SELECT a, b FROM t']
  end

  it "should handle implicit qualifiers in arguments" do
    @ds.select_hash(:t__a, :t__b).must_equal(1=>2, 3=>4)
    @ds.db.sqls.must_equal ['SELECT t.a, t.b FROM t']
  end

  it "should handle implicit aliases in arguments" do
    @ds.select_hash(:c___a, :d___b).must_equal(1=>2, 3=>4)
    @ds.db.sqls.must_equal ['SELECT c AS a, d AS b FROM t']
  end

  it "should handle implicit qualifiers and aliases in arguments" do
    @ds.select_hash(:t__c___a, :t__d___b).must_equal(1=>2, 3=>4)
    @ds.db.sqls.must_equal ['SELECT t.c AS a, t.d AS b FROM t']
  end

  it "should handle SQL::Identifiers in arguments" do
    @ds.select_hash(Sequel.identifier(:a), Sequel.identifier(:b)).must_equal(1=>2, 3=>4)
    @ds.db.sqls.must_equal ['SELECT a, b FROM t']
  end

  it "should handle SQL::QualifiedIdentifiers in arguments" do
    @ds.select_hash(Sequel.qualify(:t, :a), Sequel.identifier(:b).qualify(:t)).must_equal(1=>2, 3=>4)
    @ds.db.sqls.must_equal ['SELECT t.a, t.b FROM t']
  end

  it "should handle SQL::AliasedExpressions in arguments" do
    @ds.select_hash(Sequel.expr(:c).as(:a), Sequel.expr(:t).as(:b)).must_equal(1=>2, 3=>4)
    @ds.db.sqls.must_equal ['SELECT c AS a, t AS b FROM t']
  end

  it "should work with arrays of columns" do
    @db.fetch = [{:a=>1, :b=>2, :c=>3}, {:a=>4, :b=>5, :c=>6}]
    @ds.select_hash([:a, :c], :b).must_equal([1, 3]=>2, [4, 6]=>5)
    @ds.db.sqls.must_equal ['SELECT a, c, b FROM t']
    @ds.select_hash(:a, [:b, :c]).must_equal(1=>[2, 3], 4=>[5, 6])
    @ds.db.sqls.must_equal ['SELECT a, b, c FROM t']
    @ds.select_hash([:a, :b], [:b, :c]).must_equal([1, 2]=>[2, 3], [4, 5]=>[5, 6])
    @ds.db.sqls.must_equal ['SELECT a, b, b, c FROM t']
  end

  it "should raise an error if the resulting symbol cannot be determined" do
    proc{@ds.select_hash(Sequel.expr(:c).as(:a), Sequel.function(:b))}.must_raise(Sequel::Error)
  end
end

describe "Sequel::Dataset#select_hash_groups" do
  before do
    @db = Sequel.mock(:fetch=>[{:a=>1, :b=>2}, {:a=>3, :b=>4}])
    @ds = @db[:t]
  end

  it "should do select and to_hash in one step" do
    @ds.select_hash_groups(:a, :b).must_equal(1=>[2], 3=>[4])
    @ds.db.sqls.must_equal ['SELECT a, b FROM t']
  end

  it "should handle implicit qualifiers in arguments" do
    @ds.select_hash_groups(:t__a, :t__b).must_equal(1=>[2], 3=>[4])
    @ds.db.sqls.must_equal ['SELECT t.a, t.b FROM t']
  end

  it "should handle implicit aliases in arguments" do
    @ds.select_hash_groups(:c___a, :d___b).must_equal(1=>[2], 3=>[4])
    @ds.db.sqls.must_equal ['SELECT c AS a, d AS b FROM t']
  end

  it "should handle implicit qualifiers and aliases in arguments" do
    @ds.select_hash_groups(:t__c___a, :t__d___b).must_equal(1=>[2], 3=>[4])
    @ds.db.sqls.must_equal ['SELECT t.c AS a, t.d AS b FROM t']
  end

  it "should handle SQL::Identifiers in arguments" do
    @ds.select_hash_groups(Sequel.identifier(:a), Sequel.identifier(:b)).must_equal(1=>[2], 3=>[4])
    @ds.db.sqls.must_equal ['SELECT a, b FROM t']
  end

  it "should handle SQL::QualifiedIdentifiers in arguments" do
    @ds.select_hash_groups(Sequel.qualify(:t, :a), Sequel.identifier(:b).qualify(:t)).must_equal(1=>[2], 3=>[4])
    @ds.db.sqls.must_equal ['SELECT t.a, t.b FROM t']
  end

  it "should handle SQL::AliasedExpressions in arguments" do
    @ds.select_hash_groups(Sequel.expr(:c).as(:a), Sequel.expr(:t).as(:b)).must_equal(1=>[2], 3=>[4])
    @ds.db.sqls.must_equal ['SELECT c AS a, t AS b FROM t']
  end

  it "should work with arrays of columns" do
    @db.fetch = [{:a=>1, :b=>2, :c=>3}, {:a=>4, :b=>5, :c=>6}]
    @ds.select_hash_groups([:a, :c], :b).must_equal([1, 3]=>[2], [4, 6]=>[5])
    @ds.db.sqls.must_equal ['SELECT a, c, b FROM t']
    @ds.select_hash_groups(:a, [:b, :c]).must_equal(1=>[[2, 3]], 4=>[[5, 6]])
    @ds.db.sqls.must_equal ['SELECT a, b, c FROM t']
    @ds.select_hash_groups([:a, :b], [:b, :c]).must_equal([1, 2]=>[[2, 3]], [4, 5]=>[[5, 6]])
    @ds.db.sqls.must_equal ['SELECT a, b, b, c FROM t']
  end

  it "should raise an error if the resulting symbol cannot be determined" do
    proc{@ds.select_hash_groups(Sequel.expr(:c).as(:a), Sequel.function(:b))}.must_raise(Sequel::Error)
  end
end

describe "Modifying joined datasets" do
  before do
    @ds = Sequel.mock.from(:b, :c).join(:d, [:id]).where(:id => 2)
    meta_def(@ds, :supports_modifying_joins?){true}
  end

  it "should allow deleting from joined datasets" do
    @ds.delete
    @ds.db.sqls.must_equal ['DELETE FROM b, c WHERE (id = 2)']
  end

  it "should allow updating joined datasets" do
    @ds.update(:a=>1)
    @ds.db.sqls.must_equal ['UPDATE b, c INNER JOIN d USING (id) SET a = 1 WHERE (id = 2)']
  end
end

describe "Dataset#lock_style and for_update" do
  before do
    @ds = Sequel.mock.dataset.from(:t)
  end
  
  it "#for_update should use FOR UPDATE" do
    @ds.for_update.sql.must_equal "SELECT * FROM t FOR UPDATE"
  end
  
  it "#lock_style should accept symbols" do
    @ds.lock_style(:update).sql.must_equal "SELECT * FROM t FOR UPDATE"
  end
  
  it "#lock_style should accept strings for arbitrary SQL" do
    @ds.lock_style("FOR SHARE").sql.must_equal "SELECT * FROM t FOR SHARE"
  end
end

describe "Custom ASTTransformer" do
  it "should transform given objects" do
    c = Class.new(Sequel::ASTTransformer) do
      def v(s)
        (s.is_a?(Symbol) || s.is_a?(String)) ? :"#{s}#{s}" : super
      end
    end.new
    ds = Sequel.mock.dataset.from(:t).cross_join(:a___g).join(:b___h, [:c]).join(:d___i, :e=>:f)
    ds.sql.must_equal 'SELECT * FROM t CROSS JOIN a AS g INNER JOIN b AS h USING (c) INNER JOIN d AS i ON (i.e = h.f)'
    ds.clone(:from=>c.transform(ds.opts[:from]), :join=>c.transform(ds.opts[:join])).sql.must_equal 'SELECT * FROM tt CROSS JOIN aa AS g INNER JOIN bb AS h USING (cc) INNER JOIN dd AS i ON (ii.ee = hh.ff)'
  end
end

describe "Dataset#returning" do
  before do
    @db = Sequel.mock(:fetch=>proc{|s| {:foo=>s}})
    @db.extend_datasets{def supports_returning?(type) true end}
    @ds = @db[:t].returning(:foo)
    @pr = proc do
      sc = class << @ds; self; end
      Sequel::Dataset.def_sql_method(sc, :delete, %w'delete from where returning')
      Sequel::Dataset.def_sql_method(sc, :insert, %w'insert into columns values returning')
      Sequel::Dataset.def_sql_method(sc, :update, %w'update table set where returning')
    end
  end
  
  it "should use RETURNING clause in the SQL if the dataset supports it" do
    @pr.call
    @ds.delete_sql.must_equal "DELETE FROM t RETURNING foo"
    @ds.insert_sql(1).must_equal "INSERT INTO t VALUES (1) RETURNING foo"
    @ds.update_sql(:foo=>1).must_equal "UPDATE t SET foo = 1 RETURNING foo"
  end
  
  it "should not use RETURNING clause in the SQL if the dataset does not support it" do
    @ds.delete_sql.must_equal "DELETE FROM t"
    @ds.insert_sql(1).must_equal "INSERT INTO t VALUES (1)"
    @ds.update_sql(:foo=>1).must_equal "UPDATE t SET foo = 1"
  end

  it "should have insert, update, and delete yield to blocks if RETURNING is used" do
    @pr.call
    h = {}
    @ds.delete{|r| h = r}
    h.must_equal(:foo=>"DELETE FROM t RETURNING foo")
    @ds.insert(1){|r| h = r}
    h.must_equal(:foo=>"INSERT INTO t VALUES (1) RETURNING foo")
    @ds.update(:foo=>1){|r| h = r}
    h.must_equal(:foo=>"UPDATE t SET foo = 1 RETURNING foo")
  end

  it "should have insert, update, and delete return arrays of hashes if RETURNING is used and a block is not given" do
    @pr.call
    @ds.delete.must_equal [{:foo=>"DELETE FROM t RETURNING foo"}]
    @ds.insert(1).must_equal [{:foo=>"INSERT INTO t VALUES (1) RETURNING foo"}]
    @ds.update(:foo=>1).must_equal [{:foo=>"UPDATE t SET foo = 1 RETURNING foo"}]
  end

  it "should raise an error if RETURNING is not supported" do
    @db.extend_datasets{def supports_returning?(type) false end}
    proc{@db[:t].returning}.must_raise(Sequel::Error)
  end
end

describe "Dataset emulating bitwise operator support" do
  before do
    @ds = Sequel::Database.new.dataset
    @ds.quote_identifiers = true
    def @ds.complex_expression_sql_append(sql, op, args)
      complex_expression_arg_pairs_append(sql, args){|a, b| Sequel.function(:bitand, a, b)}
    end
  end

  it "should work with any numbers of arguments for operators" do
    @ds.select(Sequel::SQL::ComplexExpression.new(:&, :x)).sql.must_equal 'SELECT "x"'
    @ds.select(Sequel.expr(:x) & 1).sql.must_equal 'SELECT bitand("x", 1)'
    @ds.select(Sequel.expr(:x) & 1 & 2).sql.must_equal 'SELECT bitand(bitand("x", 1), 2)'
  end
end

describe "Dataset feature defaults" do
  it "should not require aliases for recursive CTEs by default" do
    Sequel::Database.new.dataset.recursive_cte_requires_column_aliases?.must_equal false
  end

  it "should not require placeholder type specifiers by default" do
    Sequel::Database.new.dataset.requires_placeholder_type_specifiers?.must_equal false
  end
end

describe "Dataset extensions" do
  before(:all) do
    class << Sequel
      alias _extension extension
      remove_method :extension
      def extension(*)
      end
    end
  end
  after(:all) do
    class << Sequel
      remove_method :extension
      alias extension _extension
      remove_method :_extension
    end
  end
  before do
    @ds = Sequel.mock.dataset
  end

  it "should be able to register an extension with a module Database#extension extend the module" do
    Sequel::Dataset.register_extension(:foo, Module.new{def a; 1; end})
    @ds.extension(:foo).a.must_equal 1
  end

  it "should be able to register an extension with a block and Database#extension call the block" do
    @ds.quote_identifiers = false
    Sequel::Dataset.register_extension(:foo){|db| db.quote_identifiers = true}
    @ds.extension(:foo).quote_identifiers?.must_equal true
  end

  it "should be able to register an extension with a callable and Database#extension call the callable" do
    @ds.quote_identifiers = false
    Sequel::Dataset.register_extension(:foo, proc{|db| db.quote_identifiers = true})
    @ds.extension(:foo).quote_identifiers?.must_equal true
  end

  it "should be able to load multiple extensions in the same call" do
    @ds.quote_identifiers = false
    @ds.identifier_input_method = :downcase
    Sequel::Dataset.register_extension(:foo, proc{|ds| ds.quote_identifiers = true})
    Sequel::Dataset.register_extension(:bar, proc{|ds| ds.identifier_input_method = nil})
    ds = @ds.extension(:foo, :bar)
    ds.quote_identifiers?.must_equal true
    ds.identifier_input_method.must_equal nil
  end

  it "should have #extension not modify the receiver" do
    Sequel::Dataset.register_extension(:foo, Module.new{def a; 1; end})
    @ds.extension(:foo)
    proc{@ds.a}.must_raise(NoMethodError)
  end

  it "should have #extension not return a cloned dataset" do
    @ds.extend(Module.new{def b; 2; end})
    Sequel::Dataset.register_extension(:foo, Module.new{def a; 1; end})
    v = @ds.extension(:foo)
    v.must_equal(@ds)
    v.must_be_kind_of(Sequel::Dataset)
    v.b.must_equal 2
  end

  it "should have #extension! modify the receiver" do
    Sequel::Dataset.register_extension(:foo, Module.new{def a; 1; end})
    @ds.extension!(:foo)
    @ds.a.must_equal 1
  end

  it "should have #extension! return the receiver" do
    Sequel::Dataset.register_extension(:foo, Module.new{def a; 1; end})
    @ds.extension!(:foo).must_be_same_as(@ds)
  end

  it "should register a Database extension for modifying all datasets when registering with a module" do
    Sequel::Dataset.register_extension(:foo, Module.new{def a; 1; end})
    Sequel.mock.extension(:foo).dataset.a.must_equal 1
  end

  it "should raise an Error if registering with both a module and a block" do
    proc{Sequel::Dataset.register_extension(:foo, Module.new){}}.must_raise(Sequel::Error)
  end

  it "should raise an Error if attempting to load an incompatible extension" do
    proc{@ds.extension(:foo2)}.must_raise(Sequel::Error)
  end
end

describe "Dataset#schema_and_table" do
  before do
    @ds = Sequel.mock[:test]
  end

  it "should correctly handle symbols" do
    @ds.schema_and_table(:s).must_equal [nil, 's']
    @ds.schema_and_table(:s___a).must_equal [nil, 's']
    @ds.schema_and_table(:t__s).must_equal ['t', 's']
    @ds.schema_and_table(:t__s___a).must_equal ['t', 's']
  end

  it "should correctly handle strings" do
    @ds.schema_and_table('s').must_equal [nil, 's']
  end

  it "should correctly handle literal strings" do
    s = Sequel.lit('s')
    @ds.schema_and_table(s).last.must_be_same_as(s)
  end

  it "should correctly handle identifiers" do
    @ds.schema_and_table(Sequel.identifier(:s)).must_equal [nil, 's']
  end

  it "should correctly handle qualified identifiers" do
    @ds.schema_and_table(Sequel.qualify(:t, :s)).must_equal ['t', 's']
  end
end

describe "Dataset#split_qualifiers" do
  before do
    @ds = Sequel.mock[:test]
  end

  it "should correctly handle symbols" do
    @ds.split_qualifiers(:s).must_equal ['s']
    @ds.split_qualifiers(:s___a).must_equal ['s']
    @ds.split_qualifiers(:t__s).must_equal ['t', 's']
    @ds.split_qualifiers(:t__s___a).must_equal ['t', 's']
  end

  it "should correctly handle strings" do
    @ds.split_qualifiers('s').must_equal ['s']
  end

  it "should correctly handle identifiers" do
    @ds.split_qualifiers(Sequel.identifier(:s)).must_equal ['s']
  end

  it "should correctly handle simple qualified identifiers" do
    @ds.split_qualifiers(Sequel.qualify(:t, :s)).must_equal ['t', 's']
  end

  it "should correctly handle complex qualified identifiers" do
    @ds.split_qualifiers(Sequel.qualify(:d__t, :s)).must_equal ['d', 't', 's']
    @ds.split_qualifiers(Sequel.qualify(Sequel.qualify(:d, :t), :s)).must_equal ['d', 't', 's']
    @ds.split_qualifiers(Sequel.qualify(:d, :t__s)).must_equal ['d', 't', 's']
    @ds.split_qualifiers(Sequel.qualify(:d, Sequel.qualify(:t, :s))).must_equal ['d', 't', 's']
    @ds.split_qualifiers(Sequel.qualify(:d__t, :s__s2)).must_equal ['d', 't', 's', 's2']
    @ds.split_qualifiers(Sequel.qualify(Sequel.qualify(:d, :t), Sequel.qualify(:s, :s2))).must_equal ['d', 't', 's', 's2']
  end
end

describe "Dataset#paged_each" do
  before do
    @ds = Sequel.mock[:test].order(:x)
    @db = (0...10).map{|i| {:x=>i}}
    @ds._fetch = @db
    @rows = []
    @proc = lambda{|row| @rows << row}
  end

  it "should yield rows to the passed block" do
    @ds.paged_each(&@proc)
    @rows.must_equal @db
  end

  it "should return enumerator when called without block" do
    @ds.paged_each.each(&@proc)
    @rows.must_equal @db
  end

  it "should respect the row_proc" do
    @ds.row_proc = lambda{|row| {:x=>row[:x]*2}}
    @ds.paged_each(&@proc)
    @rows.must_equal @db.map{|row| {:x=>row[:x]*2}}
  end

  it "should use a transaction to ensure consistent results" do
    @ds.paged_each(&@proc)
    sqls = @ds.db.sqls
    sqls[0].must_equal 'BEGIN'
    sqls[-1].must_equal 'COMMIT'
  end

  it "should use a limit and offset to go through the dataset in chunks at a time" do
    @ds.paged_each(&@proc)
    @ds.db.sqls[1...-1].must_equal ['SELECT * FROM test ORDER BY x LIMIT 1000 OFFSET 0']
  end

  it "should accept a :rows_per_fetch option to change the number of rows per fetch" do
    @ds._fetch = @db.each_slice(3).to_a
    @ds.paged_each(:rows_per_fetch=>3, &@proc)
    @rows.must_equal @db
    @ds.db.sqls[1...-1].must_equal ['SELECT * FROM test ORDER BY x LIMIT 3 OFFSET 0',
      'SELECT * FROM test ORDER BY x LIMIT 3 OFFSET 3',
      'SELECT * FROM test ORDER BY x LIMIT 3 OFFSET 6',
      'SELECT * FROM test ORDER BY x LIMIT 3 OFFSET 9']
  end

  it "should handle cases where the last query returns nothing" do
    @ds._fetch = @db.each_slice(5).to_a
    @ds.paged_each(:rows_per_fetch=>5, &@proc)
    @rows.must_equal @db
    @ds.db.sqls[1...-1].must_equal ['SELECT * FROM test ORDER BY x LIMIT 5 OFFSET 0',
      'SELECT * FROM test ORDER BY x LIMIT 5 OFFSET 5',
      'SELECT * FROM test ORDER BY x LIMIT 5 OFFSET 10']
  end

  it "should respect an existing server option to use" do
    @ds = Sequel.mock(:servers=>{:foo=>{}})[:test].order(:x)
    @ds._fetch = @db
    @ds.server(:foo).paged_each(&@proc)
    @rows.must_equal @db
    @ds.db.sqls.must_equal ["BEGIN -- foo", "SELECT * FROM test ORDER BY x LIMIT 1000 OFFSET 0 -- foo", "COMMIT -- foo"]
  end

  it "should require an order" do
    lambda{@ds.unordered.paged_each(&@proc)}.must_raise(Sequel::Error)
  end

  it "should handle an existing limit and/or offset" do
    @ds._fetch = @db.each_slice(3).to_a
    @ds.limit(5).paged_each(:rows_per_fetch=>3, &@proc)
    @ds.db.sqls[1...-1].must_equal ["SELECT * FROM test ORDER BY x LIMIT 3 OFFSET 0", "SELECT * FROM test ORDER BY x LIMIT 2 OFFSET 3"]

    @ds._fetch = @db.each_slice(3).to_a
    @ds.limit(5, 2).paged_each(:rows_per_fetch=>3, &@proc)
    @ds.db.sqls[1...-1].must_equal ["SELECT * FROM test ORDER BY x LIMIT 3 OFFSET 2", "SELECT * FROM test ORDER BY x LIMIT 2 OFFSET 5"]

    @ds._fetch = @db.each_slice(3).to_a
    @ds.limit(nil, 2).paged_each(:rows_per_fetch=>3, &@proc)
    @ds.db.sqls[1...-1].must_equal ["SELECT * FROM test ORDER BY x LIMIT 3 OFFSET 2", "SELECT * FROM test ORDER BY x LIMIT 3 OFFSET 5", "SELECT * FROM test ORDER BY x LIMIT 3 OFFSET 8", "SELECT * FROM test ORDER BY x LIMIT 3 OFFSET 11"]
  end

  it "should support :strategy=>:filter" do
    @ds._fetch = @db.each_slice(5).to_a
    @ds.paged_each(:rows_per_fetch=>5, :strategy=>:filter, &@proc)
    @ds.db.sqls[1...-1].must_equal ["SELECT * FROM test ORDER BY x LIMIT 5", "SELECT * FROM test WHERE (x > 4) ORDER BY x LIMIT 5", "SELECT * FROM test WHERE (x > 9) ORDER BY x LIMIT 5"]
    @rows.must_equal @db

    @rows = []
    db = @db.map{|h| h[:y] = h[:x] % 5; h[:z] = h[:x] % 9; h}.sort_by{|h| [h[:z], -h[:y], h[:x]]}
    @ds._fetch = db.each_slice(5).to_a
    @ds.order(Sequel.identifier(:z), Sequel.desc(Sequel.qualify(:test, :y)), Sequel.asc(:x)).paged_each(:rows_per_fetch=>5, :strategy=>:filter, &@proc)
    @ds.db.sqls[1...-1].must_equal ["SELECT * FROM test ORDER BY z, test.y DESC, x ASC LIMIT 5",
      "SELECT * FROM test WHERE ((z > 3) OR ((z = 3) AND (test.y < 3)) OR ((z = 3) AND (test.y = 3) AND (x > 3))) ORDER BY z, test.y DESC, x ASC LIMIT 5",
      "SELECT * FROM test WHERE ((z > 8) OR ((z = 8) AND (test.y < 3)) OR ((z = 8) AND (test.y = 3) AND (x > 8))) ORDER BY z, test.y DESC, x ASC LIMIT 5"]
    @rows.must_equal db
  end

  it "should support :strategy=>:filter with :filter_values option" do
    db = @db.map{|h| h[:y] = h[:x] % 5; h[:z] = h[:x] % 9; h}.sort_by{|h| [h[:z], -h[:y], h[:x]]}
    @ds._fetch = db.each_slice(5).to_a
    @ds.order(Sequel.identifier(:z), Sequel.desc(Sequel.qualify(:test, :y) * 2), Sequel.asc(:x)).paged_each(:rows_per_fetch=>5, :strategy=>:filter, :filter_values=>proc{|row, expr| [row[expr[0].value], row[expr[1].args.first.column] * expr[1].args.last, row[expr[2]]]}, &@proc)
    @ds.db.sqls[1...-1].must_equal ["SELECT * FROM test ORDER BY z, (test.y * 2) DESC, x ASC LIMIT 5",
      "SELECT * FROM test WHERE ((z > 3) OR ((z = 3) AND ((test.y * 2) < 6)) OR ((z = 3) AND ((test.y * 2) = 6) AND (x > 3))) ORDER BY z, (test.y * 2) DESC, x ASC LIMIT 5",
      "SELECT * FROM test WHERE ((z > 8) OR ((z = 8) AND ((test.y * 2) < 6)) OR ((z = 8) AND ((test.y * 2) = 6) AND (x > 8))) ORDER BY z, (test.y * 2) DESC, x ASC LIMIT 5"]
    @rows.must_equal db
  end
end

describe "Dataset#current_datetime" do
  after do
    Sequel.datetime_class = Time
  end

  it "should return an instance of Sequel.datetime_class for the current datetime" do
    t = Sequel::Dataset.new(nil).current_datetime 
    t.must_be_kind_of(Time)
    (Time.now - t < 0.1).must_equal true

    Sequel.datetime_class = DateTime
    t = Sequel::Dataset.new(nil).current_datetime 
    t.must_be_kind_of(DateTime)
    (DateTime.now - t < (0.1/86400)).must_equal true
  end
end

describe "Dataset#escape_like" do
  before do
    @ds = Sequel.mock[:test]
  end

  it "should escape % and _ and \\ characters" do
    @ds.escape_like("foo\\%_bar").must_equal "foo\\\\\\%\\_bar"
  end
end

describe "Dataset#supports_replace?" do
  it "should be false by default" do
    Sequel::Dataset.new(nil).supports_replace?.must_equal false
  end
end

describe "Dataset#supports_lateral_subqueries?" do
  it "should be false by default" do
    Sequel::Dataset.new(nil).supports_lateral_subqueries?.must_equal false
  end
end

describe "Frozen Datasets" do
  before do
    @ds = Sequel.mock[:test].freeze
  end

  it "should be returned by Dataset#freeze" do
    @ds.must_be :frozen?
  end

  it "should have Dataset#freeze return receiver" do
    @ds = Sequel.mock[:test]
    @ds.freeze.must_be_same_as(@ds)
  end

  it "should have Dataset#freeze be a no-op" do
    @ds.freeze.must_be_same_as(@ds)
  end

  it "should have clones be frozen" do
    @ds.clone.must_be :frozen?
  end

  it "should be equal to unfrozen ones" do
    @ds.must_equal @ds.db[:test]
  end

  it "should have dups not be frozen" do
    @ds.dup.wont_be :frozen?
  end

  it "should raise an error when calling mutation methods" do
    proc{@ds.select!(:a)}.must_raise RuntimeError
    proc{@ds.identifier_input_method = :a}.must_raise RuntimeError
    proc{@ds.identifier_output_method = :a}.must_raise RuntimeError
    proc{@ds.quote_identifiers = false}.must_raise RuntimeError
    proc{@ds.row_proc = proc{}}.must_raise RuntimeError
    proc{@ds.extension! :query}.must_raise RuntimeError
    proc{@ds.naked!}.must_raise RuntimeError
    proc{@ds.from_self!}.must_raise RuntimeError
  end

  it "should not raise an error when calling query methods" do
    @ds.select(:a).sql.must_equal 'SELECT a FROM test'
  end
end

describe "Dataset mutation methods" do
  def m(&block)
    ds = Sequel.mock[:t]
    def ds.supports_cte?(*) true end
    ds.instance_exec(&block)
    ds.sql
  end

  it "should modify the dataset in place" do
    dsc = Sequel.mock[:u]
    dsc.instance_variable_set(:@columns, [:v])

    m{and!(:a=>1).or!(:b=>2)}.must_equal "SELECT * FROM t WHERE ((a = 1) OR (b = 2))"
    m{select!(:f).graph!(dsc, :b=>:c).set_graph_aliases!(:e=>[:m, :n]).add_graph_aliases!(:d=>[:g, :c])}.must_equal "SELECT m.n AS e, g.c AS d FROM t LEFT OUTER JOIN u ON (u.b = t.c)"
    m{cross_join!(:a)}.must_equal "SELECT * FROM t CROSS JOIN a"
    m{distinct!}.must_equal "SELECT DISTINCT * FROM t"
    m{except!(dsc)}.must_equal "SELECT * FROM (SELECT * FROM t EXCEPT SELECT * FROM u) AS t1"
    m{exclude!(:a=>1)}.must_equal "SELECT * FROM t WHERE (a != 1)"
    m{exclude_having!(:a=>1)}.must_equal "SELECT * FROM t HAVING (a != 1)"
    m{exclude_where!(:a=>1)}.must_equal "SELECT * FROM t WHERE (a != 1)"
    m{filter!(:a=>1)}.must_equal "SELECT * FROM t WHERE (a = 1)"
    m{for_update!}.must_equal "SELECT * FROM t FOR UPDATE"
    m{from!(:p)}.must_equal "SELECT * FROM p"
    m{full_join!(:a, [:b])}.must_equal "SELECT * FROM t FULL JOIN a USING (b)"
    m{full_outer_join!(:a, [:b])}.must_equal "SELECT * FROM t FULL OUTER JOIN a USING (b)"
    m{grep!(:a, 'b')}.must_equal "SELECT * FROM t WHERE ((a LIKE 'b' ESCAPE '\\'))"
    m{group!(:a)}.must_equal "SELECT * FROM t GROUP BY a"
    m{group_and_count!(:a)}.must_equal "SELECT a, count(*) AS count FROM t GROUP BY a"
    m{group_by!(:a)}.must_equal "SELECT * FROM t GROUP BY a"
    m{having!(:a)}.must_equal "SELECT * FROM t HAVING a"
    m{inner_join!(:a, [:b])}.must_equal "SELECT * FROM t INNER JOIN a USING (b)"
    m{intersect!(dsc)}.must_equal "SELECT * FROM (SELECT * FROM t INTERSECT SELECT * FROM u) AS t1"
    m{where!(:a).invert!}.must_equal "SELECT * FROM t WHERE NOT a"
    m{join!(:a, [:b])}.must_equal "SELECT * FROM t INNER JOIN a USING (b)"
    m{join_table!(:inner, :a, [:b])}.must_equal "SELECT * FROM t INNER JOIN a USING (b)"
    m{left_join!(:a, [:b])}.must_equal "SELECT * FROM t LEFT JOIN a USING (b)"
    m{left_outer_join!(:a, [:b])}.must_equal "SELECT * FROM t LEFT OUTER JOIN a USING (b)"
    m{limit!(1)}.must_equal "SELECT * FROM t LIMIT 1"
    m{lock_style!(:update)}.must_equal "SELECT * FROM t FOR UPDATE"
    m{natural_full_join!(:a)}.must_equal "SELECT * FROM t NATURAL FULL JOIN a"
    m{natural_join!(:a)}.must_equal "SELECT * FROM t NATURAL JOIN a"
    m{natural_left_join!(:a)}.must_equal "SELECT * FROM t NATURAL LEFT JOIN a"
    m{natural_right_join!(:a)}.must_equal "SELECT * FROM t NATURAL RIGHT JOIN a"
    m{offset!(1)}.must_equal "SELECT * FROM t OFFSET 1"
    m{order!(:a).reverse_order!}.must_equal "SELECT * FROM t ORDER BY a DESC"
    m{order_by!(:a).order_more!(:b).order_append!(:c).order_prepend!(:d).reverse!}.must_equal "SELECT * FROM t ORDER BY d DESC, a DESC, b DESC, c DESC"
    m{qualify!}.must_equal "SELECT t.* FROM t"
    m{right_join!(:a, [:b])}.must_equal "SELECT * FROM t RIGHT JOIN a USING (b)"
    m{right_outer_join!(:a, [:b])}.must_equal "SELECT * FROM t RIGHT OUTER JOIN a USING (b)"
    m{select!(:a)}.must_equal "SELECT a FROM t"
    m{select_all!(:t).select_more!(:b).select_append!(:c)}.must_equal "SELECT t.*, b, c FROM t"
    m{select_group!(:a)}.must_equal "SELECT a FROM t GROUP BY a"
    m{where!(:a).unfiltered!}.must_equal "SELECT * FROM t"
    m{group!(:a).ungrouped!}.must_equal "SELECT * FROM t"
    m{limit!(1).unlimited!}.must_equal "SELECT * FROM t"
    m{order!(:a).unordered!}.must_equal "SELECT * FROM t"
    m{union!(dsc)}.must_equal "SELECT * FROM (SELECT * FROM t UNION SELECT * FROM u) AS t1"
    m{with!(:a, dsc)}.must_equal "WITH a AS (SELECT * FROM u) SELECT * FROM t"
    m{with_recursive!(:a, dsc, dsc)}.must_equal "WITH a AS (SELECT * FROM u UNION ALL SELECT * FROM u) SELECT * FROM t"
    m{with_sql!('SELECT foo')}.must_equal "SELECT foo"

    dsc.server!(:a)
    dsc.opts[:server].must_equal :a
    dsc.graph!(dsc, {:b=>:c}, :table_alias=>:foo).ungraphed!.opts[:graph].must_equal nil
  end
end

describe "Dataset emulated complex expression operators" do
  before do
    @ds = Sequel.mock[:test]
    def @ds.complex_expression_sql_append(sql, op, args)
      case op
      when :&, :|, :^, :%, :<<, :>>, :'B~'
        complex_expression_emulate_append(sql, op, args)
      else
        super
      end
    end
    @n = Sequel.expr(:x).sql_number
  end

  it "should emulate &" do
    @ds.literal(Sequel::SQL::NumericExpression.new(:&, @n)).must_equal "x"
    @ds.literal(@n & 1).must_equal "BITAND(x, 1)"
    @ds.literal(@n & 1 & 2).must_equal "BITAND(BITAND(x, 1), 2)"
  end

  it "should emulate |" do
    @ds.literal(Sequel::SQL::NumericExpression.new(:|, @n)).must_equal "x"
    @ds.literal(@n | 1).must_equal "BITOR(x, 1)"
    @ds.literal(@n | 1 | 2).must_equal "BITOR(BITOR(x, 1), 2)"
  end

  it "should emulate ^" do
    @ds.literal(Sequel::SQL::NumericExpression.new(:^, @n)).must_equal "x"
    @ds.literal(@n ^ 1).must_equal "BITXOR(x, 1)"
    @ds.literal(@n ^ 1 ^ 2).must_equal "BITXOR(BITXOR(x, 1), 2)"
  end

  it "should emulate %" do
    @ds.literal(Sequel::SQL::NumericExpression.new(:%, @n)).must_equal "x"
    @ds.literal(@n % 1).must_equal "MOD(x, 1)"
    @ds.literal(@n % 1 % 2).must_equal "MOD(MOD(x, 1), 2)"
  end

  it "should emulate >>" do
    @ds.literal(Sequel::SQL::NumericExpression.new(:>>, @n)).must_equal "x"
    @ds.literal(@n >> 1).must_equal "(x / power(2, 1))"
    @ds.literal(@n >> 1 >> 2).must_equal "(x / power(2, 1) / power(2, 2))"
  end

  it "should emulate <<" do
    @ds.literal(Sequel::SQL::NumericExpression.new(:<<, @n)).must_equal "x"
    @ds.literal(@n << 1).must_equal "(x * power(2, 1))"
    @ds.literal(@n << 1 << 2).must_equal "(x * power(2, 1) * power(2, 2))"
  end

  it "should emulate B~" do
    @ds.literal(~@n).must_equal "((0 - x) - 1)"
  end
end

describe "#joined_dataset?" do
  before do
    @ds = Sequel.mock.dataset
  end

  it "should be false if the dataset has 0 or 1 from table" do
    @ds.joined_dataset?.must_equal false
    @ds.from(:a).joined_dataset?.must_equal false
  end

  it "should be true if the dataset has 2 or more from tables" do
    @ds.from(:a, :b).joined_dataset?.must_equal true
    @ds.from(:a, :b, :c).joined_dataset?.must_equal true
  end

  it "should be true if the dataset has any join tables" do
    @ds.from(:a).cross_join(:b).joined_dataset?.must_equal true
  end
end

describe "#unqualified_column_for" do
  before do
    @ds = Sequel.mock.dataset
  end

  it "should handle Symbols" do
    @ds.unqualified_column_for(:a).must_equal Sequel.identifier('a')
    @ds.unqualified_column_for(:b__a).must_equal Sequel.identifier('a')
    @ds.unqualified_column_for(:a___c).must_equal Sequel.identifier('a').as('c')
    @ds.unqualified_column_for(:b__a___c).must_equal Sequel.identifier('a').as('c')
  end

  it "should handle SQL::Identifiers" do
    @ds.unqualified_column_for(Sequel.identifier(:a)).must_equal Sequel.identifier(:a)
  end

  it "should handle SQL::QualifiedIdentifiers" do
    @ds.unqualified_column_for(Sequel.qualify(:b, :a)).must_equal Sequel.identifier('a')
    @ds.unqualified_column_for(Sequel.qualify(:b, 'a')).must_equal Sequel.identifier('a')
  end

  it "should handle SQL::AliasedExpressions" do
    @ds.unqualified_column_for(Sequel.qualify(:b, :a).as(:c)).must_equal Sequel.identifier('a').as(:c)
  end

  it "should return nil for other objects" do
    @ds.unqualified_column_for(Object.new).must_equal nil
    @ds.unqualified_column_for('a').must_equal nil
  end

  it "should return nil for other objects inside SQL::AliasedExpressions" do
    @ds.unqualified_column_for(Sequel.as(Object.new, 'a')).must_equal nil
    @ds.unqualified_column_for(Sequel.as('a', 'b')).must_equal nil
  end
end
