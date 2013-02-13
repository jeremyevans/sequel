require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Dataset" do
  before do
    @dataset = Sequel::Dataset.new("db")
  end
  
  specify "should accept database and opts in initialize" do
    db = "db"
    opts = {:from => :test}
    d = Sequel::Dataset.new(db, opts)
    d.db.should be(db)
    d.opts.should be(opts)
    
    d = Sequel::Dataset.new(db)
    d.db.should be(db)
    d.opts.should be_a_kind_of(Hash)
    d.opts.should == {}
  end
  
  specify "should provide clone for chainability" do
    d1 = @dataset.clone(:from => [:test])
    d1.class.should == @dataset.class
    d1.should_not == @dataset
    d1.db.should be(@dataset.db)
    d1.opts[:from].should == [:test]
    @dataset.opts[:from].should be_nil
    
    d2 = d1.clone(:order => [:name])
    d2.class.should == @dataset.class
    d2.should_not == d1
    d2.should_not == @dataset
    d2.db.should be(@dataset.db)
    d2.opts[:from].should == [:test]
    d2.opts[:order].should == [:name]
    d1.opts[:order].should be_nil
  end
  
  specify "should include Enumerable" do
    Sequel::Dataset.included_modules.should include(Enumerable)
  end
  
  specify "should yield rows to each" do
    ds = Sequel.mock[:t]
    ds._fetch = {:x=>1}
    called = false
    ds.each{|a| called = true; a.should == {:x=>1}}
    called.should be_true
  end
  
  specify "should get quote_identifiers default from database" do
    db = Sequel::Database.new(:quote_identifiers=>true)
    db[:a].quote_identifiers?.should == true
    db = Sequel::Database.new(:quote_identifiers=>false)
    db[:a].quote_identifiers?.should == false
  end

  specify "should get identifier_input_method default from database" do
    db = Sequel::Database.new(:identifier_input_method=>:upcase)
    db[:a].identifier_input_method.should == :upcase
    db = Sequel::Database.new(:identifier_input_method=>:downcase)
    db[:a].identifier_input_method.should == :downcase
  end

  specify "should get identifier_output_method default from database" do
    db = Sequel::Database.new(:identifier_output_method=>:upcase)
    db[:a].identifier_output_method.should == :upcase
    db = Sequel::Database.new(:identifier_output_method=>:downcase)
    db[:a].identifier_output_method.should == :downcase
  end
  
  specify "should have quote_identifiers= method which changes literalization of identifiers" do
    @dataset.quote_identifiers = true
    @dataset.literal(:a).should == '"a"'
    @dataset.quote_identifiers = false
    @dataset.literal(:a).should == 'a'
  end
  
  specify "should have identifier_input_method= method which changes literalization of identifiers" do
    @dataset.identifier_input_method = :upcase
    @dataset.literal(:a).should == 'A'
    @dataset.identifier_input_method = :downcase
    @dataset.literal(:A).should == 'a'
    @dataset.identifier_input_method = :reverse
    @dataset.literal(:at_b).should == 'b_ta'
  end
  
  specify "should have identifier_output_method= method which changes identifiers returned from the database" do
    @dataset.send(:output_identifier, "at_b_C").should == :at_b_C
    @dataset.identifier_output_method = :upcase
    @dataset.send(:output_identifier, "at_b_C").should == :AT_B_C
    @dataset.identifier_output_method = :downcase
    @dataset.send(:output_identifier, "at_b_C").should == :at_b_c
    @dataset.identifier_output_method = :reverse
    @dataset.send(:output_identifier, "at_b_C").should == :C_b_ta
  end
  
  specify "should have output_identifier handle empty identifiers" do
    @dataset.send(:output_identifier, "").should == :untitled
    @dataset.identifier_output_method = :upcase
    @dataset.send(:output_identifier, "").should == :UNTITLED
    @dataset.identifier_output_method = :downcase
    @dataset.send(:output_identifier, "").should == :untitled
    @dataset.identifier_output_method = :reverse
    @dataset.send(:output_identifier, "").should == :deltitnu
  end
end

describe "Dataset#clone" do
  before do
    @dataset = Sequel::Dataset.new(nil).from(:items)
  end
  
  specify "should create an exact copy of the dataset" do
    @dataset.row_proc = Proc.new{|r| r}
    clone = @dataset.clone

    clone.object_id.should_not === @dataset.object_id
    clone.class.should == @dataset.class
    clone.db.should == @dataset.db
    clone.opts.should == @dataset.opts
    clone.row_proc.should == @dataset.row_proc
  end
  
  specify "should deep-copy the dataset opts" do
    clone = @dataset.clone

    clone.opts.should_not equal(@dataset.opts)
    @dataset.filter!(:a => 'b')
    clone.opts[:filter].should be_nil

    clone = @dataset.clone(:from => [:other])
    @dataset.opts[:from].should == [:items]
    clone.opts[:from].should == [:other]
  end
  
  specify "should merge the specified options" do
    clone = @dataset.clone(1 => 2)
    clone.opts.should == {1 => 2, :from => [:items]}
  end
  
  specify "should overwrite existing options" do
    clone = @dataset.clone(:from => [:other])
    clone.opts.should == {:from => [:other]}
  end
  
  specify "should return an object with the same modules included" do
    m = Module.new do
      def __xyz__; "xyz"; end
    end
    @dataset.extend(m)
    @dataset.clone({}).should respond_to(:__xyz__)
  end
end

describe "Dataset#==" do
  before do
    @db = Sequel.mock
    @h = {}
  end
  
  specify "should be the true for dataset with the same db, opts, and SQL" do
    @db[:t].should == @db[:t]
  end

  specify "should be different for datasets with different dbs" do
    @db[:t].should_not == Sequel.mock[:t]
  end
  
  specify "should be different for datasets with different opts" do
    @db[:t].should_not == @db[:t].clone(:blah=>1)
  end
  
  specify "should be different for datasets with different SQL" do
    ds = @db[:t]
    ds.quote_identifiers = true
    ds.should_not == @db[:t]
  end
end

describe "Dataset#hash" do
  before do
    @db = Sequel.mock
    @h = {}
  end
  
  specify "should be the same for dataset with the same db, opts, and SQL" do
    @db[:t].hash.should == @db[:t].hash
    @h[@db[:t]] = 1
    @h[@db[:t]].should == 1
  end

  specify "should be different for datasets with different dbs" do
    @db[:t].hash.should_not == Sequel.mock[:t].hash
  end
  
  specify "should be different for datasets with different opts" do
    @db[:t].hash.should_not == @db[:t].clone(:blah=>1).hash
  end
  
  specify "should be different for datasets with different SQL" do
    ds = @db[:t]
    ds.quote_identifiers = true
    ds.hash.should_not == @db[:t].hash
  end
end

describe "A simple dataset" do
  before do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should format a select statement" do
    @dataset.select_sql.should == 'SELECT * FROM test'
  end
  
  specify "should format a delete statement" do
    @dataset.delete_sql.should == 'DELETE FROM test'
  end
  
  specify "should format a truncate statement" do
    @dataset.truncate_sql.should == 'TRUNCATE TABLE test'
  end
  
  specify "should format a truncate statement with multiple tables if supported" do
    @dataset.meta_def(:check_truncation_allowed!){}
    @dataset.from(:test, :test2).truncate_sql.should == 'TRUNCATE TABLE test, test2'
  end
  
  specify "should format an insert statement with default values" do
    @dataset.insert_sql.should == 'INSERT INTO test DEFAULT VALUES'
  end
  
  specify "should use a single column with a default value when the dataset doesn't support using insert statement with default values" do
    @dataset.meta_def(:insert_supports_empty_values?){false}
    @dataset.meta_def(:columns){[:a, :b]}
    @dataset.insert_sql.should == 'INSERT INTO test (b) VALUES (DEFAULT)'
  end
  
  specify "should format an insert statement with hash" do
    @dataset.insert_sql(:name => 'wxyz', :price => 342).
      should match(/INSERT INTO test \(name, price\) VALUES \('wxyz', 342\)|INSERT INTO test \(price, name\) VALUES \(342, 'wxyz'\)/)

      @dataset.insert_sql({}).should == "INSERT INTO test DEFAULT VALUES"
  end

  specify "should format an insert statement with string keys" do
    @dataset.insert_sql('name' => 'wxyz', 'price' => 342).
      should match(/INSERT INTO test \(name, price\) VALUES \('wxyz', 342\)|INSERT INTO test \(price, name\) VALUES \(342, 'wxyz'\)/)
  end
  
  specify "should format an insert statement with an arbitrary value" do
    @dataset.insert_sql(123).should == "INSERT INTO test VALUES (123)"
  end
  
  specify "should format an insert statement with sub-query" do
    @dataset.insert_sql(@dataset.from(:something).filter(:x => 2)).should == "INSERT INTO test SELECT * FROM something WHERE (x = 2)"
  end
  
  specify "should format an insert statement with array" do
    @dataset.insert_sql('a', 2, 6.5).should == "INSERT INTO test VALUES ('a', 2, 6.5)"
  end
  
  specify "should format an update statement" do
    @dataset.update_sql(:name => 'abc').should == "UPDATE test SET name = 'abc'"
  end

  specify "should be able to return rows for arbitrary SQL" do
    @dataset.clone(:sql => 'xxx yyy zzz').select_sql.should == "xxx yyy zzz"
  end

  specify "should use the :sql option for all sql methods" do
    sql = "X"
    ds = @dataset.clone(:sql=>sql)
    ds.sql.should == sql
    ds.select_sql.should == sql
    ds.insert_sql.should == sql
    ds.delete_sql.should == sql
    ds.update_sql.should == sql
    ds.truncate_sql.should == sql
  end
end

describe "A dataset with multiple tables in its FROM clause" do
  before do
    @dataset = Sequel::Dataset.new(nil).from(:t1, :t2)
  end

  specify "should raise on #update_sql" do
    proc {@dataset.update_sql(:a=>1)}.should raise_error(Sequel::InvalidOperation)
  end

  specify "should raise on #delete_sql" do
    proc {@dataset.delete_sql}.should raise_error(Sequel::InvalidOperation)
  end
  
  specify "should raise on #truncate_sql" do
    proc {@dataset.truncate_sql}.should raise_error(Sequel::InvalidOperation)
  end

  specify "should raise on #insert_sql" do
    proc {@dataset.insert_sql}.should raise_error(Sequel::InvalidOperation)
  end

  specify "should generate a select query FROM all specified tables" do
    @dataset.select_sql.should == "SELECT * FROM t1, t2"
  end
end

describe "Dataset#unused_table_alias" do
  before do
    @ds = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should return given symbol if it hasn't already been used" do
    @ds.unused_table_alias(:blah).should == :blah
  end

  specify "should return a symbol specifying an alias that hasn't already been used if it has already been used" do
    @ds.unused_table_alias(:test).should == :test_0
    @ds.from(:test, :test_0).unused_table_alias(:test).should == :test_1
    @ds.from(:test, :test_0).cross_join(:test_1).unused_table_alias(:test).should == :test_2
  end

  specify "should return an appropriate symbol if given other forms of identifiers" do
    @ds.unused_table_alias('test').should == :test_0
    @ds.unused_table_alias(:b__t___test).should == :test_0
    @ds.unused_table_alias(:b__test).should == :test_0
    @ds.unused_table_alias(Sequel.qualify(:b, :test)).should == :test_0
    @ds.unused_table_alias(Sequel.expr(:b).as(:test)).should == :test_0
    @ds.unused_table_alias(Sequel.expr(:b).as(Sequel.identifier(:test))).should == :test_0
    @ds.unused_table_alias(Sequel.expr(:b).as('test')).should == :test_0
    @ds.unused_table_alias(Sequel.identifier(:test)).should == :test_0
  end
end

describe "Dataset#exists" do
  before do
    @ds1 = Sequel.mock[:test]
    @ds2 = @ds1.filter(Sequel.expr(:price) < 100)
    @ds3 = @ds1.filter(Sequel.expr(:price) > 50)
  end
  
  specify "should work in filters" do
    @ds1.filter(@ds2.exists).sql.should ==
      'SELECT * FROM test WHERE (EXISTS (SELECT * FROM test WHERE (price < 100)))'
    @ds1.filter(@ds2.exists & @ds3.exists).sql.should ==
      'SELECT * FROM test WHERE ((EXISTS (SELECT * FROM test WHERE (price < 100))) AND (EXISTS (SELECT * FROM test WHERE (price > 50))))'
  end

  specify "should work in select" do
    @ds1.select(@ds2.exists.as(:a), @ds3.exists.as(:b)).sql.should ==
      'SELECT (EXISTS (SELECT * FROM test WHERE (price < 100))) AS a, (EXISTS (SELECT * FROM test WHERE (price > 50))) AS b FROM test'
  end
end

describe "Dataset#where" do
  before do
    @dataset = Sequel.mock[:test]
    @d1 = @dataset.where(:region => 'Asia')
    @d2 = @dataset.where('region = ?', 'Asia')
    @d3 = @dataset.where("a = 1")
  end
  
  specify "should just clone if given an empty argument" do
    @dataset.where({}).sql.should == @dataset.sql
    @dataset.where([]).sql.should == @dataset.sql
    @dataset.where('').sql.should == @dataset.sql

    @dataset.filter({}).sql.should == @dataset.sql
    @dataset.filter([]).sql.should == @dataset.sql
    @dataset.filter('').sql.should == @dataset.sql
  end
  
  specify "should work with hashes" do
    @dataset.where(:name => 'xyz', :price => 342).select_sql.
      should match(/WHERE \(\(name = 'xyz'\) AND \(price = 342\)\)|WHERE \(\(price = 342\) AND \(name = 'xyz'\)\)/)
  end
  
  specify "should work with a string with placeholders and arguments for those placeholders" do
    @dataset.where('price < ? AND id in ?', 100, [1, 2, 3]).select_sql.should == "SELECT * FROM test WHERE (price < 100 AND id in (1, 2, 3))"
  end
  
  specify "should not modify passed array with placeholders" do
    a = ['price < ? AND id in ?', 100, 1, 2, 3]
    b = a.dup
    @dataset.where(a)
    b.should == a
  end

  specify "should work with strings (custom SQL expressions)" do
    @dataset.where('(a = 1 AND b = 2)').select_sql.should ==
      "SELECT * FROM test WHERE ((a = 1 AND b = 2))"
  end
    
  specify "should work with a string with named placeholders and a hash of placeholder value arguments" do
    @dataset.where('price < :price AND id in :ids', :price=>100, :ids=>[1, 2, 3]).select_sql.should ==
      "SELECT * FROM test WHERE (price < 100 AND id in (1, 2, 3))"
  end
    
  specify "should not modify passed array with named placeholders" do
    a = ['price < :price AND id in :ids', {:price=>100}]
    b = a.dup
    @dataset.where(a)
    b.should == a
  end

  specify "should not replace named placeholders that don't exist in the hash" do
    @dataset.where('price < :price AND id in :ids', :price=>100).select_sql.should == "SELECT * FROM test WHERE (price < 100 AND id in :ids)"
  end
    
  specify "should handle partial names" do
    @dataset.where('price < :price AND id = :p', :p=>2, :price=>100).select_sql.should == "SELECT * FROM test WHERE (price < 100 AND id = 2)"
  end
  
  specify "should affect select, delete and update statements" do
    @d1.select_sql.should == "SELECT * FROM test WHERE (region = 'Asia')"
    @d1.delete_sql.should == "DELETE FROM test WHERE (region = 'Asia')"
    @d1.update_sql(:GDP => 0).should == "UPDATE test SET GDP = 0 WHERE (region = 'Asia')"
    
    @d2.select_sql.should == "SELECT * FROM test WHERE (region = 'Asia')"
    @d2.delete_sql.should == "DELETE FROM test WHERE (region = 'Asia')"
    @d2.update_sql(:GDP => 0).should == "UPDATE test SET GDP = 0 WHERE (region = 'Asia')"
    
    @d3.select_sql.should == "SELECT * FROM test WHERE (a = 1)"
    @d3.delete_sql.should == "DELETE FROM test WHERE (a = 1)"
    @d3.update_sql(:GDP => 0).should == "UPDATE test SET GDP = 0 WHERE (a = 1)"
    
  end
  
  specify "should be composable using AND operator (for scoping)" do
    # hashes are merged, no problem
    @d1.where(:size => 'big').select_sql.should == "SELECT * FROM test WHERE ((region = 'Asia') AND (size = 'big'))"
    
    # hash and string
    @d1.where('population > 1000').select_sql.should == "SELECT * FROM test WHERE ((region = 'Asia') AND (population > 1000))"
    @d1.where('(a > 1) OR (b < 2)').select_sql.should == "SELECT * FROM test WHERE ((region = 'Asia') AND ((a > 1) OR (b < 2)))"
    
    # hash and array
    @d1.where('GDP > ?', 1000).select_sql.should == "SELECT * FROM test WHERE ((region = 'Asia') AND (GDP > 1000))"
    
    # array and array
    @d2.where('GDP > ?', 1000).select_sql.should == "SELECT * FROM test WHERE ((region = 'Asia') AND (GDP > 1000))"
    
    # array and hash
    @d2.where(:name => ['Japan', 'China']).select_sql.should == "SELECT * FROM test WHERE ((region = 'Asia') AND (name IN ('Japan', 'China')))"
      
    # array and string
    @d2.where('GDP > ?').select_sql.should == "SELECT * FROM test WHERE ((region = 'Asia') AND (GDP > ?))"
    
    # string and string
    @d3.where('b = 2').select_sql.should == "SELECT * FROM test WHERE ((a = 1) AND (b = 2))"
    
    # string and hash
    @d3.where(:c => 3).select_sql.should == "SELECT * FROM test WHERE ((a = 1) AND (c = 3))"
      
    # string and array
    @d3.where('d = ?', 4).select_sql.should == "SELECT * FROM test WHERE ((a = 1) AND (d = 4))"
  end
      
  specify "should be composable using AND operator (for scoping) with block" do
    @d3.where{e < 5}.select_sql.should == "SELECT * FROM test WHERE ((a = 1) AND (e < 5))"
  end
  
  specify "should accept ranges" do
    @dataset.filter(:id => 4..7).sql.should == 'SELECT * FROM test WHERE ((id >= 4) AND (id <= 7))'
    @dataset.filter(:id => 4...7).sql.should == 'SELECT * FROM test WHERE ((id >= 4) AND (id < 7))'

    @dataset.filter(:table__id => 4..7).sql.should == 'SELECT * FROM test WHERE ((table.id >= 4) AND (table.id <= 7))'
    @dataset.filter(:table__id => 4...7).sql.should == 'SELECT * FROM test WHERE ((table.id >= 4) AND (table.id < 7))'
  end

  specify "should accept nil" do
    @dataset.filter(:owner_id => nil).sql.should == 'SELECT * FROM test WHERE (owner_id IS NULL)'
  end

  specify "should accept a subquery" do
    @dataset.filter('gdp > ?', @d1.select(Sequel.function(:avg, :gdp))).sql.should == "SELECT * FROM test WHERE (gdp > (SELECT avg(gdp) FROM test WHERE (region = 'Asia')))"
  end
  
  specify "should handle all types of IN/NOT IN queries with empty arrays" do
    @dataset.filter(:id => []).sql.should == "SELECT * FROM test WHERE (id != id)"
    @dataset.filter([:id1, :id2] => []).sql.should == "SELECT * FROM test WHERE ((id1 != id1) AND (id2 != id2))"
    @dataset.exclude(:id => []).sql.should == "SELECT * FROM test WHERE (id = id)"
    @dataset.exclude([:id1, :id2] => []).sql.should == "SELECT * FROM test WHERE ((id1 = id1) AND (id2 = id2))"
  end

  specify "should handle all types of IN/NOT IN queries with empty arrays" do
    begin
      Sequel.empty_array_handle_nulls = false
      @dataset.filter(:id => []).sql.should == "SELECT * FROM test WHERE (1 = 0)"
      @dataset.filter([:id1, :id2] => []).sql.should == "SELECT * FROM test WHERE (1 = 0)"
      @dataset.exclude(:id => []).sql.should == "SELECT * FROM test WHERE (1 = 1)"
      @dataset.exclude([:id1, :id2] => []).sql.should == "SELECT * FROM test WHERE (1 = 1)"
    ensure
      Sequel.empty_array_handle_nulls = true
    end
  end

  specify "should handle all types of IN/NOT IN queries" do
    @dataset.filter(:id => @d1.select(:id)).sql.should == "SELECT * FROM test WHERE (id IN (SELECT id FROM test WHERE (region = 'Asia')))"
    @dataset.filter(:id => [1, 2]).sql.should == "SELECT * FROM test WHERE (id IN (1, 2))"
    @dataset.filter([:id1, :id2] => @d1.select(:id1, :id2)).sql.should == "SELECT * FROM test WHERE ((id1, id2) IN (SELECT id1, id2 FROM test WHERE (region = 'Asia')))"
    @dataset.filter([:id1, :id2] => Sequel.value_list([[1, 2], [3,4]])).sql.should == "SELECT * FROM test WHERE ((id1, id2) IN ((1, 2), (3, 4)))"
    @dataset.filter([:id1, :id2] => [[1, 2], [3,4]]).sql.should == "SELECT * FROM test WHERE ((id1, id2) IN ((1, 2), (3, 4)))"

    @dataset.exclude(:id => @d1.select(:id)).sql.should == "SELECT * FROM test WHERE (id NOT IN (SELECT id FROM test WHERE (region = 'Asia')))"
    @dataset.exclude(:id => [1, 2]).sql.should == "SELECT * FROM test WHERE (id NOT IN (1, 2))"
    @dataset.exclude([:id1, :id2] => @d1.select(:id1, :id2)).sql.should == "SELECT * FROM test WHERE ((id1, id2) NOT IN (SELECT id1, id2 FROM test WHERE (region = 'Asia')))"
    @dataset.exclude([:id1, :id2] => Sequel.value_list([[1, 2], [3,4]])).sql.should == "SELECT * FROM test WHERE ((id1, id2) NOT IN ((1, 2), (3, 4)))"
    @dataset.exclude([:id1, :id2] => [[1, 2], [3,4]]).sql.should == "SELECT * FROM test WHERE ((id1, id2) NOT IN ((1, 2), (3, 4)))"
  end

  specify "should handle IN/NOT IN queries with multiple columns and an array where the database doesn't support it" do
    @dataset.meta_def(:supports_multiple_column_in?){false}
    @dataset.filter([:id1, :id2] => [[1, 2], [3,4]]).sql.should == "SELECT * FROM test WHERE (((id1 = 1) AND (id2 = 2)) OR ((id1 = 3) AND (id2 = 4)))"
    @dataset.exclude([:id1, :id2] => [[1, 2], [3,4]]).sql.should == "SELECT * FROM test WHERE (((id1 != 1) OR (id2 != 2)) AND ((id1 != 3) OR (id2 != 4)))"
    @dataset.filter([:id1, :id2] => Sequel.value_list([[1, 2], [3,4]])).sql.should == "SELECT * FROM test WHERE (((id1 = 1) AND (id2 = 2)) OR ((id1 = 3) AND (id2 = 4)))"
    @dataset.exclude([:id1, :id2] => Sequel.value_list([[1, 2], [3,4]])).sql.should == "SELECT * FROM test WHERE (((id1 != 1) OR (id2 != 2)) AND ((id1 != 3) OR (id2 != 4)))"
  end

  specify "should handle IN/NOT IN queries with multiple columns and a dataset where the database doesn't support it" do
    @dataset.meta_def(:supports_multiple_column_in?){false}
    db = Sequel.mock(:fetch=>[{:id1=>1, :id2=>2}, {:id1=>3, :id2=>4}])
    d1 = db[:test].select(:id1, :id2).filter(:region=>'Asia').columns(:id1, :id2)
    @dataset.filter([:id1, :id2] => d1).sql.should == "SELECT * FROM test WHERE (((id1 = 1) AND (id2 = 2)) OR ((id1 = 3) AND (id2 = 4)))"
    db.sqls.should == ["SELECT id1, id2 FROM test WHERE (region = 'Asia')"]
    @dataset.exclude([:id1, :id2] => d1).sql.should == "SELECT * FROM test WHERE (((id1 != 1) OR (id2 != 2)) AND ((id1 != 3) OR (id2 != 4)))"
    db.sqls.should == ["SELECT id1, id2 FROM test WHERE (region = 'Asia')"]
  end
  
  specify "should handle IN/NOT IN queries with multiple columns and an empty dataset where the database doesn't support it" do
    @dataset.meta_def(:supports_multiple_column_in?){false}
    db = Sequel.mock
    d1 = db[:test].select(:id1, :id2).filter(:region=>'Asia').columns(:id1, :id2)
    @dataset.filter([:id1, :id2] => d1).sql.should == "SELECT * FROM test WHERE ((id1 != id1) AND (id2 != id2))"
    db.sqls.should == ["SELECT id1, id2 FROM test WHERE (region = 'Asia')"]
    @dataset.exclude([:id1, :id2] => d1).sql.should == "SELECT * FROM test WHERE ((id1 = id1) AND (id2 = id2))"
    db.sqls.should == ["SELECT id1, id2 FROM test WHERE (region = 'Asia')"]
  end
  
  specify "should handle IN/NOT IN queries with multiple columns and an empty dataset where the database doesn't support it with correct NULL handling" do
    begin
      Sequel.empty_array_handle_nulls = false
      @dataset.meta_def(:supports_multiple_column_in?){false}
      db = Sequel.mock
      d1 = db[:test].select(:id1, :id2).filter(:region=>'Asia').columns(:id1, :id2)
      @dataset.filter([:id1, :id2] => d1).sql.should == "SELECT * FROM test WHERE (1 = 0)"
      db.sqls.should == ["SELECT id1, id2 FROM test WHERE (region = 'Asia')"]
      @dataset.exclude([:id1, :id2] => d1).sql.should == "SELECT * FROM test WHERE (1 = 1)"
      db.sqls.should == ["SELECT id1, id2 FROM test WHERE (region = 'Asia')"]
    ensure
      Sequel.empty_array_handle_nulls = true
    end
  end
  
  specify "should handle IN/NOT IN queries for datasets with row_procs" do
    @dataset.meta_def(:supports_multiple_column_in?){false}
    db = Sequel.mock(:fetch=>[{:id1=>1, :id2=>2}, {:id1=>3, :id2=>4}])
    d1 = db[:test].select(:id1, :id2).filter(:region=>'Asia').columns(:id1, :id2)
    d1.row_proc = proc{|h| Object.new}
    @dataset.filter([:id1, :id2] => d1).sql.should == "SELECT * FROM test WHERE (((id1 = 1) AND (id2 = 2)) OR ((id1 = 3) AND (id2 = 4)))"
    db.sqls.should == ["SELECT id1, id2 FROM test WHERE (region = 'Asia')"]
    @dataset.exclude([:id1, :id2] => d1).sql.should == "SELECT * FROM test WHERE (((id1 != 1) OR (id2 != 2)) AND ((id1 != 3) OR (id2 != 4)))"
    db.sqls.should == ["SELECT id1, id2 FROM test WHERE (region = 'Asia')"]
  end
  
  specify "should accept a subquery for an EXISTS clause" do
    a = @dataset.filter(Sequel.expr(:price) < 100)
    @dataset.filter(a.exists).sql.should == 'SELECT * FROM test WHERE (EXISTS (SELECT * FROM test WHERE (price < 100)))'
  end
  
  specify "should accept proc expressions" do
    d = @d1.select(Sequel.function(:avg, :gdp))
    @dataset.filter{gdp > d}.sql.should == "SELECT * FROM test WHERE (gdp > (SELECT avg(gdp) FROM test WHERE (region = 'Asia')))"
    @dataset.filter{a < 1}.sql.should == 'SELECT * FROM test WHERE (a < 1)'
    @dataset.filter{(a >= 1) & (b <= 2)}.sql.should == 'SELECT * FROM test WHERE ((a >= 1) AND (b <= 2))'
    @dataset.filter{c.like 'ABC%'}.sql.should == "SELECT * FROM test WHERE (c LIKE 'ABC%' ESCAPE '\\')"
    @dataset.filter{c.like 'ABC%', '%XYZ'}.sql.should == "SELECT * FROM test WHERE ((c LIKE 'ABC%' ESCAPE '\\') OR (c LIKE '%XYZ' ESCAPE '\\'))"
  end
  
  specify "should work for grouped datasets" do
    @dataset.group(:a).filter(:b => 1).sql.should == 'SELECT * FROM test WHERE (b = 1) GROUP BY a'
  end

  specify "should accept true and false as arguments" do
    @dataset.filter(true).sql.should == "SELECT * FROM test WHERE 't'"
    @dataset.filter(Sequel::SQLTRUE).sql.should == "SELECT * FROM test WHERE 't'"
    @dataset.filter(false).sql.should == "SELECT * FROM test WHERE 'f'"
    @dataset.filter(Sequel::SQLFALSE).sql.should == "SELECT * FROM test WHERE 'f'"
  end

  specify "should use boolean expression if dataset does not support where true/false" do
    def @dataset.supports_where_true?() false end
    @dataset.filter(true).sql.should == "SELECT * FROM test WHERE (1 = 1)"
    @dataset.filter(Sequel::SQLTRUE).sql.should == "SELECT * FROM test WHERE (1 = 1)"
    @dataset.filter(false).sql.should == "SELECT * FROM test WHERE (1 = 0)"
    @dataset.filter(Sequel::SQLFALSE).sql.should == "SELECT * FROM test WHERE (1 = 0)"
  end

  specify "should allow the use of multiple arguments" do
    @dataset.filter(:a, :b).sql.should == 'SELECT * FROM test WHERE (a AND b)'
    @dataset.filter(:a, :b=>1).sql.should == 'SELECT * FROM test WHERE (a AND (b = 1))'
    @dataset.filter(:a, Sequel.expr(:c) > 3, :b=>1).sql.should == 'SELECT * FROM test WHERE (a AND (c > 3) AND (b = 1))'
  end

  specify "should allow the use of blocks and arguments simultaneously" do
    @dataset.filter(Sequel.expr(:zz) < 3){yy > 3}.sql.should == 'SELECT * FROM test WHERE ((zz < 3) AND (yy > 3))'
  end

  specify "should yield a VirtualRow to the block" do
    x = nil
    @dataset.filter{|r| x = r; false}
    x.should be_a_kind_of(Sequel::SQL::VirtualRow)
    @dataset.filter{|r| ((r.name < 'b') & {r.table__id => 1}) | r.is_active(r.blah, r.xx, r.x__y_z)}.sql.should ==
      "SELECT * FROM test WHERE (((name < 'b') AND (table.id = 1)) OR is_active(blah, xx, x.y_z))"
  end

  specify "should instance_eval the block in the context of a VirtualRow if the block doesn't request an argument" do
    x = nil
    @dataset.filter{x = self; false}
    x.should be_a_kind_of(Sequel::SQL::VirtualRow)
    @dataset.filter{((name < 'b') & {table__id => 1}) | is_active(blah, xx, x__y_z)}.sql.should ==
      "SELECT * FROM test WHERE (((name < 'b') AND (table.id = 1)) OR is_active(blah, xx, x.y_z))"
  end

  specify "should raise an error if an invalid argument is used" do
    proc{@dataset.filter(1)}.should raise_error(Sequel::Error)
  end

  specify "should raise an error if a NumericExpression or StringExpression is used" do
    proc{@dataset.filter(Sequel.expr(:x) + 1)}.should raise_error(Sequel::Error)
    proc{@dataset.filter(Sequel.expr(:x).sql_string)}.should raise_error(Sequel::Error)
  end
end

describe "Dataset#or" do
  before do
    @dataset = Sequel::Dataset.new(nil).from(:test)
    @d1 = @dataset.where(:x => 1)
  end
  
  specify "should raise if no filter exists" do
    proc {@dataset.or(:a => 1)}.should raise_error(Sequel::Error)
  end
  
  specify "should add an alternative expression to the where clause" do
    @d1.or(:y => 2).sql.should == 'SELECT * FROM test WHERE ((x = 1) OR (y = 2))'
  end
  
  specify "should accept all forms of filters" do
    @d1.or('y > ?', 2).sql.should == 'SELECT * FROM test WHERE ((x = 1) OR (y > 2))'
    @d1.or(Sequel.expr(:yy) > 3).sql.should == 'SELECT * FROM test WHERE ((x = 1) OR (yy > 3))'
  end    

  specify "should accept blocks passed to filter" do
    @d1.or{yy > 3}.sql.should == 'SELECT * FROM test WHERE ((x = 1) OR (yy > 3))'
  end
  
  specify "should correctly add parens to give predictable results" do
    @d1.filter(:y => 2).or(:z => 3).sql.should == 'SELECT * FROM test WHERE (((x = 1) AND (y = 2)) OR (z = 3))'
    @d1.or(:y => 2).filter(:z => 3).sql.should == 'SELECT * FROM test WHERE (((x = 1) OR (y = 2)) AND (z = 3))'
  end

  specify "should allow the use of blocks and arguments simultaneously" do
    @d1.or(Sequel.expr(:zz) < 3){yy > 3}.sql.should == 'SELECT * FROM test WHERE ((x = 1) OR ((zz < 3) AND (yy > 3)))'
  end
end

describe "Dataset#and" do
  before do
    @dataset = Sequel::Dataset.new(nil).from(:test)
    @d1 = @dataset.where(:x => 1)
  end
  
  specify "should raise if no filter exists" do
    proc {@dataset.and(:a => 1)}.should raise_error(Sequel::Error)
    proc {@dataset.where(:a => 1).group(:t).and(:b => 2)}.should_not raise_error(Sequel::Error)
    @dataset.where(:a => 1).group(:t).and(:b => 2).sql.should == "SELECT * FROM test WHERE ((a = 1) AND (b = 2)) GROUP BY t"
  end
  
  specify "should add an alternative expression to the where clause" do
    @d1.and(:y => 2).sql.should == 'SELECT * FROM test WHERE ((x = 1) AND (y = 2))'
  end
  
  specify "should accept different types of filters" do
    @d1.and('y > ?', 2).sql.should == 'SELECT * FROM test WHERE ((x = 1) AND (y > 2))'
    @d1.and(Sequel.expr(:yy) > 3).sql.should == 'SELECT * FROM test WHERE ((x = 1) AND (yy > 3))'
  end
      
  specify "should accept blocks passed to filter" do
    @d1.and{yy > 3}.sql.should == 'SELECT * FROM test WHERE ((x = 1) AND (yy > 3))'
  end
  
  specify "should correctly add parens to give predictable results" do
    @d1.or(:y => 2).and(:z => 3).sql.should == 'SELECT * FROM test WHERE (((x = 1) OR (y = 2)) AND (z = 3))'
    @d1.and(:y => 2).or(:z => 3).sql.should == 'SELECT * FROM test WHERE (((x = 1) AND (y = 2)) OR (z = 3))'
  end
end

describe "Dataset#exclude" do
  before do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end

  specify "should correctly negate the expression when one condition is given" do
    @dataset.exclude(:region=>'Asia').select_sql.should == "SELECT * FROM test WHERE (region != 'Asia')"
  end

  specify "should affect the having clause if having clause is already used" do
    @dataset.group_and_count(:name).having{count > 2}.exclude{count > 5}.sql.should == "SELECT name, count(*) AS count FROM test GROUP BY name HAVING ((count > 2) AND (count <= 5))"
  end

  specify "should take multiple conditions as a hash and express the logic correctly in SQL" do
    @dataset.exclude(:region => 'Asia', :name => 'Japan').select_sql.
      should match(Regexp.union(/WHERE \(\(region != 'Asia'\) OR \(name != 'Japan'\)\)/,
                                /WHERE \(\(name != 'Japan'\) OR \(region != 'Asia'\)\)/))
  end

  specify "should parenthesize a single string condition correctly" do
    @dataset.exclude("region = 'Asia' AND name = 'Japan'").select_sql.should == "SELECT * FROM test WHERE NOT (region = 'Asia' AND name = 'Japan')"
  end

  specify "should parenthesize an array condition correctly" do
    @dataset.exclude('region = ? AND name = ?', 'Asia', 'Japan').select_sql.should == "SELECT * FROM test WHERE NOT (region = 'Asia' AND name = 'Japan')"
  end

  specify "should correctly parenthesize when it is used twice" do
    @dataset.exclude(:region => 'Asia').exclude(:name => 'Japan').select_sql.should == "SELECT * FROM test WHERE ((region != 'Asia') AND (name != 'Japan'))"
  end
  
  specify "should support proc expressions" do
    @dataset.exclude{id < 6}.sql.should == 'SELECT * FROM test WHERE (id >= 6)'
  end
  
  specify "should allow the use of blocks and arguments simultaneously" do
    @dataset.exclude(:id => (7..11)){id < 6}.sql.should == 'SELECT * FROM test WHERE ((id < 7) OR (id > 11) OR (id >= 6))'
    @dataset.exclude([:id, 1], [:x, 3]){id < 6}.sql.should == 'SELECT * FROM test WHERE ((id != 1) OR (x != 3) OR (id >= 6))'
  end
end

describe "Dataset#exclude_where" do
  before do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end

  specify "should correctly negate the expression and add it to the where clause" do
    @dataset.exclude_where(:region=>'Asia').sql.should == "SELECT * FROM test WHERE (region != 'Asia')"
    @dataset.exclude_where(:region=>'Asia').exclude_where(:region=>'NA').sql.should == "SELECT * FROM test WHERE ((region != 'Asia') AND (region != 'NA'))"
  end

  specify "should affect the where clause even if having clause is already used" do
    @dataset.group_and_count(:name).having{count > 2}.exclude_where(:region=>'Asia').sql.should ==
      "SELECT name, count(*) AS count FROM test WHERE (region != 'Asia') GROUP BY name HAVING (count > 2)"
  end
end

describe "Dataset#exclude_having" do
  specify "should correctly negate the expression and add it to the having clause" do
    Sequel::Dataset.new(nil).from(:test).exclude_having{count > 2}.exclude_having{count < 0}.sql.should == "SELECT * FROM test HAVING ((count <= 2) AND (count >= 0))"
  end
end

describe "Dataset#invert" do
  before do
    @d = Sequel::Dataset.new(nil).from(:test)
  end

  specify "should raise error if the dataset is not filtered" do
    proc{@d.invert}.should raise_error(Sequel::Error)
  end

  specify "should invert current filter if dataset is filtered" do
    @d.filter(:x).invert.sql.should == 'SELECT * FROM test WHERE NOT x'
  end

  specify "should invert both having and where if both are preset" do
    @d.filter(:x).group(:x).having(:x).invert.sql.should == 'SELECT * FROM test WHERE NOT x GROUP BY x HAVING NOT x'
  end
end

describe "Dataset#having" do
  before do
    @dataset = Sequel::Dataset.new(nil).from(:test)
    @grouped = @dataset.group(:region).select(:region, Sequel.function(:sum, :population), Sequel.function(:avg, :gdp))
  end

  specify "should just clone if given an empty argument" do
    @dataset.having({}).sql.should == @dataset.sql
    @dataset.having([]).sql.should == @dataset.sql
    @dataset.having('').sql.should == @dataset.sql
  end
  
  specify "should affect select statements" do
    @grouped.having('sum(population) > 10').select_sql.should == "SELECT region, sum(population), avg(gdp) FROM test GROUP BY region HAVING (sum(population) > 10)"
  end

  specify "should support proc expressions" do
    @grouped.having{Sequel.function(:sum, :population) > 10}.sql.should == "SELECT region, sum(population), avg(gdp) FROM test GROUP BY region HAVING (sum(population) > 10)"
  end

  specify "should work with and on the having clause" do
    @grouped.having(Sequel.expr(:a) > 1).and(Sequel.expr(:b) < 2).sql.should == "SELECT region, sum(population), avg(gdp) FROM test GROUP BY region HAVING ((a > 1) AND (b < 2))"
  end
end

describe "a grouped dataset" do
  before do
    @dataset = Sequel::Dataset.new(nil).from(:test).group(:type_id)
  end

  specify "should raise when trying to generate an update statement" do
    proc {@dataset.update_sql(:id => 0)}.should raise_error
  end

  specify "should raise when trying to generate a delete statement" do
    proc {@dataset.delete_sql}.should raise_error
  end
  
  specify "should raise when trying to generate a truncate statement" do
    proc {@dataset.truncate_sql}.should raise_error
  end

  specify "should raise when trying to generate an insert statement" do
    proc {@dataset.insert_sql}.should raise_error
  end

  specify "should specify the grouping in generated select statement" do
    @dataset.select_sql.should ==
      "SELECT * FROM test GROUP BY type_id"
  end
  
  specify "should format the right statement for counting (as a subquery)" do
    db = Sequel.mock
    db[:test].select(:name).group(:name).count
    db.sqls.should == ["SELECT COUNT(*) AS count FROM (SELECT name FROM test GROUP BY name) AS t1 LIMIT 1"]
  end
end

describe "Dataset#group_by" do
  before do
    @dataset = Sequel.mock[:test].group_by(:type_id)
  end

  specify "should raise when trying to generate an update statement" do
    proc {@dataset.update_sql(:id => 0)}.should raise_error
  end

  specify "should raise when trying to generate a delete statement" do
    proc {@dataset.delete_sql}.should raise_error
  end

  specify "should specify the grouping in generated select statement" do
    @dataset.select_sql.should == "SELECT * FROM test GROUP BY type_id"
    @dataset.group_by(:a, :b).select_sql.should == "SELECT * FROM test GROUP BY a, b"
    @dataset.group_by(:type_id=>nil).select_sql.should == "SELECT * FROM test GROUP BY (type_id IS NULL)"
  end

  specify "should ungroup when passed nil or no arguments" do
    @dataset.group_by.select_sql.should == "SELECT * FROM test"
    @dataset.group_by(nil).select_sql.should == "SELECT * FROM test"
  end

  specify "should undo previous grouping" do
    @dataset.group_by(:a).group_by(:b).select_sql.should == "SELECT * FROM test GROUP BY b"
    @dataset.group_by(:a, :b).group_by.select_sql.should == "SELECT * FROM test"
  end

  specify "should be aliased as #group" do
    @dataset.group(:type_id=>nil).select_sql.should == "SELECT * FROM test GROUP BY (type_id IS NULL)"
  end

  specify "should take a virtual row block" do
    @dataset.group{type_id > 1}.sql.should == "SELECT * FROM test GROUP BY (type_id > 1)"
    @dataset.group_by{type_id > 1}.sql.should == "SELECT * FROM test GROUP BY (type_id > 1)"
    @dataset.group{[type_id > 1, type_id < 2]}.sql.should == "SELECT * FROM test GROUP BY (type_id > 1), (type_id < 2)"
    @dataset.group(:foo){type_id > 1}.sql.should == "SELECT * FROM test GROUP BY foo, (type_id > 1)"
  end

  specify "should support a #group_rollup method if the database supports it" do
    @dataset.meta_def(:supports_group_rollup?){true}
    @dataset.group(:type_id).group_rollup.select_sql.should == "SELECT * FROM test GROUP BY ROLLUP(type_id)"
    @dataset.group(:type_id, :b).group_rollup.select_sql.should == "SELECT * FROM test GROUP BY ROLLUP(type_id, b)"
    @dataset.meta_def(:uses_with_rollup?){true}
    @dataset.group(:type_id).group_rollup.select_sql.should == "SELECT * FROM test GROUP BY type_id WITH ROLLUP"
    @dataset.group(:type_id, :b).group_rollup.select_sql.should == "SELECT * FROM test GROUP BY type_id, b WITH ROLLUP"
  end

  specify "should support a #group_cube method if the database supports it" do
    @dataset.meta_def(:supports_group_cube?){true}
    @dataset.group(:type_id).group_cube.select_sql.should == "SELECT * FROM test GROUP BY CUBE(type_id)"
    @dataset.group(:type_id, :b).group_cube.select_sql.should == "SELECT * FROM test GROUP BY CUBE(type_id, b)"
    @dataset.meta_def(:uses_with_rollup?){true}
    @dataset.group(:type_id).group_cube.select_sql.should == "SELECT * FROM test GROUP BY type_id WITH CUBE"
    @dataset.group(:type_id, :b).group_cube.select_sql.should == "SELECT * FROM test GROUP BY type_id, b WITH CUBE"
  end

  specify "should have #group_cube and #group_rollup methods raise an Error if not supported it" do
    proc{@dataset.group(:type_id).group_rollup}.should raise_error(Sequel::Error)
    proc{@dataset.group(:type_id).group_cube}.should raise_error(Sequel::Error)
  end
end

describe "Dataset#as" do
  specify "should set up an alias" do
    dataset = Sequel::Dataset.new(nil).from(:test)
    dataset.select(dataset.limit(1).select(:name).as(:n)).sql.should == 'SELECT (SELECT name FROM test LIMIT 1) AS n FROM test'
  end
end

describe "Dataset#literal" do
  before do
    @ds = Sequel::Database.new.dataset
  end
  
  specify "should convert qualified symbol notation into dot notation" do
    @ds.literal(:abc__def).should == 'abc.def'
  end
  
  specify "should convert AS symbol notation into SQL AS notation" do
    @ds.literal(:xyz___x).should == 'xyz AS x'
    @ds.literal(:abc__def___x).should == 'abc.def AS x'
  end
  
  specify "should support names with digits" do
    @ds.literal(:abc2).should == 'abc2'
    @ds.literal(:xx__yy3).should == 'xx.yy3'
    @ds.literal(:ab34__temp3_4ax).should == 'ab34.temp3_4ax'
    @ds.literal(:x1___y2).should == 'x1 AS y2'
    @ds.literal(:abc2__def3___ggg4).should == 'abc2.def3 AS ggg4'
  end
  
  specify "should support upper case and lower case" do
    @ds.literal(:ABC).should == 'ABC'
    @ds.literal(:Zvashtoy__aBcD).should == 'Zvashtoy.aBcD'
  end

  specify "should support spaces inside column names" do
    @ds.quote_identifiers = true
    @ds.literal(:"AB C").should == '"AB C"'
    @ds.literal(:"Zvas htoy__aB cD").should == '"Zvas htoy"."aB cD"'
    @ds.literal(:"aB cD___XX XX").should == '"aB cD" AS "XX XX"'
    @ds.literal(:"Zva shtoy__aB cD___XX XX").should == '"Zva shtoy"."aB cD" AS "XX XX"'
  end
end

describe "Dataset#literal" do
  before do
    @dataset = Sequel::Database.new.from(:test)
  end
  
  specify "should escape strings properly" do
    @dataset.literal('abc').should == "'abc'"
    @dataset.literal('a"x"bc').should == "'a\"x\"bc'"
    @dataset.literal("a'bc").should == "'a''bc'"
    @dataset.literal("a''bc").should == "'a''''bc'"
    @dataset.literal("a\\bc").should == "'a\\bc'"
    @dataset.literal("a\\\\bc").should == "'a\\\\bc'"
    @dataset.literal("a\\'bc").should == "'a\\''bc'"
  end
  
  specify "should escape blobs as strings by default" do
    @dataset.literal(Sequel.blob('abc')).should == "'abc'"
  end

  specify "should literalize numbers properly" do
    @dataset.literal(1).should == "1"
    @dataset.literal(1.5).should == "1.5"
  end

  specify "should literalize nil as NULL" do
    @dataset.literal(nil).should == "NULL"
  end

  specify "should literalize an array properly" do
    @dataset.literal([]).should == "(NULL)"
    @dataset.literal([1, 'abc', 3]).should == "(1, 'abc', 3)"
    @dataset.literal([1, "a'b''c", 3]).should == "(1, 'a''b''''c', 3)"
  end

  specify "should literalize symbols as column references" do
    @dataset.literal(:name).should == "name"
    @dataset.literal(:items__name).should == "items.name"
    @dataset.literal(:"items__na#m$e").should == "items.na#m$e"
  end

  specify "should call sql_literal_append with dataset and sql on type if not natively supported and the object responds to it" do
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
    @dataset.literal(@a.new).should == "called ds"
  end
  
  specify "should call sql_literal with dataset on type if not natively supported and the object responds to it" do
    @a = Class.new do
      def sql_literal(ds)
        "called #{ds.blah}"
      end
    end
    def @dataset.blah
      "ds"
    end
    @dataset.literal(@a.new).should == "called ds"
  end
  
  specify "should raise an error for unsupported types with no sql_literal method" do
    proc {@dataset.literal(Object.new)}.should raise_error
  end
  
  specify "should literalize datasets as subqueries" do
    d = @dataset.from(:test)
    d.literal(d).should == "(#{d.sql})"
  end
  
  specify "should literalize Sequel::SQLTime properly" do
    t = Sequel::SQLTime.now
    s = t.strftime("'%H:%M:%S")
    @dataset.literal(t).should == "#{s}.#{sprintf('%06i', t.usec)}'"
  end
  
  specify "should literalize Time properly" do
    t = Time.now
    s = t.strftime("'%Y-%m-%d %H:%M:%S")
    @dataset.literal(t).should == "#{s}.#{sprintf('%06i', t.usec)}'"
  end
  
  specify "should literalize DateTime properly" do
    t = DateTime.now
    s = t.strftime("'%Y-%m-%d %H:%M:%S")
    @dataset.literal(t).should == "#{s}.#{sprintf('%06i', t.sec_fraction * (RUBY_VERSION < '1.9.0' ? 86400000000 : 1000000))}'"
  end
  
  specify "should literalize Date properly" do
    d = Date.today
    s = d.strftime("'%Y-%m-%d'")
    @dataset.literal(d).should == s
  end

  specify "should literalize Date properly, even if to_s is overridden" do
    d = Date.today
    def d.to_s; "adsf" end
    s = d.strftime("'%Y-%m-%d'")
    @dataset.literal(d).should == s
  end

  specify "should literalize Time, DateTime, Date properly if SQL standard format is required" do
    @dataset.meta_def(:requires_sql_standard_datetimes?){true}

    t = Time.now
    s = t.strftime("TIMESTAMP '%Y-%m-%d %H:%M:%S")
    @dataset.literal(t).should == "#{s}.#{sprintf('%06i', t.usec)}'"

    t = DateTime.now
    s = t.strftime("TIMESTAMP '%Y-%m-%d %H:%M:%S")
    @dataset.literal(t).should == "#{s}.#{sprintf('%06i', t.sec_fraction* (RUBY_VERSION < '1.9.0' ? 86400000000 : 1000000))}'"

    d = Date.today
    s = d.strftime("DATE '%Y-%m-%d'")
    @dataset.literal(d).should == s
  end
  
  specify "should literalize Time and DateTime properly if the database support timezones in timestamps" do
    @dataset.meta_def(:supports_timestamp_timezones?){true}

    t = Time.now.utc
    s = t.strftime("'%Y-%m-%d %H:%M:%S")
    @dataset.literal(t).should == "#{s}.#{sprintf('%06i', t.usec)}+0000'"

    t = DateTime.now.new_offset(0)
    s = t.strftime("'%Y-%m-%d %H:%M:%S")
    @dataset.literal(t).should == "#{s}.#{sprintf('%06i', t.sec_fraction* (RUBY_VERSION < '1.9.0' ? 86400000000 : 1000000))}+0000'"
  end
  
  specify "should literalize Time and DateTime properly if the database doesn't support usecs in timestamps" do
    @dataset.meta_def(:supports_timestamp_usecs?){false}
    
    t = Time.now.utc
    s = t.strftime("'%Y-%m-%d %H:%M:%S")
    @dataset.literal(t).should == "#{s}'"

    t = DateTime.now.new_offset(0)
    s = t.strftime("'%Y-%m-%d %H:%M:%S")
    @dataset.literal(t).should == "#{s}'"
    
    @dataset.meta_def(:supports_timestamp_timezones?){true}
    
    t = Time.now.utc
    s = t.strftime("'%Y-%m-%d %H:%M:%S")
    @dataset.literal(t).should == "#{s}+0000'"

    t = DateTime.now.new_offset(0)
    s = t.strftime("'%Y-%m-%d %H:%M:%S")
    @dataset.literal(t).should == "#{s}+0000'"
  end
  
  specify "should not modify literal strings" do
    @dataset.quote_identifiers = true
    @dataset.literal(Sequel.lit('col1 + 2')).should == 'col1 + 2'
    @dataset.update_sql(Sequel::SQL::Identifier.new(Sequel.lit('a')) => Sequel.lit('a + 2')).should == 'UPDATE "test" SET a = a + 2'
  end

  specify "should literalize BigDecimal instances correctly" do
    @dataset.literal(BigDecimal.new("80")).should == "80.0"
    @dataset.literal(BigDecimal.new("NaN")).should == "'NaN'"
    @dataset.literal(BigDecimal.new("Infinity")).should == "'Infinity'"
    @dataset.literal(BigDecimal.new("-Infinity")).should == "'-Infinity'"
  end

  specify "should literalize PlaceholderLiteralStrings correctly" do
    @dataset.literal(Sequel::SQL::PlaceholderLiteralString.new('? = ?', [1, 2])).should == '1 = 2'
    @dataset.literal(Sequel::SQL::PlaceholderLiteralString.new('? = ?', [1, 2], true)).should == '(1 = 2)'
    @dataset.literal(Sequel::SQL::PlaceholderLiteralString.new(':a = :b', :a=>1, :b=>2)).should == '1 = 2'
    @dataset.literal(Sequel::SQL::PlaceholderLiteralString.new(':a = :b', {:a=>1, :b=>2}, true)).should == '(1 = 2)'
    @dataset.literal(Sequel::SQL::PlaceholderLiteralString.new(['', ' = ', ''], [1, 2])).should == '1 = 2'
    @dataset.literal(Sequel::SQL::PlaceholderLiteralString.new(['', ' = ', ''], [1, 2], true)).should == '(1 = 2)'
    @dataset.literal(Sequel::SQL::PlaceholderLiteralString.new(['', ' = '], [1, 2])).should == '1 = 2'
    @dataset.literal(Sequel::SQL::PlaceholderLiteralString.new(['', ' = '], [1, 2], true)).should == '(1 = 2)'
  end

  specify "should raise an Error if the object can't be literalized" do
    proc{@dataset.literal(Object.new)}.should raise_error(Sequel::Error)
  end
end

describe "Dataset#from" do
  before do
    @dataset = Sequel::Dataset.new(nil)
  end

  specify "should accept a Dataset" do
    proc {@dataset.from(@dataset)}.should_not raise_error
  end

  specify "should format a Dataset as a subquery if it has had options set" do
    @dataset.from(@dataset.from(:a).where(:a=>1)).select_sql.should == "SELECT * FROM (SELECT * FROM a WHERE (a = 1)) AS t1"
  end
  
  specify "should automatically alias sub-queries" do
    @dataset.from(@dataset.from(:a).group(:b)).select_sql.should == "SELECT * FROM (SELECT * FROM a GROUP BY b) AS t1"
      
    d1 = @dataset.from(:a).group(:b)
    d2 = @dataset.from(:c).group(:d)
    @dataset.from(d1, d2).sql.should == "SELECT * FROM (SELECT * FROM a GROUP BY b) AS t1, (SELECT * FROM c GROUP BY d) AS t2"
  end
  
  specify "should accept a hash for aliasing" do
    @dataset.from(:a => :b).sql.should == "SELECT * FROM a AS b"
    @dataset.from(:a => 'b').sql.should == "SELECT * FROM a AS b"
    @dataset.from(@dataset.from(:a).group(:b) => :c).sql.should == "SELECT * FROM (SELECT * FROM a GROUP BY b) AS c"
  end

  specify "should always use a subquery if given a dataset" do
    @dataset.from(@dataset.from(:a)).select_sql.should == "SELECT * FROM (SELECT * FROM a) AS t1"
  end
  
  specify "should treat string arguments as identifiers" do
    @dataset.quote_identifiers = true
    @dataset.from('a').select_sql.should == "SELECT * FROM \"a\""
  end
  
  specify "should not treat literal strings or blobs as identifiers" do
    @dataset.quote_identifiers = true
    @dataset.from(Sequel.lit('a')).select_sql.should == "SELECT * FROM a"
    @dataset.from(Sequel.blob('a')).select_sql.should == "SELECT * FROM 'a'"
  end
  
  specify "should remove all FROM tables if called with no arguments" do
    @dataset.from.sql.should == 'SELECT *'
  end
  
  specify "should accept sql functions" do
    @dataset.from(Sequel.function(:abc, :def)).select_sql.should == "SELECT * FROM abc(def)"
    @dataset.from(Sequel.function(:a, :i)).select_sql.should == "SELECT * FROM a(i)"
  end

  specify "should accept :schema__table___alias symbol format" do
    @dataset.from(:abc__def).select_sql.should == "SELECT * FROM abc.def"
    @dataset.from(:a_b__c).select_sql.should == "SELECT * FROM a_b.c"
    @dataset.from(:'#__#').select_sql.should == 'SELECT * FROM #.#'
    @dataset.from(:abc__def___d).select_sql.should == "SELECT * FROM abc.def AS d"
    @dataset.from(:a_b__d_e___f_g).select_sql.should == "SELECT * FROM a_b.d_e AS f_g"
    @dataset.from(:'#__#___#').select_sql.should == 'SELECT * FROM #.# AS #'
    @dataset.from(:abc___def).select_sql.should == "SELECT * FROM abc AS def"
    @dataset.from(:a_b___c_d).select_sql.should == "SELECT * FROM a_b AS c_d"
    @dataset.from(:'#___#').select_sql.should == 'SELECT * FROM # AS #'
  end

  specify "should not handle :foo__schema__table___alias specially" do
    @dataset.from(:foo__schema__table___alias).select_sql.should == "SELECT * FROM foo.schema__table AS alias"
  end

  specify "should hoist WITH clauses from subqueries if the dataset doesn't support CTEs in subselects" do
    @dataset.meta_def(:supports_cte?){true}
    @dataset.meta_def(:supports_cte_in_subselect?){false}
    @dataset.from(@dataset.from(:a).with(:a, @dataset.from(:b))).sql.should == 'WITH a AS (SELECT * FROM b) SELECT * FROM (SELECT * FROM a) AS t1'
    @dataset.from(@dataset.from(:a).with(:a, @dataset.from(:b)), @dataset.from(:c).with(:c, @dataset.from(:d))).sql.should == 'WITH a AS (SELECT * FROM b), c AS (SELECT * FROM d) SELECT * FROM (SELECT * FROM a) AS t1, (SELECT * FROM c) AS t2'
  end
end

describe "Dataset#select" do
  before do
    @d = Sequel::Dataset.new(nil).from(:test)
  end

  specify "should accept variable arity" do
    @d.select(:name).sql.should == 'SELECT name FROM test'
    @d.select(:a, :b, :test__c).sql.should == 'SELECT a, b, test.c FROM test'
  end
  
  specify "should accept symbols and literal strings" do
    @d.select(Sequel.lit('aaa')).sql.should == 'SELECT aaa FROM test'
    @d.select(:a, Sequel.lit('b')).sql.should == 'SELECT a, b FROM test'
    @d.select(:test__cc, Sequel.lit('test.d AS e')).sql.should == 'SELECT test.cc, test.d AS e FROM test'
    @d.select(Sequel.lit('test.d AS e'), :test__cc).sql.should == 'SELECT test.d AS e, test.cc FROM test'
    @d.select(:test__name___n).sql.should == 'SELECT test.name AS n FROM test'
  end
  
  specify "should accept ColumnAlls" do
    @d.select(Sequel::SQL::ColumnAll.new(:test)).sql.should == 'SELECT test.* FROM test'
  end
  
  specify "should accept QualifiedIdentifiers" do
    @d.select(Sequel.expr(:test__name).as(:n)).sql.should == 'SELECT test.name AS n FROM test'
  end

  specify "should use the wildcard if no arguments are given" do
    @d.select.sql.should == 'SELECT * FROM test'
  end
  
  specify "should handle array condition specifiers that are aliased" do
    @d.select(Sequel.as([[:b, :c]], :n)).sql.should == 'SELECT (b = c) AS n FROM test'
  end

  specify "should accept a hash for AS values" do
    @d.select(:name => 'n', :__ggh => 'age').sql.should =~ /SELECT ((name AS n, __ggh AS age)|(__ggh AS age, name AS n)) FROM test/
  end

  specify "should override the previous select option" do
    @d.select!(:a, :b, :c).select.sql.should == 'SELECT * FROM test'
    @d.select!(:price).select(:name).sql.should == 'SELECT name FROM test'
  end
  
  specify "should accept arbitrary objects and literalize them correctly" do
    @d.select(1, :a, 't').sql.should == "SELECT 1, a, 't' FROM test"
    @d.select(nil, Sequel.function(:sum, :t), :x___y).sql.should == "SELECT NULL, sum(t), x AS y FROM test"
    @d.select(nil, 1, :x => :y).sql.should == "SELECT NULL, 1, x AS y FROM test"
  end

  specify "should accept a block that yields a virtual row" do
    @d.select{|o| o.a}.sql.should == 'SELECT a FROM test'
    @d.select{a(1)}.sql.should == 'SELECT a(1) FROM test'
    @d.select{|o| o.a(1, 2)}.sql.should == 'SELECT a(1, 2) FROM test'
    @d.select{[a, a(1, 2)]}.sql.should == 'SELECT a, a(1, 2) FROM test'
  end

  specify "should merge regular arguments with argument returned from block" do
    @d.select(:b){a}.sql.should == 'SELECT b, a FROM test'
    @d.select(:b, :c){|o| o.a(1)}.sql.should == 'SELECT b, c, a(1) FROM test'
    @d.select(:b){[a, a(1, 2)]}.sql.should == 'SELECT b, a, a(1, 2) FROM test'
    @d.select(:b, :c){|o| [o.a, o.a(1, 2)]}.sql.should == 'SELECT b, c, a, a(1, 2) FROM test'
  end
end

describe "Dataset#select_group" do
  before do
    @d = Sequel::Dataset.new(nil).from(:test)
  end

  specify "should set both SELECT and GROUP" do
    @d.select_group(:name).sql.should == 'SELECT name FROM test GROUP BY name'
    @d.select_group(:a, :b__c, :d___e).sql.should == 'SELECT a, b.c, d AS e FROM test GROUP BY a, b.c, d'
  end

  specify "should remove from both SELECT and GROUP if no arguments" do
    @d.select_group(:name).select_group.sql.should == 'SELECT * FROM test'
  end

  specify "should accept virtual row blocks" do
    @d.select_group{name}.sql.should == 'SELECT name FROM test GROUP BY name'
    @d.select_group{[name, f(v).as(a)]}.sql.should == 'SELECT name, f(v) AS a FROM test GROUP BY name, f(v)'
    @d.select_group(:name){f(v).as(a)}.sql.should == 'SELECT name, f(v) AS a FROM test GROUP BY name, f(v)'
  end
end
  
describe "Dataset#select_all" do
  before do
    @d = Sequel::Dataset.new(nil).from(:test)
  end

  specify "should select the wildcard" do
    @d.select_all.sql.should == 'SELECT * FROM test'
  end
  
  specify "should override the previous select option" do
    @d.select!(:a, :b, :c).select_all.sql.should == 'SELECT * FROM test'
  end

  specify "should select all columns in a table if given an argument" do
    @d.select_all(:test).sql.should == 'SELECT test.* FROM test'
  end
  
  specify "should select all columns all tables if given a multiple arguments" do
    @d.select_all(:test, :foo).sql.should == 'SELECT test.*, foo.* FROM test'
  end
  
  specify "should work correctly with qualified symbols" do
    @d.select_all(:sch__test).sql.should == 'SELECT sch.test.* FROM test'
  end
  
  specify "should work correctly with aliased symbols" do
    @d.select_all(:test___al).sql.should == 'SELECT al.* FROM test'
    @d.select_all(:sch__test___al).sql.should == 'SELECT al.* FROM test'
  end
  
  specify "should work correctly with SQL::Identifiers" do
    @d.select_all(Sequel.identifier(:test)).sql.should == 'SELECT test.* FROM test'
  end
  
  specify "should work correctly with SQL::QualifiedIdentifier" do
    @d.select_all(Sequel.qualify(:sch, :test)).sql.should == 'SELECT sch.test.* FROM test'
  end
  
  specify "should work correctly with SQL::AliasedExpressions" do
    @d.select_all(Sequel.expr(:test).as(:al)).sql.should == 'SELECT al.* FROM test'
  end
  
  specify "should work correctly with SQL::JoinClauses" do
    d = @d.cross_join(:foo).cross_join(:test___al)
    @d.select_all(*d.opts[:join]).sql.should == 'SELECT foo.*, al.* FROM test'
  end
end

describe "Dataset#select_more" do
  before do
    @d = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should act like #select for datasets with no selection" do
    @d.select_more(:a, :b).sql.should == 'SELECT a, b FROM test'
    @d.select_all.select_more(:a, :b).sql.should == 'SELECT a, b FROM test'
    @d.select(:blah).select_all.select_more(:a, :b).sql.should == 'SELECT a, b FROM test'
  end

  specify "should add to the currently selected columns" do
    @d.select(:a).select_more(:b).sql.should == 'SELECT a, b FROM test'
    @d.select(Sequel::SQL::ColumnAll.new(:a)).select_more(Sequel::SQL::ColumnAll.new(:b)).sql.should == 'SELECT a.*, b.* FROM test'
  end

  specify "should accept a block that yields a virtual row" do
    @d.select(:a).select_more{|o| o.b}.sql.should == 'SELECT a, b FROM test'
    @d.select(Sequel::SQL::ColumnAll.new(:a)).select_more(Sequel::SQL::ColumnAll.new(:b)){b(1)}.sql.should == 'SELECT a.*, b.*, b(1) FROM test'
  end
end

describe "Dataset#select_append" do
  before do
    @d = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should select * in addition to columns if no columns selected" do
    @d.select_append(:a, :b).sql.should == 'SELECT *, a, b FROM test'
    @d.select_all.select_append(:a, :b).sql.should == 'SELECT *, a, b FROM test'
    @d.select(:blah).select_all.select_append(:a, :b).sql.should == 'SELECT *, a, b FROM test'
  end

  specify "should add to the currently selected columns" do
    @d.select(:a).select_append(:b).sql.should == 'SELECT a, b FROM test'
    @d.select(Sequel::SQL::ColumnAll.new(:a)).select_append(Sequel::SQL::ColumnAll.new(:b)).sql.should == 'SELECT a.*, b.* FROM test'
  end

  specify "should accept a block that yields a virtual row" do
    @d.select(:a).select_append{|o| o.b}.sql.should == 'SELECT a, b FROM test'
    @d.select(Sequel::SQL::ColumnAll.new(:a)).select_append(Sequel::SQL::ColumnAll.new(:b)){b(1)}.sql.should == 'SELECT a.*, b.*, b(1) FROM test'
  end

  specify "should select from all from and join tables if SELECT *, column not supported" do
    @d.meta_def(:supports_select_all_and_column?){false}
    @d.select_append(:b).sql.should == 'SELECT test.*, b FROM test'
    @d.from(:test, :c).select_append(:b).sql.should == 'SELECT test.*, c.*, b FROM test, c'
    @d.cross_join(:c).select_append(:b).sql.should == 'SELECT test.*, c.*, b FROM test CROSS JOIN c'
    @d.cross_join(:c).cross_join(:d).select_append(:b).sql.should == 'SELECT test.*, c.*, d.*, b FROM test CROSS JOIN c CROSS JOIN d'
  end
end

describe "Dataset#order" do
  before do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should include an ORDER BY clause in the select statement" do
    @dataset.order(:name).sql.should == 'SELECT * FROM test ORDER BY name'
  end
  
  specify "should accept multiple arguments" do
    @dataset.order(:name, Sequel.desc(:price)).sql.should == 'SELECT * FROM test ORDER BY name, price DESC'
  end
  
  specify "should accept :nulls options for asc and desc" do
    @dataset.order(Sequel.asc(:name, :nulls=>:last), Sequel.desc(:price, :nulls=>:first)).sql.should == 'SELECT * FROM test ORDER BY name ASC NULLS LAST, price DESC NULLS FIRST'
  end
  
  specify "should override a previous ordering" do
    @dataset.order(:name).order(:stamp).sql.should == 'SELECT * FROM test ORDER BY stamp'
  end
  
  specify "should accept a literal string" do
    @dataset.order(Sequel.lit('dada ASC')).sql.should == 'SELECT * FROM test ORDER BY dada ASC'
  end
  
  specify "should accept a hash as an expression" do
    @dataset.order(:name=>nil).sql.should == 'SELECT * FROM test ORDER BY (name IS NULL)'
  end
  
  specify "should accept a nil to remove ordering" do
    @dataset.order(:bah).order(nil).sql.should == 'SELECT * FROM test'
  end

  specify "should accept a block that yields a virtual row" do
    @dataset.order{|o| o.a}.sql.should == 'SELECT * FROM test ORDER BY a'
    @dataset.order{a(1)}.sql.should == 'SELECT * FROM test ORDER BY a(1)'
    @dataset.order{|o| o.a(1, 2)}.sql.should == 'SELECT * FROM test ORDER BY a(1, 2)'
    @dataset.order{[a, a(1, 2)]}.sql.should == 'SELECT * FROM test ORDER BY a, a(1, 2)'
  end

  specify "should merge regular arguments with argument returned from block" do
    @dataset.order(:b){a}.sql.should == 'SELECT * FROM test ORDER BY b, a'
    @dataset.order(:b, :c){|o| o.a(1)}.sql.should == 'SELECT * FROM test ORDER BY b, c, a(1)'
    @dataset.order(:b){[a, a(1, 2)]}.sql.should == 'SELECT * FROM test ORDER BY b, a, a(1, 2)'
    @dataset.order(:b, :c){|o| [o.a, o.a(1, 2)]}.sql.should == 'SELECT * FROM test ORDER BY b, c, a, a(1, 2)'
  end
end

describe "Dataset#unfiltered" do
  specify "should remove filtering from the dataset" do
    Sequel::Dataset.new(nil).from(:test).filter(:score=>1).unfiltered.sql.should == 'SELECT * FROM test'
  end
end

describe "Dataset#unlimited" do
  specify "should remove limit and offset from the dataset" do
    Sequel::Dataset.new(nil).from(:test).limit(1, 2).unlimited.sql.should == 'SELECT * FROM test'
  end
end

describe "Dataset#ungrouped" do
  specify "should remove group and having clauses from the dataset" do
    Sequel::Dataset.new(nil).from(:test).group(:a).having(:b).ungrouped.sql.should == 'SELECT * FROM test'
  end
end

describe "Dataset#unordered" do
  specify "should remove ordering from the dataset" do
    Sequel::Dataset.new(nil).from(:test).order(:name).unordered.sql.should == 'SELECT * FROM test'
  end
end

describe "Dataset#with_sql" do
  before do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should use static sql" do
    @dataset.with_sql('SELECT 1 FROM test').sql.should == 'SELECT 1 FROM test'
  end
  
  specify "should work with placeholders" do
    @dataset.with_sql('SELECT ? FROM test', 1).sql.should == 'SELECT 1 FROM test'
  end

  specify "should work with named placeholders" do
    @dataset.with_sql('SELECT :x FROM test', :x=>1).sql.should == 'SELECT 1 FROM test'
  end

  specify "should keep row_proc" do
    @dataset.with_sql('SELECT 1 FROM test').row_proc.should == @dataset.row_proc
  end

  specify "should work with method symbols and arguments" do
    @dataset.with_sql(:delete_sql).sql.should == 'DELETE FROM test'
    @dataset.with_sql(:insert_sql, :b=>1).sql.should == 'INSERT INTO test (b) VALUES (1)'
    @dataset.with_sql(:update_sql, :b=>1).sql.should == 'UPDATE test SET b = 1'
  end
  
end

describe "Dataset#order_by" do
  before do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should include an ORDER BY clause in the select statement" do
    @dataset.order_by(:name).sql.should == 'SELECT * FROM test ORDER BY name'
  end
  
  specify "should accept multiple arguments" do
    @dataset.order_by(:name, Sequel.desc(:price)).sql.should == 'SELECT * FROM test ORDER BY name, price DESC'
  end
  
  specify "should override a previous ordering" do
    @dataset.order_by(:name).order(:stamp).sql.should == 'SELECT * FROM test ORDER BY stamp'
  end
  
  specify "should accept a string" do
    @dataset.order_by(Sequel.lit('dada ASC')).sql.should == 'SELECT * FROM test ORDER BY dada ASC'
  end

  specify "should accept a nil to remove ordering" do
    @dataset.order_by(:bah).order_by(nil).sql.should == 'SELECT * FROM test'
  end
end

describe "Dataset#order_more and order_append" do
  before do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should include an ORDER BY clause in the select statement" do
    @dataset.order_more(:name).sql.should == 'SELECT * FROM test ORDER BY name'
    @dataset.order_append(:name).sql.should == 'SELECT * FROM test ORDER BY name'
  end
  
  specify "should add to the end of a previous ordering" do
    @dataset.order(:name).order_more(Sequel.desc(:stamp)).sql.should == 'SELECT * FROM test ORDER BY name, stamp DESC'
    @dataset.order(:name).order_append(Sequel.desc(:stamp)).sql.should == 'SELECT * FROM test ORDER BY name, stamp DESC'
  end

  specify "should accept a block that yields a virtual row" do
    @dataset.order(:a).order_more{|o| o.b}.sql.should == 'SELECT * FROM test ORDER BY a, b'
    @dataset.order(:a, :b).order_more(:c, :d){[e, f(1, 2)]}.sql.should == 'SELECT * FROM test ORDER BY a, b, c, d, e, f(1, 2)'
    @dataset.order(:a).order_append{|o| o.b}.sql.should == 'SELECT * FROM test ORDER BY a, b'
    @dataset.order(:a, :b).order_append(:c, :d){[e, f(1, 2)]}.sql.should == 'SELECT * FROM test ORDER BY a, b, c, d, e, f(1, 2)'
  end
end

describe "Dataset#order_prepend" do
  before do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should include an ORDER BY clause in the select statement" do
    @dataset.order_prepend(:name).sql.should == 'SELECT * FROM test ORDER BY name'
  end
  
  specify "should add to the beginning of a previous ordering" do
    @dataset.order(:name).order_prepend(Sequel.desc(:stamp)).sql.should == 'SELECT * FROM test ORDER BY stamp DESC, name'
  end

  specify "should accept a block that yields a virtual row" do
    @dataset.order(:a).order_prepend{|o| o.b}.sql.should == 'SELECT * FROM test ORDER BY b, a'
    @dataset.order(:a, :b).order_prepend(:c, :d){[e, f(1, 2)]}.sql.should == 'SELECT * FROM test ORDER BY c, d, e, f(1, 2), a, b'
  end
end

describe "Dataset#reverse_order" do
  before do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should use DESC as default order" do
    @dataset.reverse_order(:name).sql.should == 'SELECT * FROM test ORDER BY name DESC'
  end
  
  specify "should invert the order given" do
    @dataset.reverse_order(Sequel.desc(:name)).sql.should == 'SELECT * FROM test ORDER BY name ASC'
  end
  
  specify "should invert the order for ASC expressions" do
    @dataset.reverse_order(Sequel.asc(:name)).sql.should == 'SELECT * FROM test ORDER BY name DESC'
  end
  
  specify "should accept multiple arguments" do
    @dataset.reverse_order(:name, Sequel.desc(:price)).sql.should == 'SELECT * FROM test ORDER BY name DESC, price ASC'
  end

  specify "should handles NULLS ordering correctly when reversing" do
    @dataset.reverse_order(Sequel.asc(:name, :nulls=>:first), Sequel.desc(:price, :nulls=>:last)).sql.should == 'SELECT * FROM test ORDER BY name DESC NULLS LAST, price ASC NULLS FIRST'
  end

  specify "should reverse a previous ordering if no arguments are given" do
    @dataset.order(:name).reverse_order.sql.should == 'SELECT * FROM test ORDER BY name DESC'
    @dataset.order(Sequel.desc(:clumsy), :fool).reverse_order.sql.should == 'SELECT * FROM test ORDER BY clumsy ASC, fool DESC'
  end
  
  specify "should return an unordered dataset for a dataset with no order" do
    @dataset.unordered.reverse_order.sql.should == 'SELECT * FROM test'
  end
  
  specify "should have #reverse alias" do
    @dataset.order(:name).reverse.sql.should == 'SELECT * FROM test ORDER BY name DESC'
  end

  specify "should accept a block" do
    @dataset.reverse{name}.sql.should == 'SELECT * FROM test ORDER BY name DESC'
    @dataset.reverse_order{name}.sql.should == 'SELECT * FROM test ORDER BY name DESC'
    @dataset.reverse(:foo){name}.sql.should == 'SELECT * FROM test ORDER BY foo DESC, name DESC'
    @dataset.reverse_order(:foo){name}.sql.should == 'SELECT * FROM test ORDER BY foo DESC, name DESC'
    @dataset.reverse(Sequel.desc(:foo)){name}.sql.should == 'SELECT * FROM test ORDER BY foo ASC, name DESC'
    @dataset.reverse_order(Sequel.desc(:foo)){name}.sql.should == 'SELECT * FROM test ORDER BY foo ASC, name DESC'
  end
end

describe "Dataset#limit" do
  before do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should include a LIMIT clause in the select statement" do
    @dataset.limit(10).sql.should == 'SELECT * FROM test LIMIT 10'
  end
  
  specify "should accept ranges" do
    @dataset.limit(3..7).sql.should == 'SELECT * FROM test LIMIT 5 OFFSET 3'
    @dataset.limit(3...7).sql.should == 'SELECT * FROM test LIMIT 4 OFFSET 3'
  end
  
  specify "should include an offset if a second argument is given" do
    @dataset.limit(6, 10).sql.should == 'SELECT * FROM test LIMIT 6 OFFSET 10'
  end
    
  specify "should convert regular strings to integers" do
    @dataset.limit('6', 'a() - 1').sql.should == 'SELECT * FROM test LIMIT 6 OFFSET 0'
  end
  
  specify "should not convert literal strings to integers" do
    @dataset.limit(Sequel.lit('6'), Sequel.lit('a() - 1')).sql.should == 'SELECT * FROM test LIMIT 6 OFFSET a() - 1'
  end
    
  specify "should not convert other objects" do
    @dataset.limit(6, Sequel.function(:a) - 1).sql.should == 'SELECT * FROM test LIMIT 6 OFFSET (a() - 1)'
  end
  
  specify "should be able to reset limit and offset with nil values" do
    @dataset.limit(6).limit(nil).sql.should == 'SELECT * FROM test'
    @dataset.limit(6, 1).limit(nil).sql.should == 'SELECT * FROM test OFFSET 1'
    @dataset.limit(6, 1).limit(nil, nil).sql.should == 'SELECT * FROM test'
  end
  
  specify "should work with fixed sql datasets" do
    @dataset.opts[:sql] = 'select * from cccc'
    @dataset.limit(6, 10).sql.should == 'SELECT * FROM (select * from cccc) AS t1 LIMIT 6 OFFSET 10'
  end
  
  specify "should raise an error if an invalid limit or offset is used" do
    proc{@dataset.limit(-1)}.should raise_error(Sequel::Error)
    proc{@dataset.limit(0)}.should raise_error(Sequel::Error)
    proc{@dataset.limit(1)}.should_not raise_error(Sequel::Error)
    proc{@dataset.limit(1, -1)}.should raise_error(Sequel::Error)
    proc{@dataset.limit(1, 0)}.should_not raise_error(Sequel::Error)
    proc{@dataset.limit(1, 1)}.should_not raise_error(Sequel::Error)
  end
end

describe "Dataset#naked" do
  specify "should returned clone dataset without row_proc" do
    d = Sequel::Dataset.new(nil)
    d.row_proc = Proc.new{|r| r}
    d.naked.row_proc.should be_nil
    d.row_proc.should_not be_nil
  end
end

describe "Dataset#naked!" do
  specify "should remove any existing row_proc" do
    d = Sequel::Dataset.new(nil)
    d.row_proc = Proc.new{|r| r}
    d.naked!.row_proc.should be_nil
    d.row_proc.should be_nil
  end
end

describe "Dataset#qualified_column_name" do
  before do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should return the literal value if not given a symbol" do
    @dataset.literal(@dataset.send(:qualified_column_name, 'ccc__b', :items)).should == "'ccc__b'"
    @dataset.literal(@dataset.send(:qualified_column_name, 3, :items)).should == '3'
    @dataset.literal(@dataset.send(:qualified_column_name, Sequel.lit('a'), :items)).should == 'a'
  end
  
  specify "should qualify the column with the supplied table name if given an unqualified symbol" do
    @dataset.literal(@dataset.send(:qualified_column_name, :b1, :items)).should == 'items.b1'
  end

  specify "should not changed the qualifed column's table if given a qualified symbol" do
    @dataset.literal(@dataset.send(:qualified_column_name, :ccc__b, :items)).should == 'ccc.b'
  end

  specify "should handle an aliased identifier" do
    @dataset.literal(@dataset.send(:qualified_column_name, :ccc, Sequel.expr(:items).as(:i))).should == 'i.ccc'
  end
end

describe "Dataset#map" do
  before do
    @d = Sequel.mock(:fetch=>[{:a => 1, :b => 2}, {:a => 3, :b => 4}, {:a => 5, :b => 6}])[:items]
  end
  
  specify "should provide the usual functionality if no argument is given" do
    @d.map{|n| n[:a] + n[:b]}.should == [3, 7, 11]
  end
  
  specify "should map using #[column name] if column name is given" do
    @d.map(:a).should == [1, 3, 5]
  end
  
  specify "should support multiple column names if an array of column names is given" do
    @d.map([:a, :b]).should == [[1, 2], [3, 4], [5, 6]]
  end
  
  specify "should not call the row_proc if an argument is given" do
    @d.row_proc = proc{|r| h = {}; r.keys.each{|k| h[k] = r[k] * 2}; h}
    @d.map(:a).should == [1, 3, 5]
    @d.map([:a, :b]).should == [[1, 2], [3, 4], [5, 6]]
  end

  specify "should call the row_proc if no argument is given" do
    @d.row_proc = proc{|r| h = {}; r.keys.each{|k| h[k] = r[k] * 2}; h}
    @d.map{|n| n[:a] + n[:b]}.should == [6, 14, 22]
  end
  
  specify "should return the complete dataset values if nothing is given" do
    @d.map.to_a.should == [{:a => 1, :b => 2}, {:a => 3, :b => 4}, {:a => 5, :b => 6}]
  end
end

describe "Dataset#to_hash" do
  before do
    @d = Sequel.mock(:fetch=>[{:a => 1, :b => 2}, {:a => 3, :b => 4}, {:a => 5, :b => 6}])[:items]
  end
  
  specify "should provide a hash with the first column as key and the second as value" do
    @d.to_hash(:a, :b).should == {1 => 2, 3 => 4, 5 => 6}
    @d.to_hash(:b, :a).should == {2 => 1, 4 => 3, 6 => 5}
  end
  
  specify "should provide a hash with the first column as key and the entire hash as value if the value column is blank or nil" do
    @d.to_hash(:a).should == {1 => {:a => 1, :b => 2}, 3 => {:a => 3, :b => 4}, 5 => {:a => 5, :b => 6}}
    @d.to_hash(:b).should == {2 => {:a => 1, :b => 2}, 4 => {:a => 3, :b => 4}, 6 => {:a => 5, :b => 6}}
  end

  specify "should support using an array of columns as either the key or the value" do
    @d.to_hash([:a, :b], :b).should == {[1, 2] => 2, [3, 4] => 4, [5, 6] => 6}
    @d.to_hash(:b, [:a, :b]).should == {2 => [1, 2], 4 => [3, 4], 6 => [5, 6]}
    @d.to_hash([:b, :a], [:a, :b]).should == {[2, 1] => [1, 2], [4, 3] => [3, 4], [6, 5] => [5, 6]}
    @d.to_hash([:a, :b]).should == {[1, 2] => {:a => 1, :b => 2}, [3, 4] => {:a => 3, :b => 4}, [5, 6] => {:a => 5, :b => 6}}
  end

  specify "should not call the row_proc if two arguments are given" do
    @d.row_proc = proc{|r| h = {}; r.keys.each{|k| h[k] = r[k] * 2}; h}
    @d.to_hash(:a, :b).should == {1 => 2, 3 => 4, 5 => 6}
    @d.to_hash(:b, :a).should == {2 => 1, 4 => 3, 6 => 5}
    @d.to_hash([:a, :b], :b).should == {[1, 2] => 2, [3, 4] => 4, [5, 6] => 6}
    @d.to_hash(:b, [:a, :b]).should == {2 => [1, 2], 4 => [3, 4], 6 => [5, 6]}
    @d.to_hash([:b, :a], [:a, :b]).should == {[2, 1] => [1, 2], [4, 3] => [3, 4], [6, 5] => [5, 6]}
  end

  specify "should call the row_proc if only a single argument is given" do
    @d.row_proc = proc{|r| h = {}; r.keys.each{|k| h[k] = r[k] * 2}; h}
    @d.to_hash(:a).should == {2 => {:a => 2, :b => 4}, 6 => {:a => 6, :b => 8}, 10 => {:a => 10, :b => 12}}
    @d.to_hash(:b).should == {4 => {:a => 2, :b => 4}, 8 => {:a => 6, :b => 8}, 12 => {:a => 10, :b => 12}}
    @d.to_hash([:a, :b]).should == {[2, 4] => {:a => 2, :b => 4}, [6, 8] => {:a => 6, :b => 8}, [10, 12] => {:a => 10, :b => 12}}
  end
end

describe "Dataset#to_hash_groups" do
  before do
    @d = Sequel.mock(:fetch=>[{:a => 1, :b => 2}, {:a => 3, :b => 4}, {:a => 1, :b => 6}, {:a => 7, :b => 4}])[:items]
  end
  
  specify "should provide a hash with the first column as key and the second as value" do
    @d.to_hash_groups(:a, :b).should == {1 => [2, 6], 3 => [4], 7 => [4]}
    @d.to_hash_groups(:b, :a).should == {2 => [1], 4=>[3, 7], 6=>[1]}
  end
  
  specify "should provide a hash with the first column as key and the entire hash as value if the value column is blank or nil" do
    @d.to_hash_groups(:a).should == {1 => [{:a => 1, :b => 2}, {:a => 1, :b => 6}], 3 => [{:a => 3, :b => 4}], 7 => [{:a => 7, :b => 4}]}
    @d.to_hash_groups(:b).should == {2 => [{:a => 1, :b => 2}], 4 => [{:a => 3, :b => 4}, {:a => 7, :b => 4}], 6 => [{:a => 1, :b => 6}]}
  end

  specify "should support using an array of columns as either the key or the value" do
    @d.to_hash_groups([:a, :b], :b).should == {[1, 2] => [2], [3, 4] => [4], [1, 6] => [6], [7, 4]=>[4]}
    @d.to_hash_groups(:b, [:a, :b]).should == {2 => [[1, 2]], 4 => [[3, 4], [7, 4]], 6 => [[1, 6]]}
    @d.to_hash_groups([:b, :a], [:a, :b]).should == {[2, 1] => [[1, 2]], [4, 3] => [[3, 4]], [6, 1] => [[1, 6]], [4, 7]=>[[7, 4]]}
    @d.to_hash_groups([:a, :b]).should == {[1, 2] => [{:a => 1, :b => 2}], [3, 4] => [{:a => 3, :b => 4}], [1, 6] => [{:a => 1, :b => 6}], [7, 4] => [{:a => 7, :b => 4}]}
  end

  specify "should not call the row_proc if two arguments are given" do
    @d.row_proc = proc{|r| h = {}; r.keys.each{|k| h[k] = r[k] * 2}; h}
    @d.to_hash_groups(:a, :b).should == {1 => [2, 6], 3 => [4], 7 => [4]}
    @d.to_hash_groups(:b, :a).should == {2 => [1], 4=>[3, 7], 6=>[1]}
    @d.to_hash_groups([:a, :b], :b).should == {[1, 2] => [2], [3, 4] => [4], [1, 6] => [6], [7, 4]=>[4]}
    @d.to_hash_groups(:b, [:a, :b]).should == {2 => [[1, 2]], 4 => [[3, 4], [7, 4]], 6 => [[1, 6]]}
    @d.to_hash_groups([:b, :a], [:a, :b]).should == {[2, 1] => [[1, 2]], [4, 3] => [[3, 4]], [6, 1] => [[1, 6]], [4, 7]=>[[7, 4]]}
  end

  specify "should call the row_proc if only a single argument is given" do
    @d.row_proc = proc{|r| h = {}; r.keys.each{|k| h[k] = r[k] * 2}; h}
    @d.to_hash_groups(:a).should == {2 => [{:a => 2, :b => 4}, {:a => 2, :b => 12}], 6 => [{:a => 6, :b => 8}], 14 => [{:a => 14, :b => 8}]}
    @d.to_hash_groups(:b).should == {4 => [{:a => 2, :b => 4}], 8 => [{:a => 6, :b => 8}, {:a => 14, :b => 8}], 12 => [{:a => 2, :b => 12}]}
    @d.to_hash_groups([:a, :b]).should == {[2, 4] => [{:a => 2, :b => 4}], [6, 8] => [{:a => 6, :b => 8}], [2, 12] => [{:a => 2, :b => 12}], [14, 8] => [{:a => 14, :b => 8}]}
  end
end

describe "Dataset#distinct" do
  before do
    @db = Sequel.mock
    @dataset = @db[:test].select(:name)
  end
  
  specify "should include DISTINCT clause in statement" do
    @dataset.distinct.sql.should == 'SELECT DISTINCT name FROM test'
  end
  
  specify "should raise an error if columns given and DISTINCT ON is not supported" do
    proc{@dataset.distinct}.should_not raise_error
    proc{@dataset.distinct(:a)}.should raise_error(Sequel::InvalidOperation)
  end
  
  specify "should use DISTINCT ON if columns are given and DISTINCT ON is supported" do
    @dataset.meta_def(:supports_distinct_on?){true}
    @dataset.distinct(:a, :b).sql.should == 'SELECT DISTINCT ON (a, b) name FROM test'
    @dataset.distinct(Sequel.cast(:stamp, :integer), :node_id=>nil).sql.should == 'SELECT DISTINCT ON (CAST(stamp AS integer), (node_id IS NULL)) name FROM test'
  end

  specify "should do a subselect for count" do
    @dataset.distinct.count
    @db.sqls.should == ['SELECT COUNT(*) AS count FROM (SELECT DISTINCT name FROM test) AS t1 LIMIT 1']
  end
end

describe "Dataset#count" do
  before do
    @db = Sequel.mock(:fetch=>{:count=>1})
    @dataset = @db.from(:test).columns(:count)
  end
  
  specify "should format SQL properly" do
    @dataset.count.should == 1
    @db.sqls.should == ['SELECT COUNT(*) AS count FROM test LIMIT 1']
  end
  
  specify "should accept an argument" do
    @dataset.count(:foo).should == 1
    @db.sqls.should == ['SELECT COUNT(foo) AS count FROM test LIMIT 1']
  end
  
  specify "should work with a nil argument" do
    @dataset.count(nil).should == 1
    @db.sqls.should == ['SELECT COUNT(NULL) AS count FROM test LIMIT 1']
  end
  
  specify "should accept a virtual row block" do
    @dataset.count{foo(bar)}.should == 1
    @db.sqls.should == ['SELECT COUNT(foo(bar)) AS count FROM test LIMIT 1']
  end
  
  specify "should raise an Error if given an argument and a block" do
    proc{@dataset.count(:foo){foo(bar)}}.should raise_error(Sequel::Error)
  end
  
  specify "should include the where clause if it's there" do
    @dataset.filter(Sequel.expr(:abc) < 30).count.should == 1
    @db.sqls.should == ['SELECT COUNT(*) AS count FROM test WHERE (abc < 30) LIMIT 1']
  end
  
  specify "should count properly for datasets with fixed sql" do
    @dataset.opts[:sql] = "select abc from xyz"
    @dataset.count.should == 1
    @db.sqls.should == ["SELECT COUNT(*) AS count FROM (select abc from xyz) AS t1 LIMIT 1"]
  end

  specify "should count properly when using UNION, INTERSECT, or EXCEPT" do
    @dataset.union(@dataset).count.should == 1
    @db.sqls.should == ["SELECT COUNT(*) AS count FROM (SELECT * FROM test UNION SELECT * FROM test) AS t1 LIMIT 1"]
    @dataset.intersect(@dataset).count.should == 1
    @db.sqls.should == ["SELECT COUNT(*) AS count FROM (SELECT * FROM test INTERSECT SELECT * FROM test) AS t1 LIMIT 1"]
    @dataset.except(@dataset).count.should == 1
    @db.sqls.should == ["SELECT COUNT(*) AS count FROM (SELECT * FROM test EXCEPT SELECT * FROM test) AS t1 LIMIT 1"]
  end

  specify "should return limit if count is greater than it" do
    @dataset.limit(5).count.should == 1
    @db.sqls.should == ["SELECT COUNT(*) AS count FROM (SELECT * FROM test LIMIT 5) AS t1 LIMIT 1"]
  end
  
  specify "should work correctly with offsets" do
    @dataset.limit(nil, 5).count.should == 1
    @db.sqls.should == ["SELECT COUNT(*) AS count FROM (SELECT * FROM test OFFSET 5) AS t1 LIMIT 1"]
  end
  
  it "should work on a graphed_dataset" do
    @dataset.should_receive(:columns).twice.and_return([:a])
    @dataset.graph(@dataset, [:a], :table_alias=>:test2).count.should == 1
    @db.sqls.should == ['SELECT COUNT(*) AS count FROM test LEFT OUTER JOIN test AS test2 USING (a) LIMIT 1']
  end

  specify "should not cache the columns value" do
    ds = @dataset.from(:blah).columns(:a)
    ds.columns.should == [:a]
    ds.count.should == 1
    @db.sqls.should == ['SELECT COUNT(*) AS count FROM blah LIMIT 1']
    ds.columns.should == [:a]
  end
end

describe "Dataset#group_and_count" do
  before do
    @ds = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should format SQL properly" do
    @ds.group_and_count(:name).sql.should == "SELECT name, count(*) AS count FROM test GROUP BY name"
  end

  specify "should accept multiple columns for grouping" do
    @ds.group_and_count(:a, :b).sql.should == "SELECT a, b, count(*) AS count FROM test GROUP BY a, b"
  end

  specify "should format column aliases in the select clause but not in the group clause" do
    @ds.group_and_count(:name___n).sql.should == "SELECT name AS n, count(*) AS count FROM test GROUP BY name"
    @ds.group_and_count(:name__n).sql.should == "SELECT name.n, count(*) AS count FROM test GROUP BY name.n"
  end

  specify "should handle identifiers" do
    @ds.group_and_count(Sequel.identifier(:name___n)).sql.should == "SELECT name___n, count(*) AS count FROM test GROUP BY name___n"
  end

  specify "should handle literal strings" do
    @ds.group_and_count(Sequel.lit("name")).sql.should == "SELECT name, count(*) AS count FROM test GROUP BY name"
  end

  specify "should handle aliased expressions" do
    @ds.group_and_count(Sequel.expr(:name).as(:n)).sql.should == "SELECT name AS n, count(*) AS count FROM test GROUP BY name"
    @ds.group_and_count(Sequel.identifier(:name).as(:n)).sql.should == "SELECT name AS n, count(*) AS count FROM test GROUP BY name"
  end

  specify "should take a virtual row block" do
    @ds.group_and_count{(type_id > 1).as(t)}.sql.should == "SELECT (type_id > 1) AS t, count(*) AS count FROM test GROUP BY (type_id > 1)"
    @ds.group_and_count{[(type_id > 1).as(t), type_id < 2]}.sql.should == "SELECT (type_id > 1) AS t, (type_id < 2), count(*) AS count FROM test GROUP BY (type_id > 1), (type_id < 2)"
    @ds.group_and_count(:foo){type_id > 1}.sql.should == "SELECT foo, (type_id > 1), count(*) AS count FROM test GROUP BY foo, (type_id > 1)"
  end
end

describe "Dataset#empty?" do
  specify "should return true if records exist in the dataset" do
    db = Sequel.mock(:fetch=>proc{|sql| {1=>1} unless sql =~ /WHERE 'f'/})
    db.from(:test).should_not be_empty
    db.sqls.should == ['SELECT 1 AS one FROM test LIMIT 1']
    db.from(:test).filter(false).should be_empty
    db.sqls.should == ["SELECT 1 AS one FROM test WHERE 'f' LIMIT 1"]
  end
end

describe "Dataset#first_source_alias" do
  before do
    @ds = Sequel::Dataset.new(nil)
  end
  
  specify "should be the entire first source if not aliased" do
    @ds.from(:t).first_source_alias.should == :t
    @ds.from(Sequel.identifier(:t__a)).first_source_alias.should == Sequel.identifier(:t__a)
    @ds.from(:s__t).first_source_alias.should == :s__t
    @ds.from(Sequel.qualify(:s, :t)).first_source_alias.should == Sequel.qualify(:s, :t)
  end
  
  specify "should be the alias if aliased" do
    @ds.from(:t___a).first_source_alias.should == :a
    @ds.from(:s__t___a).first_source_alias.should == :a
    @ds.from(Sequel.expr(:t).as(:a)).first_source_alias.should == :a
  end
  
  specify "should be aliased as first_source" do
    @ds.from(:t).first_source.should == :t
    @ds.from(Sequel.identifier(:t__a)).first_source.should == Sequel.identifier(:t__a)
    @ds.from(:s__t___a).first_source.should == :a
    @ds.from(Sequel.expr(:t).as(:a)).first_source.should == :a
  end
  
  specify "should raise exception if table doesn't have a source" do
    proc{@ds.first_source_alias.should == :t}.should raise_error(Sequel::Error)
  end
end

describe "Dataset#first_source_table" do
  before do
    @ds = Sequel::Dataset.new(nil)
  end
  
  specify "should be the entire first source if not aliased" do
    @ds.from(:t).first_source_table.should == :t
    @ds.from(Sequel.identifier(:t__a)).first_source_table.should == Sequel.identifier(:t__a)
    @ds.from(:s__t).first_source_table.should == :s__t
    @ds.from(Sequel.qualify(:s, :t)).first_source_table.should == Sequel.qualify(:s, :t)
  end
  
  specify "should be the unaliased part if aliased" do
    @ds.literal(@ds.from(:t___a).first_source_table).should == "t"
    @ds.literal(@ds.from(:s__t___a).first_source_table).should == "s.t"
    @ds.literal(@ds.from(Sequel.expr(:t).as(:a)).first_source_table).should == "t"
  end
  
  specify "should raise exception if table doesn't have a source" do
    proc{@ds.first_source_table.should == :t}.should raise_error(Sequel::Error)
  end
end

describe "Dataset#from_self" do
  before do
    @ds = Sequel::Dataset.new(nil).from(:test).select(:name).limit(1)
  end
  
  specify "should set up a default alias" do
    @ds.from_self.sql.should == 'SELECT * FROM (SELECT name FROM test LIMIT 1) AS t1'
  end
  
  specify "should modify only the new dataset" do
    @ds.from_self.select(:bogus).sql.should == 'SELECT bogus FROM (SELECT name FROM test LIMIT 1) AS t1'
  end
  
  specify "should use the user-specified alias" do
    @ds.from_self(:alias=>:some_name).sql.should == 'SELECT * FROM (SELECT name FROM test LIMIT 1) AS some_name'
  end
  
  specify "should use the user-specified alias for joins" do
    @ds.from_self(:alias=>:some_name).inner_join(:posts, :alias=>:name).sql.should == \
      'SELECT * FROM (SELECT name FROM test LIMIT 1) AS some_name INNER JOIN posts ON (posts.alias = some_name.name)'
  end
  
  specify "should not remove non-SQL options such as :server" do
    @ds.server(:blah).from_self(:alias=>:some_name).opts[:server].should == :blah
  end

  specify "should hoist WITH clauses in current dataset if dataset doesn't support WITH in subselect" do
    ds = Sequel::Dataset.new(nil)
    ds.meta_def(:supports_cte?){true}
    ds.meta_def(:supports_cte_in_subselect?){false}
    ds.from(:a).with(:a, ds.from(:b)).from_self.sql.should == 'WITH a AS (SELECT * FROM b) SELECT * FROM (SELECT * FROM a) AS t1'
    ds.from(:a, :c).with(:a, ds.from(:b)).with(:c, ds.from(:d)).from_self.sql.should == 'WITH a AS (SELECT * FROM b), c AS (SELECT * FROM d) SELECT * FROM (SELECT * FROM a, c) AS t1'
  end

  specify "should have working mutation method" do
    @ds.from_self!
    @ds.sql.should == 'SELECT * FROM (SELECT name FROM test LIMIT 1) AS t1'
  end
end

describe "Dataset#join_table" do
  before do
    @d = Sequel::Dataset.new(nil).from(:items)
    @d.quote_identifiers = true
  end
  
  specify "should format the JOIN clause properly" do
    @d.join_table(:left_outer, :categories, :category_id => :id).sql.should == 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."category_id" = "items"."id")'
  end
  
  specify "should handle multiple conditions on the same join table column" do
    @d.join_table(:left_outer, :categories, [[:category_id, :id], [:category_id, 0..100]]).sql.should == 
      'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON (("categories"."category_id" = "items"."id") AND ("categories"."category_id" >= 0) AND ("categories"."category_id" <= 100))'
  end
  
  specify "should include WHERE clause if applicable" do
    @d.filter(Sequel.expr(:price) < 100).join_table(:right_outer, :categories, :category_id => :id).sql.should ==
      'SELECT * FROM "items" RIGHT OUTER JOIN "categories" ON ("categories"."category_id" = "items"."id") WHERE ("price" < 100)'
  end
  
  specify "should include ORDER BY clause if applicable" do
    @d.order(:stamp).join_table(:full_outer, :categories, :category_id => :id).sql.should == 'SELECT * FROM "items" FULL OUTER JOIN "categories" ON ("categories"."category_id" = "items"."id") ORDER BY "stamp"'
  end
  
  specify "should support multiple joins" do
    @d.join_table(:inner, :b, :items_id=>:id).join_table(:left_outer, :c, :b_id => :b__id).sql.should == 'SELECT * FROM "items" INNER JOIN "b" ON ("b"."items_id" = "items"."id") LEFT OUTER JOIN "c" ON ("c"."b_id" = "b"."id")'
  end
  
  specify "should support arbitrary join types" do
    @d.join_table(:magic, :categories, :category_id=>:id).sql.should == 'SELECT * FROM "items" MAGIC JOIN "categories" ON ("categories"."category_id" = "items"."id")'
  end

  specify "should support many join methods" do
    @d.left_outer_join(:categories, :category_id=>:id).sql.should == 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."category_id" = "items"."id")'
    @d.right_outer_join(:categories, :category_id=>:id).sql.should == 'SELECT * FROM "items" RIGHT OUTER JOIN "categories" ON ("categories"."category_id" = "items"."id")'
    @d.full_outer_join(:categories, :category_id=>:id).sql.should == 'SELECT * FROM "items" FULL OUTER JOIN "categories" ON ("categories"."category_id" = "items"."id")'
    @d.inner_join(:categories, :category_id=>:id).sql.should == 'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."category_id" = "items"."id")'
    @d.left_join(:categories, :category_id=>:id).sql.should == 'SELECT * FROM "items" LEFT JOIN "categories" ON ("categories"."category_id" = "items"."id")'
    @d.right_join(:categories, :category_id=>:id).sql.should == 'SELECT * FROM "items" RIGHT JOIN "categories" ON ("categories"."category_id" = "items"."id")'
    @d.full_join(:categories, :category_id=>:id).sql.should == 'SELECT * FROM "items" FULL JOIN "categories" ON ("categories"."category_id" = "items"."id")'
    @d.natural_join(:categories).sql.should == 'SELECT * FROM "items" NATURAL JOIN "categories"'
    @d.natural_left_join(:categories).sql.should == 'SELECT * FROM "items" NATURAL LEFT JOIN "categories"'
    @d.natural_right_join(:categories).sql.should == 'SELECT * FROM "items" NATURAL RIGHT JOIN "categories"'
    @d.natural_full_join(:categories).sql.should == 'SELECT * FROM "items" NATURAL FULL JOIN "categories"'
    @d.cross_join(:categories).sql.should == 'SELECT * FROM "items" CROSS JOIN "categories"'
  end
  
  specify "should raise an error if additional arguments are provided to join methods that don't take conditions" do
    proc{@d.natural_join(:categories, :id=>:id)}.should raise_error(ArgumentError)
    proc{@d.natural_left_join(:categories, :id=>:id)}.should raise_error(ArgumentError)
    proc{@d.natural_right_join(:categories, :id=>:id)}.should raise_error(ArgumentError)
    proc{@d.natural_full_join(:categories, :id=>:id)}.should raise_error(ArgumentError)
    proc{@d.cross_join(:categories, :id=>:id)}.should raise_error(ArgumentError)
  end

  specify "should raise an error if blocks are provided to join methods that don't pass them" do
    proc{@d.natural_join(:categories){}}.should raise_error(Sequel::Error)
    proc{@d.natural_left_join(:categories){}}.should raise_error(Sequel::Error)
    proc{@d.natural_right_join(:categories){}}.should raise_error(Sequel::Error)
    proc{@d.natural_full_join(:categories){}}.should raise_error(Sequel::Error)
    proc{@d.cross_join(:categories){}}.should raise_error(Sequel::Error)
  end

  specify "should default to a plain join if nil is used for the type" do
    @d.join_table(nil, :categories, :category_id=>:id).sql.should == 'SELECT * FROM "items"  JOIN "categories" ON ("categories"."category_id" = "items"."id")'
  end

  specify "should use an inner join for Dataset#join" do
    @d.join(:categories, :category_id=>:id).sql.should == 'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."category_id" = "items"."id")'
  end
  
  specify "should support aliased tables using the deprecated argument" do
    @d.from('stats').join('players', {:id => :player_id}, 'p').sql.should == 'SELECT * FROM "stats" INNER JOIN "players" AS "p" ON ("p"."id" = "stats"."player_id")'
  end

  specify "should support aliased tables using the :table_alias option" do
    @d.from('stats').join('players', {:id => :player_id}, :table_alias=>:p).sql.should == 'SELECT * FROM "stats" INNER JOIN "players" AS "p" ON ("p"."id" = "stats"."player_id")'
  end
  
  specify "should support aliased tables using an implicit alias" do
    @d.from('stats').join(Sequel.expr(:players).as(:p), {:id => :player_id}).sql.should == 'SELECT * FROM "stats" INNER JOIN "players" AS "p" ON ("p"."id" = "stats"."player_id")'
  end
  
  specify "should support using an alias for the FROM when doing the first join with unqualified condition columns" do
    @d.from(:foo=>:f).join_table(:inner, :bar, :id => :bar_id).sql.should == 'SELECT * FROM "foo" AS "f" INNER JOIN "bar" ON ("bar"."id" = "f"."bar_id")'
  end
  
  specify "should support implicit schemas in from table symbols" do
    @d.from(:s__t).join(:u__v, {:id => :player_id}).sql.should == 'SELECT * FROM "s"."t" INNER JOIN "u"."v" ON ("u"."v"."id" = "s"."t"."player_id")'
  end

  specify "should support implicit aliases in from table symbols" do
    @d.from(:t___z).join(:v___y, {:id => :player_id}).sql.should == 'SELECT * FROM "t" AS "z" INNER JOIN "v" AS "y" ON ("y"."id" = "z"."player_id")'
    @d.from(:s__t___z).join(:u__v___y, {:id => :player_id}).sql.should == 'SELECT * FROM "s"."t" AS "z" INNER JOIN "u"."v" AS "y" ON ("y"."id" = "z"."player_id")'
  end
  
  specify "should support AliasedExpressions" do
    @d.from(Sequel.expr(:s).as(:t)).join(Sequel.expr(:u).as(:v), {:id => :player_id}).sql.should == 'SELECT * FROM "s" AS "t" INNER JOIN "u" AS "v" ON ("v"."id" = "t"."player_id")'
  end

  specify "should support the :implicit_qualifier option" do
    @d.from('stats').join('players', {:id => :player_id}, :implicit_qualifier=>:p).sql.should == 'SELECT * FROM "stats" INNER JOIN "players" ON ("players"."id" = "p"."player_id")'
  end
  
  specify "should not qualify if :qualify=>false option is given" do
    @d.from('stats').join(:players, {:id => :player_id}, :qualify=>false).sql.should == 'SELECT * FROM "stats" INNER JOIN "players" ON ("id" = "player_id")'
  end
  
  specify "should do deep qualification if :qualify=>:deep option is given" do
    @d.from('stats').join(:players, {Sequel.function(:f, :id) => Sequel.subscript(:player_id, 0)}, :qualify=>:deep).sql.should == 'SELECT * FROM "stats" INNER JOIN "players" ON (f("players"."id") = "stats"."player_id"[0])'
  end
  
  specify "should allow for arbitrary conditions in the JOIN clause" do
    @d.join_table(:left_outer, :categories, :status => 0).sql.should == 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."status" = 0)'
    @d.join_table(:left_outer, :categories, :categorizable_type => "Post").sql.should == 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."categorizable_type" = \'Post\')'
    @d.join_table(:left_outer, :categories, :timestamp => Sequel::CURRENT_TIMESTAMP).sql.should == 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."timestamp" = CURRENT_TIMESTAMP)'
    @d.join_table(:left_outer, :categories, :status => [1, 2, 3]).sql.should == 'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."status" IN (1, 2, 3))'
  end
  
  specify "should raise error for a table without a source" do
    proc {Sequel::Dataset.new(nil).join('players', :id => :player_id)}.should raise_error(Sequel::Error)
  end

  specify "should support joining datasets" do
    ds = Sequel::Dataset.new(nil).from(:categories)
    @d.join_table(:left_outer, ds, :item_id => :id).sql.should == 'SELECT * FROM "items" LEFT OUTER JOIN (SELECT * FROM categories) AS "t1" ON ("t1"."item_id" = "items"."id")'
    ds.filter!(:active => true)
    @d.join_table(:left_outer, ds, :item_id => :id).sql.should == 'SELECT * FROM "items" LEFT OUTER JOIN (SELECT * FROM categories WHERE (active IS TRUE)) AS "t1" ON ("t1"."item_id" = "items"."id")'
    @d.from_self.join_table(:left_outer, ds, :item_id => :id).sql.should == 'SELECT * FROM (SELECT * FROM "items") AS "t1" LEFT OUTER JOIN (SELECT * FROM categories WHERE (active IS TRUE)) AS "t2" ON ("t2"."item_id" = "t1"."id")'
  end
  
  specify "should support joining datasets and aliasing the join" do
    ds = Sequel::Dataset.new(nil).from(:categories)
    @d.join_table(:left_outer, ds, {:ds__item_id => :id}, :ds).sql.should == 'SELECT * FROM "items" LEFT OUTER JOIN (SELECT * FROM categories) AS "ds" ON ("ds"."item_id" = "items"."id")'      
  end
  
  specify "should support joining multiple datasets" do
    ds = Sequel::Dataset.new(nil).from(:categories)
    ds2 = Sequel::Dataset.new(nil).from(:nodes).select(:name)
    ds3 = Sequel::Dataset.new(nil).from(:attributes).filter("name = 'blah'")

    @d.join_table(:left_outer, ds, :item_id => :id).join_table(:inner, ds2, :node_id=>:id).join_table(:right_outer, ds3, :attribute_id=>:id).sql.should ==
      'SELECT * FROM "items" LEFT OUTER JOIN (SELECT * FROM categories) AS "t1" ON ("t1"."item_id" = "items"."id") ' \
      'INNER JOIN (SELECT name FROM nodes) AS "t2" ON ("t2"."node_id" = "t1"."id") ' \
      'RIGHT OUTER JOIN (SELECT * FROM attributes WHERE (name = \'blah\')) AS "t3" ON ("t3"."attribute_id" = "t2"."id")'
  end

  specify "should support using an SQL String as the join condition" do
    @d.join(:categories, "c.item_id = items.id", :c).sql.should == 'SELECT * FROM "items" INNER JOIN "categories" AS "c" ON (c.item_id = items.id)'
  end
  
  specify "should support using a boolean column as the join condition" do
    @d.join(:categories, :active).sql.should == 'SELECT * FROM "items" INNER JOIN "categories" ON "active"'
  end

  specify "should support using an expression as the join condition" do
    @d.join(:categories, Sequel.expr(:number) > 10).sql.should == 'SELECT * FROM "items" INNER JOIN "categories" ON ("number" > 10)'
  end

  specify "should support natural and cross joins" do
    @d.join_table(:natural, :categories).sql.should == 'SELECT * FROM "items" NATURAL JOIN "categories"'
    @d.join_table(:cross, :categories, nil).sql.should == 'SELECT * FROM "items" CROSS JOIN "categories"'
    @d.join_table(:natural, :categories, nil, :c).sql.should == 'SELECT * FROM "items" NATURAL JOIN "categories" AS "c"'
  end

  specify "should support joins with a USING clause if an array of symbols is used" do
    @d.join(:categories, [:id]).sql.should == 'SELECT * FROM "items" INNER JOIN "categories" USING ("id")'
    @d.join(:categories, [:id1, :id2]).sql.should == 'SELECT * FROM "items" INNER JOIN "categories" USING ("id1", "id2")'
  end

  specify "should emulate JOIN USING (poorly) if the dataset doesn't support it" do
    @d.meta_def(:supports_join_using?){false}
    @d.join(:categories, [:id]).sql.should == 'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."id" = "items"."id")'
  end

  specify "should hoist WITH clauses from subqueries if the dataset doesn't support CTEs in subselects" do
    @d.meta_def(:supports_cte?){true}
    @d.meta_def(:supports_cte_in_subselect?){false}
    @d.join(Sequel::Dataset.new(nil).from(:categories).with(:a, Sequel::Dataset.new(nil).from(:b)), [:id]).sql.should == 'WITH "a" AS (SELECT * FROM b) SELECT * FROM "items" INNER JOIN (SELECT * FROM categories) AS "t1" USING ("id")'
  end

  specify "should raise an error if using an array of symbols with a block" do
    proc{@d.join(:categories, [:id]){|j,lj,js|}}.should raise_error(Sequel::Error)
  end

  specify "should support using a block that receieves the join table/alias, last join table/alias, and array of previous joins" do
    @d.join(:categories) do |join_alias, last_join_alias, joins| 
      join_alias.should == :categories
      last_join_alias.should == :items
      joins.should == []
    end

    @d.from(:items=>:i).join(:categories, nil, :c) do |join_alias, last_join_alias, joins| 
      join_alias.should == :c
      last_join_alias.should == :i
      joins.should == []
    end

    @d.from(:items___i).join(:categories, nil, :c) do |join_alias, last_join_alias, joins| 
      join_alias.should == :c
      last_join_alias.should == :i
      joins.should == []
    end

    @d.join(:blah).join(:categories, nil, :c) do |join_alias, last_join_alias, joins| 
      join_alias.should == :c
      last_join_alias.should == :blah
      joins.should be_a_kind_of(Array)
      joins.length.should == 1
      joins.first.should be_a_kind_of(Sequel::SQL::JoinClause)
      joins.first.join_type.should == :inner
    end

    @d.join_table(:natural, :blah, nil, :b).join(:categories, nil, :c) do |join_alias, last_join_alias, joins| 
      join_alias.should == :c
      last_join_alias.should == :b
      joins.should be_a_kind_of(Array)
      joins.length.should == 1
      joins.first.should be_a_kind_of(Sequel::SQL::JoinClause)
      joins.first.join_type.should == :natural
    end

    @d.join(:blah).join(:categories).join(:blah2) do |join_alias, last_join_alias, joins| 
      join_alias.should == :blah2
      last_join_alias.should == :categories
      joins.should be_a_kind_of(Array)
      joins.length.should == 2
      joins.first.should be_a_kind_of(Sequel::SQL::JoinClause)
      joins.first.table.should == :blah
      joins.last.should be_a_kind_of(Sequel::SQL::JoinClause)
      joins.last.table.should == :categories
    end
  end

  specify "should use the block result as the only condition if no condition is given" do
    @d.join(:categories){|j,lj,js| {Sequel.qualify(j, :b)=>Sequel.qualify(lj, :c)}}.sql.should == 'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."b" = "items"."c")'
    @d.join(:categories){|j,lj,js| Sequel.qualify(j, :b) > Sequel.qualify(lj, :c)}.sql.should == 'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."b" > "items"."c")'
  end

  specify "should combine the block conditions and argument conditions if both given" do
    @d.join(:categories, :a=>:d){|j,lj,js| {Sequel.qualify(j, :b)=>Sequel.qualify(lj, :c)}}.sql.should == 'SELECT * FROM "items" INNER JOIN "categories" ON (("categories"."a" = "items"."d") AND ("categories"."b" = "items"."c"))'
    @d.join(:categories, :a=>:d){|j,lj,js| Sequel.qualify(j, :b) > Sequel.qualify(lj, :c)}.sql.should == 'SELECT * FROM "items" INNER JOIN "categories" ON (("categories"."a" = "items"."d") AND ("categories"."b" > "items"."c"))'
  end

  specify "should prefer explicit aliases over implicit" do
    @d.from(:items___i).join(:categories___c, {:category_id => :id}, {:table_alias=>:c2, :implicit_qualifier=>:i2}).sql.should == 'SELECT * FROM "items" AS "i" INNER JOIN "categories" AS "c2" ON ("c2"."category_id" = "i2"."id")'
    @d.from(Sequel.expr(:items).as(:i)).join(Sequel.expr(:categories).as(:c), {:category_id => :id}, {:table_alias=>:c2, :implicit_qualifier=>:i2}).sql.should ==
      'SELECT * FROM "items" AS "i" INNER JOIN "categories" AS "c2" ON ("c2"."category_id" = "i2"."id")'
  end
  
  specify "should not allow insert, update, delete, or truncate" do
    proc{@d.join(:categories, :a=>:d).insert_sql}.should raise_error(Sequel::InvalidOperation)
    proc{@d.join(:categories, :a=>:d).update_sql(:a=>1)}.should raise_error(Sequel::InvalidOperation)
    proc{@d.join(:categories, :a=>:d).delete_sql}.should raise_error(Sequel::InvalidOperation)
    proc{@d.join(:categories, :a=>:d).truncate_sql}.should raise_error(Sequel::InvalidOperation)
  end

  specify "should raise an error if an invalid option is passed" do
    proc{@d.join(:c, [:id], nil)}.should raise_error(Sequel::Error)
    proc{@d.join(:c, [:id], Sequel.qualify(:d, :c))}.should raise_error(Sequel::Error)
  end
end

describe "Dataset#[]=" do
  specify "should perform an update on the specified filter" do
    db = Sequel.mock
    ds = db[:items]
    ds[:a => 1] = {:x => 3}
    db.sqls.should == ['UPDATE items SET x = 3 WHERE (a = 1)']
  end
end

describe "Dataset#set" do
  specify "should act as alias to #update" do
    db = Sequel.mock
    ds = db[:items]
    ds.set({:x => 3})
    db.sqls.should == ['UPDATE items SET x = 3']
  end
end

describe "Dataset#insert_multiple" do
  before do
    @db = Sequel.mock(:autoid=>2)
    @ds = @db[:items]
  end
  
  specify "should insert all items in the supplied array" do
    @ds.insert_multiple(['aa', 5, 3, {:a => 2}])
    @db.sqls.should == ["INSERT INTO items VALUES ('aa')",
      "INSERT INTO items VALUES (5)",
      "INSERT INTO items VALUES (3)",
      "INSERT INTO items (a) VALUES (2)"]
  end
  
  specify "should pass array items through the supplied block if given" do
    @ds.insert_multiple(["inevitable", "hello", "the ticking clock"]){|i| i.gsub('l', 'r')}
    @db.sqls.should == ["INSERT INTO items VALUES ('inevitabre')",
      "INSERT INTO items VALUES ('herro')",
      "INSERT INTO items VALUES ('the ticking crock')"]
  end

  specify "should return array of inserted ids" do
    @ds.insert_multiple(['aa', 5, 3, {:a => 2}]).should == [2, 3, 4, 5]
  end

  specify "should work exactly like in metioned in the example" do
    @ds.insert_multiple([{:x=>1}, {:x=>2}]){|row| row[:y] = row[:x] * 2 ; row }
    sqls = @db.sqls
    ["INSERT INTO items (x, y) VALUES (1, 2)", "INSERT INTO items (y, x) VALUES (2, 1)"].should include(sqls[0])
    ["INSERT INTO items (x, y) VALUES (2, 4)", "INSERT INTO items (y, x) VALUES (4, 2)"].should include(sqls[1])
  end
end

describe "Dataset aggregate methods" do
  before do
    @d = Sequel.mock(:fetch=>proc{|s| {1=>s}})[:test]
  end
  
  specify "should include min" do
    @d.min(:a).should == 'SELECT min(a) FROM test LIMIT 1'
  end
  
  specify "should include max" do
    @d.max(:b).should == 'SELECT max(b) FROM test LIMIT 1'
  end
  
  specify "should include sum" do
    @d.sum(:c).should == 'SELECT sum(c) FROM test LIMIT 1'
  end
  
  specify "should include avg" do
    @d.avg(:d).should == 'SELECT avg(d) FROM test LIMIT 1'
  end
  
  specify "should accept qualified columns" do
    @d.avg(:test__bc).should == 'SELECT avg(test.bc) FROM test LIMIT 1'
  end
  
  specify "should use a subselect for the same conditions as count" do
    d = @d.order(:a).limit(5)
    d.avg(:a).should == 'SELECT avg(a) FROM (SELECT * FROM test ORDER BY a LIMIT 5) AS t1 LIMIT 1'
    d.sum(:a).should == 'SELECT sum(a) FROM (SELECT * FROM test ORDER BY a LIMIT 5) AS t1 LIMIT 1'
    d.min(:a).should == 'SELECT min(a) FROM (SELECT * FROM test ORDER BY a LIMIT 5) AS t1 LIMIT 1'
    d.max(:a).should == 'SELECT max(a) FROM (SELECT * FROM test ORDER BY a LIMIT 5) AS t1 LIMIT 1'
  end
  
  specify "should accept virtual row blocks" do
    @d.avg{a(b)}.should == 'SELECT avg(a(b)) FROM test LIMIT 1'
    @d.sum{a(b)}.should == 'SELECT sum(a(b)) FROM test LIMIT 1'
    @d.min{a(b)}.should == 'SELECT min(a(b)) FROM test LIMIT 1'
    @d.max{a(b)}.should == 'SELECT max(a(b)) FROM test LIMIT 1'
  end
end

describe "Dataset#range" do
  before do
    @db = Sequel.mock(:fetch=>{:v1 => 1, :v2 => 10})
    @ds = @db[:test]
  end
  
  specify "should generate a correct SQL statement" do
    @ds.range(:stamp)
    @db.sqls.should == ["SELECT min(stamp) AS v1, max(stamp) AS v2 FROM test LIMIT 1"]

    @ds.filter(Sequel.expr(:price) > 100).range(:stamp)
    @db.sqls.should == ["SELECT min(stamp) AS v1, max(stamp) AS v2 FROM test WHERE (price > 100) LIMIT 1"]
  end
  
  specify "should return a range object" do
    @ds.range(:tryme).should == (1..10)
  end
  
  specify "should use a subselect for the same conditions as count" do
    @ds.order(:stamp).limit(5).range(:stamp).should == (1..10)
    @db.sqls.should == ['SELECT min(stamp) AS v1, max(stamp) AS v2 FROM (SELECT * FROM test ORDER BY stamp LIMIT 5) AS t1 LIMIT 1']
  end
  
  specify "should accept virtual row blocks" do
    @ds.range{a(b)}
    @db.sqls.should == ["SELECT min(a(b)) AS v1, max(a(b)) AS v2 FROM test LIMIT 1"]
  end
end

describe "Dataset#interval" do
  before do
    @db = Sequel.mock(:fetch=>{:v => 1234})
    @ds = @db[:test]
  end
  
  specify "should generate the correct SQL statement" do
    @ds.interval(:stamp)
    @db.sqls.should == ["SELECT (max(stamp) - min(stamp)) FROM test LIMIT 1"]

    @ds.filter(Sequel.expr(:price) > 100).interval(:stamp)
    @db.sqls.should == ["SELECT (max(stamp) - min(stamp)) FROM test WHERE (price > 100) LIMIT 1"]
  end
  
  specify "should use a subselect for the same conditions as count" do
    @ds.order(:stamp).limit(5).interval(:stamp).should == 1234
    @db.sqls.should == ['SELECT (max(stamp) - min(stamp)) FROM (SELECT * FROM test ORDER BY stamp LIMIT 5) AS t1 LIMIT 1']
  end

  specify "should accept virtual row blocks" do
    @ds.interval{a(b)}
    @db.sqls.should == ["SELECT (max(a(b)) - min(a(b))) FROM test LIMIT 1"]
  end
end

describe "Dataset #first and #last" do
  before do
    @db = Sequel.mock(:fetch=>proc{|s| {:s=>s}})
    @d = @db[:test]
  end
  
  specify "should return a single record if no argument is given" do
    @d.order(:a).first.should == {:s=>'SELECT * FROM test ORDER BY a LIMIT 1'}
    @d.order(:a).last.should == {:s=>'SELECT * FROM test ORDER BY a DESC LIMIT 1'}
  end

  specify "should return the first/last matching record if argument is not an Integer" do
    @d.order(:a).first(:z => 26).should == {:s=>'SELECT * FROM test WHERE (z = 26) ORDER BY a LIMIT 1'}
    @d.order(:a).first('z = ?', 15).should == {:s=>'SELECT * FROM test WHERE (z = 15) ORDER BY a LIMIT 1'}
    @d.order(:a).last(:z => 26).should == {:s=>'SELECT * FROM test WHERE (z = 26) ORDER BY a DESC LIMIT 1'}
    @d.order(:a).last('z = ?', 15).should == {:s=>'SELECT * FROM test WHERE (z = 15) ORDER BY a DESC LIMIT 1'}
  end
  
  specify "should set the limit and return an array of records if the given number is > 1" do
    i = rand(10) + 10
    r = @d.order(:a).first(i).should == [{:s=>"SELECT * FROM test ORDER BY a LIMIT #{i}"}]
    i = rand(10) + 10
    r = @d.order(:a).last(i).should == [{:s=>"SELECT * FROM test ORDER BY a DESC LIMIT #{i}"}]
  end
  
  specify "should return the first matching record if a block is given without an argument" do
    @d.first{z > 26}.should == {:s=>'SELECT * FROM test WHERE (z > 26) LIMIT 1'}
    @d.order(:name).last{z > 26}.should == {:s=>'SELECT * FROM test WHERE (z > 26) ORDER BY name DESC LIMIT 1'}
  end
  
  specify "should combine block and standard argument filters if argument is not an Integer" do
    @d.first(:y=>25){z > 26}.should == {:s=>'SELECT * FROM test WHERE ((z > 26) AND (y = 25)) LIMIT 1'}
    @d.order(:name).last('y = ?', 16){z > 26}.should == {:s=>'SELECT * FROM test WHERE ((z > 26) AND (y = 16)) ORDER BY name DESC LIMIT 1'}
  end
  
  specify "should filter and return an array of records if an Integer argument is provided and a block is given" do
    i = rand(10) + 10
    r = @d.order(:a).first(i){z > 26}.should == [{:s=>"SELECT * FROM test WHERE (z > 26) ORDER BY a LIMIT #{i}"}]
    i = rand(10) + 10
    r = @d.order(:a).last(i){z > 26}.should == [{:s=>"SELECT * FROM test WHERE (z > 26) ORDER BY a DESC LIMIT #{i}"}]
  end
  
  specify "#last should raise if no order is given" do
    proc {@d.last}.should raise_error(Sequel::Error)
    proc {@d.last(2)}.should raise_error(Sequel::Error)
    proc {@d.order(:a).last}.should_not raise_error
    proc {@d.order(:a).last(2)}.should_not raise_error
  end
  
  specify "#last should invert the order" do
    @d.order(:a).last.should == {:s=>'SELECT * FROM test ORDER BY a DESC LIMIT 1'}
    @d.order(Sequel.desc(:b)).last.should == {:s=>'SELECT * FROM test ORDER BY b ASC LIMIT 1'}
    @d.order(:c, :d).last.should == {:s=>'SELECT * FROM test ORDER BY c DESC, d DESC LIMIT 1'}
    @d.order(Sequel.desc(:e), :f).last.should == {:s=>'SELECT * FROM test ORDER BY e ASC, f DESC LIMIT 1'}
  end
end

describe "Dataset compound operations" do
  before do
    @a = Sequel::Dataset.new(nil).from(:a).filter(:z => 1)
    @b = Sequel::Dataset.new(nil).from(:b).filter(:z => 2)
  end
  
  specify "should support UNION and UNION ALL" do
    @a.union(@b).sql.should == "SELECT * FROM (SELECT * FROM a WHERE (z = 1) UNION SELECT * FROM b WHERE (z = 2)) AS t1"
    @b.union(@a, true).sql.should == "SELECT * FROM (SELECT * FROM b WHERE (z = 2) UNION ALL SELECT * FROM a WHERE (z = 1)) AS t1"
    @b.union(@a, :all=>true).sql.should == "SELECT * FROM (SELECT * FROM b WHERE (z = 2) UNION ALL SELECT * FROM a WHERE (z = 1)) AS t1"
  end

  specify "should support INTERSECT and INTERSECT ALL" do
    @a.intersect(@b).sql.should == "SELECT * FROM (SELECT * FROM a WHERE (z = 1) INTERSECT SELECT * FROM b WHERE (z = 2)) AS t1"
    @b.intersect(@a, true).sql.should == "SELECT * FROM (SELECT * FROM b WHERE (z = 2) INTERSECT ALL SELECT * FROM a WHERE (z = 1)) AS t1"
    @b.intersect(@a, :all=>true).sql.should == "SELECT * FROM (SELECT * FROM b WHERE (z = 2) INTERSECT ALL SELECT * FROM a WHERE (z = 1)) AS t1"
  end

  specify "should support EXCEPT and EXCEPT ALL" do
    @a.except(@b).sql.should == "SELECT * FROM (SELECT * FROM a WHERE (z = 1) EXCEPT SELECT * FROM b WHERE (z = 2)) AS t1"
    @b.except(@a, true).sql.should == "SELECT * FROM (SELECT * FROM b WHERE (z = 2) EXCEPT ALL SELECT * FROM a WHERE (z = 1)) AS t1"
    @b.except(@a, :all=>true).sql.should == "SELECT * FROM (SELECT * FROM b WHERE (z = 2) EXCEPT ALL SELECT * FROM a WHERE (z = 1)) AS t1"
  end
    
  specify "should support :alias option for specifying identifier" do
    @a.union(@b, :alias=>:xx).sql.should == "SELECT * FROM (SELECT * FROM a WHERE (z = 1) UNION SELECT * FROM b WHERE (z = 2)) AS xx"
    @a.intersect(@b, :alias=>:xx).sql.should == "SELECT * FROM (SELECT * FROM a WHERE (z = 1) INTERSECT SELECT * FROM b WHERE (z = 2)) AS xx"
    @a.except(@b, :alias=>:xx).sql.should == "SELECT * FROM (SELECT * FROM a WHERE (z = 1) EXCEPT SELECT * FROM b WHERE (z = 2)) AS xx"
  end

  specify "should support :from_self=>false option to not wrap the compound in a SELECT * FROM (...)" do
    @b.union(@a, :from_self=>false).sql.should == "SELECT * FROM b WHERE (z = 2) UNION SELECT * FROM a WHERE (z = 1)"
    @b.intersect(@a, :from_self=>false).sql.should == "SELECT * FROM b WHERE (z = 2) INTERSECT SELECT * FROM a WHERE (z = 1)"
    @b.except(@a, :from_self=>false).sql.should == "SELECT * FROM b WHERE (z = 2) EXCEPT SELECT * FROM a WHERE (z = 1)"
      
    @b.union(@a, :from_self=>false, :all=>true).sql.should == "SELECT * FROM b WHERE (z = 2) UNION ALL SELECT * FROM a WHERE (z = 1)"
    @b.intersect(@a, :from_self=>false, :all=>true).sql.should == "SELECT * FROM b WHERE (z = 2) INTERSECT ALL SELECT * FROM a WHERE (z = 1)"
    @b.except(@a, :from_self=>false, :all=>true).sql.should == "SELECT * FROM b WHERE (z = 2) EXCEPT ALL SELECT * FROM a WHERE (z = 1)"
  end

  specify "should raise an InvalidOperation if INTERSECT or EXCEPT is used and they are not supported" do
    @a.meta_def(:supports_intersect_except?){false}
    proc{@a.intersect(@b)}.should raise_error(Sequel::InvalidOperation)
    proc{@a.intersect(@b, true)}.should raise_error(Sequel::InvalidOperation)
    proc{@a.except(@b)}.should raise_error(Sequel::InvalidOperation)
    proc{@a.except(@b, true)}.should raise_error(Sequel::InvalidOperation)
  end
    
  specify "should raise an InvalidOperation if INTERSECT ALL or EXCEPT ALL is used and they are not supported" do
    @a.meta_def(:supports_intersect_except_all?){false}
    proc{@a.intersect(@b)}.should_not raise_error
    proc{@a.intersect(@b, true)}.should raise_error(Sequel::InvalidOperation)
    proc{@a.except(@b)}.should_not raise_error
    proc{@a.except(@b, true)}.should raise_error(Sequel::InvalidOperation)
  end
    
  specify "should handle chained compound operations" do
    @a.union(@b).union(@a, true).sql.should == "SELECT * FROM (SELECT * FROM (SELECT * FROM a WHERE (z = 1) UNION SELECT * FROM b WHERE (z = 2)) AS t1 UNION ALL SELECT * FROM a WHERE (z = 1)) AS t1"
    @a.intersect(@b, true).intersect(@a).sql.should == "SELECT * FROM (SELECT * FROM (SELECT * FROM a WHERE (z = 1) INTERSECT ALL SELECT * FROM b WHERE (z = 2)) AS t1 INTERSECT SELECT * FROM a WHERE (z = 1)) AS t1"
    @a.except(@b).except(@a, true).sql.should == "SELECT * FROM (SELECT * FROM (SELECT * FROM a WHERE (z = 1) EXCEPT SELECT * FROM b WHERE (z = 2)) AS t1 EXCEPT ALL SELECT * FROM a WHERE (z = 1)) AS t1"
  end
  
  specify "should use a subselect when using a compound operation with a dataset that already has a compound operation" do
    @a.union(@b.union(@a, true)).sql.should == "SELECT * FROM (SELECT * FROM a WHERE (z = 1) UNION SELECT * FROM (SELECT * FROM b WHERE (z = 2) UNION ALL SELECT * FROM a WHERE (z = 1)) AS t1) AS t1"
    @a.intersect(@b.intersect(@a), true).sql.should == "SELECT * FROM (SELECT * FROM a WHERE (z = 1) INTERSECT ALL SELECT * FROM (SELECT * FROM b WHERE (z = 2) INTERSECT SELECT * FROM a WHERE (z = 1)) AS t1) AS t1"
    @a.except(@b.except(@a, true)).sql.should == "SELECT * FROM (SELECT * FROM a WHERE (z = 1) EXCEPT SELECT * FROM (SELECT * FROM b WHERE (z = 2) EXCEPT ALL SELECT * FROM a WHERE (z = 1)) AS t1) AS t1"
  end

  specify "should order and limit properly when using UNION, INTERSECT, or EXCEPT" do
    @dataset = Sequel::Dataset.new(nil).from(:test)
    @dataset.union(@dataset).limit(2).sql.should == "SELECT * FROM (SELECT * FROM test UNION SELECT * FROM test) AS t1 LIMIT 2"
    @dataset.limit(2).intersect(@dataset).sql.should == "SELECT * FROM (SELECT * FROM (SELECT * FROM test LIMIT 2) AS t1 INTERSECT SELECT * FROM test) AS t1"
    @dataset.except(@dataset.limit(2)).sql.should == "SELECT * FROM (SELECT * FROM test EXCEPT SELECT * FROM (SELECT * FROM test LIMIT 2) AS t1) AS t1"

    @dataset.union(@dataset).order(:num).sql.should == "SELECT * FROM (SELECT * FROM test UNION SELECT * FROM test) AS t1 ORDER BY num"
    @dataset.order(:num).intersect(@dataset).sql.should == "SELECT * FROM (SELECT * FROM (SELECT * FROM test ORDER BY num) AS t1 INTERSECT SELECT * FROM test) AS t1"
    @dataset.except(@dataset.order(:num)).sql.should == "SELECT * FROM (SELECT * FROM test EXCEPT SELECT * FROM (SELECT * FROM test ORDER BY num) AS t1) AS t1"

    @dataset.limit(2).order(:a).union(@dataset.limit(3).order(:b)).order(:c).limit(4).sql.should ==
      "SELECT * FROM (SELECT * FROM (SELECT * FROM test ORDER BY a LIMIT 2) AS t1 UNION SELECT * FROM (SELECT * FROM test ORDER BY b LIMIT 3) AS t1) AS t1 ORDER BY c LIMIT 4"
  end

  specify "should hoist WITH clauses in given dataset if dataset doesn't support WITH in subselect" do
    ds = Sequel::Dataset.new(nil)
    ds.meta_def(:supports_cte?){true}
    ds.meta_def(:supports_cte_in_subselect?){false}
    ds.from(:a).union(ds.from(:c).with(:c, ds.from(:d)), :from_self=>false).sql.should == 'WITH c AS (SELECT * FROM d) SELECT * FROM a UNION SELECT * FROM c'
    ds.from(:a).except(ds.from(:c).with(:c, ds.from(:d))).sql.should == 'WITH c AS (SELECT * FROM d) SELECT * FROM (SELECT * FROM a EXCEPT SELECT * FROM c) AS t1'
    ds.from(:a).with(:a, ds.from(:b)).intersect(ds.from(:c).with(:c, ds.from(:d)), :from_self=>false).sql.should == 'WITH a AS (SELECT * FROM b), c AS (SELECT * FROM d) SELECT * FROM a INTERSECT SELECT * FROM c'
  end
end

describe "Dataset#[]" do
  before do
    @db = Sequel.mock(:fetch=>{1 => 2, 3 => 4})
    @d = @db[:items]
  end
  
  specify "should return a single record filtered according to the given conditions" do
    @d[:name => 'didi'].should == {1 => 2, 3 => 4}
    @db.sqls.should == ["SELECT * FROM items WHERE (name = 'didi') LIMIT 1"]

    @d[:id => 5..45].should == {1 => 2, 3 => 4}
    @db.sqls.should == ["SELECT * FROM items WHERE ((id >= 5) AND (id <= 45)) LIMIT 1"]
  end
end

describe "Dataset#single_record" do
  before do
    @db = Sequel.mock
  end
  
  specify "should call each with a limit of 1 and return the record" do
    @db.fetch = {:a=>1}
    @db[:test].single_record.should == {:a=>1}
    @db.sqls.should == ['SELECT * FROM test LIMIT 1']
  end
  
  specify "should return nil if no record is present" do
    @db[:test].single_record.should be_nil
    @db.sqls.should == ['SELECT * FROM test LIMIT 1']
  end
end

describe "Dataset#single_value" do
  before do
    @db = Sequel.mock
  end
  
  specify "should call each and return the first value of the first record" do
    @db.fetch = {:a=>1}
    @db[:test].single_value.should == 1
    @db.sqls.should == ['SELECT * FROM test LIMIT 1']
  end
  
  specify "should return nil if no records" do
    @db[:test].single_value.should be_nil
    @db.sqls.should == ['SELECT * FROM test LIMIT 1']
  end
  
  it "should work on a graphed_dataset" do
    @db.fetch = {:a=>1}
    ds = @db[:test].columns(:a)
    ds.graph(ds, [:a], :table_alias=>:test2).single_value.should == 1
    @db.sqls.should == ['SELECT test.a, test2.a AS test2_a FROM test LEFT OUTER JOIN test AS test2 USING (a) LIMIT 1']
  end
end

describe "Dataset#get" do
  before do
    @d = Sequel.mock(:fetch=>proc{|s| {:name=>s}})[:test]
  end
  
  specify "should select the specified column and fetch its value" do
    @d.get(:name).should == "SELECT name FROM test LIMIT 1"
    @d.get(:abc).should == "SELECT abc FROM test LIMIT 1"
  end
  
  specify "should work with filters" do
    @d.filter(:id => 1).get(:name).should == "SELECT name FROM test WHERE (id = 1) LIMIT 1"
  end
  
  specify "should work with aliased fields" do
    @d.get(Sequel.expr(:x__b).as(:name)).should == "SELECT x.b AS name FROM test LIMIT 1"
  end
  
  specify "should accept a block that yields a virtual row" do
    @d.get{|o| o.x__b.as(:name)}.should == "SELECT x.b AS name FROM test LIMIT 1"
    @d.get{x(1).as(:name)}.should == "SELECT x(1) AS name FROM test LIMIT 1"
  end
  
  specify "should raise an error if both a regular argument and block argument are used" do
    proc{@d.get(:name){|o| o.x__b.as(:name)}}.should raise_error(Sequel::Error)
  end
  
  specify "should support false and nil values" do
    @d.get(false).should == "SELECT 'f' FROM test LIMIT 1"
    @d.get(nil).should == "SELECT NULL FROM test LIMIT 1"
  end

  specify "should support an array of expressions to get an array of results" do
    @d._fetch = {:name=>1, :abc=>2}
    @d.get([:name, :abc]).should == [1, 2]
    @d.db.sqls.should == ['SELECT name, abc FROM test LIMIT 1']
  end
  
  specify "should support an array with a single expression" do
    @d.get([:name]).should == ['SELECT name FROM test LIMIT 1']
  end
  
  specify "should handle an array with aliases" do
    @d._fetch = {:name=>1, :abc=>2}
    @d.get([:n___name, Sequel.as(:a, :abc)]).should == [1, 2]
    @d.db.sqls.should == ['SELECT n AS name, a AS abc FROM test LIMIT 1']
  end
  
  specify "should raise an Error if an alias cannot be determined" do
    @d._fetch = {:name=>1, :abc=>2}
    proc{@d.get([Sequel.+(:a, 1), :a])}.should raise_error(Sequel::Error)
  end
  
  specify "should support an array of expressions in a virtual row" do
    @d._fetch = {:name=>1, :abc=>2}
    @d.get{[name, n__abc]}.should == [1, 2]
    @d.db.sqls.should == ['SELECT name, n.abc FROM test LIMIT 1']
  end
  
  specify "should work with static SQL" do
    @d.with_sql('SELECT foo').get(:name).should == "SELECT foo"
    @d._fetch = {:name=>1, :abc=>2}
    @d.with_sql('SELECT foo').get{[name, n__abc]}.should == [1, 2]
    @d.db.sqls.should == ['SELECT foo'] * 2
  end
end

describe "Dataset#set_row_proc" do
  before do
    @db = Sequel.mock(:fetch=>[{:a=>1}, {:a=>2}])
    @dataset = @db[:items]
    @dataset.row_proc = proc{|h| h[:der] = h[:a] + 2; h}
  end
  
  specify "should cause dataset to pass all rows through the filter" do
    rows = @dataset.all
    rows.map{|h| h[:der]}.should == [3, 4]
    @db.sqls.should == ['SELECT * FROM items']
  end
  
  specify "should be copied over when dataset is cloned" do
    @dataset.filter(:a => 1).all.should == [{:a=>1, :der=>3}, {:a=>2, :der=>4}]
  end
end

describe "Dataset#<<" do
  before do
    @db = Sequel.mock
  end

  specify "should call #insert" do
    @db[:items] << {:name => 1}
    @db.sqls.should == ['INSERT INTO items (name) VALUES (1)']
  end

  specify "should be chainable" do
    @db[:items] << {:name => 1} << @db[:old_items].select(:name)
    @db.sqls.should == ['INSERT INTO items (name) VALUES (1)', 'INSERT INTO items SELECT name FROM old_items']
  end
end

describe "Dataset#columns" do
  before do
    @dataset = Sequel.mock[:items]
  end
  
  specify "should return the value of @columns if @columns is not nil" do
    @dataset.columns(:a, :b, :c).columns.should == [:a, :b, :c]
    @dataset.db.sqls.should == []
  end
  
  specify "should attempt to get a single record and return @columns if @columns is nil" do
    @dataset.db.columns = [:a]
    @dataset.columns.should == [:a]
    @dataset.db.sqls.should == ['SELECT * FROM items LIMIT 1']
  end
  
  specify "should be cleared if you change the selected columns" do
    @dataset.db.columns = [[:a], [:b]]
    @dataset.columns.should == [:a]
    @dataset.db.sqls.should == ['SELECT * FROM items LIMIT 1']
    @dataset.columns.should == [:a]
    @dataset.db.sqls.should == []
    ds = @dataset.select{foo{}}
    ds.columns.should == [:b]
    @dataset.db.sqls.should == ['SELECT foo() FROM items LIMIT 1']
  end
  
  specify "should be cleared if you change the FROM table" do
    @dataset.db.columns = [[:a], [:b]]
    @dataset.columns.should == [:a]
    @dataset.db.sqls.should == ['SELECT * FROM items LIMIT 1']
    ds = @dataset.from(:foo)
    ds.columns.should == [:b]
    @dataset.db.sqls.should == ['SELECT * FROM foo LIMIT 1']
  end
  
  specify "should be cleared if you join a table to the dataset" do
    @dataset.db.columns = [[:a], [:a, :b]]
    @dataset.columns.should == [:a]
    @dataset.db.sqls.should == ['SELECT * FROM items LIMIT 1']
    ds = @dataset.cross_join(:foo)
    ds.columns.should == [:a, :b]
    @dataset.db.sqls.should == ['SELECT * FROM items CROSS JOIN foo LIMIT 1']
  end
  
  specify "should be cleared if you set custom SQL for the dataset" do
    @dataset.db.columns = [[:a], [:b]]
    @dataset.columns.should == [:a]
    @dataset.db.sqls.should == ['SELECT * FROM items LIMIT 1']
    ds = @dataset.with_sql('SELECT b FROM foo')
    ds.columns.should == [:b]
    @dataset.db.sqls.should == ['SELECT b FROM foo']
  end
  
  specify "should ignore any filters, orders, or DISTINCT clauses" do
    @dataset.db.columns = [:a]
    @dataset.filter!(:b=>100).order!(:b).distinct!
    @dataset.columns.should == [:a]
    @dataset.db.sqls.should == ['SELECT * FROM items LIMIT 1']
  end
end

describe "Dataset#columns!" do
  specify "should always attempt to get a record and return @columns" do
    ds = Sequel.mock(:columns=>[[:a, :b, :c], [:d, :e, :f]])[:items]
    ds.columns!.should == [:a, :b, :c]
    ds.db.sqls.should == ['SELECT * FROM items LIMIT 1']
    ds.columns!.should == [:d, :e, :f]
    ds.db.sqls.should == ['SELECT * FROM items LIMIT 1']
  end
end

describe "Dataset#import" do
  before do
    @db = Sequel.mock
    @ds = @db[:items]
  end
  
  specify "should return nil without a query if no values" do
    @ds.import(['x', 'y'], []).should == nil
    @db.sqls.should == []
  end

  specify "should accept string keys as column names" do
    @ds.import(['x', 'y'], [[1, 2], [3, 4]])
    @db.sqls.should == ['BEGIN',
      "INSERT INTO items (x, y) VALUES (1, 2)",
      "INSERT INTO items (x, y) VALUES (3, 4)",
      'COMMIT']
  end

  specify "should accept a columns array and a values array" do
    @ds.import([:x, :y], [[1, 2], [3, 4]])
    @db.sqls.should == ['BEGIN',
      "INSERT INTO items (x, y) VALUES (1, 2)",
      "INSERT INTO items (x, y) VALUES (3, 4)",
      'COMMIT']
  end

  specify "should accept a columns array and a dataset" do
    @ds2 = @ds.from(:cats).filter(:purr => true).select(:a, :b)
    
    @ds.import([:x, :y], @ds2)
    @db.sqls.should == ['BEGIN',
      "INSERT INTO items (x, y) SELECT a, b FROM cats WHERE (purr IS TRUE)",
      'COMMIT']
  end

  specify "should accept a columns array and a values array with :commit_every option" do
    @ds.import([:x, :y], [[1, 2], [3, 4], [5, 6]], :commit_every => 3)
    @db.sqls.should == ['BEGIN',
      "INSERT INTO items (x, y) VALUES (1, 2)",
      "INSERT INTO items (x, y) VALUES (3, 4)",
      "INSERT INTO items (x, y) VALUES (5, 6)",
      'COMMIT']
  end
  specify "should accept a columns array and a values array with :slice option" do
    @ds.import([:x, :y], [[1, 2], [3, 4], [5, 6]], :slice => 2)
    @db.sqls.should == ['BEGIN',
      "INSERT INTO items (x, y) VALUES (1, 2)",
      "INSERT INTO items (x, y) VALUES (3, 4)",
      'COMMIT',
      'BEGIN',
      "INSERT INTO items (x, y) VALUES (5, 6)",
      'COMMIT']
  end
end

describe "Dataset#multi_insert" do
  before do
    @db = Sequel.mock(:servers=>{:s1=>{}})
    @ds = @db[:items]
    @list = [{:name => 'abc'}, {:name => 'def'}, {:name => 'ghi'}]
  end
  
  specify "should return nil without a query if no values" do
    @ds.multi_insert([]).should == nil
    @db.sqls.should == []
  end

  specify "should issue multiple insert statements inside a transaction" do
    @ds.multi_insert(@list)
    @db.sqls.should == ['BEGIN',
      "INSERT INTO items (name) VALUES ('abc')",
      "INSERT INTO items (name) VALUES ('def')",
      "INSERT INTO items (name) VALUES ('ghi')",
      'COMMIT']
  end
  
  specify "should respect :server option" do
    @ds.multi_insert(@list, :server=>:s1)
    @db.sqls.should == ['BEGIN -- s1',
      "INSERT INTO items (name) VALUES ('abc') -- s1",
      "INSERT INTO items (name) VALUES ('def') -- s1",
      "INSERT INTO items (name) VALUES ('ghi') -- s1",
      'COMMIT -- s1']
  end
  
  specify "should respect existing :server option on dataset" do
    @ds.server(:s1).multi_insert(@list)
    @db.sqls.should == ['BEGIN -- s1',
      "INSERT INTO items (name) VALUES ('abc') -- s1",
      "INSERT INTO items (name) VALUES ('def') -- s1",
      "INSERT INTO items (name) VALUES ('ghi') -- s1",
      'COMMIT -- s1']
  end
  
  specify "should respect :return=>:primary_key option" do
    @db.autoid = 1
    @ds.multi_insert(@list, :return=>:primary_key).should == [1, 2, 3]
    @db.sqls.should == ['BEGIN',
      "INSERT INTO items (name) VALUES ('abc')",
      "INSERT INTO items (name) VALUES ('def')",
      "INSERT INTO items (name) VALUES ('ghi')",
      'COMMIT']
  end
  
  specify "should handle different formats for tables" do
    @ds = @ds.from(:sch__tab)
    @ds.multi_insert(@list)
    @db.sqls.should == ['BEGIN',
      "INSERT INTO sch.tab (name) VALUES ('abc')",
      "INSERT INTO sch.tab (name) VALUES ('def')",
      "INSERT INTO sch.tab (name) VALUES ('ghi')",
      'COMMIT']

    @ds = @ds.from(Sequel.qualify(:sch, :tab))
    @ds.multi_insert(@list)
    @db.sqls.should == ['BEGIN',
      "INSERT INTO sch.tab (name) VALUES ('abc')",
      "INSERT INTO sch.tab (name) VALUES ('def')",
      "INSERT INTO sch.tab (name) VALUES ('ghi')",
      'COMMIT']

    @ds = @ds.from(Sequel.identifier(:sch__tab))
    @ds.multi_insert(@list)
    @db.sqls.should == ['BEGIN',
      "INSERT INTO sch__tab (name) VALUES ('abc')",
      "INSERT INTO sch__tab (name) VALUES ('def')",
      "INSERT INTO sch__tab (name) VALUES ('ghi')",
      'COMMIT']
  end
  
  specify "should accept the :commit_every option for committing every x records" do
    @ds.multi_insert(@list, :commit_every => 1)
    @db.sqls.should == ['BEGIN',
      "INSERT INTO items (name) VALUES ('abc')",
      'COMMIT',
      'BEGIN',
      "INSERT INTO items (name) VALUES ('def')",
      'COMMIT',
      'BEGIN',
      "INSERT INTO items (name) VALUES ('ghi')",
      'COMMIT']
  end

  specify "should accept the :slice option for committing every x records" do
    @ds.multi_insert(@list, :slice => 2)
    @db.sqls.should == ['BEGIN',
      "INSERT INTO items (name) VALUES ('abc')",
      "INSERT INTO items (name) VALUES ('def')",
      'COMMIT',
      'BEGIN',
      "INSERT INTO items (name) VALUES ('ghi')",
      'COMMIT']
  end
  
  specify "should accept string keys as column names" do
    @ds.multi_insert([{'x'=>1, 'y'=>2}, {'x'=>3, 'y'=>4}])
    sqls = @db.sqls
    ["INSERT INTO items (x, y) VALUES (1, 2)", "INSERT INTO items (y, x) VALUES (2, 1)"].should include(sqls.slice!(1))
    ["INSERT INTO items (x, y) VALUES (3, 4)", "INSERT INTO items (y, x) VALUES (4, 3)"].should include(sqls.slice!(1))
    sqls.should == ['BEGIN', 'COMMIT']
  end

  specify "should not do anything if no hashes are provided" do
    @ds.multi_insert([])
    @db.sqls.should == []
  end
end

describe "Dataset" do
  before do
    @d = Sequel::Dataset.new(nil).from(:x)
  end

  specify "should support self-changing select!" do
    @d.select!(:y)
    @d.sql.should == "SELECT y FROM x"
  end
  
  specify "should support self-changing from!" do
    @d.from!(:y)
    @d.sql.should == "SELECT * FROM y"
  end

  specify "should support self-changing order!" do
    @d.order!(:y)
    @d.sql.should == "SELECT * FROM x ORDER BY y"
  end
  
  specify "should support self-changing filter!" do
    @d.filter!(:y => 1)
    @d.sql.should == "SELECT * FROM x WHERE (y = 1)"
  end

  specify "should support self-changing filter! with block" do
    @d.filter!{y < 2}
    @d.sql.should == "SELECT * FROM x WHERE (y < 2)"
  end
  
  specify "should raise for ! methods that don't return a dataset" do
    proc {@d.opts!}.should raise_error(NameError)
  end
  
  specify "should raise for missing methods" do
    proc {@d.xuyz}.should raise_error(NameError)
    proc {@d.xyz!}.should raise_error(NameError)
    proc {@d.xyz?}.should raise_error(NameError)
  end
  
  specify "should support chaining of bang methods" do
      @d.order!(:y).filter!(:y => 1).sql.should == "SELECT * FROM x WHERE (y = 1) ORDER BY y"
  end
end

describe "Dataset#to_csv" do
  before do
    @ds = Sequel.mock(:fetch=>[{:a=>1, :b=>2, :c=>3}, {:a=>4, :b=>5, :c=>6}, {:a=>7, :b=>8, :c=>9}])[:items].columns(:a, :b, :c)
  end
  
  specify "should format a CSV representation of the records" do
    @ds.to_csv.should == "a, b, c\r\n1, 2, 3\r\n4, 5, 6\r\n7, 8, 9\r\n"
  end

  specify "should exclude column titles if so specified" do
    @ds.to_csv(false).should == "1, 2, 3\r\n4, 5, 6\r\n7, 8, 9\r\n"
  end
end

describe "Dataset#update_sql" do
  before do
    @ds = Sequel::Dataset.new(nil).from(:items)
  end
  
  specify "should accept strings" do
    @ds.update_sql("a = b").should == "UPDATE items SET a = b"
  end
  
  specify "should handle implicitly qualified symbols" do
    @ds.update_sql(:items__a=>:b).should == "UPDATE items SET items.a = b"
  end
  
  specify "should accept hash with string keys" do
    @ds.update_sql('c' => 'd').should == "UPDATE items SET c = 'd'"
  end

  specify "should accept array subscript references" do
    @ds.update_sql((Sequel.subscript(:day, 1)) => 'd').should == "UPDATE items SET day[1] = 'd'"
  end
end

describe "Dataset#insert_sql" do
  before do
    @ds = Sequel::Dataset.new(nil).from(:items)
  end
  
  specify "should accept hash with symbol keys" do
    @ds.insert_sql(:c => 'd').should == "INSERT INTO items (c) VALUES ('d')"
  end

  specify "should accept hash with string keys" do
    @ds.insert_sql('c' => 'd').should == "INSERT INTO items (c) VALUES ('d')"
  end

  specify "should quote string keys" do
    @ds.quote_identifiers = true
    @ds.insert_sql('c' => 'd').should == "INSERT INTO \"items\" (\"c\") VALUES ('d')"
  end

  specify "should accept array subscript references" do
    @ds.insert_sql((Sequel.subscript(:day, 1)) => 'd').should == "INSERT INTO items (day[1]) VALUES ('d')"
  end

  specify "should raise an Error if the dataset has no sources" do
    proc{Sequel::Database.new.dataset.insert_sql}.should raise_error(Sequel::Error)
  end
  
  specify "should accept datasets" do
    @ds.insert_sql(@ds).should == "INSERT INTO items SELECT * FROM items"
  end
  
  specify "should accept datasets with columns" do
    @ds.insert_sql([:a, :b], @ds).should == "INSERT INTO items (a, b) SELECT * FROM items"
  end
  
  specify "should raise if given bad values" do
    proc{@ds.clone(:values=>'a').send(:_insert_sql)}.should raise_error(Sequel::Error)
  end
  
  specify "should accept separate values" do
    @ds.insert_sql(1).should == "INSERT INTO items VALUES (1)"
    @ds.insert_sql(1, 2).should == "INSERT INTO items VALUES (1, 2)"
    @ds.insert_sql(1, 2, 3).should == "INSERT INTO items VALUES (1, 2, 3)"
  end
  
  specify "should accept a single array of values" do
    @ds.insert_sql([1, 2, 3]).should == "INSERT INTO items VALUES (1, 2, 3)"
  end
  
  specify "should accept an array of columns and an array of values" do
    @ds.insert_sql([:a, :b, :c], [1, 2, 3]).should == "INSERT INTO items (a, b, c) VALUES (1, 2, 3)"
  end
  
  specify "should raise an array if the columns and values differ in size" do
    proc{@ds.insert_sql([:a, :b], [1, 2, 3])}.should raise_error(Sequel::Error)
  end
  
  specify "should accept a single LiteralString" do
    @ds.insert_sql(Sequel.lit('VALUES (1, 2, 3)')).should == "INSERT INTO items VALUES (1, 2, 3)"
  end
  
  specify "should accept an array of columns and an LiteralString" do
    @ds.insert_sql([:a, :b, :c], Sequel.lit('VALUES (1, 2, 3)')).should == "INSERT INTO items (a, b, c) VALUES (1, 2, 3)"
  end
end

describe "Dataset#inspect" do
  before do
    class ::InspectDataset < Sequel::Dataset; end
  end
  after do
    Object.send(:remove_const, :InspectDataset) if defined?(::InspectDataset)
  end

  specify "should include the class name and the corresponding SQL statement" do
    Sequel::Dataset.new(nil).from(:blah).inspect.should == '#<Sequel::Dataset: "SELECT * FROM blah">'
    InspectDataset.new(nil).from(:blah).inspect.should == '#<InspectDataset: "SELECT * FROM blah">'
  end

  specify "should skip anonymous classes" do
    Class.new(Class.new(Sequel::Dataset)).new(nil).from(:blah).inspect.should == '#<Sequel::Dataset: "SELECT * FROM blah">'
    Class.new(InspectDataset).new(nil).from(:blah).inspect.should == '#<InspectDataset: "SELECT * FROM blah">'
  end
end

describe "Dataset#all" do
  before do
    @dataset = Sequel.mock(:fetch=>[{:x => 1, :y => 2}, {:x => 3, :y => 4}])[:items]
  end

  specify "should return an array with all records" do
    @dataset.all.should == [{:x => 1, :y => 2}, {:x => 3, :y => 4}]
    @dataset.db.sqls.should == ["SELECT * FROM items"]
  end
  
  specify "should iterate over the array if a block is given" do
    a = []
    @dataset.all{|r| a << r.values_at(:x, :y)}.should == [{:x => 1, :y => 2}, {:x => 3, :y => 4}]
    a.should == [[1, 2], [3, 4]]
    @dataset.db.sqls.should == ["SELECT * FROM items"]
  end
end

describe "Dataset#grep" do
  before do
    @ds = Sequel.mock[:posts]
  end
  
  specify "should format a SQL filter correctly" do
    @ds.grep(:title, 'ruby').sql.should == "SELECT * FROM posts WHERE ((title LIKE 'ruby' ESCAPE '\\'))"
  end

  specify "should support multiple columns" do
    @ds.grep([:title, :body], 'ruby').sql.should == "SELECT * FROM posts WHERE ((title LIKE 'ruby' ESCAPE '\\') OR (body LIKE 'ruby' ESCAPE '\\'))"
  end
  
  specify "should support multiple search terms" do
    @ds.grep(:title, ['abc', 'def']).sql.should == "SELECT * FROM posts WHERE ((title LIKE 'abc' ESCAPE '\\') OR (title LIKE 'def' ESCAPE '\\'))"
  end
  
  specify "should support multiple columns and search terms" do
    @ds.grep([:title, :body], ['abc', 'def']).sql.should == "SELECT * FROM posts WHERE ((title LIKE 'abc' ESCAPE '\\') OR (title LIKE 'def' ESCAPE '\\') OR (body LIKE 'abc' ESCAPE '\\') OR (body LIKE 'def' ESCAPE '\\'))"
  end
  
  specify "should support the :all_patterns option" do
    @ds.grep([:title, :body], ['abc', 'def'], :all_patterns=>true).sql.should == "SELECT * FROM posts WHERE (((title LIKE 'abc' ESCAPE '\\') OR (body LIKE 'abc' ESCAPE '\\')) AND ((title LIKE 'def' ESCAPE '\\') OR (body LIKE 'def' ESCAPE '\\')))"
  end
  
  specify "should support the :all_columns option" do
    @ds.grep([:title, :body], ['abc', 'def'], :all_columns=>true).sql.should == "SELECT * FROM posts WHERE (((title LIKE 'abc' ESCAPE '\\') OR (title LIKE 'def' ESCAPE '\\')) AND ((body LIKE 'abc' ESCAPE '\\') OR (body LIKE 'def' ESCAPE '\\')))"
  end
  
  specify "should support the :case_insensitive option" do
    @ds.grep([:title, :body], ['abc', 'def'], :case_insensitive=>true).sql.should == "SELECT * FROM posts WHERE ((UPPER(title) LIKE UPPER('abc') ESCAPE '\\') OR (UPPER(title) LIKE UPPER('def') ESCAPE '\\') OR (UPPER(body) LIKE UPPER('abc') ESCAPE '\\') OR (UPPER(body) LIKE UPPER('def') ESCAPE '\\'))"
  end
  
  specify "should support the :all_patterns and :all_columns options together" do
    @ds.grep([:title, :body], ['abc', 'def'], :all_patterns=>true, :all_columns=>true).sql.should == "SELECT * FROM posts WHERE ((title LIKE 'abc' ESCAPE '\\') AND (body LIKE 'abc' ESCAPE '\\') AND (title LIKE 'def' ESCAPE '\\') AND (body LIKE 'def' ESCAPE '\\'))"
  end
  
  specify "should support the :all_patterns and :case_insensitive options together" do
    @ds.grep([:title, :body], ['abc', 'def'], :all_patterns=>true, :case_insensitive=>true).sql.should == "SELECT * FROM posts WHERE (((UPPER(title) LIKE UPPER('abc') ESCAPE '\\') OR (UPPER(body) LIKE UPPER('abc') ESCAPE '\\')) AND ((UPPER(title) LIKE UPPER('def') ESCAPE '\\') OR (UPPER(body) LIKE UPPER('def') ESCAPE '\\')))"
  end
  
  specify "should support the :all_columns and :case_insensitive options together" do
    @ds.grep([:title, :body], ['abc', 'def'], :all_columns=>true, :case_insensitive=>true).sql.should == "SELECT * FROM posts WHERE (((UPPER(title) LIKE UPPER('abc') ESCAPE '\\') OR (UPPER(title) LIKE UPPER('def') ESCAPE '\\')) AND ((UPPER(body) LIKE UPPER('abc') ESCAPE '\\') OR (UPPER(body) LIKE UPPER('def') ESCAPE '\\')))"
  end
  
  specify "should support the :all_patterns, :all_columns, and :case_insensitive options together" do
    @ds.grep([:title, :body], ['abc', 'def'], :all_patterns=>true, :all_columns=>true, :case_insensitive=>true).sql.should == "SELECT * FROM posts WHERE ((UPPER(title) LIKE UPPER('abc') ESCAPE '\\') AND (UPPER(body) LIKE UPPER('abc') ESCAPE '\\') AND (UPPER(title) LIKE UPPER('def') ESCAPE '\\') AND (UPPER(body) LIKE UPPER('def') ESCAPE '\\'))"
  end

  specify "should not support regexps if the database doesn't supports it" do
    proc{@ds.grep(:title, /ruby/).sql}.should raise_error(Sequel::InvalidOperation)
    proc{@ds.grep(:title, [/^ruby/, 'ruby']).sql}.should raise_error(Sequel::InvalidOperation)
  end

  specify "should support regexps if the database supports it" do
    def @ds.supports_regexp?; true end
    @ds.grep(:title, /ruby/).sql.should == "SELECT * FROM posts WHERE ((title ~ 'ruby'))"
    @ds.grep(:title, [/^ruby/, 'ruby']).sql.should == "SELECT * FROM posts WHERE ((title ~ '^ruby') OR (title LIKE 'ruby' ESCAPE '\\'))"
  end

  specify "should support searching against other columns" do
    @ds.grep(:title, :body).sql.should == "SELECT * FROM posts WHERE ((title LIKE body ESCAPE '\\'))"
  end
end

describe "Dataset default #fetch_rows, #insert, #update, #delete, #with_sql_delete, #truncate, #execute" do
  before do
    @db = Sequel::Database.new
    @ds = @db[:items]
  end

  specify "#fetch_rows should raise a Sequel::NotImplemented" do
    proc{@ds.fetch_rows(''){}}.should raise_error(Sequel::NotImplemented)
  end

  specify "#delete should execute delete SQL" do
    @db.should_receive(:execute).once.with('DELETE FROM items', :server=>:default)
    @ds.delete
    @db.should_receive(:execute_dui).once.with('DELETE FROM items', :server=>:default)
    @ds.delete
  end

  specify "#with_sql_delete should execute delete SQL" do
    sql = 'DELETE FROM foo'
    @db.should_receive(:execute).once.with(sql, :server=>:default)
    @ds.with_sql_delete(sql)
    @db.should_receive(:execute_dui).once.with(sql, :server=>:default)
    @ds.with_sql_delete(sql)
  end

  specify "#insert should execute insert SQL" do
    @db.should_receive(:execute).once.with('INSERT INTO items DEFAULT VALUES', :server=>:default)
    @ds.insert([])
    @db.should_receive(:execute_insert).once.with('INSERT INTO items DEFAULT VALUES', :server=>:default)
    @ds.insert([])
  end

  specify "#update should execute update SQL" do
    @db.should_receive(:execute).once.with('UPDATE items SET number = 1', :server=>:default)
    @ds.update(:number=>1)
    @db.should_receive(:execute_dui).once.with('UPDATE items SET number = 1', :server=>:default)
    @ds.update(:number=>1)
  end
  
  specify "#truncate should execute truncate SQL" do
    @db.should_receive(:execute).once.with('TRUNCATE TABLE items', :server=>:default)
    @ds.truncate.should == nil
    @db.should_receive(:execute_ddl).once.with('TRUNCATE TABLE items', :server=>:default)
    @ds.truncate.should == nil
  end
  
  specify "#truncate should raise an InvalidOperation exception if the dataset is filtered" do
    proc{@ds.filter(:a=>1).truncate}.should raise_error(Sequel::InvalidOperation)
    proc{@ds.having(:a=>1).truncate}.should raise_error(Sequel::InvalidOperation)
  end
  
  specify "#execute should execute the SQL on the database" do
    @db.should_receive(:execute).once.with('SELECT 1', :server=>:read_only)
    @ds.send(:execute, 'SELECT 1')
  end
end

describe "Dataset prepared statements and bound variables " do
  before do
    @db = Sequel.mock
    @ds = @db[:items]
    @ds.meta_def(:insert_sql){|*v| "#{super(*v)}#{' RETURNING *' if opts.has_key?(:returning)}" }
  end
  
  specify "#call should take a type and bind hash and interpolate it" do
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
    @db.sqls.should == [
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
    
  specify "#prepare should take a type and name and store it in the database for later use with call" do
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
    @db.prepared_statements.keys.sort_by{|k| k.to_s}.should == [:dn, :en, :fn, :in, :ins, :sh, :shg, :sm, :sn, :un]
    [:en, :sn, :sm, :sh, :shg, :fn, :dn, :un, :in, :ins].each_with_index{|x, i| @db.prepared_statements[x].should == pss[i]}
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
    @db.sqls.should == [
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
    
  specify "#call should default to using :all if an invalid type is given" do
    @ds.filter(:num=>:$n).call(:select_all, :n=>1)
    @db.sqls.should == ['SELECT * FROM items WHERE (num = 1)']
  end

  specify "#inspect should indicate it is a prepared statement with the prepared SQL" do
    @ds.filter(:num=>:$n).prepare(:select, :sn).inspect.should == \
      '<Sequel::Mock::Dataset/PreparedStatement "SELECT * FROM items WHERE (num = $n)">'
  end
    
  specify "should handle literal strings" do
    @ds.filter("num = ?", :$n).call(:select, :n=>1)
    @db.sqls.should == ['SELECT * FROM items WHERE (num = 1)']
  end
    
  specify "should handle columns on prepared statements correctly" do
    @db.columns = [:num]
    @ds.meta_def(:select_where_sql){|sql| super(sql); sql << " OR #{columns.first} = 1" if opts[:where]}
    @ds.filter(:num=>:$n).prepare(:select, :sn).sql.should == 'SELECT * FROM items WHERE (num = $n) OR num = 1'
    @db.sqls.should == ['SELECT * FROM items LIMIT 1']
  end
    
  specify "should handle datasets using static sql and placeholders" do
    @db["SELECT * FROM items WHERE (num = ?)", :$n].call(:select, :n=>1)
    @db.sqls.should == ['SELECT * FROM items WHERE (num = 1)']
  end
    
  specify "should handle subselects" do
    @ds.filter(:$b).filter(:num=>@ds.select(:num).filter(:num=>:$n)).filter(:$c).call(:select, :n=>1, :b=>0, :c=>2)
    @db.sqls.should == ['SELECT * FROM items WHERE (0 AND (num IN (SELECT num FROM items WHERE (num = 1))) AND 2)']
  end
    
  specify "should handle subselects in subselects" do
    @ds.filter(:$b).filter(:num=>@ds.select(:num).filter(:num=>@ds.select(:num).filter(:num=>:$n))).call(:select, :n=>1, :b=>0)
    @db.sqls.should == ['SELECT * FROM items WHERE (0 AND (num IN (SELECT num FROM items WHERE (num IN (SELECT num FROM items WHERE (num = 1))))))']
  end
    
  specify "should handle subselects with literal strings" do
    @ds.filter(:$b).filter(:num=>@ds.select(:num).filter("num = ?", :$n)).call(:select, :n=>1, :b=>0)
    @db.sqls.should == ['SELECT * FROM items WHERE (0 AND (num IN (SELECT num FROM items WHERE (num = 1))))']
  end
    
  specify "should handle subselects with static sql and placeholders" do
    @ds.filter(:$b).filter(:num=>@db["SELECT num FROM items WHERE (num = ?)", :$n]).call(:select, :n=>1, :b=>0)
    @db.sqls.should == ['SELECT * FROM items WHERE (0 AND (num IN (SELECT num FROM items WHERE (num = 1))))']
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
    @ps.each{|p| p.extend(Sequel::Dataset::UnnumberedArgumentMapper)}
  end

  specify "#inspect should show the actual SQL submitted to the database" do
    @ps.first.inspect.should == '<Sequel::Mock::Dataset/PreparedStatement "SELECT * FROM items WHERE (num = ?)">'
  end
  
  specify "should submit the SQL to the database with placeholders and bind variables" do
    @ps.each{|p| p.prepared_sql; p.call(:n=>1)}
    @db.sqls.should == ["SELECT * FROM items WHERE (num = ?) -- args: [1]",
      "SELECT * FROM items WHERE (num = ?) -- args: [1]",
      "SELECT * FROM items WHERE (num = ?) LIMIT 1 -- args: [1]",
      "DELETE FROM items WHERE (num = ?) -- args: [1]",
      "INSERT INTO items (num) VALUES (?) -- args: [1]",
      "UPDATE items SET num = ? WHERE (num = ?) -- args: [1, 1]"]
  end

  specify "should handle unrecognized statement types as :all" do
    ps = @ds.prepare(:select_all, :s)
    ps.extend(Sequel::Dataset::UnnumberedArgumentMapper)
    ps.prepared_sql
    ps.call(:n=>1)
    @db.sqls.should == ["SELECT * FROM items WHERE (num = ?) -- args: [1]"]
  end
end

describe "Sequel::Dataset#server" do
  specify "should set the server to use for the dataset" do
    @db = Sequel.mock(:servers=>{:s=>{}, :i=>{}, :d=>{}, :u=>{}})
    @ds = @db[:items].server(:s)
    @ds.all
    @ds.server(:i).insert(:a=>1)
    @ds.server(:d).delete
    @ds.server(:u).update(:a=>Sequel.expr(:a)+1)
    @db.sqls.should == ['SELECT * FROM items -- s', 'INSERT INTO items (a) VALUES (1) -- i', 'DELETE FROM items -- d', 'UPDATE items SET a = (a + 1) -- u']
  end
end

describe "Sequel::Dataset#each_server" do
  specify "should yield a dataset for each server" do
    @db = Sequel.mock(:servers=>{:s=>{}, :i=>{}})
    @ds = @db[:items]
    @ds.each_server do |ds|
      ds.should be_a_kind_of(Sequel::Dataset)
      ds.should_not == @ds
      ds.sql.should == @ds.sql
      ds.all
    end
    @db.sqls.sort.should == ['SELECT * FROM items', 'SELECT * FROM items -- i', 'SELECT * FROM items -- s']
  end
end

describe "Sequel::Dataset #set_defaults" do
  before do
    @ds = Sequel::Dataset.new(nil).from(:items).set_defaults(:x=>1)
  end

  specify "should set the default values for inserts" do
    @ds.insert_sql.should == "INSERT INTO items (x) VALUES (1)"
    @ds.insert_sql(:x=>2).should == "INSERT INTO items (x) VALUES (2)"
    @ds.insert_sql(:y=>2).should =~ /INSERT INTO items \([xy], [xy]\) VALUES \([21], [21]\)/
    @ds.set_defaults(:y=>2).insert_sql.should =~ /INSERT INTO items \([xy], [xy]\) VALUES \([21], [21]\)/
    @ds.set_defaults(:x=>2).insert_sql.should == "INSERT INTO items (x) VALUES (2)"
  end

  specify "should set the default values for updates" do
    @ds.update_sql.should == "UPDATE items SET x = 1"
    @ds.update_sql(:x=>2).should == "UPDATE items SET x = 2"
    @ds.update_sql(:y=>2).should =~ /UPDATE items SET (x = 1|y = 2), (x = 1|y = 2)/
    @ds.set_defaults(:y=>2).update_sql.should =~ /UPDATE items SET (x = 1|y = 2), (x = 1|y = 2)/
    @ds.set_defaults(:x=>2).update_sql.should == "UPDATE items SET x = 2"
  end
end

describe "Sequel::Dataset #set_overrides" do
  before do
    @ds = Sequel::Dataset.new(nil).from(:items).set_overrides(:x=>1)
  end

  specify "should override the given values for inserts" do
    @ds.insert_sql.should == "INSERT INTO items (x) VALUES (1)"
    @ds.insert_sql(:x=>2).should == "INSERT INTO items (x) VALUES (1)"
    @ds.insert_sql(:y=>2).should =~ /INSERT INTO items \([xy], [xy]\) VALUES \([21], [21]\)/
    @ds.set_overrides(:y=>2).insert_sql.should =~ /INSERT INTO items \([xy], [xy]\) VALUES \([21], [21]\)/
    @ds.set_overrides(:x=>2).insert_sql.should == "INSERT INTO items (x) VALUES (1)"
  end

  specify "should override the given values for updates" do
    @ds.update_sql.should == "UPDATE items SET x = 1"
    @ds.update_sql(:x=>2).should == "UPDATE items SET x = 1"
    @ds.update_sql(:y=>2).should =~ /UPDATE items SET (x = 1|y = 2), (x = 1|y = 2)/
    @ds.set_overrides(:y=>2).update_sql.should =~ /UPDATE items SET (x = 1|y = 2), (x = 1|y = 2)/
    @ds.set_overrides(:x=>2).update_sql.should == "UPDATE items SET x = 1"
  end
end

describe "Sequel::Dataset#qualify" do
  specify "should qualify to the given table" do
    Sequel::Dataset.new(nil).from(:t).filter{a<b}.qualify(:e).sql.should == 'SELECT e.* FROM t WHERE (e.a < e.b)'
  end

  specify "should qualify to the first source if no table if given" do
    Sequel::Dataset.new(nil).from(:t).filter{a<b}.qualify.sql.should == 'SELECT t.* FROM t WHERE (t.a < t.b)'
  end
end

describe "Sequel::Dataset#qualify_to" do
  specify "should qualify to the given table" do
    Sequel::Dataset.new(nil).from(:t).filter{a<b}.qualify_to(:e).sql.should == 'SELECT e.* FROM t WHERE (e.a < e.b)'
  end
end

describe "Sequel::Dataset#qualify_to_first_source" do
  before do
    @ds = Sequel::Database.new[:t]
  end

  specify "should qualify_to the first source" do
    @ds.qualify_to_first_source.sql.should == 'SELECT t.* FROM t'
    @ds.should_receive(:qualify_to).with(:t).once
    @ds.qualify_to_first_source
  end

  specify "should handle the select, order, where, having, and group options/clauses" do
    @ds.select(:a).filter(:a=>1).order(:a).group(:a).having(:a).qualify_to_first_source.sql.should == 'SELECT t.a FROM t WHERE (t.a = 1) GROUP BY t.a HAVING t.a ORDER BY t.a'
  end

  specify "should handle the select using a table.* if all columns are currently selected" do
    @ds.filter(:a=>1).order(:a).group(:a).having(:a).qualify_to_first_source.sql.should == 'SELECT t.* FROM t WHERE (t.a = 1) GROUP BY t.a HAVING t.a ORDER BY t.a'
  end

  specify "should handle hashes in select option" do
    @ds.select(:a=>:b).qualify_to_first_source.sql.should == 'SELECT t.a AS b FROM t'
  end

  specify "should handle symbols" do
    @ds.select(:a, :b__c, :d___e, :f__g___h).qualify_to_first_source.sql.should == 'SELECT t.a, b.c, t.d AS e, f.g AS h FROM t'
  end

  specify "should handle arrays" do
    @ds.filter(:a=>[:b, :c]).qualify_to_first_source.sql.should == 'SELECT t.* FROM t WHERE (t.a IN (t.b, t.c))'
  end

  specify "should handle hashes" do
    @ds.select(Sequel.case({:b=>{:c=>1}}, false)).qualify_to_first_source.sql.should == "SELECT (CASE WHEN t.b THEN (t.c = 1) ELSE 'f' END) FROM t"
  end

  specify "should handle SQL::Identifiers" do
    @ds.select{a}.qualify_to_first_source.sql.should == 'SELECT t.a FROM t'
  end

  specify "should handle SQL::OrderedExpressions" do
    @ds.order(Sequel.desc(:a), Sequel.asc(:b)).qualify_to_first_source.sql.should == 'SELECT t.* FROM t ORDER BY t.a DESC, t.b ASC'
  end

  specify "should handle SQL::AliasedExpressions" do
    @ds.select(Sequel.expr(:a).as(:b)).qualify_to_first_source.sql.should == 'SELECT t.a AS b FROM t'
  end

  specify "should handle SQL::CaseExpressions" do
    @ds.filter{Sequel.case({a=>b}, c, d)}.qualify_to_first_source.sql.should == 'SELECT t.* FROM t WHERE (CASE t.d WHEN t.a THEN t.b ELSE t.c END)'
  end

  specify "should handle SQL:Casts" do
    @ds.filter{a.cast(:boolean)}.qualify_to_first_source.sql.should == 'SELECT t.* FROM t WHERE CAST(t.a AS boolean)'
  end

  specify "should handle SQL::Functions" do
    @ds.filter{a(b, 1)}.qualify_to_first_source.sql.should == 'SELECT t.* FROM t WHERE a(t.b, 1)'
  end

  specify "should handle SQL::ComplexExpressions" do
    @ds.filter{(a+b)<(c-3)}.qualify_to_first_source.sql.should == 'SELECT t.* FROM t WHERE ((t.a + t.b) < (t.c - 3))'
  end

  specify "should handle SQL::ValueLists" do
    @ds.filter(:a=>Sequel.value_list([:b, :c])).qualify_to_first_source.sql.should == 'SELECT t.* FROM t WHERE (t.a IN (t.b, t.c))'
  end

  specify "should handle SQL::Subscripts" do
    @ds.filter{a.sql_subscript(b,3)}.qualify_to_first_source.sql.should == 'SELECT t.* FROM t WHERE t.a[t.b, 3]'
  end

  specify "should handle SQL::PlaceholderLiteralStrings" do
    @ds.filter('? > ?', :a, 1).qualify_to_first_source.sql.should == 'SELECT t.* FROM t WHERE (t.a > 1)'
  end

  specify "should handle SQL::PlaceholderLiteralStrings with named placeholders" do
    @ds.filter(':a > :b', :a=>:c, :b=>1).qualify_to_first_source.sql.should == 'SELECT t.* FROM t WHERE (t.c > 1)'
  end

  specify "should handle SQL::Wrappers" do
    @ds.filter(Sequel::SQL::Wrapper.new(:a)).qualify_to_first_source.sql.should == 'SELECT t.* FROM t WHERE t.a'
  end

  specify "should handle SQL::WindowFunctions" do
    @ds.meta_def(:supports_window_functions?){true}
    @ds.select{sum(:over, :args=>:a, :partition=>:b, :order=>:c){}}.qualify_to_first_source.sql.should == 'SELECT sum(t.a) OVER (PARTITION BY t.b ORDER BY t.c) FROM t'
  end

  specify "should handle all other objects by returning them unchanged" do
    @ds.select("a").filter{a(3)}.filter('blah').order(Sequel.lit('true')).group(Sequel.lit('a > ?', 1)).having(false).qualify_to_first_source.sql.should == "SELECT 'a' FROM t WHERE (a(3) AND (blah)) GROUP BY a > 1 HAVING 'f' ORDER BY true"
  end
end

describe "Sequel::Dataset#unbind" do
  before do
    @ds = Sequel::Database.new[:t]
    @u = proc{|ds| ds, bv = ds.unbind; [ds.sql, bv]}
  end

  specify "should unbind values assigned to equality and inequality statements" do
    @ds.filter(:foo=>1).unbind.first.sql.should == "SELECT * FROM t WHERE (foo = $foo)"
    @ds.exclude(:foo=>1).unbind.first.sql.should == "SELECT * FROM t WHERE (foo != $foo)"
    @ds.filter{foo > 1}.unbind.first.sql.should == "SELECT * FROM t WHERE (foo > $foo)"
    @ds.filter{foo >= 1}.unbind.first.sql.should == "SELECT * FROM t WHERE (foo >= $foo)"
    @ds.filter{foo < 1}.unbind.first.sql.should == "SELECT * FROM t WHERE (foo < $foo)"
    @ds.filter{foo <= 1}.unbind.first.sql.should == "SELECT * FROM t WHERE (foo <= $foo)"
  end

  specify "should return variables that could be used bound to recreate the previous query" do
    @ds.filter(:foo=>1).unbind.last.should == {:foo=>1}
    @ds.exclude(:foo=>1).unbind.last.should == {:foo=>1}
  end

  specify "should handle numerics, strings, dates, times, and datetimes" do
    @u[@ds.filter(:foo=>1)].should == ["SELECT * FROM t WHERE (foo = $foo)", {:foo=>1}]
    @u[@ds.filter(:foo=>1.0)].should == ["SELECT * FROM t WHERE (foo = $foo)", {:foo=>1.0}]
    @u[@ds.filter(:foo=>BigDecimal.new('1.0'))].should == ["SELECT * FROM t WHERE (foo = $foo)", {:foo=>BigDecimal.new('1.0')}]
    @u[@ds.filter(:foo=>'a')].should == ["SELECT * FROM t WHERE (foo = $foo)", {:foo=>'a'}]
    @u[@ds.filter(:foo=>Date.today)].should == ["SELECT * FROM t WHERE (foo = $foo)", {:foo=>Date.today}]
    t = Time.now
    @u[@ds.filter(:foo=>t)].should == ["SELECT * FROM t WHERE (foo = $foo)", {:foo=>t}]
    dt = DateTime.now
    @u[@ds.filter(:foo=>dt)].should == ["SELECT * FROM t WHERE (foo = $foo)", {:foo=>dt}]
  end

  specify "should not unbind literal strings" do
    @u[@ds.filter(:foo=>Sequel.lit('a'))].should == ["SELECT * FROM t WHERE (foo = a)", {}]
  end

  specify "should not unbind Identifiers, QualifiedIdentifiers, or Symbols used as booleans" do
    @u[@ds.filter(:foo).filter{bar}.filter{foo__bar}].should == ["SELECT * FROM t WHERE (foo AND bar AND foo.bar)", {}]
  end

  specify "should not unbind for values it doesn't understand" do
    @u[@ds.filter(:foo=>Class.new{def sql_literal(ds) 'bar' end}.new)].should == ["SELECT * FROM t WHERE (foo = bar)", {}]
  end

  specify "should handle QualifiedIdentifiers" do
    @u[@ds.filter{foo__bar > 1}].should == ["SELECT * FROM t WHERE (foo.bar > $foo.bar)", {:"foo.bar"=>1}]
  end

  specify "should handle wrapped objects" do
    @u[@ds.filter{Sequel::SQL::Wrapper.new(foo__bar) > Sequel::SQL::Wrapper.new(1)}].should == ["SELECT * FROM t WHERE (foo.bar > $foo.bar)", {:"foo.bar"=>1}]
  end

  specify "should handle deep nesting" do
    @u[@ds.filter{foo > 1}.and{bar < 2}.or(:baz=>3).and(Sequel.case({~Sequel.expr(:x=>4)=>true}, false))].should == ["SELECT * FROM t WHERE ((((foo > $foo) AND (bar < $bar)) OR (baz = $baz)) AND (CASE WHEN (x != $x) THEN 't' ELSE 'f' END))", {:foo=>1, :bar=>2, :baz=>3, :x=>4}]
  end

  specify "should handle JOIN ON" do
    @u[@ds.cross_join(:x).join(:a, [:u]).join(:b, [[:c, :d], [:e,1]])].should == ["SELECT * FROM t CROSS JOIN x INNER JOIN a USING (u) INNER JOIN b ON ((b.c = a.d) AND (b.e = $b.e))", {:"b.e"=>1}]
  end

  specify "should raise an UnbindDuplicate exception if same variable is used with multiple different values" do
    proc{@ds.filter(:foo=>1).or(:foo=>2).unbind}.should raise_error(Sequel::UnbindDuplicate)
  end

  specify "should handle case where the same variable has the same value in multiple places " do
    @u[@ds.filter(:foo=>1).or(:foo=>1)].should == ["SELECT * FROM t WHERE ((foo = $foo) OR (foo = $foo))", {:foo=>1}]
  end

  specify "should raise Error for unhandled objects inside Identifiers and QualifiedIndentifiers" do
    proc{@ds.filter(Sequel::SQL::Identifier.new([]) > 1).unbind}.should raise_error(Sequel::Error)
    proc{@ds.filter{foo.qualify({}) > 1}.unbind}.should raise_error(Sequel::Error)
  end
end

describe "Sequel::Dataset #with and #with_recursive" do
  before do
    @db = Sequel::Database.new
    @ds = @db[:t]
  end
  
  specify "#with should take a name and dataset and use a WITH clause" do
    @ds.with(:t, @db[:x]).sql.should == 'WITH t AS (SELECT * FROM x) SELECT * FROM t'
  end

  specify "#with_recursive should take a name, nonrecursive dataset, and recursive dataset, and use a WITH clause" do
    @ds.with_recursive(:t, @db[:x], @db[:t]).sql.should == 'WITH t AS (SELECT * FROM x UNION ALL SELECT * FROM t) SELECT * FROM t'
  end
  
  specify "#with and #with_recursive should add to existing WITH clause if called multiple times" do
    @ds.with(:t, @db[:x]).with(:j, @db[:y]).sql.should == 'WITH t AS (SELECT * FROM x), j AS (SELECT * FROM y) SELECT * FROM t'
    @ds.with_recursive(:t, @db[:x], @db[:t]).with_recursive(:j, @db[:y], @db[:j]).sql.should == 'WITH t AS (SELECT * FROM x UNION ALL SELECT * FROM t), j AS (SELECT * FROM y UNION ALL SELECT * FROM j) SELECT * FROM t'
    @ds.with(:t, @db[:x]).with_recursive(:j, @db[:y], @db[:j]).sql.should == 'WITH t AS (SELECT * FROM x), j AS (SELECT * FROM y UNION ALL SELECT * FROM j) SELECT * FROM t'
  end
  
  specify "#with and #with_recursive should take an :args option" do
    @ds.with(:t, @db[:x], :args=>[:b]).sql.should == 'WITH t(b) AS (SELECT * FROM x) SELECT * FROM t'
    @ds.with_recursive(:t, @db[:x], @db[:t], :args=>[:b, :c]).sql.should == 'WITH t(b, c) AS (SELECT * FROM x UNION ALL SELECT * FROM t) SELECT * FROM t'
  end
  
  specify "#with and #with_recursive should quote the columns in the :args option" do
    @ds.quote_identifiers = true
    @ds.with(:t, @db[:x], :args=>[:b]).sql.should == 'WITH "t"("b") AS (SELECT * FROM x) SELECT * FROM "t"'
    @ds.with_recursive(:t, @db[:x], @db[:t], :args=>[:b, :c]).sql.should == 'WITH "t"("b", "c") AS (SELECT * FROM x UNION ALL SELECT * FROM t) SELECT * FROM "t"'
  end
  
  specify "#with_recursive should take an :union_all=>false option" do
    @ds.with_recursive(:t, @db[:x], @db[:t], :union_all=>false).sql.should == 'WITH t AS (SELECT * FROM x UNION SELECT * FROM t) SELECT * FROM t'
  end

  specify "#with and #with_recursive should raise an error unless the dataset supports CTEs" do
    @ds.meta_def(:supports_cte?){false}
    proc{@ds.with(:t, @db[:x], :args=>[:b])}.should raise_error(Sequel::Error)
    proc{@ds.with_recursive(:t, @db[:x], @db[:t], :args=>[:b, :c])}.should raise_error(Sequel::Error)
  end

  specify "#with should work on insert, update, and delete statements if they support it" do
    [:insert, :update, :delete].each do |m|
      @ds.meta_def(:"#{m}_clause_methods"){[:"#{m}_with_sql"] + super()}
    end
    @ds.with(:t, @db[:x]).insert_sql(1).should == 'WITH t AS (SELECT * FROM x) INSERT INTO t VALUES (1)'
    @ds.with(:t, @db[:x]).update_sql(:foo=>1).should == 'WITH t AS (SELECT * FROM x) UPDATE t SET foo = 1'
    @ds.with(:t, @db[:x]).delete_sql.should == 'WITH t AS (SELECT * FROM x) DELETE FROM t'
  end

  specify "should hoist WITH clauses in given dataset(s) if dataset doesn't support WITH in subselect" do
    @ds.meta_def(:supports_cte?){true}
    @ds.meta_def(:supports_cte_in_subselect?){false}
    @ds.with(:t, @ds.from(:s).with(:s, @ds.from(:r))).sql.should == 'WITH s AS (SELECT * FROM r), t AS (SELECT * FROM s) SELECT * FROM t'
    @ds.with_recursive(:t, @ds.from(:s).with(:s, @ds.from(:r)), @ds.from(:q).with(:q, @ds.from(:p))).sql.should == 'WITH s AS (SELECT * FROM r), q AS (SELECT * FROM p), t AS (SELECT * FROM s UNION ALL SELECT * FROM q) SELECT * FROM t'
  end
end

describe Sequel::SQL::Constants do
  before do
    @db = Sequel::Database.new
  end
  
  it "should have CURRENT_DATE" do
    @db.literal(Sequel::SQL::Constants::CURRENT_DATE).should == 'CURRENT_DATE'
    @db.literal(Sequel::CURRENT_DATE).should == 'CURRENT_DATE'
  end

  it "should have CURRENT_TIME" do
    @db.literal(Sequel::SQL::Constants::CURRENT_TIME).should == 'CURRENT_TIME'
    @db.literal(Sequel::CURRENT_TIME).should == 'CURRENT_TIME'
  end

  it "should have CURRENT_TIMESTAMP" do
    @db.literal(Sequel::SQL::Constants::CURRENT_TIMESTAMP).should == 'CURRENT_TIMESTAMP'
    @db.literal(Sequel::CURRENT_TIMESTAMP).should == 'CURRENT_TIMESTAMP'
  end

  it "should have NULL" do
    @db.literal(Sequel::SQL::Constants::NULL).should == 'NULL'
    @db.literal(Sequel::NULL).should == 'NULL'
  end

  it "should have NOTNULL" do
    @db.literal(Sequel::SQL::Constants::NOTNULL).should == 'NOT NULL'
    @db.literal(Sequel::NOTNULL).should == 'NOT NULL'
  end

  it "should have TRUE and SQLTRUE" do
    @db.literal(Sequel::SQL::Constants::TRUE).should == "'t'"
    @db.literal(Sequel::TRUE).should == "'t'"
    @db.literal(Sequel::SQL::Constants::SQLTRUE).should == "'t'"
    @db.literal(Sequel::SQLTRUE).should == "'t'"
  end

  it "should have FALSE and SQLFALSE" do
    @db.literal(Sequel::SQL::Constants::FALSE).should == "'f'"
    @db.literal(Sequel::FALSE).should == "'f'"
    @db.literal(Sequel::SQL::Constants::SQLFALSE).should == "'f'"
    @db.literal(Sequel::SQLFALSE).should == "'f'"
  end
end

describe "Sequel timezone support" do
  before do
    @db = Sequel::Database.new
    @dataset = @db.dataset
    @dataset.meta_def(:supports_timestamp_timezones?){true}
    @dataset.meta_def(:supports_timestamp_usecs?){false}
    @offset = sprintf("%+03i%02i", *(Time.now.utc_offset/60).divmod(60))
  end
  after do
    Sequel.default_timezone = nil
    Sequel.datetime_class = Time
  end
  
  specify "should handle an database timezone of :utc when literalizing values" do
    Sequel.database_timezone = :utc

    t = Time.now
    s = t.getutc.strftime("'%Y-%m-%d %H:%M:%S")
    @dataset.literal(t).should == "#{s}+0000'"

    t = DateTime.now
    s = t.new_offset(0).strftime("'%Y-%m-%d %H:%M:%S")
    @dataset.literal(t).should == "#{s}+0000'"
  end
  
  specify "should handle an database timezone of :local when literalizing values" do
    Sequel.database_timezone = :local

    t = Time.now.utc
    s = t.getlocal.strftime("'%Y-%m-%d %H:%M:%S")
    @dataset.literal(t).should == "#{s}#{@offset}'"

    t = DateTime.now.new_offset(0)
    s = t.new_offset(DateTime.now.offset).strftime("'%Y-%m-%d %H:%M:%S")
    @dataset.literal(t).should == "#{s}#{@offset}'"
  end
  
  specify "should have Database#timezone override Sequel.database_timezone" do
    Sequel.database_timezone = :local
    @db.timezone = :utc

    t = Time.now
    s = t.getutc.strftime("'%Y-%m-%d %H:%M:%S")
    @dataset.literal(t).should == "#{s}+0000'"

    t = DateTime.now
    s = t.new_offset(0).strftime("'%Y-%m-%d %H:%M:%S")
    @dataset.literal(t).should == "#{s}+0000'"

    Sequel.database_timezone = :utc
    @db.timezone = :local

    t = Time.now.utc
    s = t.getlocal.strftime("'%Y-%m-%d %H:%M:%S")
    @dataset.literal(t).should == "#{s}#{@offset}'"

    t = DateTime.now.new_offset(0)
    s = t.new_offset(DateTime.now.offset).strftime("'%Y-%m-%d %H:%M:%S")
    @dataset.literal(t).should == "#{s}#{@offset}'"
  end
  
  specify "should handle converting database timestamps into application timestamps" do
    Sequel.database_timezone = :utc
    Sequel.application_timezone = :local
    t = Time.now.utc
    Sequel.database_to_application_timestamp(t).to_s.should == t.getlocal.to_s
    Sequel.database_to_application_timestamp(t.to_s).to_s.should == t.getlocal.to_s
    Sequel.database_to_application_timestamp(t.strftime('%Y-%m-%d %H:%M:%S')).to_s.should == t.getlocal.to_s
    
    Sequel.datetime_class = DateTime
    dt = DateTime.now
    dt2 = dt.new_offset(0)
    Sequel.database_to_application_timestamp(dt2).to_s.should == dt.to_s
    Sequel.database_to_application_timestamp(dt2.to_s).to_s.should == dt.to_s
    Sequel.database_to_application_timestamp(dt2.strftime('%Y-%m-%d %H:%M:%S')).to_s.should == dt.to_s
    
    Sequel.datetime_class = Time
    Sequel.database_timezone = :local
    Sequel.application_timezone = :utc
    Sequel.database_to_application_timestamp(t.getlocal).to_s.should == t.to_s
    Sequel.database_to_application_timestamp(t.getlocal.to_s).to_s.should == t.to_s
    Sequel.database_to_application_timestamp(t.getlocal.strftime('%Y-%m-%d %H:%M:%S')).to_s.should == t.to_s
    
    Sequel.datetime_class = DateTime
    Sequel.database_to_application_timestamp(dt).to_s.should == dt2.to_s
    Sequel.database_to_application_timestamp(dt.to_s).to_s.should == dt2.to_s
    Sequel.database_to_application_timestamp(dt.strftime('%Y-%m-%d %H:%M:%S')).to_s.should == dt2.to_s
  end
  
  specify "should handle typecasting timestamp columns" do
    Sequel.typecast_timezone = :utc
    Sequel.application_timezone = :local
    t = Time.now.utc
    @db.typecast_value(:datetime, t).to_s.should == t.getlocal.to_s
    @db.typecast_value(:datetime, t.to_s).to_s.should == t.getlocal.to_s
    @db.typecast_value(:datetime, t.strftime('%Y-%m-%d %H:%M:%S')).to_s.should == t.getlocal.to_s
    
    Sequel.datetime_class = DateTime
    dt = DateTime.now
    dt2 = dt.new_offset(0)
    @db.typecast_value(:datetime, dt2).to_s.should == dt.to_s
    @db.typecast_value(:datetime, dt2.to_s).to_s.should == dt.to_s
    @db.typecast_value(:datetime, dt2.strftime('%Y-%m-%d %H:%M:%S')).to_s.should == dt.to_s
    
    Sequel.datetime_class = Time
    Sequel.typecast_timezone = :local
    Sequel.application_timezone = :utc
    @db.typecast_value(:datetime, t.getlocal).to_s.should == t.to_s
    @db.typecast_value(:datetime, t.getlocal.to_s).to_s.should == t.to_s
    @db.typecast_value(:datetime, t.getlocal.strftime('%Y-%m-%d %H:%M:%S')).to_s.should == t.to_s
    
    Sequel.datetime_class = DateTime
    @db.typecast_value(:datetime, dt).to_s.should == dt2.to_s
    @db.typecast_value(:datetime, dt.to_s).to_s.should == dt2.to_s
    @db.typecast_value(:datetime, dt.strftime('%Y-%m-%d %H:%M:%S')).to_s.should == dt2.to_s
  end
  
  specify "should handle converting database timestamp columns from an array of values" do
    Sequel.database_timezone = :utc
    Sequel.application_timezone = :local
    t = Time.now.utc
    Sequel.database_to_application_timestamp([t.year, t.mon, t.day, t.hour, t.min, t.sec]).to_s.should == t.getlocal.to_s
    
    Sequel.datetime_class = DateTime
    dt = DateTime.now
    dt2 = dt.new_offset(0)
    Sequel.database_to_application_timestamp([dt2.year, dt2.mon, dt2.day, dt2.hour, dt2.min, dt2.sec]).to_s.should == dt.to_s
    
    Sequel.datetime_class = Time
    Sequel.database_timezone = :local
    Sequel.application_timezone = :utc
    t = t.getlocal
    Sequel.database_to_application_timestamp([t.year, t.mon, t.day, t.hour, t.min, t.sec]).to_s.should == t.getutc.to_s
    
    Sequel.datetime_class = DateTime
    Sequel.database_to_application_timestamp([dt.year, dt.mon, dt.day, dt.hour, dt.min, dt.sec]).to_s.should == dt2.to_s
  end
  
  specify "should raise an InvalidValue error when an error occurs while converting a timestamp" do
    proc{Sequel.database_to_application_timestamp([0, 0, 0, 0, 0, 0])}.should raise_error(Sequel::InvalidValue)
  end
  
  specify "should raise an error when attempting to typecast to a timestamp from an unsupported type" do
    proc{Sequel.database_to_application_timestamp(Object.new)}.should raise_error(Sequel::InvalidValue)
  end

  specify "should raise an InvalidValue error when the DateTime class is used and when a bad application timezone is used when attempting to convert timestamps" do
    Sequel.application_timezone = :blah
    Sequel.datetime_class = DateTime
    proc{Sequel.database_to_application_timestamp('2009-06-01 10:20:30')}.should raise_error(Sequel::InvalidValue)
  end
  
  specify "should raise an InvalidValue error when the DateTime class is used and when a bad database timezone is used when attempting to convert timestamps" do
    Sequel.database_timezone = :blah
    Sequel.datetime_class = DateTime
    proc{Sequel.database_to_application_timestamp('2009-06-01 10:20:30')}.should raise_error(Sequel::InvalidValue)
  end

  specify "should have Sequel.default_timezone= should set all other timezones" do
    Sequel.database_timezone.should == nil
    Sequel.application_timezone.should == nil
    Sequel.typecast_timezone.should == nil
    Sequel.default_timezone = :utc
    Sequel.database_timezone.should == :utc
    Sequel.application_timezone.should == :utc
    Sequel.typecast_timezone.should == :utc
  end
end

describe "Sequel::Dataset#select_map" do
  before do
    @ds = Sequel.mock(:fetch=>[{:c=>1}, {:c=>2}])[:t]
  end

  specify "should do select and map in one step" do
    @ds.select_map(:a).should == [1, 2]
    @ds.db.sqls.should == ['SELECT a FROM t']
  end

  specify "should handle implicit qualifiers in arguments" do
    @ds.select_map(:a__b).should == [1, 2]
    @ds.db.sqls.should == ['SELECT a.b FROM t']
  end

  specify "should raise if multiple arguments and can't determine alias" do
    proc{@ds.select_map([Sequel.function(:a), :b])}.should raise_error(Sequel::Error)
    proc{@ds.select_map(Sequel.function(:a)){b}}.should raise_error(Sequel::Error)
    proc{@ds.select_map{[a{}, b]}}.should raise_error(Sequel::Error)
  end

  specify "should handle implicit aliases in arguments" do
    @ds.select_map(:a___b).should == [1, 2]
    @ds.db.sqls.should == ['SELECT a AS b FROM t']
  end

  specify "should handle other objects" do
    @ds.select_map(Sequel.lit("a").as(:b)).should == [1, 2]
    @ds.db.sqls.should == ['SELECT a AS b FROM t']
  end
  
  specify "should handle identifiers with strings" do
    @ds.select_map([Sequel::SQL::Identifier.new('c'), :c]).should == [[1, 1], [2, 2]]
    @ds.db.sqls.should == ['SELECT c, c FROM t']
  end
  
  specify "should accept a block" do
    @ds.select_map{a(t__c)}.should == [1, 2]
    @ds.db.sqls.should == ['SELECT a(t.c) FROM t']
  end

  specify "should accept a block with an array of columns" do
    @ds.select_map{[a(t__c).as(c), a(t__c).as(c)]}.should == [[1, 1], [2, 2]]
    @ds.db.sqls.should == ['SELECT a(t.c) AS c, a(t.c) AS c FROM t']
  end

  specify "should accept a block with a column" do
    @ds.select_map(:c){a(t__c).as(c)}.should == [[1, 1], [2, 2]]
    @ds.db.sqls.should == ['SELECT c, a(t.c) AS c FROM t']
  end

  specify "should accept a block and array of arguments" do
    @ds.select_map([:c, :c]){[a(t__c).as(c), a(t__c).as(c)]}.should == [[1, 1, 1, 1], [2, 2, 2, 2]]
    @ds.db.sqls.should == ['SELECT c, c, a(t.c) AS c, a(t.c) AS c FROM t']
  end

  specify "should handle an array of columns" do
    @ds.select_map([:c, :c]).should == [[1, 1], [2, 2]]
    @ds.db.sqls.should == ['SELECT c, c FROM t']
    @ds.select_map([Sequel.expr(:d).as(:c), Sequel.qualify(:b, :c), Sequel.identifier(:c), Sequel.identifier(:c).qualify(:b), :a__c, :a__d___c]).should == [[1, 1, 1, 1, 1, 1], [2, 2, 2, 2, 2, 2]]
    @ds.db.sqls.should == ['SELECT d AS c, b.c, c, b.c, a.c, a.d AS c FROM t']
  end

  specify "should handle an array with a single element" do
    @ds.select_map([:c]).should == [[1], [2]]
    @ds.db.sqls.should == ['SELECT c FROM t']
  end
end

describe "Sequel::Dataset#select_order_map" do
  before do
    @ds = Sequel.mock(:fetch=>[{:c=>1}, {:c=>2}])[:t]
  end

  specify "should do select and map in one step" do
    @ds.select_order_map(:a).should == [1, 2]
    @ds.db.sqls.should == ['SELECT a FROM t ORDER BY a']
  end

  specify "should handle implicit qualifiers in arguments" do
    @ds.select_order_map(:a__b).should == [1, 2]
    @ds.db.sqls.should == ['SELECT a.b FROM t ORDER BY a.b']
  end

  specify "should raise if multiple arguments and can't determine alias" do
    proc{@ds.select_order_map([Sequel.function(:a), :b])}.should raise_error(Sequel::Error)
    proc{@ds.select_order_map(Sequel.function(:a)){b}}.should raise_error(Sequel::Error)
    proc{@ds.select_order_map{[a{}, b]}}.should raise_error(Sequel::Error)
  end

  specify "should handle implicit aliases in arguments" do
    @ds.select_order_map(:a___b).should == [1, 2]
    @ds.db.sqls.should == ['SELECT a AS b FROM t ORDER BY a']
  end

  specify "should handle implicit qualifiers and aliases in arguments" do
    @ds.select_order_map(:t__a___b).should == [1, 2]
    @ds.db.sqls.should == ['SELECT t.a AS b FROM t ORDER BY t.a']
  end

  specify "should handle AliasedExpressions" do
    @ds.select_order_map(Sequel.lit("a").as(:b)).should == [1, 2]
    @ds.db.sqls.should == ['SELECT a AS b FROM t ORDER BY a']
  end
  
  specify "should handle OrderedExpressions" do
    @ds.select_order_map(Sequel.desc(:a)).should == [1, 2]
    @ds.db.sqls.should == ['SELECT a FROM t ORDER BY a DESC']
  end
  
  specify "should accept a block" do
    @ds.select_order_map{a(t__c)}.should == [1, 2]
    @ds.db.sqls.should == ['SELECT a(t.c) FROM t ORDER BY a(t.c)']
  end

  specify "should accept a block with an array of columns" do
    @ds.select_order_map{[c.desc, a(t__c).as(c)]}.should == [[1, 1], [2, 2]]
    @ds.db.sqls.should == ['SELECT c, a(t.c) AS c FROM t ORDER BY c DESC, a(t.c)']
  end

  specify "should accept a block with a column" do
    @ds.select_order_map(:c){a(t__c).as(c)}.should == [[1, 1], [2, 2]]
    @ds.db.sqls.should == ['SELECT c, a(t.c) AS c FROM t ORDER BY c, a(t.c)']
  end

  specify "should accept a block and array of arguments" do
    @ds.select_order_map([:c, :c]){[a(t__c).as(c), c.desc]}.should == [[1, 1, 1, 1], [2, 2, 2, 2]]
    @ds.db.sqls.should == ['SELECT c, c, a(t.c) AS c, c FROM t ORDER BY c, c, a(t.c), c DESC']
  end

  specify "should handle an array of columns" do
    @ds.select_order_map([:c, :c]).should == [[1, 1], [2, 2]]
    @ds.db.sqls.should == ['SELECT c, c FROM t ORDER BY c, c']
    @ds.select_order_map([Sequel.expr(:d).as(:c), Sequel.qualify(:b, :c), Sequel.identifier(:c), Sequel.identifier(:c).qualify(:b), Sequel.identifier(:c).qualify(:b).desc, :a__c, Sequel.desc(:a__d___c), Sequel.desc(Sequel.expr(:a__d___c))]).should == [[1, 1, 1, 1, 1, 1, 1, 1], [2, 2, 2, 2, 2, 2, 2, 2]]
    @ds.db.sqls.should == ['SELECT d AS c, b.c, c, b.c, b.c, a.c, a.d AS c, a.d AS c FROM t ORDER BY d, b.c, c, b.c, b.c DESC, a.c, a.d DESC, a.d DESC']
  end

  specify "should handle an array with a single element" do
    @ds.select_order_map([:c]).should == [[1], [2]]
    @ds.db.sqls.should == ['SELECT c FROM t ORDER BY c']
  end
end

describe "Sequel::Dataset#select_hash" do
  before do
    @db = Sequel.mock(:fetch=>[{:a=>1, :b=>2}, {:a=>3, :b=>4}])
    @ds = @db[:t]
  end

  specify "should do select and to_hash in one step" do
    @ds.select_hash(:a, :b).should == {1=>2, 3=>4}
    @ds.db.sqls.should == ['SELECT a, b FROM t']
  end

  specify "should handle implicit qualifiers in arguments" do
    @ds.select_hash(:t__a, :t__b).should == {1=>2, 3=>4}
    @ds.db.sqls.should == ['SELECT t.a, t.b FROM t']
  end

  specify "should handle implicit aliases in arguments" do
    @ds.select_hash(:c___a, :d___b).should == {1=>2, 3=>4}
    @ds.db.sqls.should == ['SELECT c AS a, d AS b FROM t']
  end

  specify "should handle implicit qualifiers and aliases in arguments" do
    @ds.select_hash(:t__c___a, :t__d___b).should == {1=>2, 3=>4}
    @ds.db.sqls.should == ['SELECT t.c AS a, t.d AS b FROM t']
  end

  specify "should handle SQL::Identifiers in arguments" do
    @ds.select_hash(Sequel.identifier(:a), Sequel.identifier(:b)).should == {1=>2, 3=>4}
    @ds.db.sqls.should == ['SELECT a, b FROM t']
  end

  specify "should handle SQL::QualifiedIdentifiers in arguments" do
    @ds.select_hash(Sequel.qualify(:t, :a), Sequel.identifier(:b).qualify(:t)).should == {1=>2, 3=>4}
    @ds.db.sqls.should == ['SELECT t.a, t.b FROM t']
  end

  specify "should handle SQL::AliasedExpressions in arguments" do
    @ds.select_hash(Sequel.expr(:c).as(:a), Sequel.expr(:t).as(:b)).should == {1=>2, 3=>4}
    @ds.db.sqls.should == ['SELECT c AS a, t AS b FROM t']
  end

  specify "should work with arrays of columns" do
    @db.fetch = [{:a=>1, :b=>2, :c=>3}, {:a=>4, :b=>5, :c=>6}]
    @ds.select_hash([:a, :c], :b).should == {[1, 3]=>2, [4, 6]=>5}
    @ds.db.sqls.should == ['SELECT a, c, b FROM t']
    @ds.select_hash(:a, [:b, :c]).should == {1=>[2, 3], 4=>[5, 6]}
    @ds.db.sqls.should == ['SELECT a, b, c FROM t']
    @ds.select_hash([:a, :b], [:b, :c]).should == {[1, 2]=>[2, 3], [4, 5]=>[5, 6]}
    @ds.db.sqls.should == ['SELECT a, b, b, c FROM t']
  end

  specify "should raise an error if the resulting symbol cannot be determined" do
    proc{@ds.select_hash(Sequel.expr(:c).as(:a), Sequel.function(:b))}.should raise_error(Sequel::Error)
  end
end

describe "Sequel::Dataset#select_hash_groups" do
  before do
    @db = Sequel.mock(:fetch=>[{:a=>1, :b=>2}, {:a=>3, :b=>4}])
    @ds = @db[:t]
  end

  specify "should do select and to_hash in one step" do
    @ds.select_hash_groups(:a, :b).should == {1=>[2], 3=>[4]}
    @ds.db.sqls.should == ['SELECT a, b FROM t']
  end

  specify "should handle implicit qualifiers in arguments" do
    @ds.select_hash_groups(:t__a, :t__b).should == {1=>[2], 3=>[4]}
    @ds.db.sqls.should == ['SELECT t.a, t.b FROM t']
  end

  specify "should handle implicit aliases in arguments" do
    @ds.select_hash_groups(:c___a, :d___b).should == {1=>[2], 3=>[4]}
    @ds.db.sqls.should == ['SELECT c AS a, d AS b FROM t']
  end

  specify "should handle implicit qualifiers and aliases in arguments" do
    @ds.select_hash_groups(:t__c___a, :t__d___b).should == {1=>[2], 3=>[4]}
    @ds.db.sqls.should == ['SELECT t.c AS a, t.d AS b FROM t']
  end

  specify "should handle SQL::Identifiers in arguments" do
    @ds.select_hash_groups(Sequel.identifier(:a), Sequel.identifier(:b)).should == {1=>[2], 3=>[4]}
    @ds.db.sqls.should == ['SELECT a, b FROM t']
  end

  specify "should handle SQL::QualifiedIdentifiers in arguments" do
    @ds.select_hash_groups(Sequel.qualify(:t, :a), Sequel.identifier(:b).qualify(:t)).should == {1=>[2], 3=>[4]}
    @ds.db.sqls.should == ['SELECT t.a, t.b FROM t']
  end

  specify "should handle SQL::AliasedExpressions in arguments" do
    @ds.select_hash_groups(Sequel.expr(:c).as(:a), Sequel.expr(:t).as(:b)).should == {1=>[2], 3=>[4]}
    @ds.db.sqls.should == ['SELECT c AS a, t AS b FROM t']
  end

  specify "should work with arrays of columns" do
    @db.fetch = [{:a=>1, :b=>2, :c=>3}, {:a=>4, :b=>5, :c=>6}]
    @ds.select_hash_groups([:a, :c], :b).should == {[1, 3]=>[2], [4, 6]=>[5]}
    @ds.db.sqls.should == ['SELECT a, c, b FROM t']
    @ds.select_hash_groups(:a, [:b, :c]).should == {1=>[[2, 3]], 4=>[[5, 6]]}
    @ds.db.sqls.should == ['SELECT a, b, c FROM t']
    @ds.select_hash_groups([:a, :b], [:b, :c]).should == {[1, 2]=>[[2, 3]], [4, 5]=>[[5, 6]]}
    @ds.db.sqls.should == ['SELECT a, b, b, c FROM t']
  end

  specify "should raise an error if the resulting symbol cannot be determined" do
    proc{@ds.select_hash_groups(Sequel.expr(:c).as(:a), Sequel.function(:b))}.should raise_error(Sequel::Error)
  end
end

describe "Modifying joined datasets" do
  before do
    @ds = Sequel.mock.from(:b, :c).join(:d, [:id]).where(:id => 2)
    @ds.meta_def(:supports_modifying_joins?){true}
  end

  specify "should allow deleting from joined datasets" do
    @ds.delete
    @ds.db.sqls.should == ['DELETE FROM b, c WHERE (id = 2)']
  end

  specify "should allow updating joined datasets" do
    @ds.update(:a=>1)
    @ds.db.sqls.should == ['UPDATE b, c INNER JOIN d USING (id) SET a = 1 WHERE (id = 2)']
  end
end

describe "Dataset#lock_style and for_update" do
  before do
    @ds = Sequel::Dataset.new(nil).from(:t)
  end
  
  specify "#for_update should use FOR UPDATE" do
    @ds.for_update.sql.should == "SELECT * FROM t FOR UPDATE"
  end
  
  specify "#lock_style should accept symbols" do
    @ds.lock_style(:update).sql.should == "SELECT * FROM t FOR UPDATE"
  end
  
  specify "#lock_style should accept strings for arbitrary SQL" do
    @ds.lock_style("FOR SHARE").sql.should == "SELECT * FROM t FOR SHARE"
  end
end

describe "Custom ASTTransformer" do
  specify "should transform given objects" do
    c = Class.new(Sequel::ASTTransformer) do
      def v(s)
        (s.is_a?(Symbol) || s.is_a?(String)) ? :"#{s}#{s}" : super
      end
    end.new
    ds = Sequel::Dataset.new(nil).from(:t).cross_join(:a___g).join(:b___h, [:c]).join(:d___i, :e=>:f)
    ds.sql.should == 'SELECT * FROM t CROSS JOIN a AS g INNER JOIN b AS h USING (c) INNER JOIN d AS i ON (i.e = h.f)'
    ds.clone(:from=>c.transform(ds.opts[:from]), :join=>c.transform(ds.opts[:join])).sql.should == 'SELECT * FROM tt CROSS JOIN aa AS gg INNER JOIN bb AS hh USING (cc) INNER JOIN dd AS ii ON (ii.ee = hh.ff)'
  end
end

describe "Dataset#returning" do
  before do
    @ds = Sequel.mock(:fetch=>proc{|s| {:foo=>s}})[:t].returning(:foo)
    @pr = proc do
      [:insert, :update, :delete].each do |m|
        @ds.meta_def(:"#{m}_clause_methods"){super() + [:"#{m}_returning_sql"]}
      end
    end
  end
  
  specify "should use RETURNING clause in the SQL if the dataset supports it" do
    @pr.call
    @ds.delete_sql.should == "DELETE FROM t RETURNING foo"
    @ds.insert_sql(1).should == "INSERT INTO t VALUES (1) RETURNING foo"
    @ds.update_sql(:foo=>1).should == "UPDATE t SET foo = 1 RETURNING foo"
  end
  
  specify "should not use RETURNING clause in the SQL if the dataset does not support it" do
    @ds.delete_sql.should == "DELETE FROM t"
    @ds.insert_sql(1).should == "INSERT INTO t VALUES (1)"
    @ds.update_sql(:foo=>1).should == "UPDATE t SET foo = 1"
  end

  specify "should have insert, update, and delete yield to blocks if RETURNING is used" do
    @pr.call
    h = {}
    @ds.delete{|r| h = r}
    h.should == {:foo=>"DELETE FROM t RETURNING foo"}
    @ds.insert(1){|r| h = r}
    h.should == {:foo=>"INSERT INTO t VALUES (1) RETURNING foo"}
    @ds.update(:foo=>1){|r| h = r}
    h.should == {:foo=>"UPDATE t SET foo = 1 RETURNING foo"}
  end

  specify "should have insert, update, and delete return arrays of hashes if RETURNING is used and a block is not given" do
    @pr.call
    h = {}
    @ds.delete.should == [{:foo=>"DELETE FROM t RETURNING foo"}]
    @ds.insert(1).should == [{:foo=>"INSERT INTO t VALUES (1) RETURNING foo"}]
    @ds.update(:foo=>1).should == [{:foo=>"UPDATE t SET foo = 1 RETURNING foo"}]
  end
end

describe "Dataset emulating bitwise operator support" do
  before do
    @ds = Sequel::Database.new.dataset
    @ds.quote_identifiers = true
    def @ds.complex_expression_sql_append(sql, op, args)
      sql << complex_expression_arg_pairs(args){|a, b| "bitand(#{literal(a)}, #{literal(b)})"}
    end
  end

  it "should work with any numbers of arguments for operators" do
    @ds.select(Sequel::SQL::ComplexExpression.new(:&, :x)).sql.should == 'SELECT "x"'
    @ds.select(Sequel.expr(:x) & 1).sql.should == 'SELECT bitand("x", 1)'
    @ds.select(Sequel.expr(:x) & 1 & 2).sql.should == 'SELECT bitand(bitand("x", 1), 2)'
  end
end

describe "Dataset feature defaults" do
  it "should not require aliases for recursive CTEs by default" do
    Sequel::Database.new.dataset.recursive_cte_requires_column_aliases?.should be_false
  end

  it "should not require placeholder type specifiers by default" do
    Sequel::Database.new.dataset.requires_placeholder_type_specifiers?.should be_false
  end
end

describe "Dataset extensions" do
  before(:all) do
    class << Sequel
      alias _extension extension
      def extension(*)
      end
    end
  end
  after(:all) do
    class << Sequel
      alias extension _extension
    end
  end
  before do
    @ds = Sequel::Dataset.new(nil)
  end

  specify "should be able to register an extension with a module Database#extension extend the module" do
    Sequel::Dataset.register_extension(:foo, Module.new{def a; 1; end})
    @ds.extension(:foo).a.should == 1
  end

  specify "should be able to register an extension with a block and Database#extension call the block" do
    @ds.quote_identifiers = false
    Sequel::Dataset.register_extension(:foo){|db| db.quote_identifiers = true}
    @ds.extension(:foo).quote_identifiers?.should be_true
  end

  specify "should be able to register an extension with a callable and Database#extension call the callable" do
    @ds.quote_identifiers = false
    Sequel::Dataset.register_extension(:foo, proc{|db| db.quote_identifiers = true})
    @ds.extension(:foo).quote_identifiers?.should be_true
  end

  specify "should be able to load multiple extensions in the same call" do
    @ds.quote_identifiers = false
    @ds.identifier_input_method = :downcase
    Sequel::Dataset.register_extension(:foo, proc{|ds| ds.quote_identifiers = true})
    Sequel::Dataset.register_extension(:bar, proc{|ds| ds.identifier_input_method = nil})
    ds = @ds.extension(:foo, :bar)
    ds.quote_identifiers?.should be_true
    ds.identifier_input_method.should be_nil
  end

  specify "should have #extension not modify the receiver" do
    Sequel::Dataset.register_extension(:foo, Module.new{def a; 1; end})
    @ds.extension(:foo)
    proc{@ds.a}.should raise_error(NoMethodError)
  end

  specify "should have #extension not return a cloned dataset" do
    @ds.extend(Module.new{def b; 2; end})
    Sequel::Dataset.register_extension(:foo, Module.new{def a; 1; end})
    v = @ds.extension(:foo)
    v.should_not equal(@ds)
    v.should be_a_kind_of(Sequel::Dataset)
    v.b.should == 2
  end

  specify "should have #extension! modify the receiver" do
    Sequel::Dataset.register_extension(:foo, Module.new{def a; 1; end})
    @ds.extension!(:foo)
    @ds.a.should == 1
  end

  specify "should have #extension! return the receiver" do
    Sequel::Dataset.register_extension(:foo, Module.new{def a; 1; end})
    @ds.extension!(:foo).should equal(@ds)
  end

  specify "should register a Database extension for modifying all datasets when registering with a module" do
    Sequel::Dataset.register_extension(:foo, Module.new{def a; 1; end})
    Sequel.mock.extension(:foo).dataset.a.should == 1
  end

  specify "should raise an Error if registering with both a module and a block" do
    proc{Sequel::Dataset.register_extension(:foo, Module.new){}}.should raise_error(Sequel::Error)
  end

  specify "should raise an Error if attempting to load an incompatible extension" do
    proc{@ds.extension(:foo2)}.should raise_error(Sequel::Error)
  end
end

describe "Dataset#schema_and_table" do
  before do
    @ds = Sequel.mock[:test]
  end

  it "should correctly handle symbols" do
    @ds.schema_and_table(:s).should == [nil, 's']
    @ds.schema_and_table(:s___a).should == [nil, 's']
    @ds.schema_and_table(:t__s).should == ['t', 's']
    @ds.schema_and_table(:t__s___a).should == ['t', 's']
  end

  it "should correctly handle strings" do
    @ds.schema_and_table('s').should == [nil, 's']
  end

  it "should correctly handle literal strings" do
    s = Sequel.lit('s')
    @ds.schema_and_table(s).last.should equal(s)
  end

  it "should correctly handle identifiers" do
    @ds.schema_and_table(Sequel.identifier(:s)).should == [nil, 's']
  end

  it "should correctly handle qualified identifiers" do
    @ds.schema_and_table(Sequel.qualify(:t, :s)).should == ['t', 's']
  end

  it "should respect default_schema" do
    @ds.db.default_schema = :foo
    @ds.schema_and_table(:s).should == ['foo', 's']
    @ds.schema_and_table(:s, nil).should == [nil, 's']
  end
end

describe "Dataset#split_qualifiers" do
  before do
    @ds = Sequel.mock[:test]
  end

  it "should correctly handle symbols" do
    @ds.split_qualifiers(:s).should == ['s']
    @ds.split_qualifiers(:s___a).should == ['s']
    @ds.split_qualifiers(:t__s).should == ['t', 's']
    @ds.split_qualifiers(:t__s___a).should == ['t', 's']
  end

  it "should correctly handle strings" do
    @ds.split_qualifiers('s').should == ['s']
  end

  it "should correctly handle identifiers" do
    @ds.split_qualifiers(Sequel.identifier(:s)).should == ['s']
  end

  it "should correctly handle simple qualified identifiers" do
    @ds.split_qualifiers(Sequel.qualify(:t, :s)).should == ['t', 's']
  end

  it "should correctly handle complex qualified identifiers" do
    @ds.split_qualifiers(Sequel.qualify(:d__t, :s)).should == ['d', 't', 's']
    @ds.split_qualifiers(Sequel.qualify(Sequel.qualify(:d, :t), :s)).should == ['d', 't', 's']
    @ds.split_qualifiers(Sequel.qualify(:d, :t__s)).should == ['d', 't', 's']
    @ds.split_qualifiers(Sequel.qualify(:d, Sequel.qualify(:t, :s))).should == ['d', 't', 's']
    @ds.split_qualifiers(Sequel.qualify(:d__t, :s__s2)).should == ['d', 't', 's', 's2']
    @ds.split_qualifiers(Sequel.qualify(Sequel.qualify(:d, :t), Sequel.qualify(:s, :s2))).should == ['d', 't', 's', 's2']
  end

  it "should respect default_schema" do
    @ds.db.default_schema = :foo
    @ds.split_qualifiers(:s).should == ['foo', 's']
    @ds.split_qualifiers(:s, nil).should == ['s']
    @ds.split_qualifiers(Sequel.qualify(:d__t, :s)).should == ['d', 't', 's']
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
    @rows.should == @db
  end

  it "should respect the row_proc" do
    @ds.row_proc = lambda{|row| {:x=>row[:x]*2}}
    @ds.paged_each(&@proc)
    @rows.should == @db.map{|row| {:x=>row[:x]*2}}
  end

  it "should use a transaction to ensure consistent results" do
    @ds.paged_each(&@proc)
    sqls = @ds.db.sqls
    sqls[0].should == 'BEGIN'
    sqls[-1].should == 'COMMIT'
  end

  it "should use a limit and offset to go through the dataset in chunks at a time" do
    @ds.paged_each(&@proc)
    @ds.db.sqls[1...-1].should == ['SELECT * FROM test ORDER BY x LIMIT 1000 OFFSET 0']
  end

  it "should accept a :rows_per_fetch option to change the number of rows per fetch" do
    @ds._fetch = @db.each_slice(3).to_a
    @ds.paged_each(:rows_per_fetch=>3, &@proc)
    @rows.should == @db
    @ds.db.sqls[1...-1].should == ['SELECT * FROM test ORDER BY x LIMIT 3 OFFSET 0',
      'SELECT * FROM test ORDER BY x LIMIT 3 OFFSET 3',
      'SELECT * FROM test ORDER BY x LIMIT 3 OFFSET 6',
      'SELECT * FROM test ORDER BY x LIMIT 3 OFFSET 9']
  end

  it "should handle cases where the last query returns nothing" do
    @ds._fetch = @db.each_slice(5).to_a
    @ds.paged_each(:rows_per_fetch=>5, &@proc)
    @rows.should == @db
    @ds.db.sqls[1...-1].should == ['SELECT * FROM test ORDER BY x LIMIT 5 OFFSET 0',
      'SELECT * FROM test ORDER BY x LIMIT 5 OFFSET 5',
      'SELECT * FROM test ORDER BY x LIMIT 5 OFFSET 10']
  end

  it "should respect an existing server option to use" do
    @ds = Sequel.mock(:servers=>{:foo=>{}})[:test].order(:x)
    @ds._fetch = @db
    @ds.server(:foo).paged_each(&@proc)
    @rows.should == @db
    @ds.db.sqls.should == ["BEGIN -- foo", "SELECT * FROM test ORDER BY x LIMIT 1000 OFFSET 0 -- foo", "COMMIT -- foo"]
  end

  it "should require an order" do
    lambda{@ds.unordered.paged_each(&@proc)}.should raise_error(Sequel::Error)
  end

  it "should handle an existing limit and/or offset" do
    @ds._fetch = @db.each_slice(3).to_a
    @ds.limit(5).paged_each(:rows_per_fetch=>3, &@proc)
    @ds.db.sqls[1...-1].should == ["SELECT * FROM test ORDER BY x LIMIT 3 OFFSET 0", "SELECT * FROM test ORDER BY x LIMIT 2 OFFSET 3"]

    @ds._fetch = @db.each_slice(3).to_a
    @ds.limit(5, 2).paged_each(:rows_per_fetch=>3, &@proc)
    @ds.db.sqls[1...-1].should == ["SELECT * FROM test ORDER BY x LIMIT 3 OFFSET 2", "SELECT * FROM test ORDER BY x LIMIT 2 OFFSET 5"]

    @ds._fetch = @db.each_slice(3).to_a
    @ds.limit(nil, 2).paged_each(:rows_per_fetch=>3, &@proc)
    @ds.db.sqls[1...-1].should == ["SELECT * FROM test ORDER BY x LIMIT 3 OFFSET 2", "SELECT * FROM test ORDER BY x LIMIT 3 OFFSET 5", "SELECT * FROM test ORDER BY x LIMIT 3 OFFSET 8", "SELECT * FROM test ORDER BY x LIMIT 3 OFFSET 11"]
  end
end

describe "Dataset#escape_like" do
  before do
    @ds = Sequel.mock[:test]
  end

  it "should escape % and _ and \\ characters" do
    @ds.escape_like("foo\\%_bar").should == "foo\\\\\\%\\_bar"
  end
end

