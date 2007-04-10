require File.join(File.dirname(__FILE__), '../lib/sequel')

context "Dataset" do
  setup do
    @dataset = Sequel::Dataset.new("db")
  end
  
  specify "should accept database, opts and record_class in initialize" do
    db = 'db'
    opts = {:from => :test}
    a_class = Class.new
    d = Sequel::Dataset.new(db, opts, a_class)
    d.db.should_be db
    d.opts.should_be opts
    d.record_class.should_be a_class
    
    d = Sequel::Dataset.new(db)
    d.db.should_be db
    d.opts.should_be_a_kind_of Hash
    d.opts.should == {}
    d.record_class.should_be_nil
  end
  
  specify "should provide dup_merge for chainability." do
    d1 = @dataset.dup_merge(:from => :test)
    d1.class.should == @dataset.class
    d1.should_not == @dataset
    d1.db.should_be @dataset.db
    d1.opts[:from].should == :test
    @dataset.opts[:from].should_be_nil
    
    d2 = d1.dup_merge(:order => :name)
    d2.class.should == @dataset.class
    d2.should_not == d1
    d2.should_not == @dataset
    d2.db.should_be @dataset.db
    d2.opts[:from].should == :test
    d2.opts[:order].should == :name
    d1.opts[:order].should_be_nil
    
    # dup_merge should preserve @record_class
    a_class = Class.new
    d3 = Sequel::Dataset.new("db", nil, a_class)
    d3.db.should_not_be @dataset.db
    d4 = @dataset.dup_merge({})
    d4.db.should_be @dataset.db
    d3.record_class.should_be a_class
    d4.record_class.should_be_nil
    d5 = d3.dup_merge(:from => :test)
    d5.db.should_be d3.db
    d5.record_class.should == a_class
  end
  
  specify "should include Enumerable" do
    Sequel::Dataset.included_modules.should_include Enumerable
  end
end

context "A simple dataset" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should format a select statement" do
    @dataset.select_sql.should == 'SELECT * FROM test'
  end
  
  specify "should format a delete statement" do
    @dataset.delete_sql.should == 'DELETE FROM test'
  end
  
  specify "should format an insert statement" do
    @dataset.insert_sql.should == 'INSERT INTO test DEFAULT VALUES'
    @dataset.insert_sql(:name => 'wxyz', :price => 342).
      should_match /INSERT INTO test \(name, price\) VALUES \('wxyz', 342\)|INSERT INTO test \(price, name\) VALUES \(342, 'wxyz'\)/
    @dataset.insert_sql('a', 2, 6.5).should ==
      "INSERT INTO test VALUES ('a', 2, 6.5)"
  end
  
  specify "should format an update statement" do
    @dataset.update_sql(:name => 'abc').should ==
      "UPDATE test SET name = 'abc'"
  end
end

context "A dataset with multiple tables in its FROM clause" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:t1, :t2)
  end

  specify "should raise on #update_sql" do
    proc {@dataset.update_sql(:a=>1)}.should.raise
  end

  specify "should raise on #delete_sql" do
    proc {@dataset.delete_sql}.should.raise
  end

  specify "should generate a select query FROM all specified tables" do
    @dataset.select_sql.should == "SELECT * FROM t1, t2"
  end
end

context "Dataset#where" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test)
    @d1 = @dataset.where(:region => 'Asia')
    @d2 = @dataset.where('(region = ?)', 'Asia')
    @d3 = @dataset.where("(a = 1)")
  end
  
  specify "should work with hashes" do
    @dataset.where(:name => 'xyz', :price => 342).select_sql.
      should_match /WHERE \(name = 'xyz'\) AND \(price = 342\)|WHERE \(price = 342\) AND \(name = 'xyz'\)/
  end
  
  specify "should work with arrays (ala ActiveRecord)" do
    @dataset.where('price < ? AND id in (?)', 100, [1, 2, 3]).select_sql.should ==
      "SELECT * FROM test WHERE price < 100 AND id in (1, 2, 3)"
  end
  
  specify "should work with strings (custom SQL expressions)" do
    @dataset.where('(a = 1 AND b = 2)').select_sql.should ==
      "SELECT * FROM test WHERE (a = 1 AND b = 2)"
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
    @d1.where(:size => 'big').select_sql.should == 
      "SELECT * FROM test WHERE (region = 'Asia') AND (size = 'big')"
    
    # hash and string
    @d1.where('population > 1000').select_sql.should ==
      "SELECT * FROM test WHERE (region = 'Asia') AND (population > 1000)"
    @d1.where('(a > 1) OR (b < 2)').select_sql.should ==
    "SELECT * FROM test WHERE (region = 'Asia') AND ((a > 1) OR (b < 2))"
    
    # hash and array
    @d1.filter('(GDP > ?)', 1000).select_sql.should == 
      "SELECT * FROM test WHERE (region = 'Asia') AND (GDP > 1000)"
    
    # array and array
    @d2.filter('(GDP > ?)', 1000).select_sql.should ==
      "SELECT * FROM test WHERE (region = 'Asia') AND (GDP > 1000)"
    
    # array and hash
    @d2.filter(:name => ['Japan', 'China']).select_sql.should ==
      "SELECT * FROM test WHERE (region = 'Asia') AND (name IN ('Japan', 'China'))"
      
    # array and string
    @d2.filter('GDP > ?').select_sql.should ==
      "SELECT * FROM test WHERE (region = 'Asia') AND (GDP > ?)"
    
    # string and string
    @d3.filter('b = 2').select_sql.should ==
      "SELECT * FROM test WHERE (a = 1) AND (b = 2)"
    
    # string and hash
    @d3.filter(:c => 3).select_sql.should == 
      "SELECT * FROM test WHERE (a = 1) AND (c = 3)"
      
    # string and array
    @d3.filter('(d = ?)', 4).select_sql.should ==
      "SELECT * FROM test WHERE (a = 1) AND (d = 4)"
  end
  
  specify "should raise if the dataset is grouped" do
    proc {@dataset.group(:t).where(:a => 1)}.should.raise
  end
end

context "Dataset#having filter" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test)
    @grouped = @dataset.group(:region).select(:region, :population.SUM, :gdp.AVG)
    @d1 = @grouped.having('sum(population) > 10')
    @d2 = @grouped.having(:region => 'Asia')
    @fields = "region, sum(population), avg(gdp)"
  end

  specify "should raise if the dataset is not grouped" do
    proc {@dataset.having('avg(gdp) > 10')}.should.raise
  end

  specify "should affect select statements" do
    @d1.select_sql.should ==
      "SELECT #{@fields} FROM test GROUP BY region HAVING sum(population) > 10"
  end
end

context "a grouped dataset" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test).group(:type_id)
  end

  specify "should raise when trying to generate an update statement" do
    proc {@dataset.update_sql(:id => 0)}.should.raise
  end

  specify "should raise when trying to generate a delete statement" do
    proc {@dataset.delete_sql}.should.raise
  end

  specify "should specify the grouping in generated select statement" do
    @dataset.select_sql.should ==
      "SELECT * FROM test GROUP BY type_id"
  end
end


context "Dataset#literal" do
  setup do
    @dataset = Sequel::Dataset.new(nil)
  end
  
  specify "should escape strings properly" do
    @dataset.literal('abc').should == "'abc'"
    @dataset.literal('a"x"bc').should == "'a\"x\"bc'"
    @dataset.literal("a'bc").should == "'a''bc'"
    @dataset.literal("a''bc").should == "'a''''bc'"
  end
  
  specify "should literalize numbers properly" do
    @dataset.literal(1).should == "1"
    @dataset.literal(1.5).should == "1.5"
  end
  
  specify "should literalize nil as NULL" do
    @dataset.literal(nil).should == "NULL"
  end
  
  specify "should literalize an array properly" do
    @dataset.literal([]).should == "NULL"
    @dataset.literal([1, 'abc', 3]).should == "1, 'abc', 3"
    @dataset.literal([1, "a'b''c", 3]).should == "1, 'a''b''''c', 3"
  end
  
  specify "should literalize symbols as column references" do
    @dataset.literal(:name).should == "name"
    @dataset.literal(:items__name).should == "items.name"
  end
  
  specify "should raise an error for unsupported types" do
    proc {@dataset.literal({})}.should.raise
  end
  
  specify "should literalize datasets as subqueries" do
    @dataset.literal(@dataset).should == "(#{@dataset.sql})"
  end
end