require File.join(File.dirname(__FILE__), "spec_helper")

context "Dataset" do
  setup do
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
end

context "Dataset#clone" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:items)
  end
  
  specify "should create an exact copy of the dataset" do
    @c = Class.new
    @dataset.set_model(@c)
    @clone = @dataset.clone

    @clone.should_not === @dataset
    @clone.class.should == @dataset.class
    @clone.model_classes.should == @dataset.model_classes
  end
  
  specify "should deep-copy the dataset opts" do
    @clone = @dataset.clone

    @clone.opts.should_not equal(@dataset.opts)
    @dataset.filter!(:a => 'b')
    @clone.opts[:filter].should be_nil
  end
  
  specify "should return a clone self" do
    clone = @dataset.clone({})
    clone.class.should == @dataset.class
    clone.db.should == @dataset.db
    clone.opts.should == @dataset.opts
  end
  
  specify "should merge the specified options" do
    clone = @dataset.clone(1 => 2)
    clone.opts.should == {1 => 2, :from => [:items]}
  end
  
  specify "should overwrite existing options" do
    clone = @dataset.clone(:from => [:other])
    clone.opts.should == {:from => [:other]}
  end
  
  specify "should create a clone with a deep copy of options" do
    clone = @dataset.clone(:from => [:other])
    @dataset.opts[:from].should == [:items]
    clone.opts[:from].should == [:other]
  end
  
  specify "should return an object with the same modules included" do
    m = Module.new do
      def __xyz__; "xyz"; end
    end
    @dataset.extend(m)
    @dataset.clone({}).should respond_to(:__xyz__)
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
  
  specify "should format an insert statement with default values" do
    @dataset.insert_sql.should == 'INSERT INTO test DEFAULT VALUES'
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
  
  specify "should format an insert statement with an object that respond_to? :values" do
    dbb = Sequel::Database.new
    
    v = Object.new
    def v.values; {:a => 1}; end
    
    @dataset.insert_sql(v).should == "INSERT INTO test (a) VALUES (1)"
    
    def v.values; {}; end
    @dataset.insert_sql(v).should == "INSERT INTO test DEFAULT VALUES"
  end
  
  specify "should format an insert statement with an arbitrary value" do
    @dataset.insert_sql(123).should == "INSERT INTO test VALUES (123)"
  end
  
  specify "should format an insert statement with sub-query" do
    @sub = Sequel::Dataset.new(nil).from(:something).filter(:x => 2)
    @dataset.insert_sql(@sub).should == \
      "INSERT INTO test (SELECT * FROM something WHERE (x = 2))"
  end
  
  specify "should format an insert statement with array" do
    @dataset.insert_sql('a', 2, 6.5).should ==
      "INSERT INTO test VALUES ('a', 2, 6.5)"
  end
  
  specify "should format an update statement" do
    @dataset.update_sql(:name => 'abc').should ==
      "UPDATE test SET name = 'abc'"
  end

  specify "should be able to return rows for arbitrary SQL" do
    @dataset.select_sql(:sql => 'xxx yyy zzz').should ==
      "xxx yyy zzz"
  end
end

context "A dataset with multiple tables in its FROM clause" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:t1, :t2)
  end

  specify "should raise on #update_sql" do
    proc {@dataset.update_sql(:a=>1)}.should raise_error(Sequel::Error::InvalidOperation)
  end

  specify "should raise on #delete_sql" do
    proc {@dataset.delete_sql}.should raise_error(Sequel::Error::InvalidOperation)
  end

  specify "should generate a select query FROM all specified tables" do
    @dataset.select_sql.should == "SELECT * FROM t1, t2"
  end
end

context "Dataset#where" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test)
    @d1 = @dataset.where(:region => 'Asia')
    @d2 = @dataset.where('region = ?', 'Asia')
    @d3 = @dataset.where("a = 1")
  end
  
  specify "should work with hashes" do
    @dataset.where(:name => 'xyz', :price => 342).select_sql.
      should match(/WHERE \(\(name = 'xyz'\) AND \(price = 342\)\)|WHERE \(\(price = 342\) AND \(name = 'xyz'\)\)/)
  end
  
  specify "should work with arrays (ala ActiveRecord)" do
    @dataset.where('price < ? AND id in ?', 100, [1, 2, 3]).select_sql.should ==
      "SELECT * FROM test WHERE (price < 100 AND id in (1, 2, 3))"
  end
  
  specify "should work with strings (custom SQL expressions)" do
    @dataset.where('(a = 1 AND b = 2)').select_sql.should ==
      "SELECT * FROM test WHERE ((a = 1 AND b = 2))"
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
      "SELECT * FROM test WHERE ((region = 'Asia') AND (size = 'big'))"
    
    # hash and string
    @d1.where('population > 1000').select_sql.should ==
      "SELECT * FROM test WHERE ((region = 'Asia') AND (population > 1000))"
    @d1.where('(a > 1) OR (b < 2)').select_sql.should ==
    "SELECT * FROM test WHERE ((region = 'Asia') AND ((a > 1) OR (b < 2)))"
    
    # hash and array
    @d1.where('GDP > ?', 1000).select_sql.should == 
      "SELECT * FROM test WHERE ((region = 'Asia') AND (GDP > 1000))"
    
    # array and array
    @d2.where('GDP > ?', 1000).select_sql.should ==
      "SELECT * FROM test WHERE ((region = 'Asia') AND (GDP > 1000))"
    
    # array and hash
    @d2.where(:name => ['Japan', 'China']).select_sql.should ==
      "SELECT * FROM test WHERE ((region = 'Asia') AND (name IN ('Japan', 'China')))"
      
    # array and string
    @d2.where('GDP > ?').select_sql.should ==
      "SELECT * FROM test WHERE ((region = 'Asia') AND (GDP > ?))"
    
    # string and string
    @d3.where('b = 2').select_sql.should ==
      "SELECT * FROM test WHERE ((a = 1) AND (b = 2))"
    
    # string and hash
    @d3.where(:c => 3).select_sql.should == 
      "SELECT * FROM test WHERE ((a = 1) AND (c = 3))"
      
    # string and array
    @d3.where('d = ?', 4).select_sql.should ==
      "SELECT * FROM test WHERE ((a = 1) AND (d = 4))"
  end
      
  specify "should be composable using AND operator (for scoping) with block" do
    @d3.where{:e < 5}.select_sql.should ==
      "SELECT * FROM test WHERE ((a = 1) AND (e < 5))"
  end
  
  specify "should raise if the dataset is grouped" do
    proc {@dataset.group(:t).where(:a => 1)}.should_not raise_error
    @dataset.group(:t).where(:a => 1).sql.should ==
      "SELECT * FROM test WHERE (a = 1) GROUP BY t"
  end
  
  specify "should accept ranges" do
    @dataset.filter(:id => 4..7).sql.should ==
      'SELECT * FROM test WHERE ((id >= 4) AND (id <= 7))'
    @dataset.filter(:id => 4...7).sql.should ==
      'SELECT * FROM test WHERE ((id >= 4) AND (id < 7))'

    @dataset.filter(:table__id => 4..7).sql.should ==
      'SELECT * FROM test WHERE ((table.id >= 4) AND (table.id <= 7))'
    @dataset.filter(:table__id => 4...7).sql.should ==
      'SELECT * FROM test WHERE ((table.id >= 4) AND (table.id < 7))'
  end

  specify "should accept nil" do
    @dataset.filter(:owner_id => nil).sql.should ==
      'SELECT * FROM test WHERE (owner_id IS NULL)'
  end

  specify "should accept a subquery" do
    @dataset.filter('gdp > ?', @d1.select(:avg[:gdp])).sql.should ==
      "SELECT * FROM test WHERE (gdp > (SELECT avg(gdp) FROM test WHERE (region = 'Asia')))"

    @dataset.filter(:id => @d1.select(:id)).sql.should ==
      "SELECT * FROM test WHERE (id IN (SELECT id FROM test WHERE (region = 'Asia')))"
  end
  
  specify "should accept a subquery for an EXISTS clause" do
    a = @dataset.filter(:price < 100)
    @dataset.filter(a.exists).sql.should ==
      'SELECT * FROM test WHERE (EXISTS (SELECT * FROM test WHERE (price < 100)))'
  end
  
  specify "should accept proc expressions" do
    d = @d1.select(:avg[:gdp])
    @dataset.filter {:gdp > d}.sql.should ==
      "SELECT * FROM test WHERE (gdp > (SELECT avg(gdp) FROM test WHERE (region = 'Asia')))"
    
    @dataset.filter {:a < 1}.sql.should ==
      'SELECT * FROM test WHERE (a < 1)'

    @dataset.filter {(:a >= 1) & (:b <= 2)}.sql.should ==
      'SELECT * FROM test WHERE ((a >= 1) AND (b <= 2))'
      
    @dataset.filter {:c.like 'ABC%'}.sql.should ==
      "SELECT * FROM test WHERE (c LIKE 'ABC%')"

    @dataset.filter {:c.like 'ABC%'}.sql.should ==
      "SELECT * FROM test WHERE (c LIKE 'ABC%')"

    @dataset.filter {:c.like 'ABC%', '%XYZ'}.sql.should ==
      "SELECT * FROM test WHERE ((c LIKE 'ABC%') OR (c LIKE '%XYZ'))"
  end
  
  specify "should work for grouped datasets" do
    @dataset.group(:a).filter(:b => 1).sql.should ==
      'SELECT * FROM test WHERE (b = 1) GROUP BY a'
  end

  specify "should accept true and false as arguments" do
    @dataset.filter(true).sql.should ==
      "SELECT * FROM test WHERE 't'"
    @dataset.filter(false).sql.should ==
      "SELECT * FROM test WHERE 'f'"
  end

  specify "should allow the use of blocks and arguments simultaneously" do
    @dataset.filter(:zz < 3){:yy > 3}.sql.should ==
      'SELECT * FROM test WHERE ((zz < 3) AND (yy > 3))'
  end

  specify "should yield a VirtualRow to the block" do
    x = nil
    @dataset.filter{|r| x = r; false}
    x.should be_a_kind_of(Sequel::SQL::VirtualRow)
    @dataset.filter{|r| ((r.name < 'b') & {r.table__id => 1}) | r.is_active(r.blah, r.xx, r.x__y_z)}.sql.should ==
      "SELECT * FROM test WHERE (((name < 'b') AND (table.id = 1)) OR is_active(blah, xx, x.y_z))"
  end
end

context "Dataset#or" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test)
    @d1 = @dataset.where(:x => 1)
  end
  
  specify "should raise if no filter exists" do
    proc {@dataset.or(:a => 1)}.should raise_error(Sequel::Error)
  end
  
  specify "should add an alternative expression to the where clause" do
    @d1.or(:y => 2).sql.should == 
      'SELECT * FROM test WHERE ((x = 1) OR (y = 2))'
  end
  
  specify "should accept all forms of filters" do
    @d1.or('y > ?', 2).sql.should ==
      'SELECT * FROM test WHERE ((x = 1) OR (y > 2))'
    @d1.or(:yy > 3).sql.should ==
      'SELECT * FROM test WHERE ((x = 1) OR (yy > 3))'
  end    

  specify "should accept blocks passed to filter" do
    @d1.or{:yy > 3}.sql.should ==
      'SELECT * FROM test WHERE ((x = 1) OR (yy > 3))'
  end
  
  specify "should correctly add parens to give predictable results" do
    @d1.filter(:y => 2).or(:z => 3).sql.should == 
      'SELECT * FROM test WHERE (((x = 1) AND (y = 2)) OR (z = 3))'

    @d1.or(:y => 2).filter(:z => 3).sql.should == 
      'SELECT * FROM test WHERE (((x = 1) OR (y = 2)) AND (z = 3))'
  end

  specify "should allow the use of blocks and arguments simultaneously" do
    @d1.or(:zz < 3){:yy > 3}.sql.should ==
      'SELECT * FROM test WHERE ((x = 1) OR ((zz < 3) AND (yy > 3)))'
  end
end

context "Dataset#and" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test)
    @d1 = @dataset.where(:x => 1)
  end
  
  specify "should raise if no filter exists" do
    proc {@dataset.and(:a => 1)}.should raise_error(Sequel::Error)
    proc {@dataset.where(:a => 1).group(:t).and(:b => 2)}.should_not raise_error(Sequel::Error)
    @dataset.where(:a => 1).group(:t).and(:b => 2).sql ==
      "SELECT * FROM test WHERE (a = 1) AND (b = 2) GROUP BY t"
  end
  
  specify "should add an alternative expression to the where clause" do
    @d1.and(:y => 2).sql.should == 
      'SELECT * FROM test WHERE ((x = 1) AND (y = 2))'
  end
  
  specify "should accept all forms of filters" do
    # probably not exhaustive, but good enough
    @d1.and('y > ?', 2).sql.should ==
      'SELECT * FROM test WHERE ((x = 1) AND (y > 2))'
    @d1.and(:yy > 3).sql.should ==
      'SELECT * FROM test WHERE ((x = 1) AND (yy > 3))'
  end
      
  specify "should accept blocks passed to filter" do
    @d1.and {:yy > 3}.sql.should ==
      'SELECT * FROM test WHERE ((x = 1) AND (yy > 3))'
  end
  
  specify "should correctly add parens to give predictable results" do
    @d1.or(:y => 2).and(:z => 3).sql.should == 
      'SELECT * FROM test WHERE (((x = 1) OR (y = 2)) AND (z = 3))'

    @d1.and(:y => 2).or(:z => 3).sql.should == 
      'SELECT * FROM test WHERE (((x = 1) AND (y = 2)) OR (z = 3))'
  end
end

context "Dataset#exclude" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end

  specify "should correctly include the NOT operator when one condition is given" do
    @dataset.exclude(:region=>'Asia').select_sql.should ==
      "SELECT * FROM test WHERE (region != 'Asia')"
  end

  specify "should take multiple conditions as a hash and express the logic correctly in SQL" do
    @dataset.exclude(:region => 'Asia', :name => 'Japan').select_sql.
      should match(Regexp.union(/WHERE \(\(region != 'Asia'\) AND \(name != 'Japan'\)\)/,
                                /WHERE \(\(name != 'Japan'\) AND \(region != 'Asia'\)\)/))
  end

  specify "should parenthesize a single string condition correctly" do
    @dataset.exclude("region = 'Asia' AND name = 'Japan'").select_sql.should ==
      "SELECT * FROM test WHERE NOT (region = 'Asia' AND name = 'Japan')"
  end

  specify "should parenthesize an array condition correctly" do
    @dataset.exclude('region = ? AND name = ?', 'Asia', 'Japan').select_sql.should ==
      "SELECT * FROM test WHERE NOT (region = 'Asia' AND name = 'Japan')"
  end

  specify "should correctly parenthesize when it is used twice" do
    @dataset.exclude(:region => 'Asia').exclude(:name => 'Japan').select_sql.should ==
      "SELECT * FROM test WHERE ((region != 'Asia') AND (name != 'Japan'))"
  end
  
  specify "should support proc expressions" do
    @dataset.exclude{:id < 6}.sql.should == 
      'SELECT * FROM test WHERE (id >= 6)'
  end
  
  specify "should allow the use of blocks and arguments simultaneously" do
    @dataset.exclude(:id => (7..11)){:id < 6}.sql.should == 
      'SELECT * FROM test WHERE (((id < 7) OR (id > 11)) OR (id >= 6))'
  end
end

context "Dataset#invert" do
  setup do
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

context "Dataset#having" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test)
    @grouped = @dataset.group(:region).select(:region, :sum[:population], :avg[:gdp])
    @d1 = @grouped.having('sum(population) > 10')
    @d2 = @grouped.having(:region => 'Asia')
    @columns = "region, sum(population), avg(gdp)"
  end

  specify "should raise if the dataset is not grouped" do
    proc {@dataset.having('avg(gdp) > 10')}.should raise_error(Sequel::Error::InvalidOperation)
  end

  specify "should affect select statements" do
    @d1.select_sql.should ==
      "SELECT #{@columns} FROM test GROUP BY region HAVING (sum(population) > 10)"
  end

  specify "should support proc expressions" do
    @grouped.having {:sum[:population] > 10}.sql.should == 
      "SELECT #{@columns} FROM test GROUP BY region HAVING (sum(population) > 10)"
  end

  specify "should work with and on the having clause" do
    @grouped.having( :a > 1 ).and( :b < 2 ).sql.should ==
      "SELECT #{@columns} FROM test GROUP BY region HAVING ((a > 1) AND (b < 2))"
  end
end

context "a grouped dataset" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test).group(:type_id)
  end

  specify "should raise when trying to generate an update statement" do
    proc {@dataset.update_sql(:id => 0)}.should raise_error
  end

  specify "should raise when trying to generate a delete statement" do
    proc {@dataset.delete_sql}.should raise_error
  end

  specify "should specify the grouping in generated select statement" do
    @dataset.select_sql.should ==
      "SELECT * FROM test GROUP BY type_id"
  end
  
  specify "should format the right statement for counting (as a subquery)" do
    db = MockDatabase.new
    db[:test].select(:name).group(:name).count
    db.sqls.should == ["SELECT COUNT(*) FROM (SELECT name FROM test GROUP BY name) t1 LIMIT 1"]
  end
end

context "Dataset#group_by" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test).group_by(:type_id)
  end

  specify "should raise when trying to generate an update statement" do
    proc {@dataset.update_sql(:id => 0)}.should raise_error
  end

  specify "should raise when trying to generate a delete statement" do
    proc {@dataset.delete_sql}.should raise_error
  end

  specify "should specify the grouping in generated select statement" do
    @dataset.select_sql.should ==
      "SELECT * FROM test GROUP BY type_id"
    @dataset.group_by(:a, :b).select_sql.should ==
      "SELECT * FROM test GROUP BY a, b"
  end

  specify "should specify the grouping in generated select statement" do
    @dataset.group_by(:type_id=>nil).select_sql.should ==
      "SELECT * FROM test GROUP BY (type_id IS NULL)"
  end

  specify "should be aliased as #group" do
    @dataset.group(:type_id=>nil).select_sql.should ==
      "SELECT * FROM test GROUP BY (type_id IS NULL)"
  end
end

context "Dataset#as" do
  specify "should set up an alias" do
    dataset = Sequel::Dataset.new(nil).from(:test)
    dataset.select(dataset.limit(1).select(:name).as(:n)).sql.should == \
      'SELECT (SELECT name FROM test LIMIT 1) AS n FROM test'
  end
end

context "Dataset#literal" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should escape strings properly" do
    @dataset.literal('abc').should == "'abc'"
    @dataset.literal('a"x"bc').should == "'a\"x\"bc'"
    @dataset.literal("a'bc").should == "'a''bc'"
    @dataset.literal("a''bc").should == "'a''''bc'"
    @dataset.literal("a\\bc").should == "'a\\\\bc'"
    @dataset.literal("a\\\\bc").should == "'a\\\\\\\\bc'"
    @dataset.literal("a\\'bc").should == "'a\\\\''bc'"
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
  end
  
  specify "should raise an error for unsupported types" do
    proc {@dataset.literal({})}.should raise_error
  end
  
  specify "should literalize datasets as subqueries" do
    d = @dataset.from(:test)
    d.literal(d).should == "(#{d.sql})"
  end
  
  specify "should literalize Time properly" do
    t = Time.now
    s = t.strftime("TIMESTAMP '%Y-%m-%d %H:%M:%S'")
    @dataset.literal(t).should == s
  end
  
  specify "should literalize Date properly" do
    d = Date.today
    s = d.strftime("DATE '%Y-%m-%d'")
    @dataset.literal(d).should == s
  end
  
  specify "should not literalize expression strings" do
    @dataset.literal('col1 + 2'.expr).should == 'col1 + 2'
    
    @dataset.update_sql(:a => 'a + 2'.expr).should == 
      'UPDATE test SET a = a + 2'
  end

  specify "should literalize BigDecimal instances correctly" do
    @dataset.literal(BigDecimal.new("80")).should == "80.0"
  end
end

context "Dataset#from" do
  setup do
    @dataset = Sequel::Dataset.new(nil)
  end

  specify "should accept a Dataset" do
    proc {@dataset.from(@dataset)}.should_not raise_error
  end

  specify "should format a Dataset as a subquery if it has had options set" do
    @dataset.from(@dataset.from(:a).where(:a=>1)).select_sql.should ==
      "SELECT * FROM (SELECT * FROM a WHERE (a = 1)) t1"
  end
  
  specify "should automatically alias sub-queries" do
    @dataset.from(@dataset.from(:a).group(:b)).select_sql.should ==
      "SELECT * FROM (SELECT * FROM a GROUP BY b) t1"
      
    d1 = @dataset.from(:a).group(:b)
    d2 = @dataset.from(:c).group(:d)
    
    @dataset.from(d1, d2).sql.should == 
      "SELECT * FROM (SELECT * FROM a GROUP BY b) t1, (SELECT * FROM c GROUP BY d) t2"
  end
  
  specify "should accept a hash for aliasing" do
    @dataset.from(:a => :b).sql.should ==
      "SELECT * FROM a b"
      
    @dataset.from(:a => 'b').sql.should ==
      "SELECT * FROM a b"

    @dataset.from(:a => :c[:d]).sql.should ==
      "SELECT * FROM a c(d)"

    @dataset.from(@dataset.from(:a).group(:b) => :c).sql.should ==
      "SELECT * FROM (SELECT * FROM a GROUP BY b) c"
  end

  specify "should always use a subquery if given a dataset" do
    @dataset.from(@dataset.from(:a)).select_sql.should ==
      "SELECT * FROM (SELECT * FROM a) t1"
  end
  
  specify "should raise if no source is given" do
    proc {@dataset.from(@dataset.from).select_sql}.should raise_error(Sequel::Error)
  end
  
  specify "should accept sql functions" do
    @dataset.from(:abc[:def]).select_sql.should ==
      "SELECT * FROM abc(def)"
    
    @dataset.from(:a[:i]).select_sql.should ==
      "SELECT * FROM a(i)"
  end
end

context "Dataset#select" do
  setup do
    @d = Sequel::Dataset.new(nil).from(:test)
  end

  specify "should accept variable arity" do
    @d.select(:name).sql.should == 'SELECT name FROM test'
    @d.select(:a, :b, :test__c).sql.should == 'SELECT a, b, test.c FROM test'
  end
  
  specify "should accept symbols and literal strings" do
    @d.select('aaa'.lit).sql.should == 'SELECT aaa FROM test'
    @d.select(:a, 'b'.lit).sql.should == 'SELECT a, b FROM test'
    @d.select(:test__cc, 'test.d AS e'.lit).sql.should == 
      'SELECT test.cc, test.d AS e FROM test'
    @d.select('test.d AS e'.lit, :test__cc).sql.should == 
      'SELECT test.d AS e, test.cc FROM test'

    # symbol helpers      
    @d.select(:test.*).sql.should ==
      'SELECT test.* FROM test'
    @d.select(:test__name.as(:n)).sql.should ==
      'SELECT test.name AS n FROM test'
    @d.select(:test__name___n).sql.should ==
      'SELECT test.name AS n FROM test'
  end

  specify "should use the wildcard if no arguments are given" do
    @d.select.sql.should == 'SELECT * FROM test'
  end
  
  specify "should accept a hash for AS values" do
    @d.select(:name => 'n', :__ggh => 'age').sql.should =~
      /SELECT ((name AS n, __ggh AS age)|(__ggh AS age, name AS n)) FROM test/
  end

  specify "should overrun the previous select option" do
    @d.select!(:a, :b, :c).select.sql.should == 'SELECT * FROM test'
    @d.select!(:price).select(:name).sql.should == 'SELECT name FROM test'
  end
  
  specify "should accept arbitrary objects and literalize them correctly" do
    @d.select(1, :a, 't').sql.should == "SELECT 1, a, 't' FROM test"

    @d.select(nil, :sum[:t], :x___y).sql.should == "SELECT NULL, sum(t), x AS y FROM test"

    @d.select(nil, 1, :x => :y).sql.should == "SELECT NULL, 1, x AS y FROM test"
  end
end

context "Dataset#select_all" do
  setup do
    @d = Sequel::Dataset.new(nil).from(:test)
  end

  specify "should select the wildcard" do
    @d.select_all.sql.should == 'SELECT * FROM test'
  end
  
  specify "should overrun the previous select option" do
    @d.select!(:a, :b, :c).select_all.sql.should == 'SELECT * FROM test'
  end
end

context "Dataset#select_more" do
  setup do
    @d = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should act like #select for datasets with no selection" do
    @d.select_more(:a, :b).sql.should == 'SELECT a, b FROM test'
    @d.select_all.select_more(:a, :b).sql.should == 'SELECT a, b FROM test'
    @d.select(:blah).select_all.select_more(:a, :b).sql.should == 'SELECT a, b FROM test'
  end

  specify "should add to the currently selected columns" do
    @d.select(:a).select_more(:b).sql.should == 'SELECT a, b FROM test'
    @d.select(:a.*).select_more(:b.*).sql.should == 'SELECT a.*, b.* FROM test'
  end
end

context "Dataset#order" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should include an ORDER BY clause in the select statement" do
    @dataset.order(:name).sql.should == 
      'SELECT * FROM test ORDER BY name'
  end
  
  specify "should accept multiple arguments" do
    @dataset.order(:name, :price.desc).sql.should ==
      'SELECT * FROM test ORDER BY name, price DESC'
  end
  
  specify "should overrun a previous ordering" do
    @dataset.order(:name).order(:stamp).sql.should ==
      'SELECT * FROM test ORDER BY stamp'
  end
  
  specify "should accept a literal string" do
    @dataset.order('dada ASC'.lit).sql.should ==
      'SELECT * FROM test ORDER BY dada ASC'
  end
  
  specify "should accept a hash as an expression" do
    @dataset.order(:name=>nil).sql.should ==
      'SELECT * FROM test ORDER BY (name IS NULL)'
  end
  
  specify "should accept a nil to remove ordering" do
    @dataset.order(:bah).order(nil).sql.should == 
      'SELECT * FROM test'
  end
end

context "Dataset#unfiltered" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should remove filtering from the dataset" do
    @dataset.filter(:score=>1).unfiltered.sql.should ==
      'SELECT * FROM test'
  end
end

context "Dataset#unordered" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should remove ordering from the dataset" do
    @dataset.order(:name).unordered.sql.should ==
      'SELECT * FROM test'
  end
end

context "Dataset#order_by" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should include an ORDER BY clause in the select statement" do
    @dataset.order_by(:name).sql.should == 
      'SELECT * FROM test ORDER BY name'
  end
  
  specify "should accept multiple arguments" do
    @dataset.order_by(:name, :price.desc).sql.should ==
      'SELECT * FROM test ORDER BY name, price DESC'
  end
  
  specify "should overrun a previous ordering" do
    @dataset.order_by(:name).order(:stamp).sql.should ==
      'SELECT * FROM test ORDER BY stamp'
  end
  
  specify "should accept a string" do
    @dataset.order_by('dada ASC'.lit).sql.should ==
      'SELECT * FROM test ORDER BY dada ASC'
  end

  specify "should accept a nil to remove ordering" do
    @dataset.order_by(:bah).order_by(nil).sql.should == 
      'SELECT * FROM test'
  end
end

context "Dataset#order_more" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should include an ORDER BY clause in the select statement" do
    @dataset.order_more(:name).sql.should == 
      'SELECT * FROM test ORDER BY name'
  end
  
  specify "should add to a previous ordering" do
    @dataset.order(:name).order_more(:stamp.desc).sql.should ==
      'SELECT * FROM test ORDER BY name, stamp DESC'
  end
end

context "Dataset#reverse_order" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should use DESC as default order" do
    @dataset.reverse_order(:name).sql.should == 
      'SELECT * FROM test ORDER BY name DESC'
  end
  
  specify "should invert the order given" do
    @dataset.reverse_order(:name.desc).sql.should ==
      'SELECT * FROM test ORDER BY name ASC'
  end
  
  specify "should invert the order for ASC expressions" do
    @dataset.reverse_order(:name.asc).sql.should ==
      'SELECT * FROM test ORDER BY name DESC'
  end
  
  specify "should accept multiple arguments" do
    @dataset.reverse_order(:name, :price.desc).sql.should ==
      'SELECT * FROM test ORDER BY name DESC, price ASC'
  end

  specify "should reverse a previous ordering if no arguments are given" do
    @dataset.order(:name).reverse_order.sql.should ==
      'SELECT * FROM test ORDER BY name DESC'
    @dataset.order(:clumsy.desc, :fool).reverse_order.sql.should ==
      'SELECT * FROM test ORDER BY clumsy ASC, fool DESC'
  end
  
  specify "should return an unordered dataset for a dataset with no order" do
    @dataset.unordered.reverse_order.sql.should == 
      'SELECT * FROM test'
  end
  
  specify "should have #reverse alias" do
    @dataset.order(:name).reverse.sql.should ==
      'SELECT * FROM test ORDER BY name DESC'
  end
end

context "Dataset#limit" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should include a LIMIT clause in the select statement" do
    @dataset.limit(10).sql.should == 
      'SELECT * FROM test LIMIT 10'
  end
  
  specify "should accept ranges" do
    @dataset.limit(3..7).sql.should ==
      'SELECT * FROM test LIMIT 5 OFFSET 3'
      
    @dataset.limit(3...7).sql.should ==
      'SELECT * FROM test LIMIT 4 OFFSET 3'
  end
  
  specify "should include an offset if a second argument is given" do
    @dataset.limit(6, 10).sql.should ==
      'SELECT * FROM test LIMIT 6 OFFSET 10'
  end
  
  specify "should work with fixed sql datasets" do
    @dataset.opts[:sql] = 'select * from cccc'
    @dataset.limit(6, 10).sql.should ==
      'SELECT * FROM (select * from cccc) t1 LIMIT 6 OFFSET 10'
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

context "Dataset#naked" do
  setup do
    @d1 = Sequel::Dataset.new(nil, {1 => 2, 3 => 4})
    @d2 = Sequel::Dataset.new(nil, {1 => 2, 3 => 4}).set_model(Object)
  end
  
  specify "should return a clone with :naked option set" do
    naked = @d1.naked
    naked.opts[:naked].should be_true
  end
  
  specify "should remove any existing reference to a model class" do
    naked = @d2.naked
    naked.opts[:models].should be_nil
  end
end

context "Dataset#qualified_column_name" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should return the literal value if not given a symbol" do
    @dataset.literal(@dataset.send(:qualified_column_name, 'ccc__b', :items)).should == "'ccc__b'"
    @dataset.literal(@dataset.send(:qualified_column_name, 3, :items)).should == '3'
    @dataset.literal(@dataset.send(:qualified_column_name, 'a'.lit, :items)).should == 'a'
  end
  
  specify "should qualify the column with the supplied table name if given an unqualified symbol" do
    @dataset.literal(@dataset.send(:qualified_column_name, :b1, :items)).should == 'items.b1'
  end

  specify "should not changed the qualifed column's table if given a qualified symbol" do
    @dataset.literal(@dataset.send(:qualified_column_name, :ccc__b, :items)).should == 'ccc.b'
  end
end

class DummyDataset < Sequel::Dataset
  VALUES = [
    {:a => 1, :b => 2},
    {:a => 3, :b => 4},
    {:a => 5, :b => 6}
  ]
  def fetch_rows(sql, &block)
    VALUES.each(&block)
  end
end

context "Dataset#map" do
  setup do
    @d = DummyDataset.new(nil).from(:items)
  end
  
  specify "should provide the usual functionality if no argument is given" do
    @d.map {|n| n[:a] + n[:b]}.should == [3, 7, 11]
  end
  
  specify "should map using #[column name] if column name is given" do
    @d.map(:a).should == [1, 3, 5]
  end
  
  specify "should return the complete dataset values if nothing is given" do
    @d.map.to_a.should == DummyDataset::VALUES
  end
end

context "Dataset#to_hash" do
  setup do
    @d = DummyDataset.new(nil).from(:items)
  end
  
  specify "should provide a hash with the first column as key and the second as value" do
    @d.to_hash(:a, :b).should == {1 => 2, 3 => 4, 5 => 6}
    @d.to_hash(:b, :a).should == {2 => 1, 4 => 3, 6 => 5}
  end
  
  specify "should provide a hash with the first column as key and the entire hash as value if the value column is blank or nil" do
    @d.to_hash(:a).should == {1 => {:a => 1, :b => 2}, 3 => {:a => 3, :b => 4}, 5 => {:a => 5, :b => 6}}
    @d.to_hash(:b).should == {2 => {:a => 1, :b => 2}, 4 => {:a => 3, :b => 4}, 6 => {:a => 5, :b => 6}}
  end
end

context "Dataset#uniq" do
  setup do
    @db = MockDatabase.new
    @dataset = @db[:test].select(:name)
  end
  
  specify "should include DISTINCT clause in statement" do
    @dataset.uniq.sql.should == 'SELECT DISTINCT name FROM test'
  end
  
  specify "should be aliased by Dataset#distinct" do
    @dataset.distinct.sql.should == 'SELECT DISTINCT name FROM test'
  end
  
  specify "should accept an expression list" do
    @dataset.uniq(:a, :b).sql.should == 'SELECT DISTINCT ON (a, b) name FROM test'

    @dataset.uniq(:stamp.cast_as(:integer), :node_id=>nil).sql.should == 'SELECT DISTINCT ON (cast(stamp AS integer), (node_id IS NULL)) name FROM test'
  end

  specify "should do a subselect for count" do
    @dataset.uniq.count
    @db.sqls.should == ['SELECT COUNT(*) FROM (SELECT DISTINCT name FROM test) t1 LIMIT 1']
  end
end

context "Dataset#count" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      def self.sql
        @@sql
      end
      
      def fetch_rows(sql)
        @@sql = sql
        yield({1 => 1})
      end
    end
    @dataset = @c.new(nil).from(:test)
  end
  
  specify "should format SQL properly" do
    @dataset.count.should == 1
    @c.sql.should == 'SELECT COUNT(*) FROM test LIMIT 1'
  end
  
  specify "should be aliased by #size" do
    @dataset.size.should == 1
  end
  
  specify "should include the where clause if it's there" do
    @dataset.filter(:abc < 30).count.should == 1
    @c.sql.should == 'SELECT COUNT(*) FROM test WHERE (abc < 30) LIMIT 1'
  end
  
  specify "should count properly for datasets with fixed sql" do
    @dataset.opts[:sql] = "select abc from xyz"
    @dataset.count.should == 1
    @c.sql.should == "SELECT COUNT(*) FROM (select abc from xyz) t1 LIMIT 1"
  end

  specify "should return limit if count is greater than it" do
    @dataset.limit(5).count.should == 1
    @c.sql.should == "SELECT COUNT(*) FROM (SELECT * FROM test LIMIT 5) t1 LIMIT 1"
  end
end


context "Dataset#group_and_count" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      def self.sql
        @@sql
      end
      
      def fetch_rows(sql)
        @@sql = sql
        yield({1 => 1})
      end
    end
    @ds = @c.new(nil).from(:test)
  end
  
  specify "should format SQL properly" do
    @ds.group_and_count(:name).sql.should == 
      "SELECT name, count(*) AS count FROM test GROUP BY name ORDER BY count"
  end

  specify "should accept multiple columns for grouping" do
    @ds.group_and_count(:a, :b).sql.should == 
      "SELECT a, b, count(*) AS count FROM test GROUP BY a, b ORDER BY count"
  end
  
  specify "should work within query block" do
    @ds.query{group_and_count(:a, :b)}.sql.should == 
      "SELECT a, b, count(*) AS count FROM test GROUP BY a, b ORDER BY count"
  end
end

context "Dataset#empty?" do
  specify "should return true if records exist in the dataset" do
    @c = Class.new(Sequel::Dataset) do
      def self.sql
        @@sql
      end
      
      def fetch_rows(sql)
        @@sql = sql
        yield({1 => 1}) unless sql =~ /WHERE 'f'/
      end
    end
    @c.new(nil).from(:test).should_not be_empty
    @c.sql.should == 'SELECT 1 FROM test LIMIT 1'
    @c.new(nil).from(:test).filter(false).should be_empty
    @c.sql.should == "SELECT 1 FROM test WHERE 'f' LIMIT 1"
  end
end

context "Dataset#join_table" do
  setup do
    @d = MockDataset.new(nil).from(:items)
    @d.quote_identifiers = true
  end
  
  specify "should format the JOIN clause properly" do
    @d.join_table(:left_outer, :categories, :category_id => :id).sql.should ==
      'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."category_id" = "items"."id")'
  end
  
  specify "should handle multiple conditions on the same join table column" do
    @d.join_table(:left_outer, :categories, [[:category_id, :id], [:category_id, 0..100]]).sql.should ==
      'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON (("categories"."category_id" = "items"."id") AND (("categories"."category_id" >= 0) AND ("categories"."category_id" <= 100)))'
  end
  
  specify "should include WHERE clause if applicable" do
    @d.filter(:price < 100).join_table(:right_outer, :categories, :category_id => :id).sql.should ==
      'SELECT * FROM "items" RIGHT OUTER JOIN "categories" ON ("categories"."category_id" = "items"."id") WHERE ("price" < 100)'
  end
  
  specify "should include ORDER BY clause if applicable" do
    @d.order(:stamp).join_table(:full_outer, :categories, :category_id => :id).sql.should ==
      'SELECT * FROM "items" FULL OUTER JOIN "categories" ON ("categories"."category_id" = "items"."id") ORDER BY "stamp"'
  end
  
  specify "should support multiple joins" do
    @d.join_table(:inner, :b, :items_id=>:id).join_table(:left_outer, :c, :b_id => :b__id).sql.should ==
      'SELECT * FROM "items" INNER JOIN "b" ON ("b"."items_id" = "items"."id") LEFT OUTER JOIN "c" ON ("c"."b_id" = "b"."id")'
  end
  
  specify "should support left outer joins" do
    @d.join_table(:left_outer, :categories, :category_id=>:id).sql.should ==
      'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."category_id" = "items"."id")'

    @d.left_outer_join(:categories, :category_id=>:id).sql.should ==
      'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."category_id" = "items"."id")'
  end

  specify "should support right outer joins" do
    @d.join_table(:right_outer, :categories, :category_id=>:id).sql.should ==
      'SELECT * FROM "items" RIGHT OUTER JOIN "categories" ON ("categories"."category_id" = "items"."id")'

    @d.right_outer_join(:categories, :category_id=>:id).sql.should ==
      'SELECT * FROM "items" RIGHT OUTER JOIN "categories" ON ("categories"."category_id" = "items"."id")'
  end

  specify "should support full outer joins" do
    @d.join_table(:full_outer, :categories, :category_id=>:id).sql.should ==
      'SELECT * FROM "items" FULL OUTER JOIN "categories" ON ("categories"."category_id" = "items"."id")'

    @d.full_outer_join(:categories, :category_id=>:id).sql.should ==
      'SELECT * FROM "items" FULL OUTER JOIN "categories" ON ("categories"."category_id" = "items"."id")'
  end

  specify "should support inner joins" do
    @d.join_table(:inner, :categories, :category_id=>:id).sql.should ==
      'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."category_id" = "items"."id")'

    @d.inner_join(:categories, :category_id=>:id).sql.should ==
      'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."category_id" = "items"."id")'
  end
  
  specify "should default to a plain join if nil is used for the type" do
    @d.join_table(nil, :categories, :category_id=>:id).sql.should ==
      'SELECT * FROM "items"  JOIN "categories" ON ("categories"."category_id" = "items"."id")'
  end

  specify "should use an inner join for Dataset#join" do
    @d.join(:categories, :category_id=>:id).sql.should ==
      'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."category_id" = "items"."id")'
  end
  
  specify "should support aliased tables" do
    @d.from('stats').join('players', {:id => :player_id}, 'p').sql.should ==
      'SELECT * FROM "stats" INNER JOIN "players" AS "p" ON ("p"."id" = "stats"."player_id")'

    ds = MockDataset.new(nil).from(:foo => :f)
    ds.quote_identifiers = true
    ds.join_table(:inner, :bar, :id => :bar_id).sql.should ==
      'SELECT * FROM "foo" "f" INNER JOIN "bar" ON ("bar"."id" = "f"."bar_id")'
  end
  
  specify "should allow for arbitrary conditions in the JOIN clause" do
    @d.join_table(:left_outer, :categories, :status => 0).sql.should ==
      'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."status" = 0)'
    @d.join_table(:left_outer, :categories, :categorizable_type => "Post").sql.should ==
      'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."categorizable_type" = \'Post\')'
    @d.join_table(:left_outer, :categories, :timestamp => "CURRENT_TIMESTAMP".lit).sql.should ==
      'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."timestamp" = CURRENT_TIMESTAMP)'
    @d.join_table(:left_outer, :categories, :status => [1, 2, 3]).sql.should ==
      'SELECT * FROM "items" LEFT OUTER JOIN "categories" ON ("categories"."status" IN (1, 2, 3))'
  end
  
  specify "should raise error for a table without a source" do
    proc {Sequel::Dataset.new(nil).join('players', :id => :player_id)}. \
      should raise_error(Sequel::Error)
  end

  specify "should support joining datasets" do
    ds = Sequel::Dataset.new(nil).from(:categories)
    
    @d.join_table(:left_outer, ds, :item_id => :id).sql.should ==
      'SELECT * FROM "items" LEFT OUTER JOIN (SELECT * FROM categories) AS "t1" ON ("t1"."item_id" = "items"."id")'
      
    ds.filter!(:active => true)

    @d.join_table(:left_outer, ds, :item_id => :id).sql.should ==
      'SELECT * FROM "items" LEFT OUTER JOIN (SELECT * FROM categories WHERE (active = \'t\')) AS "t1" ON ("t1"."item_id" = "items"."id")'
  end
  
  specify "should support joining datasets and aliasing the join" do
    ds = Sequel::Dataset.new(nil).from(:categories)
    
    @d.join_table(:left_outer, ds, {:ds__item_id => :id}, :ds).sql.should ==
      'SELECT * FROM "items" LEFT OUTER JOIN (SELECT * FROM categories) AS "ds" ON ("ds"."item_id" = "items"."id")'      
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

  specify "should support joining objects that respond to :table_name" do
    ds = Object.new
    def ds.table_name; :categories end
    
    @d.join(ds, :item_id => :id).sql.should ==
      'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."item_id" = "items"."id")'
  end
  
  specify "should support using a SQL String as the join condition" do
    @d.join(:categories, %{c.item_id = items.id}, :c).sql.should ==
      'SELECT * FROM "items" INNER JOIN "categories" AS "c" ON (c.item_id = items.id)'
  end
  
  specify "should support using a boolean column as the join condition" do
    @d.join(:categories, :active).sql.should ==
      'SELECT * FROM "items" INNER JOIN "categories" ON "active"'
  end

  specify "should support using an expression as the join condition" do
    @d.join(:categories, :number > 10).sql.should ==
      'SELECT * FROM "items" INNER JOIN "categories" ON ("number" > 10)'
  end

  specify "should support natural and cross joins using nil" do
    @d.join_table(:natural, :categories).sql.should ==
      'SELECT * FROM "items" NATURAL JOIN "categories"'
    @d.join_table(:cross, :categories, nil).sql.should ==
      'SELECT * FROM "items" CROSS JOIN "categories"'
    @d.join_table(:natural, :categories, nil, :c).sql.should ==
      'SELECT * FROM "items" NATURAL JOIN "categories" AS "c"'
  end

  specify "should support joins with a USING clause if an array of symbols is used" do
    @d.join(:categories, [:id]).sql.should ==
      'SELECT * FROM "items" INNER JOIN "categories" USING ("id")'
    @d.join(:categories, [:id1, :id2]).sql.should ==
      'SELECT * FROM "items" INNER JOIN "categories" USING ("id1", "id2")'
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
    @d.join(:categories){|j,lj,js| {:b.qualify(j)=>:c.qualify(lj)}}.sql.should ==
      'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."b" = "items"."c")'
    @d.join(:categories){|j,lj,js| :b.qualify(j) > :c.qualify(lj)}.sql.should ==
      'SELECT * FROM "items" INNER JOIN "categories" ON ("categories"."b" > "items"."c")'
  end

  specify "should combine the block conditions and argument conditions if both given" do
    @d.join(:categories, :a=>:d){|j,lj,js| {:b.qualify(j)=>:c.qualify(lj)}}.sql.should ==
      'SELECT * FROM "items" INNER JOIN "categories" ON (("categories"."a" = "items"."d") AND ("categories"."b" = "items"."c"))'
    @d.join(:categories, :a=>:d){|j,lj,js| :b.qualify(j) > :c.qualify(lj)}.sql.should ==
      'SELECT * FROM "items" INNER JOIN "categories" ON (("categories"."a" = "items"."d") AND ("categories"."b" > "items"."c"))'
  end
end

context "Dataset#[]=" do
  setup do
    c = Class.new(Sequel::Dataset) do
      def last_sql
        @@last_sql
      end
      
      def update(*args)
        @@last_sql = update_sql(*args)
      end
    end
    
    @d = c.new(nil).from(:items)
  end
  
  specify "should perform an update on the specified filter" do
    @d[:a => 1] = {:x => 3}
    @d.last_sql.should == 'UPDATE items SET x = 3 WHERE (a = 1)'
  end
end

context "Dataset#set" do
  setup do
    c = Class.new(Sequel::Dataset) do
      def last_sql
        @@last_sql
      end
      
      def update(*args, &block)
        @@last_sql = update_sql(*args, &block)
      end
    end
    
    @d = c.new(nil).from(:items)
  end
  
  specify "should act as alias to #update" do
    @d.set({:x => 3})
    @d.last_sql.should == 'UPDATE items SET x = 3'
  end
end


context "Dataset#insert_multiple" do
  setup do
    c = Class.new(Sequel::Dataset) do
      attr_reader :inserts
      def insert(arg)
        @inserts ||= []
        @inserts << arg
      end
    end
    
    @d = c.new(nil)
  end
  
  specify "should insert all items in the supplied array" do
    @d.insert_multiple [:aa, 5, 3, {1 => 2}]
    @d.inserts.should == [:aa, 5, 3, {1 => 2}]
  end
  
  specify "should pass array items through the supplied block if given" do
    a = ["inevitable", "hello", "the ticking clock"]
    @d.insert_multiple(a) {|i| i.gsub('l', 'r')}
    @d.inserts.should == ["inevitabre", "herro", "the ticking crock"]
  end
end

context "Dataset aggregate methods" do
  setup do
    c = Class.new(Sequel::Dataset) do
      def fetch_rows(sql)
        yield({1 => sql})
      end
    end
    @d = c.new(nil).from(:test)
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
end

context "Dataset#range" do
  setup do
    c = Class.new(Sequel::Dataset) do
      @@sql = nil
      
      def last_sql; @@sql; end
      
      def fetch_rows(sql)
        @@sql = sql
        yield(:v1 => 1, :v2 => 10)
      end
    end
    @d = c.new(nil).from(:test)
  end
  
  specify "should generate a correct SQL statement" do
    @d.range(:stamp)
    @d.last_sql.should == "SELECT min(stamp) AS v1, max(stamp) AS v2 FROM test LIMIT 1"

    @d.filter(:price > 100).range(:stamp)
    @d.last_sql.should == "SELECT min(stamp) AS v1, max(stamp) AS v2 FROM test WHERE (price > 100) LIMIT 1"
  end
  
  specify "should return a range object" do
    @d.range(:tryme).should == (1..10)
  end
end

context "Dataset#interval" do
  setup do
    c = Class.new(Sequel::Dataset) do
      @@sql = nil
      
      def last_sql; @@sql; end
      
      def fetch_rows(sql)
        @@sql = sql
        yield(:v => 1234)
      end
    end
    @d = c.new(nil).from(:test)
  end
  
  specify "should generate a correct SQL statement" do
    @d.interval(:stamp)
    @d.last_sql.should == "SELECT (max(stamp) - min(stamp)) FROM test LIMIT 1"

    @d.filter(:price > 100).interval(:stamp)
    @d.last_sql.should == "SELECT (max(stamp) - min(stamp)) FROM test WHERE (price > 100) LIMIT 1"
  end
  
  specify "should return an integer" do
    @d.interval(:tryme).should == 1234
  end
end

context "Dataset #first and #last" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      def each(opts = nil, &block)
        s = select_sql(opts)
        x = [:a,1,:b,2,s]
        i = /LIMIT (\d+)/.match(s)[1].to_i.times{yield x}
      end
    end
    @d = @c.new(nil).from(:test)
  end
  
  specify "should return a single record if no argument is given" do
    @d.order(:a).first.should == [:a,1,:b,2, 'SELECT * FROM test ORDER BY a LIMIT 1']
    @d.order(:a).last.should == [:a,1,:b,2, 'SELECT * FROM test ORDER BY a DESC LIMIT 1']
  end

  specify "should return the first/last matching record if argument is not an Integer" do
    @d.order(:a).first(:z => 26).should == [:a,1,:b,2, 'SELECT * FROM test WHERE (z = 26) ORDER BY a LIMIT 1']
    @d.order(:a).first('z = ?', 15).should == [:a,1,:b,2, 'SELECT * FROM test WHERE (z = 15) ORDER BY a LIMIT 1']
    @d.order(:a).last(:z => 26).should == [:a,1,:b,2, 'SELECT * FROM test WHERE (z = 26) ORDER BY a DESC LIMIT 1']
    @d.order(:a).last('z = ?', 15).should == [:a,1,:b,2, 'SELECT * FROM test WHERE (z = 15) ORDER BY a DESC LIMIT 1']
  end
  
  specify "should set the limit and return an array of records if the given number is > 1" do
    i = rand(10) + 10
    r = @d.order(:a).first(i).should == [[:a,1,:b,2, "SELECT * FROM test ORDER BY a LIMIT #{i}"]] * i
    i = rand(10) + 10
    r = @d.order(:a).last(i).should == [[:a,1,:b,2, "SELECT * FROM test ORDER BY a DESC LIMIT #{i}"]] * i
  end
  
  specify "should return the first matching record if a block is given without an argument" do
    @d.first{:z > 26}.should == [:a,1,:b,2, 'SELECT * FROM test WHERE (z > 26) LIMIT 1']
    @d.order(:name).last{:z > 26}.should == [:a,1,:b,2, 'SELECT * FROM test WHERE (z > 26) ORDER BY name DESC LIMIT 1']
  end
  
  specify "should combine block and standard argument filters if argument is not an Integer" do
    @d.first(:y=>25){:z > 26}.should == [:a,1,:b,2, 'SELECT * FROM test WHERE ((z > 26) AND (y = 25)) LIMIT 1']
    @d.order(:name).last('y = ?', 16){:z > 26}.should == [:a,1,:b,2, 'SELECT * FROM test WHERE ((z > 26) AND (y = 16)) ORDER BY name DESC LIMIT 1']
  end
  
  specify "should filter and return an array of records if an Integer argument is provided and a block is given" do
    i = rand(10) + 10
    r = @d.order(:a).first(i){:z > 26}.should == [[:a,1,:b,2, "SELECT * FROM test WHERE (z > 26) ORDER BY a LIMIT #{i}"]] * i
    i = rand(10) + 10
    r = @d.order(:a).last(i){:z > 26}.should == [[:a,1,:b,2, "SELECT * FROM test WHERE (z > 26) ORDER BY a DESC LIMIT #{i}"]] * i
  end
  
  specify "#last should raise if no order is given" do
    proc {@d.last}.should raise_error(Sequel::Error)
    proc {@d.last(2)}.should raise_error(Sequel::Error)
    proc {@d.order(:a).last}.should_not raise_error
    proc {@d.order(:a).last(2)}.should_not raise_error
  end
  
  specify "#last should invert the order" do
    @d.order(:a).last.pop.should == 'SELECT * FROM test ORDER BY a DESC LIMIT 1'
    @d.order(:b.desc).last.pop.should == 'SELECT * FROM test ORDER BY b ASC LIMIT 1'
    @d.order(:c, :d).last.pop.should == 'SELECT * FROM test ORDER BY c DESC, d DESC LIMIT 1'
    @d.order(:e.desc, :f).last.pop.should == 'SELECT * FROM test ORDER BY e ASC, f DESC LIMIT 1'
  end
end

context "Dataset set operations" do
  setup do
    @a = Sequel::Dataset.new(nil).from(:a).filter(:z => 1)
    @b = Sequel::Dataset.new(nil).from(:b).filter(:z => 2)
  end
  
  specify "should support UNION and UNION ALL" do
    @a.union(@b).sql.should == \
      "SELECT * FROM a WHERE (z = 1) UNION SELECT * FROM b WHERE (z = 2)"
    @b.union(@a, true).sql.should == \
      "SELECT * FROM b WHERE (z = 2) UNION ALL SELECT * FROM a WHERE (z = 1)"
  end

  specify "should support INTERSECT and INTERSECT ALL" do
    @a.intersect(@b).sql.should == \
      "SELECT * FROM a WHERE (z = 1) INTERSECT SELECT * FROM b WHERE (z = 2)"
    @b.intersect(@a, true).sql.should == \
      "SELECT * FROM b WHERE (z = 2) INTERSECT ALL SELECT * FROM a WHERE (z = 1)"
  end

  specify "should support EXCEPT and EXCEPT ALL" do
    @a.except(@b).sql.should == \
      "SELECT * FROM a WHERE (z = 1) EXCEPT SELECT * FROM b WHERE (z = 2)"
    @b.except(@a, true).sql.should == \
      "SELECT * FROM b WHERE (z = 2) EXCEPT ALL SELECT * FROM a WHERE (z = 1)"
  end
end

context "Dataset#[]" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      @@last_dataset = nil
      
      def self.last_dataset
        @@last_dataset
      end

      def single_record(opts = nil)
        @@last_dataset = opts ? clone(opts) : self
        {1 => 2, 3 => 4}
      end
    end
    @d = @c.new(nil).from(:test)
  end
  
  specify "should return a single record filtered according to the given conditions" do
    @d[:name => 'didi'].should == {1 => 2, 3 => 4}
    @c.last_dataset.literal(@c.last_dataset.opts[:where]).should == "(name = 'didi')"

    @d[:id => 5..45].should == {1 => 2, 3 => 4}
    @c.last_dataset.literal(@c.last_dataset.opts[:where]).should == "((id >= 5) AND (id <= 45))"
  end
end

context "Dataset#single_record" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      def fetch_rows(sql)
        yield sql
      end
    end
    @cc = Class.new(@c) do
      def fetch_rows(sql); end
    end
    
    @d = @c.new(nil).from(:test)
    @e = @cc.new(nil).from(:test)
  end
  
  specify "should call each with a limit of 1 and return the record" do
    @d.single_record.should == 'SELECT * FROM test LIMIT 1'
  end
  
  specify "should pass opts to each" do
    @d.single_record(:order => [:name]).should == 'SELECT * FROM test ORDER BY name LIMIT 1'
  end
  
  specify "should override the limit if passed as an option" do
    @d.single_record(:limit => 3).should == 'SELECT * FROM test LIMIT 1'
  end

  specify "should return nil if no record is present" do
    @e.single_record.should be_nil
  end
end

context "Dataset#single_value" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      def fetch_rows(sql)
        yield({1 => sql})
      end
    end
    @cc = Class.new(@c) do
      def fetch_rows(sql); end
    end
    
    @d = @c.new(nil).from(:test)
    @e = @cc.new(nil).from(:test)
  end
  
  specify "should call each and return the first value of the first record" do
    @d.single_value.should == 'SELECT * FROM test LIMIT 1'
  end
  
  specify "should pass opts to each" do
    @d.single_value(:from => [:blah]).should == 'SELECT * FROM blah LIMIT 1'
  end
  
  specify "should return nil if no records" do
    @e.single_value.should be_nil
  end
end

context "Dataset#get" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      attr_reader :last_sql
      
      def fetch_rows(sql)
        @last_sql = sql
        yield(:name => sql)
      end
    end
    
    @d = @c.new(nil).from(:test)
  end
  
  specify "should select the specified column and fetch its value" do
    @d.get(:name).should == "SELECT name FROM test LIMIT 1"
    @d.get(:abc).should == "SELECT abc FROM test LIMIT 1" # the first available value is returned always
  end
  
  specify "should work with filters" do
    @d.filter(:id => 1).get(:name).should == "SELECT name FROM test WHERE (id = 1) LIMIT 1"
  end
  
  specify "should work with aliased fields" do
    @d.get(:x__b.as(:name)).should == "SELECT x.b AS name FROM test LIMIT 1"
  end
end

context "Dataset#set_row_proc" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      def fetch_rows(sql, &block)
        # yield a hash with kind as the 1 bit of a number
        (1..10).each {|i| block.call({:kind => i[0]})}
      end
    end
    @dataset = @c.new(nil).from(:items)
  end
  
  specify "should cause dataset to pass all rows through the filter" do
    @dataset.row_proc = proc{|h| h[:der] = h[:kind] + 2; h}
    
    rows = @dataset.all
    rows.size.should == 10
    
    rows.each {|r| r[:der].should == (r[:kind] + 2)}
  end
  
  specify "should be copied over when dataset is cloned" do
    @dataset.row_proc = proc{|h| h[:der] = h[:kind] + 2; h}
    
    @dataset.filter(:a => 1).first.should == {:kind => 1, :der => 3}
  end
end

context "Dataset#set_model" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      def fetch_rows(sql, &block)
        # yield a hash with kind as the 1 bit of a number
        (1..10).each {|i| block.call({:kind => i[0]})}
      end
    end
    @dataset = @c.new(nil).from(:items)
    @m = Class.new do
      attr_accessor :c, :args
      def initialize(c, *args); @c = c; @args = args; end
      def ==(o); (@c == o.c) && (@args = o.args); end
    end
  end
  
  specify "should clear the models hash and restore the stock #each if nil is specified" do
    @dataset.set_model(@m)
    @dataset.set_model(nil)
    @dataset.first.should == {:kind => 1}
    @dataset.model_classes.should be_nil
  end
  
  specify "should clear the models hash and restore the stock #each if nothing is specified" do
    @dataset.set_model(@m)
    @dataset.set_model(nil)
    @dataset.first.should == {:kind => 1}
    @dataset.model_classes.should be_nil
  end
  
  specify "should alter #each to provide model instances" do
    @dataset.first.should == {:kind => 1}
    @dataset.set_model(@m)
    @dataset.first.should == @m.new({:kind => 1})
  end
  
  specify "should set opts[:naked] to nil" do
    @dataset.opts[:naked] = true
    @dataset.set_model(@m)
    @dataset.opts[:naked].should be_nil
  end
  
  specify "should send additional arguments to the models' initialize method" do
    @dataset.set_model(@m, 7, 6, 5)
    @dataset.first.should == @m.new({:kind => 1}, 7, 6, 5)
  end
  
  specify "should provide support for polymorphic model instantiation" do
    @m1 = Class.new(@m)
    @m2 = Class.new(@m)
    @dataset.set_model(:kind, 0 => @m1, 1 => @m2)
    @dataset.opts[:polymorphic_key].should == :kind
    all = @dataset.all
    all[0].class.should == @m2
    all[1].class.should == @m1
    all[2].class.should == @m2
    all[3].class.should == @m1
    #...
    
    # denude model
    @dataset.set_model(nil)
    @dataset.first.should == {:kind => 1}
  end
  
  specify "should send additional arguments for polymorphic models as well" do
    @m1 = Class.new(@m)
    @m2 = Class.new(@m)
    @dataset.set_model(:kind, {0 => @m1, 1 => @m2}, :hey => :wow)
    all = @dataset.all
    all[0].class.should == @m2; all[0].args.should == [{:hey => :wow}]
    all[1].class.should == @m1; all[1].args.should == [{:hey => :wow}]
    all[2].class.should == @m2; all[2].args.should == [{:hey => :wow}]
    all[3].class.should == @m1; all[3].args.should == [{:hey => :wow}]
  end
  
  specify "should raise for invalid parameters" do
    proc {@dataset.set_model('kind')}.should raise_error(ArgumentError)
    proc {@dataset.set_model(0)}.should raise_error(ArgumentError)
    proc {@dataset.set_model(:kind)}.should raise_error(ArgumentError) # no hash given
  end
end

context "Dataset#model_classes" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      # # We don't need that for now
      # def fetch_rows(sql, &block)
      #   (1..10).each(&block)
      # end
    end
    @dataset = @c.new(nil).from(:items)
    @m = Class.new do
      attr_accessor :c
      def initialize(c); @c = c; end
      def ==(o); @c == o.c; end
    end
  end
  
  specify "should return nil for a naked dataset" do
    @dataset.model_classes.should == nil
  end
  
  specify "should return a {nil => model_class} hash for a model dataset" do
    @dataset.set_model(@m)
    @dataset.model_classes.should == {nil => @m}
  end
  
  specify "should return the polymorphic hash for a polymorphic model dataset" do
    @m1 = Class.new(@m)
    @m2 = Class.new(@m)
    @dataset.set_model(:key, 0 => @m1, 1 => @m2)
    @dataset.model_classes.should == {0 => @m1, 1 => @m2}
  end
end

context "Dataset#polymorphic_key" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      # # We don't need this for now
      # def fetch_rows(sql, &block)
      #   (1..10).each(&block)
      # end
    end
    @dataset = @c.new(nil).from(:items)
    @m = Class.new do
      attr_accessor :c
      def initialize(c); @c = c; end
      def ==(o); @c == o.c; end
    end
  end
  
  specify "should return nil for a naked dataset" do
    @dataset.polymorphic_key.should be_nil
  end
  
  specify "should return the polymorphic key" do
    @dataset.set_model(:id, nil => @m)
    @dataset.polymorphic_key.should == :id
  end
end

context "A model dataset" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      def fetch_rows(sql, &block)
        (1..10).each(&block)
      end
    end
    @dataset = @c.new(nil).from(:items)
    @m = Class.new do
      attr_accessor :c
      def initialize(c); @c = c; end
      def ==(o); @c == o.c; end
    end
    @dataset.set_model(@m)
  end
  
  specify "should supply naked records if the naked option is specified" do
    @dataset.each {|r| r.class.should == @m}
    @dataset.each(:naked => true) {|r| r.class.should == Fixnum}
  end
end

context "A polymorphic model dataset" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      def fetch_rows(sql, &block)
        (1..10).each {|i| block.call(:bit => i[0])}
      end
    end
    @dataset = @c.new(nil).from(:items)
    @m = Class.new do
      attr_accessor :c
      def initialize(c); @c = c; end
      def ==(o); @c == o.c; end
    end
  end
  
  specify "should use a nil key in the polymorphic hash to specify the default model class" do
    @m2 = Class.new(@m)
    @dataset.set_model(:bit, nil => @m, 1 => @m2)
    all = @dataset.all
    all[0].class.should == @m2
    all[1].class.should == @m
    all[2].class.should == @m2
    all[3].class.should == @m
    #...
  end
  
  specify "should raise Sequel::Error if no suitable class is found in the polymorphic hash" do
    @m2 = Class.new(@m)
    @dataset.set_model(:bit, 1 => @m2)
    proc {@dataset.all}.should raise_error(Sequel::Error)
  end

  specify "should supply naked records if the naked option is specified" do
    @dataset.set_model(:bit, nil => @m)
    @dataset.each(:naked => true) {|r| r.class.should == Hash}
  end
end

context "A dataset with associated model class(es)" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      def fetch_rows(sql, &block)
        block.call({:x => 1, :y => 2})
      end
    end
    @dataset = @c.new(nil).from(:items)
    @m1 = Class.new do
      attr_accessor :v
      def initialize(v); @v = v; end
    end
    @m2 = Class.new do
      attr_accessor :v, :vv
      def initialize(v = nil); @v = v; end
      def self.load(v); o = new(nil); o.vv = v; o; end
    end
    @m3 = Class.new(@m2)
  end

  specify "should instantiate an instance by passing the record hash as argument" do
    @dataset.set_model(@m1)
    o = @dataset.first
    o.class.should == @m1
    o.v.should == {:x => 1, :y => 2}
  end
  
  specify "should use the .load constructor if available" do
    @dataset.set_model(@m2)
    o = @dataset.first
    o.class.should == @m2
    o.v.should == nil
    o.vv.should == {:x => 1, :y => 2}
  end
  
  specify "should use the .load constructor also for polymorphic datasets" do
    @dataset.set_model(:y, 1 => @m2, 2 => @m3)
    o = @dataset.first
    o.class.should == @m3
    o.v.should == nil
    o.vv.should == {:x => 1, :y => 2}
  end
end

context "Dataset#<<" do
  setup do
    @d = Sequel::Dataset.new(nil)
    @d.meta_def(:insert) do |*args|
      1234567890
    end
  end
  
  specify "should call #insert" do
    (@d << {:name => 1}).should == 1234567890
  end
end

context "A paginated dataset" do
  setup do
    @d = Sequel::Dataset.new(nil)
    @d.meta_def(:count) {153}
    
    @paginated = @d.paginate(1, 20)
  end
  
  specify "should raise an error if the dataset already has a limit" do
    proc{@d.limit(10).paginate(1,10)}.should raise_error(Sequel::Error)
    proc{@paginated.paginate(2,20)}.should raise_error(Sequel::Error)
  end
  
  specify "should set the limit and offset options correctly" do
    @paginated.opts[:limit].should == 20
    @paginated.opts[:offset].should == 0
  end
  
  specify "should set the page count correctly" do
    @paginated.page_count.should == 8
    @d.paginate(1, 50).page_count.should == 4
  end
  
  specify "should set the current page number correctly" do
    @paginated.current_page.should == 1
    @d.paginate(3, 50).current_page.should == 3
  end
  
  specify "should return the next page number or nil if we're on the last" do
    @paginated.next_page.should == 2
    @d.paginate(4, 50).next_page.should be_nil
  end
  
  specify "should return the previous page number or nil if we're on the last" do
    @paginated.prev_page.should be_nil
    @d.paginate(4, 50).prev_page.should == 3
  end
  
  specify "should return the page range" do
    @paginated.page_range.should == (1..8)
    @d.paginate(4, 50).page_range.should == (1..4)
  end
  
  specify "should return the record range for the current page" do
    @paginated.current_page_record_range.should == (1..20)
    @d.paginate(4, 50).current_page_record_range.should == (151..153)
    @d.paginate(5, 50).current_page_record_range.should == (0..0)
  end

  specify "should return the record count for the current page" do
    @paginated.current_page_record_count.should == 20
    @d.paginate(3, 50).current_page_record_count.should == 50
    @d.paginate(4, 50).current_page_record_count.should == 3
    @d.paginate(5, 50).current_page_record_count.should == 0
  end

  specify "should know if current page is last page" do
    @paginated.last_page?.should be_false
    @d.paginate(2, 20).last_page?.should be_false
    @d.paginate(5, 30).last_page?.should be_false
    @d.paginate(6, 30).last_page?.should be_true
  end

  specify "should know if current page is first page" do
    @paginated.first_page?.should be_true
    @d.paginate(1, 20).first_page?.should be_true
    @d.paginate(2, 20).first_page?.should be_false
  end

  specify "should work with fixed sql" do
    ds = @d.clone(:sql => 'select * from blah')
    ds.meta_def(:count) {150}
    ds.paginate(2, 50).sql.should == 'SELECT * FROM (select * from blah) t1 LIMIT 50 OFFSET 50'
  end
end

context "Dataset#each_page" do
  setup do
    @d = Sequel::Dataset.new(nil).from(:items)
    @d.meta_def(:count) {153}
  end
  
  specify "should raise an error if the dataset already has a limit" do
    proc{@d.limit(10).each_page(10){}}.should raise_error(Sequel::Error)
  end
  
  specify "should iterate over each page in the resultset as a paginated dataset" do
    a = []
    @d.each_page(50) {|p| a << p}
    a.map {|p| p.sql}.should == [
      'SELECT * FROM items LIMIT 50 OFFSET 0',
      'SELECT * FROM items LIMIT 50 OFFSET 50',
      'SELECT * FROM items LIMIT 50 OFFSET 100',
      'SELECT * FROM items LIMIT 50 OFFSET 150',
    ]
  end
end

context "Dataset#columns" do
  setup do
    @dataset = DummyDataset.new(nil).from(:items)
    @dataset.meta_def(:columns=) {|c| @columns = c}
    i = 'a' 
    @dataset.meta_def(:each) {|o| @columns = select_sql(o||@opts) + i; i = i.next}
  end
  
  specify "should return the value of @columns if @columns is not nil" do
    @dataset.columns = [:a, :b, :c]
    @dataset.columns.should == [:a, :b, :c]
  end
  
  specify "should attempt to get a single record and return @columns if @columns is nil" do
    @dataset.columns = nil
    @dataset.columns.should == 'SELECT * FROM items LIMIT 1a'
    @dataset.opts[:from] = [:nana]
    @dataset.columns.should == 'SELECT * FROM items LIMIT 1a'
  end
  
  specify "should ignore any filters, orders, or DISTINCT clauses" do
    @dataset.filter!(:b=>100).order!(:b).distinct!(:b)
    @dataset.columns = nil
    @dataset.columns.should == 'SELECT * FROM items LIMIT 1a'
  end
end

context "Dataset#columns!" do
  setup do
    @dataset = DummyDataset.new(nil).from(:items)
    i = 'a' 
    @dataset.meta_def(:each) {|o| @columns = select_sql(o||@opts) + i; i = i.next}
  end
  
  specify "should always attempt to get a record and return @columns" do
    @dataset.columns!.should == 'SELECT * FROM items LIMIT 1a'
    @dataset.columns!.should == 'SELECT * FROM items LIMIT 1b'
    @dataset.opts[:from] = [:nana]
    @dataset.columns!.should == 'SELECT * FROM nana LIMIT 1c'
  end
end

require 'stringio'

context "Dataset#print" do
  setup do
    @output = StringIO.new
    @orig_stdout = $stdout
    $stdout = @output
    @dataset = DummyDataset.new(nil).from(:items)
  end
  
  teardown do
    $stdout = @orig_stdout
  end
  
  specify "should print out a table with the values" do
    @dataset.print(:a, :b)
    @output.rewind
    @output.read.should == \
      "+-+-+\n|a|b|\n+-+-+\n|1|2|\n|3|4|\n|5|6|\n+-+-+\n"
  end

  specify "should default to the dataset's columns" do
    @dataset.meta_def(:columns) {[:a, :b]}
    @dataset.print
    @output.rewind
    @output.read.should == \
      "+-+-+\n|a|b|\n+-+-+\n|1|2|\n|3|4|\n|5|6|\n+-+-+\n"
  end
end

context "Dataset#multi_insert" do
  setup do
    @dbc = Class.new do
      attr_reader :sqls
      
      def execute(sql)
        @sqls ||= []
        @sqls << sql
      end
      
      def transaction
        @sqls ||= []
        @sqls << 'BEGIN'
        yield
        @sqls << 'COMMIT'
      end
    end
    @db = @dbc.new
    
    @ds = Sequel::Dataset.new(@db).from(:items)
    
    @list = [{:name => 'abc'}, {:name => 'def'}, {:name => 'ghi'}]
  end
  
  specify "should join all inserts into a single SQL string" do
    @ds.multi_insert(@list)
    @db.sqls.should == [
      'BEGIN',
      "INSERT INTO items (name) VALUES ('abc')",
      "INSERT INTO items (name) VALUES ('def')",
      "INSERT INTO items (name) VALUES ('ghi')",
      'COMMIT'
    ]
  end
  
  specify "should accept the :commit_every option for committing every x records" do
    @ds.multi_insert(@list, :commit_every => 2)
    @db.sqls.should == [
      'BEGIN',
      "INSERT INTO items (name) VALUES ('abc')",
      "INSERT INTO items (name) VALUES ('def')",
      'COMMIT',
      'BEGIN',
      "INSERT INTO items (name) VALUES ('ghi')",
      'COMMIT'
    ]
  end

  specify "should accept the :slice option for committing every x records" do
    @ds.multi_insert(@list, :slice => 2)
    @db.sqls.should == [
      'BEGIN',
      "INSERT INTO items (name) VALUES ('abc')",
      "INSERT INTO items (name) VALUES ('def')",
      'COMMIT',
      'BEGIN',
      "INSERT INTO items (name) VALUES ('ghi')",
      'COMMIT'
    ]
  end
  
  specify "should accept a columns array and a values array" do
    @ds.multi_insert([:x, :y], [[1, 2], [3, 4]])
    @db.sqls.should == [
      'BEGIN',
      "INSERT INTO items (x, y) VALUES (1, 2)",
      "INSERT INTO items (x, y) VALUES (3, 4)",
      'COMMIT'
    ]
  end

  specify "should accept a columns array and a dataset" do
    @ds2 = Sequel::Dataset.new(@db).from(:cats).filter(:purr => true).select(:a, :b)
    
    @ds.multi_insert([:x, :y], @ds2)
    @db.sqls.should == [
      'BEGIN',
      "INSERT INTO items (x, y) VALUES (SELECT a, b FROM cats WHERE (purr = 't'))",
      'COMMIT'
    ]
  end

  specify "should accept a columns array and a values array with slice option" do
    @ds.multi_insert([:x, :y], [[1, 2], [3, 4], [5, 6]], :slice => 2)
    @db.sqls.should == [
      'BEGIN',
      "INSERT INTO items (x, y) VALUES (1, 2)",
      "INSERT INTO items (x, y) VALUES (3, 4)",
      'COMMIT',
      'BEGIN',
      "INSERT INTO items (x, y) VALUES (5, 6)",
      'COMMIT'
    ]
  end
  
  specify "should be aliased by #import" do
    @ds.import([:x, :y], [[1, 2], [3, 4], [5, 6]], :slice => 2)
    @db.sqls.should == [
      'BEGIN',
      "INSERT INTO items (x, y) VALUES (1, 2)",
      "INSERT INTO items (x, y) VALUES (3, 4)",
      'COMMIT',
      'BEGIN',
      "INSERT INTO items (x, y) VALUES (5, 6)",
      'COMMIT'
    ]
  end

  specify "should not do anything if no columns or values are given" do
    @ds.multi_insert
    @db.sqls.should be_nil
    
    @ds.multi_insert([])
    @db.sqls.should be_nil
    
    @ds.multi_insert([], [])
    @db.sqls.should be_nil

    @ds.multi_insert([{}, {}])
    @db.sqls.should be_nil
    
    @ds.multi_insert([:a, :b], [])
    @db.sqls.should be_nil
    
    @ds.multi_insert([:x, :y], [[1, 2], [3, 4], [5, 6]], :slice => 2)
    @db.sqls.should == [
      'BEGIN',
      "INSERT INTO items (x, y) VALUES (1, 2)",
      "INSERT INTO items (x, y) VALUES (3, 4)",
      'COMMIT',
      'BEGIN',
      "INSERT INTO items (x, y) VALUES (5, 6)",
      'COMMIT'
    ]
  end
  
end

context "Dataset#query" do
  setup do
    @d = Sequel::Dataset.new(nil)
  end
  
  specify "should support #from" do
    q = @d.query {from :xxx}
    q.class.should == @d.class
    q.sql.should == "SELECT * FROM xxx"
  end
  
  specify "should support #select" do
    q = @d.query do
      select :a, :b___mongo
      from :yyy
    end
    q.class.should == @d.class
    q.sql.should == "SELECT a, b AS mongo FROM yyy"
  end
  
  specify "should support #where" do
    q = @d.query do
      from :zzz
      where(:x + 2 > :y + 3)
    end
    q.class.should == @d.class
    q.sql.should == "SELECT * FROM zzz WHERE ((x + 2) > (y + 3))"

    q = @d.from(:zzz).query do
      where((:x > 1) & (:y > 2))
    end
    q.class.should == @d.class
    q.sql.should == "SELECT * FROM zzz WHERE ((x > 1) AND (y > 2))"

    q = @d.from(:zzz).query do
      where :x => 33
    end
    q.class.should == @d.class
    q.sql.should == "SELECT * FROM zzz WHERE (x = 33)"
  end
  
  specify "should support #group_by and #having" do
    q = @d.query do
      from :abc
      group_by :id
      having(:x >= 2)
    end
    q.class.should == @d.class
    q.sql.should == "SELECT * FROM abc GROUP BY id HAVING (x >= 2)"
  end
  
  specify "should support #order, #order_by" do
    q = @d.query do
      from :xyz
      order_by :stamp
    end
    q.class.should == @d.class
    q.sql.should == "SELECT * FROM xyz ORDER BY stamp"
  end
  
  specify "should raise on non-chainable method calls" do
    proc {@d.query {first_source}}.should raise_error(Sequel::Error)
  end
  
  specify "should raise on each, insert, update, delete" do
    proc {@d.query {each}}.should raise_error(Sequel::Error)
    proc {@d.query {insert(:x => 1)}}.should raise_error(Sequel::Error)
    proc {@d.query {update(:x => 1)}}.should raise_error(Sequel::Error)
    proc {@d.query {delete}}.should raise_error(Sequel::Error)
  end
end

context "Dataset" do
  setup do
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
    @d.filter!{:y < 2}
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
      @d.order!(:y)
      @d.filter!(:y => 1)
      @d.sql.should == "SELECT * FROM x WHERE (y = 1) ORDER BY y"
  end
end

context "Dataset#transform" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      attr_accessor :raw
      attr_accessor :sql
      
      def fetch_rows(sql, &block)
        block[@raw]
      end
      
      def insert(v)
        @sql = insert_sql(v)
      end
      
      def update(v)
        @sql = update_sql(v)
      end
    end

    @ds = @c.new(nil).from(:items)
    @ds.transform(:x => [
      proc {|v| Marshal.load(v)},
      proc {|v| Marshal.dump(v)}
    ])
  end
  
  specify "should change the dataset to transform values loaded from the database" do
    @ds.raw = {:x => Marshal.dump([1, 2, 3]), :y => 'hello'}
    @ds.first.should == {:x => [1, 2, 3], :y => 'hello'}
    @ds.raw = {:x => Marshal.dump([1, 2, 3]), :y => 'hello'}
    @ds.all.should == [{:x => [1, 2, 3], :y => 'hello'}]
  end
  
  specify "should change the dataset to transform values saved to the database" do
    @ds.insert(:x => :toast)
    @ds.sql.should == "INSERT INTO items (x) VALUES ('#{Marshal.dump(:toast)}')"

    @ds.insert(:y => 'butter')
    @ds.sql.should == "INSERT INTO items (y) VALUES ('butter')"
    
    @ds.update(:x => ['dream'])
    @ds.sql.should == "UPDATE items SET x = '#{Marshal.dump(['dream'])}'"
  end
  
  specify "should be transferred to cloned datasets" do
    @ds2 = @ds.filter(:a => 1)

    @ds2.raw = {:x => Marshal.dump([1, 2, 3]), :y => 'hello'}
    @ds2.first.should == {:x => [1, 2, 3], :y => 'hello'}

    @ds2.insert(:x => :toast)
    @ds2.sql.should == "INSERT INTO items (x) VALUES ('#{Marshal.dump(:toast)}')"
  end
  
  specify "should work correctly together with set_row_proc" do
    @ds.row_proc = proc{|r| r[:z] = r[:x] * 2; r}
    @ds.raw = {:x => Marshal.dump("wow"), :y => 'hello'}
    @ds.first.should == {:x => "wow", :y => 'hello', :z => "wowwow"}

    f = nil
    @ds.raw = {:x => Marshal.dump("wow"), :y => 'hello'}
    @ds.each(:naked => true) {|r| f = r}
    f.should == {:x => "wow", :y => 'hello'}
  end
  
  specify "should leave the supplied values intact" do
    h = {:x => :toast}
    @ds.insert(h)
    h.should == {:x => :toast}
  end
end

context "Dataset#transform" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      attr_accessor :raw
      attr_accessor :sql
      
      def fetch_rows(sql, &block)
        block[@raw]
      end
      
      def insert(v)
        @sql = insert_sql(v)
      end
      
      def update(v)
        @sql = update_sql(v)
      end
    end

    @ds = @c.new(nil).from(:items)
  end
  
  specify "should raise Sequel::Error for invalid transformations" do
    proc {@ds.transform(:x => 'mau')}.should raise_error(Sequel::Error::InvalidTransform)
    proc {@ds.transform(:x => :mau)}.should raise_error(Sequel::Error::InvalidTransform)
    proc {@ds.transform(:x => [])}.should raise_error(Sequel::Error::InvalidTransform)
    proc {@ds.transform(:x => ['mau'])}.should raise_error(Sequel::Error::InvalidTransform)
    proc {@ds.transform(:x => [proc {|v|}, proc {|v|}])}.should_not raise_error(Sequel::Error::InvalidTransform)
  end
  
  specify "should support stock YAML transformation" do
    @ds.transform(:x => :yaml)

    @ds.raw = {:x => [1, 2, 3].to_yaml, :y => 'hello'}
    @ds.first.should == {:x => [1, 2, 3], :y => 'hello'}

    @ds.insert(:x => :toast)
    @ds.sql.should == "INSERT INTO items (x) VALUES ('#{:toast.to_yaml}')"
    @ds.insert(:y => 'butter')
    @ds.sql.should == "INSERT INTO items (y) VALUES ('butter')"
    @ds.update(:x => ['dream'])
    @ds.sql.should == "UPDATE items SET x = '#{['dream'].to_yaml}'"

    @ds2 = @ds.filter(:a => 1)
    @ds2.raw = {:x => [1, 2, 3].to_yaml, :y => 'hello'}
    @ds2.first.should == {:x => [1, 2, 3], :y => 'hello'}
    @ds2.insert(:x => :toast)
    @ds2.sql.should == "INSERT INTO items (x) VALUES ('#{:toast.to_yaml}')"

    @ds.row_proc = proc{|r| r[:z] = r[:x] * 2; r}
    @ds.raw = {:x => "wow".to_yaml, :y => 'hello'}
    @ds.first.should == {:x => "wow", :y => 'hello', :z => "wowwow"}
    f = nil
    @ds.raw = {:x => "wow".to_yaml, :y => 'hello'}
    @ds.each(:naked => true) {|r| f = r}
    f.should == {:x => "wow", :y => 'hello'}
  end
  
  specify "should support stock Marshal transformation with Base64 encoding" do
    @ds.transform(:x => :marshal)

    @ds.raw = {:x => [Marshal.dump([1, 2, 3])].pack('m'), :y => 'hello'}
    @ds.first.should == {:x => [1, 2, 3], :y => 'hello'}

    @ds.insert(:x => :toast)
    @ds.sql.should == "INSERT INTO items (x) VALUES ('#{[Marshal.dump(:toast)].pack('m')}')"
    @ds.insert(:y => 'butter')
    @ds.sql.should == "INSERT INTO items (y) VALUES ('butter')"
    @ds.update(:x => ['dream'])
    @ds.sql.should == "UPDATE items SET x = '#{[Marshal.dump(['dream'])].pack('m')}'"

    @ds2 = @ds.filter(:a => 1)
    @ds2.raw = {:x => [Marshal.dump([1, 2, 3])].pack('m'), :y => 'hello'}
    @ds2.first.should == {:x => [1, 2, 3], :y => 'hello'}
    @ds2.insert(:x => :toast)
    @ds2.sql.should == "INSERT INTO items (x) VALUES ('#{[Marshal.dump(:toast)].pack('m')}')"

    @ds.row_proc = proc{|r| r[:z] = r[:x] * 2; r}
    @ds.raw = {:x => [Marshal.dump("wow")].pack('m'), :y => 'hello'}
    @ds.first.should == {:x => "wow", :y => 'hello', :z => "wowwow"}
    f = nil
    @ds.raw = {:x => [Marshal.dump("wow")].pack('m'), :y => 'hello'}
    @ds.each(:naked => true) {|r| f = r}
    f.should == {:x => "wow", :y => 'hello'}
  end
  
  specify "should support loading of Marshalled values without Base64 encoding" do
    @ds.transform(:x => :marshal)

    @ds.raw = {:x => Marshal.dump([1,2,3]), :y => nil}
    @ds.first.should == {:x => [1,2,3], :y => nil}
  end
  
  specify "should return self" do
    @ds.transform(:x => :marshal).should be(@ds)
  end
end

context "A dataset with a transform" do
  setup do
    @ds = Sequel::Dataset.new(nil).from(:items)
    @ds.transform(:x => :marshal)
  end
  
  specify "should automatically transform hash filters" do
    @ds.filter(:y => 2).sql.should == 'SELECT * FROM items WHERE (y = 2)'
    
    @ds.filter(:x => 2).sql.should == "SELECT * FROM items WHERE (x = '#{[Marshal.dump(2)].pack('m')}')"
  end
end

context "Dataset#to_csv" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      attr_accessor :data
      attr_accessor :columns
      
      def fetch_rows(sql, &block)
        @data.each(&block)
      end
      
      # naked should return self here because to_csv wants a naked result set.
      def naked
        self
      end
    end
    
    @ds = @c.new(nil).from(:items)
    @ds.columns = [:a, :b, :c]
    @ds.data = [ {:a=>1, :b=>2, :c=>3}, {:a=>4, :b=>5, :c=>6}, {:a=>7, :b=>8, :c=>9} ]
  end
  
  specify "should format a CSV representation of the records" do
    @ds.to_csv.should ==
      "a, b, c\r\n1, 2, 3\r\n4, 5, 6\r\n7, 8, 9\r\n"
  end

  specify "should exclude column titles if so specified" do
    @ds.to_csv(false).should ==
      "1, 2, 3\r\n4, 5, 6\r\n7, 8, 9\r\n"
  end
end

context "Dataset#create_view" do
  setup do
    @dbc = Class.new(Sequel::Database) do
      attr_reader :sqls
      
      def execute(sql)
        @sqls ||= []
        @sqls << sql
      end
    end
    @db = @dbc.new
    
    @ds = @db[:items].order(:abc).filter(:category => 'ruby')
  end
  
  specify "should create a view with the dataset's sql" do
    @ds.create_view(:xyz)
    @db.sqls.should == ["CREATE VIEW xyz AS #{@ds.sql}"]
  end
end

context "Dataset#create_or_replace_view" do
  setup do
    @dbc = Class.new(Sequel::Database) do
      attr_reader :sqls
      
      def execute(sql)
        @sqls ||= []
        @sqls << sql
      end
    end
    @db = @dbc.new
    
    @ds = @db[:items].order(:abc).filter(:category => 'ruby')
  end
  
  specify "should create a view with the dataset's sql" do
    @ds.create_or_replace_view(:xyz)
    @db.sqls.should == ["CREATE OR REPLACE VIEW xyz AS #{@ds.sql}"]
  end
end

context "Dataset#update_sql" do
  setup do
    @ds = Sequel::Dataset.new(nil).from(:items)
  end
  
  specify "should accept strings" do
    @ds.update_sql("a = b").should == "UPDATE items SET a = b"
  end
  
  specify "should accept hash with string keys" do
    @ds.update_sql('c' => 'd').should == "UPDATE items SET c = 'd'"
  end

  specify "should accept array subscript references" do
    @ds.update_sql((:day|1) => 'd').should == "UPDATE items SET day[1] = 'd'"
  end
end

context "Dataset#insert_sql" do
  setup do
    @ds = Sequel::Dataset.new(nil).from(:items)
  end
  
  specify "should accept hash with symbol keys" do
    @ds.insert_sql(:c => 'd').should == "INSERT INTO items (c) VALUES ('d')"
  end

  specify "should accept hash with string keys" do
    @ds.insert_sql('c' => 'd').should == "INSERT INTO items (c) VALUES ('d')"
  end

  specify "should accept array subscript references" do
    @ds.insert_sql((:day|1) => 'd').should == "INSERT INTO items (day[1]) VALUES ('d')"
  end
end

class DummyMummyDataset < Sequel::Dataset
  def first
    raise if @opts[:from] == [:a]
    true
  end
end

class DummyMummyDatabase < Sequel::Database
  attr_reader :sqls
  
  def execute(sql)
    @sqls ||= []
    @sqls << sql
  end
  
  def transaction; yield; end

  def dataset
    DummyMummyDataset.new(self)
  end
end

context "Dataset#table_exists?" do
  setup do
    @db = DummyMummyDatabase.new
    @db.stub!(:tables).and_return([:a, :b])
    @db2 = DummyMummyDatabase.new
  end
  
  specify "should use Database#tables if available" do
    @db[:a].table_exists?.should be_true
    @db[:b].table_exists?.should be_true
    @db[:c].table_exists?.should be_false
  end
  
  specify "should otherwise try to select the first record from the table's dataset" do
    @db2[:a].table_exists?.should be_false
    @db2[:b].table_exists?.should be_true
  end
  
  specify "should raise Sequel::Error if dataset references more than one table" do
    proc {@db.from(:a, :b).table_exists?}.should raise_error(Sequel::Error)
  end

  specify "should raise Sequel::Error if dataset is from a subquery" do
    proc {@db.from(@db[:a]).table_exists?}.should raise_error(Sequel::Error)
  end

  specify "should raise Sequel::Error if dataset has fixed sql" do
    proc {@db['select * from blah'].table_exists?}.should raise_error(Sequel::Error)
  end
end

context "Dataset#inspect" do
  setup do
    @ds = Sequel::Dataset.new(nil).from(:blah)
  end
  
  specify "should include the class name and the corresponding SQL statement" do
    @ds.inspect.should == '#<%s: %s>' % [@ds.class.to_s, @ds.sql.inspect]
  end
end

context "Dataset#all" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      def fetch_rows(sql, &block)
        block.call({:x => 1, :y => 2})
        block.call({:x => 3, :y => 4})
        block.call(sql)
      end
    end
    @dataset = @c.new(nil).from(:items)
  end

  specify "should return an array with all records" do
    @dataset.all.should == [
      {:x => 1, :y => 2},
      {:x => 3, :y => 4},
      "SELECT * FROM items"
    ]
  end
  
  specify "should accept options and pass them to #each" do
    @dataset.all(:limit => 33).should == [
      {:x => 1, :y => 2},
      {:x => 3, :y => 4},
      "SELECT * FROM items LIMIT 33"
    ]
  end

  specify "should iterate over the array if a block is given" do
    a = []
    
    @dataset.all do |r|
      a << (r.is_a?(Hash) ? r[:x] : r)
    end
    
    a.should == [1, 3, "SELECT * FROM items"]
  end
end

context "Dataset#grep" do
  setup do
    @ds = Sequel::Dataset.new(nil).from(:posts)
  end
  
  specify "should format a SQL filter correctly" do
    @ds.grep(:title, 'ruby').sql.should ==
      "SELECT * FROM posts WHERE ((title LIKE 'ruby'))"
  end

  specify "should support multiple columns" do
    @ds.grep([:title, :body], 'ruby').sql.should ==
      "SELECT * FROM posts WHERE ((title LIKE 'ruby') OR (body LIKE 'ruby'))"
  end
  
  specify "should support multiple search terms" do
    @ds.grep(:title, ['abc', 'def']).sql.should == 
      "SELECT * FROM posts WHERE (((title LIKE 'abc') OR (title LIKE 'def')))"
  end
  
  specify "should support multiple columns and search terms" do
    @ds.grep([:title, :body], ['abc', 'def']).sql.should ==
      "SELECT * FROM posts WHERE (((title LIKE 'abc') OR (title LIKE 'def')) OR ((body LIKE 'abc') OR (body LIKE 'def')))"
  end
  
  specify "should support regexps though the database may not support it" do
    @ds.grep(:title, /ruby/).sql.should ==
      "SELECT * FROM posts WHERE ((title ~ 'ruby'))"

    @ds.grep(:title, [/^ruby/, 'ruby']).sql.should ==
      "SELECT * FROM posts WHERE (((title ~ '^ruby') OR (title LIKE 'ruby')))"
  end
end

context "Sequel.use_parse_tree" do
  specify "be false" do
    Sequel.use_parse_tree.should == false
  end
end

context "Sequel.use_parse_tree=" do
  specify "raise an error if true" do
    proc{Sequel.use_parse_tree = true}.should raise_error(Sequel::Error)
  end

  specify "do nothing if false" do
    proc{Sequel.use_parse_tree = false}.should_not raise_error
  end
end
