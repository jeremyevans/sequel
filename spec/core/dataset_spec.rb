require File.join(File.dirname(__FILE__), "spec_helper")

context "Dataset" do
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
end

context "Dataset" do
  before do
    @dataset = Sequel::Dataset.new("db")
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

context "Dataset#clone" do
  before do
    @dataset = Sequel::Dataset.new(nil).from(:items)
  end
  
  specify "should create an exact copy of the dataset" do
    @dataset.row_proc = Proc.new{|r| r}
    @clone = @dataset.clone

    @clone.should_not === @dataset
    @clone.class.should == @dataset.class
    @clone.opts.should == @dataset.opts
    @clone.row_proc.should == @dataset.row_proc
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
      "INSERT INTO test SELECT * FROM something WHERE (x = 2)"
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
    @dataset.clone(:sql => 'xxx yyy zzz').select_sql.should ==
      "xxx yyy zzz"
  end

  specify "should use the :sql option for all sql methods" do
    sql = "X"
    ds = Sequel::Dataset.new(nil, :sql=>sql)
    ds.sql.should == sql
    ds.select_sql.should == sql
    ds.insert_sql.should == sql
    ds.delete_sql.should == sql
    ds.update_sql.should == sql
    ds.truncate_sql.should == sql
  end
end

context "A dataset with multiple tables in its FROM clause" do
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

context "Dataset#exists" do
  before do
    @ds1 = Sequel::Dataset.new(nil).from(:test)
    @ds2 = @ds1.filter(:price.sql_number < 100)
    @ds3 = @ds1.filter(:price.sql_number > 50)
  end
  
  specify "should work in filters" do
    @ds1.filter(@ds2.exists).sql.should ==
      'SELECT * FROM test WHERE (EXISTS (SELECT * FROM test WHERE (price < 100)))'
    @ds1.filter(@ds2.exists & @ds3.exists).sql.should ==
      'SELECT * FROM test WHERE (EXISTS (SELECT * FROM test WHERE (price < 100)) AND EXISTS (SELECT * FROM test WHERE (price > 50)))'
  end

  specify "should work in select" do
    @ds1.select(@ds2.exists.as(:a), @ds3.exists.as(:b)).sql.should ==
      'SELECT EXISTS (SELECT * FROM test WHERE (price < 100)) AS a, EXISTS (SELECT * FROM test WHERE (price > 50)) AS b FROM test'
  end
end

context "Dataset#where" do
  before do
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
    @d3.where{:e.sql_number < 5}.select_sql.should ==
      "SELECT * FROM test WHERE ((a = 1) AND (e < 5))"
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
    @dataset.filter('gdp > ?', @d1.select(:avg.sql_function(:gdp))).sql.should ==
      "SELECT * FROM test WHERE (gdp > (SELECT avg(gdp) FROM test WHERE (region = 'Asia')))"

    @dataset.filter(:id => @d1.select(:id)).sql.should ==
      "SELECT * FROM test WHERE (id IN (SELECT id FROM test WHERE (region = 'Asia')))"
  end
  
  specify "should accept a subquery for an EXISTS clause" do
    a = @dataset.filter(:price.sql_number < 100)
    @dataset.filter(a.exists).sql.should ==
      'SELECT * FROM test WHERE (EXISTS (SELECT * FROM test WHERE (price < 100)))'
  end
  
  specify "should accept proc expressions" do
    d = @d1.select(:avg.sql_function(:gdp))
    @dataset.filter {:gdp.sql_number > d}.sql.should ==
      "SELECT * FROM test WHERE (gdp > (SELECT avg(gdp) FROM test WHERE (region = 'Asia')))"
    
    @dataset.filter {:a.sql_number < 1}.sql.should ==
      'SELECT * FROM test WHERE (a < 1)'

    @dataset.filter {(:a.sql_number >= 1) & (:b.sql_number <= 2)}.sql.should ==
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

  specify "should allow the use of multiple arguments" do
    @dataset.filter(:a, :b).sql.should ==
      'SELECT * FROM test WHERE (a AND b)'
    @dataset.filter(:a, :b=>1).sql.should ==
      'SELECT * FROM test WHERE (a AND (b = 1))'
    @dataset.filter(:a, :c.sql_number > 3, :b=>1).sql.should ==
      'SELECT * FROM test WHERE (a AND (c > 3) AND (b = 1))'
  end

  specify "should allow the use of blocks and arguments simultaneously" do
    @dataset.filter(:zz.sql_number < 3){:yy.sql_number > 3}.sql.should ==
      'SELECT * FROM test WHERE ((zz < 3) AND (yy > 3))'
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
    proc{@dataset.filter(:x + 1)}.should raise_error(Sequel::Error)
    proc{@dataset.filter(:x.sql_string)}.should raise_error(Sequel::Error)
  end
end

context "Dataset#or" do
  before do
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
    @d1.or(:yy.sql_number > 3).sql.should ==
      'SELECT * FROM test WHERE ((x = 1) OR (yy > 3))'
  end    

  specify "should accept blocks passed to filter" do
    @d1.or{:yy.sql_number > 3}.sql.should ==
      'SELECT * FROM test WHERE ((x = 1) OR (yy > 3))'
  end
  
  specify "should correctly add parens to give predictable results" do
    @d1.filter(:y => 2).or(:z => 3).sql.should == 
      'SELECT * FROM test WHERE (((x = 1) AND (y = 2)) OR (z = 3))'

    @d1.or(:y => 2).filter(:z => 3).sql.should == 
      'SELECT * FROM test WHERE (((x = 1) OR (y = 2)) AND (z = 3))'
  end

  specify "should allow the use of blocks and arguments simultaneously" do
    @d1.or(:zz.sql_number < 3){:yy.sql_number > 3}.sql.should ==
      'SELECT * FROM test WHERE ((x = 1) OR ((zz < 3) AND (yy > 3)))'
  end
end

context "Dataset#and" do
  before do
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
    @d1.and(:yy.sql_number > 3).sql.should ==
      'SELECT * FROM test WHERE ((x = 1) AND (yy > 3))'
  end
      
  specify "should accept blocks passed to filter" do
    @d1.and {:yy.sql_number > 3}.sql.should ==
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
  before do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end

  specify "should correctly negate the expression when one condition is given" do
    @dataset.exclude(:region=>'Asia').select_sql.should ==
      "SELECT * FROM test WHERE (region != 'Asia')"
  end

  specify "should take multiple conditions as a hash and express the logic correctly in SQL" do
    @dataset.exclude(:region => 'Asia', :name => 'Japan').select_sql.
      should match(Regexp.union(/WHERE \(\(region != 'Asia'\) OR \(name != 'Japan'\)\)/,
                                /WHERE \(\(name != 'Japan'\) OR \(region != 'Asia'\)\)/))
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
    @dataset.exclude{:id.sql_number < 6}.sql.should == 
      'SELECT * FROM test WHERE (id >= 6)'
  end
  
  specify "should allow the use of blocks and arguments simultaneously" do
    @dataset.exclude(:id => (7..11)){:id.sql_number < 6}.sql.should == 
      'SELECT * FROM test WHERE (((id < 7) OR (id > 11)) OR (id >= 6))'
    @dataset.exclude([:id, 1], [:x, 3]){:id.sql_number < 6}.sql.should == 
      'SELECT * FROM test WHERE (((id != 1) OR (x != 3)) OR (id >= 6))'
  end
end

context "Dataset#invert" do
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

context "Dataset#having" do
  before do
    @dataset = Sequel::Dataset.new(nil).from(:test)
    @grouped = @dataset.group(:region).select(:region, :sum.sql_function(:population), :avg.sql_function(:gdp))
    @d1 = @grouped.having('sum(population) > 10')
    @d2 = @grouped.having(:region => 'Asia')
    @columns = "region, sum(population), avg(gdp)"
  end

  specify "should affect select statements" do
    @d1.select_sql.should ==
      "SELECT #{@columns} FROM test GROUP BY region HAVING (sum(population) > 10)"
  end

  specify "should support proc expressions" do
    @grouped.having {:sum.sql_function(:population) > 10}.sql.should == 
      "SELECT #{@columns} FROM test GROUP BY region HAVING (sum(population) > 10)"
  end

  specify "should work with and on the having clause" do
    @grouped.having( :a.sql_number > 1 ).and( :b.sql_number < 2 ).sql.should ==
      "SELECT #{@columns} FROM test GROUP BY region HAVING ((a > 1) AND (b < 2))"
  end
end

context "a grouped dataset" do
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
    db = MockDatabase.new
    db[:test].select(:name).group(:name).count
    db.sqls.should == ["SELECT COUNT(*) AS count FROM (SELECT name FROM test GROUP BY name) AS t1 LIMIT 1"]
  end
end

context "Dataset#group_by" do
  before do
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
    @dataset.group_by(:type_id=>nil).select_sql.should ==
      "SELECT * FROM test GROUP BY (type_id IS NULL)"
  end

  specify "should ungroup when passed nil, empty, or no arguments" do
    @dataset.group_by.select_sql.should ==
      "SELECT * FROM test"
    @dataset.group_by(nil).select_sql.should ==
      "SELECT * FROM test"
  end

  specify "should undo previous grouping" do
    @dataset.group_by(:a).group_by(:b).select_sql.should ==
      "SELECT * FROM test GROUP BY b"
    @dataset.group_by(:a, :b).group_by.select_sql.should ==
      "SELECT * FROM test"
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
  before do
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
  
  specify "should escape blobs as strings by default" do
    @dataset.literal('abc'.to_sequel_blob).should == "'abc'"
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
    s = t.strftime("'%Y-%m-%d %H:%M:%S")
    @dataset.literal(t).should == "#{s}.#{sprintf('%06i', t.usec)}'"
  end
  
  specify "should literalize DateTime properly" do
    t = DateTime.now
    s = t.strftime("'%Y-%m-%d %H:%M:%S")
    @dataset.literal(t).should == "#{s}.#{sprintf('%06i', t.sec_fraction* 86400000000)}'"
  end
  
  specify "should literalize Date properly" do
    d = Date.today
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
    @dataset.literal(t).should == "#{s}.#{sprintf('%06i', t.sec_fraction* 86400000000)}'"

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
    @dataset.literal(t).should == "#{s}.#{sprintf('%06i', t.sec_fraction* 86400000000)}+0000'"
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
    @dataset.literal('col1 + 2'.lit).should == 'col1 + 2'
    
    @dataset.update_sql(:a => 'a + 2'.lit).should == 
      'UPDATE test SET a = a + 2'
  end

  specify "should literalize BigDecimal instances correctly" do
    @dataset.literal(BigDecimal.new("80")).should == "80.0"
    @dataset.literal(BigDecimal.new("NaN")).should == "'NaN'"
    @dataset.literal(BigDecimal.new("Infinity")).should == "'Infinity'"
    @dataset.literal(BigDecimal.new("-Infinity")).should == "'-Infinity'"
  end

  specify "should raise an Error if the object can't be literalized" do
    proc{@dataset.literal(Object.new)}.should raise_error(Sequel::Error)
  end
end

context "Dataset#from" do
  before do
    @dataset = Sequel::Dataset.new(nil)
  end

  specify "should accept a Dataset" do
    proc {@dataset.from(@dataset)}.should_not raise_error
  end

  specify "should format a Dataset as a subquery if it has had options set" do
    @dataset.from(@dataset.from(:a).where(:a=>1)).select_sql.should ==
      "SELECT * FROM (SELECT * FROM a WHERE (a = 1)) AS t1"
  end
  
  specify "should automatically alias sub-queries" do
    @dataset.from(@dataset.from(:a).group(:b)).select_sql.should ==
      "SELECT * FROM (SELECT * FROM a GROUP BY b) AS t1"
      
    d1 = @dataset.from(:a).group(:b)
    d2 = @dataset.from(:c).group(:d)
    
    @dataset.from(d1, d2).sql.should == 
      "SELECT * FROM (SELECT * FROM a GROUP BY b) AS t1, (SELECT * FROM c GROUP BY d) AS t2"
  end
  
  specify "should accept a hash for aliasing" do
    @dataset.from(:a => :b).sql.should ==
      "SELECT * FROM a AS b"
      
    @dataset.from(:a => 'b').sql.should ==
      "SELECT * FROM a AS b"

    @dataset.from(@dataset.from(:a).group(:b) => :c).sql.should ==
      "SELECT * FROM (SELECT * FROM a GROUP BY b) AS c"
  end

  specify "should always use a subquery if given a dataset" do
    @dataset.from(@dataset.from(:a)).select_sql.should ==
      "SELECT * FROM (SELECT * FROM a) AS t1"
  end
  
  specify "should remove all FROM tables if called with no arguments" do
    @dataset.from.sql.should == 'SELECT *'
  end
  
  specify "should accept sql functions" do
    @dataset.from(:abc.sql_function(:def)).select_sql.should ==
      "SELECT * FROM abc(def)"
    
    @dataset.from(:a.sql_function(:i)).select_sql.should ==
      "SELECT * FROM a(i)"
  end

  specify "should accept :schema__table___alias symbol format" do
    @dataset.from(:abc__def).select_sql.should ==
      "SELECT * FROM abc.def"
    @dataset.from(:abc__def___d).select_sql.should ==
      "SELECT * FROM abc.def AS d"
    @dataset.from(:abc___def).select_sql.should ==
      "SELECT * FROM abc AS def"
  end
end

context "Dataset#select" do
  before do
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

    @d.select(nil, :sum.sql_function(:t), :x___y).sql.should == "SELECT NULL, sum(t), x AS y FROM test"

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

context "Dataset#select_all" do
  before do
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
    @d.select(:a.*).select_more(:b.*).sql.should == 'SELECT a.*, b.* FROM test'
  end

  specify "should accept a block that yields a virtual row" do
    @d.select(:a).select_more{|o| o.b}.sql.should == 'SELECT a, b FROM test'
    @d.select(:a.*).select_more(:b.*){b(1)}.sql.should == 'SELECT a.*, b.*, b(1) FROM test'
  end
end

context "Dataset#order" do
  before do
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

context "Dataset#unfiltered" do
  specify "should remove filtering from the dataset" do
    Sequel::Dataset.new(nil).from(:test).filter(:score=>1).unfiltered.sql.should == 'SELECT * FROM test'
  end
end

context "Dataset#unlimited" do
  specify "should remove limit and offset from the dataset" do
    Sequel::Dataset.new(nil).from(:test).limit(1, 2).unlimited.sql.should == 'SELECT * FROM test'
  end
end

context "Dataset#ungrouped" do
  specify "should remove group and having clauses from the dataset" do
    Sequel::Dataset.new(nil).from(:test).group(:a).having(:b).ungrouped.sql.should == 'SELECT * FROM test'
  end
end

context "Dataset#unordered" do
  specify "should remove ordering from the dataset" do
    Sequel::Dataset.new(nil).from(:test).order(:name).unordered.sql.should == 'SELECT * FROM test'
  end
end

context "Dataset#with_sql" do
  before do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should remove use static sql" do
    @dataset.with_sql('SELECT 1 FROM test').sql.should == 'SELECT 1 FROM test'
  end
  
  specify "should keep row_proc" do
    @dataset.with_sql('SELECT 1 FROM test').row_proc.should == @dataset.row_proc
  end
end

context "Dataset#order_by" do
  before do
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
  before do
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

  specify "should accept a block that yields a virtual row" do
    @dataset.order(:a).order_more{|o| o.b}.sql.should == 'SELECT * FROM test ORDER BY a, b'
    @dataset.order(:a, :b).order_more(:c, :d){[e, f(1, 2)]}.sql.should == 'SELECT * FROM test ORDER BY a, b, c, d, e, f(1, 2)'
  end
end

context "Dataset#reverse_order" do
  before do
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
  before do
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
      'SELECT * FROM (select * from cccc) AS t1 LIMIT 6 OFFSET 10'
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
  before do
    @d1 = Sequel::Dataset.new(nil, {1 => 2, 3 => 4})
    @d2 = @d1.clone
    @d2.row_proc = Proc.new{|r| r}
  end
  
  specify "should remove any existing row_proc" do
    naked = @d2.naked
    naked.row_proc.should be_nil
  end
end

context "Dataset#qualified_column_name" do
  before do
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
  before do
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
  before do
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

context "Dataset#distinct" do
  before do
    @db = MockDatabase.new
    @dataset = @db[:test].select(:name)
  end
  
  specify "should include DISTINCT clause in statement" do
    @dataset.distinct.sql.should == 'SELECT DISTINCT name FROM test'
  end
  
  specify "should raise an error if columns given and distinct on not supported" do
    @dataset.meta_def(:supports_distinct_on?){false}
    proc{@dataset.distinct}.should_not raise_error
    proc{@dataset.distinct(:a)}.should raise_error(Sequel::InvalidOperation)
  end
  
  specify "should accept an expression list" do
    @dataset.distinct(:a, :b).sql.should == 'SELECT DISTINCT ON (a, b) name FROM test'
    @dataset.distinct(:stamp.cast(:integer), :node_id=>nil).sql.should == 'SELECT DISTINCT ON (CAST(stamp AS integer), (node_id IS NULL)) name FROM test'
  end

  specify "should do a subselect for count" do
    @dataset.distinct.count
    @db.sqls.should == ['SELECT COUNT(*) AS count FROM (SELECT DISTINCT name FROM test) AS t1 LIMIT 1']
  end
end

context "Dataset#count" do
  before do
    @c = Class.new(Sequel::Dataset) do
      def self.sql
        @@sql
      end
      
      def fetch_rows(sql)
        @columns = [sql =~ /SELECT COUNT/i ? :count : :a]
        @@sql = sql
        yield({@columns.first=>1})
      end
    end
    @dataset = @c.new(nil).from(:test)
  end
  
  specify "should format SQL properly" do
    @dataset.count.should == 1
    @c.sql.should == 'SELECT COUNT(*) AS count FROM test LIMIT 1'
  end
  
  specify "should include the where clause if it's there" do
    @dataset.filter(:abc.sql_number < 30).count.should == 1
    @c.sql.should == 'SELECT COUNT(*) AS count FROM test WHERE (abc < 30) LIMIT 1'
  end
  
  specify "should count properly for datasets with fixed sql" do
    @dataset.opts[:sql] = "select abc from xyz"
    @dataset.count.should == 1
    @c.sql.should == "SELECT COUNT(*) AS count FROM (select abc from xyz) AS t1 LIMIT 1"
  end

  specify "should count properly when using UNION, INTERSECT, or EXCEPT" do
    @dataset.union(@dataset).count.should == 1
    @c.sql.should == "SELECT COUNT(*) AS count FROM (SELECT * FROM test UNION SELECT * FROM test) AS t1 LIMIT 1"
    @dataset.intersect(@dataset).count.should == 1
    @c.sql.should == "SELECT COUNT(*) AS count FROM (SELECT * FROM test INTERSECT SELECT * FROM test) AS t1 LIMIT 1"
    @dataset.except(@dataset).count.should == 1
    @c.sql.should == "SELECT COUNT(*) AS count FROM (SELECT * FROM test EXCEPT SELECT * FROM test) AS t1 LIMIT 1"
  end

  specify "should return limit if count is greater than it" do
    @dataset.limit(5).count.should == 1
    @c.sql.should == "SELECT COUNT(*) AS count FROM (SELECT * FROM test LIMIT 5) AS t1 LIMIT 1"
  end
  
  it "should work on a graphed_dataset" do
    @dataset.should_receive(:columns).twice.and_return([:a])
    @dataset.graph(@dataset, [:a], :table_alias=>:test2).count.should == 1
    @c.sql.should == 'SELECT COUNT(*) AS count FROM test LEFT OUTER JOIN test AS test2 USING (a) LIMIT 1'
  end

  specify "should not cache the columns value" do
    ds = @dataset.from(:blah)
    ds.columns.should == [:a]
    ds.count.should == 1
    @c.sql.should == 'SELECT COUNT(*) AS count FROM blah LIMIT 1'
    ds.columns.should == [:a]
  end
end


context "Dataset#group_and_count" do
  before do
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

context "Dataset#from_self" do
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
end

context "Dataset#join_table" do
  before do
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
    @d.filter(:price.sql_number < 100).join_table(:right_outer, :categories, :category_id => :id).sql.should ==
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
  
  specify "should support aliased tables using the deprecated argument" do
    @d.from('stats').join('players', {:id => :player_id}, 'p').sql.should ==
      'SELECT * FROM "stats" INNER JOIN "players" AS "p" ON ("p"."id" = "stats"."player_id")'
  end

  specify "should support aliased tables using the :table_alias option" do
    @d.from('stats').join('players', {:id => :player_id}, :table_alias=>:p).sql.should ==
      'SELECT * FROM "stats" INNER JOIN "players" AS "p" ON ("p"."id" = "stats"."player_id")'
  end
  
  specify "should support using an alias for the FROM when doing the first join with unqualified condition columns" do
    ds = MockDataset.new(nil).from(:foo => :f)
    ds.quote_identifiers = true
    ds.join_table(:inner, :bar, :id => :bar_id).sql.should ==
      'SELECT * FROM "foo" AS "f" INNER JOIN "bar" ON ("bar"."id" = "f"."bar_id")'
  end
  
  specify "should support implicit schemas in from table symbols" do
    @d.from(:s__t).join(:u__v, {:id => :player_id}).sql.should ==
      'SELECT * FROM "s"."t" INNER JOIN "u"."v" ON ("u"."v"."id" = "s"."t"."player_id")'
  end

  specify "should support implicit aliases in from table symbols" do
    @d.from(:t___z).join(:v___y, {:id => :player_id}).sql.should ==
      'SELECT * FROM "t" AS "z" INNER JOIN "v" AS "y" ON ("y"."id" = "z"."player_id")'
    @d.from(:s__t___z).join(:u__v___y, {:id => :player_id}).sql.should ==
      'SELECT * FROM "s"."t" AS "z" INNER JOIN "u"."v" AS "y" ON ("y"."id" = "z"."player_id")'
  end
  
  specify "should support AliasedExpressions" do
    @d.from(:s.as(:t)).join(:u.as(:v), {:id => :player_id}).sql.should ==
      'SELECT * FROM "s" AS "t" INNER JOIN "u" AS "v" ON ("v"."id" = "t"."player_id")'
  end

  specify "should support the :implicit_qualifier option" do
    @d.from('stats').join('players', {:id => :player_id}, :implicit_qualifier=>:p).sql.should ==
      'SELECT * FROM "stats" INNER JOIN "players" ON ("players"."id" = "p"."player_id")'
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
      'SELECT * FROM "items" LEFT OUTER JOIN (SELECT * FROM categories WHERE (active IS TRUE)) AS "t1" ON ("t1"."item_id" = "items"."id")'

    @d.from_self.join_table(:left_outer, ds, :item_id => :id).sql.should ==
      'SELECT * FROM (SELECT * FROM "items") AS "t1" LEFT OUTER JOIN (SELECT * FROM categories WHERE (active IS TRUE)) AS "t2" ON ("t2"."item_id" = "t1"."id")'
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
    @d.join(:categories, :number.sql_number > 10).sql.should ==
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
  
  specify "should not allow insert, update, delete, or truncate" do
    proc{@d.join(:categories, :a=>:d).insert_sql}.should raise_error(Sequel::InvalidOperation)
    proc{@d.join(:categories, :a=>:d).update_sql(:a=>1)}.should raise_error(Sequel::InvalidOperation)
    proc{@d.join(:categories, :a=>:d).delete_sql}.should raise_error(Sequel::InvalidOperation)
    proc{@d.join(:categories, :a=>:d).truncate_sql}.should raise_error(Sequel::InvalidOperation)
  end
end

context "Dataset#[]=" do
  before do
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
  before do
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
  before do
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
  before do
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
  before do
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

    @d.filter(:price.sql_number > 100).range(:stamp)
    @d.last_sql.should == "SELECT min(stamp) AS v1, max(stamp) AS v2 FROM test WHERE (price > 100) LIMIT 1"
  end
  
  specify "should return a range object" do
    @d.range(:tryme).should == (1..10)
  end
end

context "Dataset#interval" do
  before do
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

    @d.filter(:price.sql_number > 100).interval(:stamp)
    @d.last_sql.should == "SELECT (max(stamp) - min(stamp)) FROM test WHERE (price > 100) LIMIT 1"
  end
  
  specify "should return an integer" do
    @d.interval(:tryme).should == 1234
  end
end

context "Dataset #first and #last" do
  before do
    @c = Class.new(Sequel::Dataset) do
      def each(&block)
        s = select_sql
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
    @d.first{:z.sql_number > 26}.should == [:a,1,:b,2, 'SELECT * FROM test WHERE (z > 26) LIMIT 1']
    @d.order(:name).last{:z.sql_number > 26}.should == [:a,1,:b,2, 'SELECT * FROM test WHERE (z > 26) ORDER BY name DESC LIMIT 1']
  end
  
  specify "should combine block and standard argument filters if argument is not an Integer" do
    @d.first(:y=>25){:z.sql_number > 26}.should == [:a,1,:b,2, 'SELECT * FROM test WHERE ((z > 26) AND (y = 25)) LIMIT 1']
    @d.order(:name).last('y = ?', 16){:z.sql_number > 26}.should == [:a,1,:b,2, 'SELECT * FROM test WHERE ((z > 26) AND (y = 16)) ORDER BY name DESC LIMIT 1']
  end
  
  specify "should filter and return an array of records if an Integer argument is provided and a block is given" do
    i = rand(10) + 10
    r = @d.order(:a).first(i){:z.sql_number > 26}.should == [[:a,1,:b,2, "SELECT * FROM test WHERE (z > 26) ORDER BY a LIMIT #{i}"]] * i
    i = rand(10) + 10
    r = @d.order(:a).last(i){:z.sql_number > 26}.should == [[:a,1,:b,2, "SELECT * FROM test WHERE (z > 26) ORDER BY a DESC LIMIT #{i}"]] * i
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

context "Dataset compound operations" do
  before do
    @a = Sequel::Dataset.new(nil).from(:a).filter(:z => 1)
    @b = Sequel::Dataset.new(nil).from(:b).filter(:z => 2)
  end
  
  specify "should support UNION and UNION ALL" do
    @a.union(@b).sql.should == \
      "SELECT * FROM (SELECT * FROM a WHERE (z = 1) UNION SELECT * FROM b WHERE (z = 2)) AS t1"
    @b.union(@a, true).sql.should == \
      "SELECT * FROM (SELECT * FROM b WHERE (z = 2) UNION ALL SELECT * FROM a WHERE (z = 1)) AS t1"
    @b.union(@a, :all=>true).sql.should == \
      "SELECT * FROM (SELECT * FROM b WHERE (z = 2) UNION ALL SELECT * FROM a WHERE (z = 1)) AS t1"
  end

  specify "should support INTERSECT and INTERSECT ALL" do
    @a.intersect(@b).sql.should == \
      "SELECT * FROM (SELECT * FROM a WHERE (z = 1) INTERSECT SELECT * FROM b WHERE (z = 2)) AS t1"
    @b.intersect(@a, true).sql.should == \
      "SELECT * FROM (SELECT * FROM b WHERE (z = 2) INTERSECT ALL SELECT * FROM a WHERE (z = 1)) AS t1"
    @b.intersect(@a, :all=>true).sql.should == \
      "SELECT * FROM (SELECT * FROM b WHERE (z = 2) INTERSECT ALL SELECT * FROM a WHERE (z = 1)) AS t1"
  end

  specify "should support EXCEPT and EXCEPT ALL" do
    @a.except(@b).sql.should == \
      "SELECT * FROM (SELECT * FROM a WHERE (z = 1) EXCEPT SELECT * FROM b WHERE (z = 2)) AS t1"
    @b.except(@a, true).sql.should == \
      "SELECT * FROM (SELECT * FROM b WHERE (z = 2) EXCEPT ALL SELECT * FROM a WHERE (z = 1)) AS t1"
    @b.except(@a, :all=>true).sql.should == \
      "SELECT * FROM (SELECT * FROM b WHERE (z = 2) EXCEPT ALL SELECT * FROM a WHERE (z = 1)) AS t1"
  end
    
  specify "should support :from_self=>false option to not wrap the compound in a SELECT * FROM (...)" do
    @b.union(@a, :from_self=>false).sql.should == \
      "SELECT * FROM b WHERE (z = 2) UNION SELECT * FROM a WHERE (z = 1)"
    @b.intersect(@a, :from_self=>false).sql.should == \
      "SELECT * FROM b WHERE (z = 2) INTERSECT SELECT * FROM a WHERE (z = 1)"
    @b.except(@a, :from_self=>false).sql.should == \
      "SELECT * FROM b WHERE (z = 2) EXCEPT SELECT * FROM a WHERE (z = 1)"
      
    @b.union(@a, :from_self=>false, :all=>true).sql.should == \
      "SELECT * FROM b WHERE (z = 2) UNION ALL SELECT * FROM a WHERE (z = 1)"
    @b.intersect(@a, :from_self=>false, :all=>true).sql.should == \
      "SELECT * FROM b WHERE (z = 2) INTERSECT ALL SELECT * FROM a WHERE (z = 1)"
    @b.except(@a, :from_self=>false, :all=>true).sql.should == \
      "SELECT * FROM b WHERE (z = 2) EXCEPT ALL SELECT * FROM a WHERE (z = 1)"
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
    @a.union(@b).union(@a, true).sql.should == \
      "SELECT * FROM (SELECT * FROM (SELECT * FROM a WHERE (z = 1) UNION SELECT * FROM b WHERE (z = 2)) AS t1 UNION ALL SELECT * FROM a WHERE (z = 1)) AS t1"
    @a.intersect(@b, true).intersect(@a).sql.should == \
      "SELECT * FROM (SELECT * FROM (SELECT * FROM a WHERE (z = 1) INTERSECT ALL SELECT * FROM b WHERE (z = 2)) AS t1 INTERSECT SELECT * FROM a WHERE (z = 1)) AS t1"
    @a.except(@b).except(@a, true).sql.should == \
      "SELECT * FROM (SELECT * FROM (SELECT * FROM a WHERE (z = 1) EXCEPT SELECT * FROM b WHERE (z = 2)) AS t1 EXCEPT ALL SELECT * FROM a WHERE (z = 1)) AS t1"
  end
  
  specify "should use a subselect when using a compound operation with a dataset that already has a compound operation" do
    @a.union(@b.union(@a, true)).sql.should == \
      "SELECT * FROM (SELECT * FROM a WHERE (z = 1) UNION SELECT * FROM (SELECT * FROM b WHERE (z = 2) UNION ALL SELECT * FROM a WHERE (z = 1)) AS t1) AS t1"
    @a.intersect(@b.intersect(@a), true).sql.should == \
      "SELECT * FROM (SELECT * FROM a WHERE (z = 1) INTERSECT ALL SELECT * FROM (SELECT * FROM b WHERE (z = 2) INTERSECT SELECT * FROM a WHERE (z = 1)) AS t1) AS t1"
    @a.except(@b.except(@a, true)).sql.should == \
      "SELECT * FROM (SELECT * FROM a WHERE (z = 1) EXCEPT SELECT * FROM (SELECT * FROM b WHERE (z = 2) EXCEPT ALL SELECT * FROM a WHERE (z = 1)) AS t1) AS t1"
  end

  specify "should order and limit properly when using UNION, INTERSECT, or EXCEPT" do
    @dataset = Sequel::Dataset.new(nil).from(:test)
    @dataset.union(@dataset).limit(2).sql.should ==
      "SELECT * FROM (SELECT * FROM test UNION SELECT * FROM test) AS t1 LIMIT 2"
    @dataset.limit(2).intersect(@dataset).sql.should == 
      "SELECT * FROM (SELECT * FROM (SELECT * FROM test LIMIT 2) AS t1 INTERSECT SELECT * FROM test) AS t1"
    @dataset.except(@dataset.limit(2)).sql.should == 
      "SELECT * FROM (SELECT * FROM test EXCEPT SELECT * FROM (SELECT * FROM test LIMIT 2) AS t1) AS t1"

    @dataset.union(@dataset).order(:num).sql.should ==
      "SELECT * FROM (SELECT * FROM test UNION SELECT * FROM test) AS t1 ORDER BY num"
    @dataset.order(:num).intersect(@dataset).sql.should == 
      "SELECT * FROM (SELECT * FROM (SELECT * FROM test ORDER BY num) AS t1 INTERSECT SELECT * FROM test) AS t1"
    @dataset.except(@dataset.order(:num)).sql.should == 
      "SELECT * FROM (SELECT * FROM test EXCEPT SELECT * FROM (SELECT * FROM test ORDER BY num) AS t1) AS t1"

    @dataset.limit(2).order(:a).union(@dataset.limit(3).order(:b)).order(:c).limit(4).sql.should ==
      "SELECT * FROM (SELECT * FROM (SELECT * FROM test ORDER BY a LIMIT 2) AS t1 UNION SELECT * FROM (SELECT * FROM test ORDER BY b LIMIT 3) AS t1) AS t1 ORDER BY c LIMIT 4"
  end

end

context "Dataset#[]" do
  before do
    @c = Class.new(Sequel::Dataset) do
      @@last_dataset = nil
      
      def self.last_dataset
        @@last_dataset
      end

      def single_record
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
  before do
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
  
  specify "should return nil if no record is present" do
    @e.single_record.should be_nil
  end
end

context "Dataset#single_value" do
  before do
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
  
  specify "should return nil if no records" do
    @e.single_value.should be_nil
  end
  
  it "should work on a graphed_dataset" do
    @d.should_receive(:columns).twice.and_return([:a])
    @d.graph(@d, [:a], :table_alias=>:test2).single_value.should == 'SELECT test.a, test2.a AS test2_a FROM test LEFT OUTER JOIN test AS test2 USING (a) LIMIT 1'
  end
end

context "Dataset#get" do
  before do
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
  
  specify "should accept a block that yields a virtual row" do
    @d.get{|o| o.x__b.as(:name)}.should == "SELECT x.b AS name FROM test LIMIT 1"
    @d.get{x(1).as(:name)}.should == "SELECT x(1) AS name FROM test LIMIT 1"
  end
  
  specify "should raise an error if both a regular argument and block argument are used" do
    proc{@d.get(:name){|o| o.x__b.as(:name)}}.should raise_error(Sequel::Error)
  end
end

context "Dataset#set_row_proc" do
  before do
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

context "Dataset#<<" do
  before do
    @d = Sequel::Dataset.new(nil)
    @d.meta_def(:insert) do |*args|
      1234567890
    end
  end
  
  specify "should call #insert" do
    (@d << {:name => 1}).should == 1234567890
  end
end

context "Dataset#columns" do
  before do
    @dataset = DummyDataset.new(nil).from(:items)
    @dataset.meta_def(:columns=) {|c| @columns = c}
    i = 'a' 
    @dataset.meta_def(:each){@columns = select_sql + i; i = i.next}
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
  before do
    @dataset = DummyDataset.new(nil).from(:items)
    i = 'a' 
    @dataset.meta_def(:each){@columns = select_sql + i; i = i.next}
  end
  
  specify "should always attempt to get a record and return @columns" do
    @dataset.columns!.should == 'SELECT * FROM items LIMIT 1a'
    @dataset.columns!.should == 'SELECT * FROM items LIMIT 1b'
    @dataset.opts[:from] = [:nana]
    @dataset.columns!.should == 'SELECT * FROM nana LIMIT 1c'
  end
end

context "Dataset#import" do
  before do
    @dbc = Class.new(Sequel::Database) do
      attr_reader :sqls
      
      def execute(sql, opts={})
        @sqls ||= []
        @sqls << sql
      end
      alias execute_dui execute
      
      def transaction(opts={})
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
  
  specify "should accept string keys as column names" do
    @ds.import(['x', 'y'], [[1, 2], [3, 4]])
    @db.sqls.should == [
      'BEGIN',
      "INSERT INTO items (x, y) VALUES (1, 2)",
      "INSERT INTO items (x, y) VALUES (3, 4)",
      'COMMIT'
    ]
  end

  specify "should accept a columns array and a values array" do
    @ds.import([:x, :y], [[1, 2], [3, 4]])
    @db.sqls.should == [
      'BEGIN',
      "INSERT INTO items (x, y) VALUES (1, 2)",
      "INSERT INTO items (x, y) VALUES (3, 4)",
      'COMMIT'
    ]
  end

  specify "should accept a columns array and a dataset" do
    @ds2 = Sequel::Dataset.new(@db).from(:cats).filter(:purr => true).select(:a, :b)
    
    @ds.import([:x, :y], @ds2)
    @db.sqls.should == [
      'BEGIN',
      "INSERT INTO items (x, y) SELECT a, b FROM cats WHERE (purr IS TRUE)",
      'COMMIT'
    ]
  end

  specify "should accept a columns array and a values array with :commit_every option" do
    @ds.import([:x, :y], [[1, 2], [3, 4], [5, 6]], :commit_every => 3)
    @db.sqls.should == [
      'BEGIN',
      "INSERT INTO items (x, y) VALUES (1, 2)",
      "INSERT INTO items (x, y) VALUES (3, 4)",
      "INSERT INTO items (x, y) VALUES (5, 6)",
      'COMMIT',
    ]
  end
  specify "should accept a columns array and a values array with slice option" do
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
end

context "Dataset#multi_insert" do
  before do
    @dbc = Class.new do
      attr_reader :sqls
      
      def execute(sql, opts={})
        @sqls ||= []
        @sqls << sql
      end
      alias execute_dui execute
      
      def transaction(opts={})
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
  
  specify "should issue multiple insert statements inside a transaction" do
    @ds.multi_insert(@list)
    @db.sqls.should == [
      'BEGIN',
      "INSERT INTO items (name) VALUES ('abc')",
      "INSERT INTO items (name) VALUES ('def')",
      "INSERT INTO items (name) VALUES ('ghi')",
      'COMMIT'
    ]
  end
  
  specify "should handle different formats for tables" do
    @ds = @ds.from(:sch__tab)
    @ds.multi_insert(@list)
    @db.sqls.should == [
      'BEGIN',
      "INSERT INTO sch.tab (name) VALUES ('abc')",
      "INSERT INTO sch.tab (name) VALUES ('def')",
      "INSERT INTO sch.tab (name) VALUES ('ghi')",
      'COMMIT'
    ]
    @db.sqls.clear

    @ds = @ds.from(:tab.qualify(:sch))
    @ds.multi_insert(@list)
    @db.sqls.should == [
      'BEGIN',
      "INSERT INTO sch.tab (name) VALUES ('abc')",
      "INSERT INTO sch.tab (name) VALUES ('def')",
      "INSERT INTO sch.tab (name) VALUES ('ghi')",
      'COMMIT'
    ]
    @db.sqls.clear
    @ds = @ds.from(:sch__tab.identifier)
    @ds.multi_insert(@list)
    @db.sqls.should == [
      'BEGIN',
      "INSERT INTO sch__tab (name) VALUES ('abc')",
      "INSERT INTO sch__tab (name) VALUES ('def')",
      "INSERT INTO sch__tab (name) VALUES ('ghi')",
      'COMMIT'
    ]
  end
  
  specify "should accept the :commit_every option for committing every x records" do
    @ds.multi_insert(@list, :commit_every => 1)
    @db.sqls.should == [
      'BEGIN',
      "INSERT INTO items (name) VALUES ('abc')",
      'COMMIT',
      'BEGIN',
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
  
  specify "should accept string keys as column names" do
    @ds.multi_insert([{'x'=>1, 'y'=>2}, {'x'=>3, 'y'=>4}])
    @db.sqls.should == [
      'BEGIN',
      "INSERT INTO items (x, y) VALUES (1, 2)",
      "INSERT INTO items (x, y) VALUES (3, 4)",
      'COMMIT'
    ]
  end

  specify "should not do anything if no hashes are provided" do
    @ds.multi_insert([])
    @db.sqls.should be_nil
  end
end

context "Dataset" do
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
    @d.filter!{:y.sql_number < 2}
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

context "Dataset#to_csv" do
  before do
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

context "Dataset#update_sql" do
  before do
    @ds = Sequel::Dataset.new(nil).from(:items)
  end
  
  specify "should accept strings" do
    @ds.update_sql("a = b").should == "UPDATE items SET a = b"
  end
  
  specify "should accept hash with string keys" do
    @ds.update_sql('c' => 'd').should == "UPDATE items SET c = 'd'"
  end

  specify "should accept array subscript references" do
    @ds.update_sql((:day.sql_subscript(1)) => 'd').should == "UPDATE items SET day[1] = 'd'"
  end
end

context "Dataset#insert_sql" do
  before do
    @ds = Sequel::Dataset.new(nil).from(:items)
  end
  
  specify "should accept hash with symbol keys" do
    @ds.insert_sql(:c => 'd').should == "INSERT INTO items (c) VALUES ('d')"
  end

  specify "should accept hash with string keys" do
    @ds.insert_sql('c' => 'd').should == "INSERT INTO items (c) VALUES ('d')"
  end

  specify "should accept array subscript references" do
    @ds.insert_sql((:day.sql_subscript(1)) => 'd').should == "INSERT INTO items (day[1]) VALUES ('d')"
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
    @ds.insert_sql('VALUES (1, 2, 3)'.lit).should == "INSERT INTO items VALUES (1, 2, 3)"
  end
  
  specify "should accept an array of columns and an LiteralString" do
    @ds.insert_sql([:a, :b, :c], 'VALUES (1, 2, 3)'.lit).should == "INSERT INTO items (a, b, c) VALUES (1, 2, 3)"
  end
  
  specify "should accept an object that responds to values and returns a hash by using that hash as the columns and values" do
    o = Object.new
    def o.values; {:c=>'d'}; end
    @ds.insert_sql(o).should == "INSERT INTO items (c) VALUES ('d')"
  end
  
  specify "should accept an object that responds to values and returns something other than a hash by using the object itself as a single value" do
    o = Date.civil(2000, 1, 1)
    def o.values; self; end
    @ds.insert_sql(o).should == "INSERT INTO items VALUES ('2000-01-01')"
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

context "Dataset#inspect" do
  before do
    @ds = Sequel::Dataset.new(nil).from(:blah)
  end
  
  specify "should include the class name and the corresponding SQL statement" do
    @ds.inspect.should == '#<%s: %s>' % [@ds.class.to_s, @ds.sql.inspect]
  end
end

context "Dataset#all" do
  before do
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
  
  specify "should iterate over the array if a block is given" do
    a = []
    
    @dataset.all do |r|
      a << (r.is_a?(Hash) ? r[:x] : r)
    end
    
    a.should == [1, 3, "SELECT * FROM items"]
  end
end

context "Dataset#grep" do
  before do
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

  specify "should support searching against other columns" do
    @ds.grep(:title, :body).sql.should ==
      "SELECT * FROM posts WHERE ((title LIKE body))"
  end
end

context "Dataset default #fetch_rows, #insert, #update, #delete, #truncate, #execute" do
  before do
    @db = Sequel::Database.new
    @ds = @db[:items]
  end

  specify "#fetch_rows should raise a NotImplementedError" do
    proc{@ds.fetch_rows(''){}}.should raise_error(NotImplementedError)
  end

  specify "#delete should execute delete SQL" do
    @db.should_receive(:execute).once.with('DELETE FROM items', :server=>:default)
    @ds.delete
    @db.should_receive(:execute_dui).once.with('DELETE FROM items', :server=>:default)
    @ds.delete
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
  end
  
  specify "#execute should execute the SQL on the database" do
    @db.should_receive(:execute).once.with('SELECT 1', :server=>:read_only)
    @ds.send(:execute, 'SELECT 1')
  end
end

context "Dataset prepared statements and bound variables " do
  before do
    @db = Sequel::Database.new
    @db.meta_def(:sqls){@sqls||=[]}
    def @db.execute(sql, opts={})
      sqls << sql
    end
    def @db.dataset
      ds = super()
      def ds.fetch_rows(sql, &block)
        execute(sql)
      end
      ds
    end
    @ds = @db[:items]
  end
  
  specify "#call should take a type and bind hash and interpolate it" do
    @ds.filter(:num=>:$n).call(:select, :n=>1)
    @ds.filter(:num=>:$n).call(:first, :n=>1)
    @ds.filter(:num=>:$n).call(:delete, :n=>1)
    @ds.filter(:num=>:$n).call(:update, {:n=>1, :n2=>2}, :num=>:$n2)
    @ds.call(:insert, {:n=>1}, :num=>:$n)
    @db.sqls.should == ['SELECT * FROM items WHERE (num = 1)',
      'SELECT * FROM items WHERE (num = 1) LIMIT 1',
      'DELETE FROM items WHERE (num = 1)',
      'UPDATE items SET num = 2 WHERE (num = 1)',
      'INSERT INTO items (num) VALUES (1)']
  end
    
  specify "#prepare should take a type and name and store it in the database for later use with call" do
    pss = []
    pss << @ds.filter(:num=>:$n).prepare(:select, :sn)
    pss << @ds.filter(:num=>:$n).prepare(:first, :fn)
    pss << @ds.filter(:num=>:$n).prepare(:delete, :dn)
    pss << @ds.filter(:num=>:$n).prepare(:update, :un, :num=>:$n2)
    pss << @ds.prepare(:insert, :in, :num=>:$n)
    @db.prepared_statements.keys.sort_by{|k| k.to_s}.should == [:dn, :fn, :in, :sn, :un]
    [:sn, :fn, :dn, :un, :in].each_with_index{|x, i| @db.prepared_statements[x].should == pss[i]}
    @db.call(:sn, :n=>1)
    @db.call(:fn, :n=>1)
    @db.call(:dn, :n=>1)
    @db.call(:un, :n=>1, :n2=>2)
    @db.call(:in, :n=>1)
    @db.sqls.should == ['SELECT * FROM items WHERE (num = 1)',
      'SELECT * FROM items WHERE (num = 1) LIMIT 1',
      'DELETE FROM items WHERE (num = 1)',
      'UPDATE items SET num = 2 WHERE (num = 1)',
      'INSERT INTO items (num) VALUES (1)']
  end
    
  specify "#inspect should indicate it is a prepared statement with the prepared SQL" do
    @ds.filter(:num=>:$n).prepare(:select, :sn).inspect.should == \
      '<Sequel::Dataset/PreparedStatement "SELECT * FROM items WHERE (num = $n)">'
  end
    
  specify "should handle literal strings" do
    @ds.filter("num = ?", :$n).call(:select, :n=>1)
    @db.sqls.should == ['SELECT * FROM items WHERE (num = 1)']
  end
    
  specify "should handle datasets using static sql and placeholders" do
    @db["SELECT * FROM items WHERE (num = ?)", :$n].call(:select, :n=>1)
    @db.sqls.should == ['SELECT * FROM items WHERE (num = 1)']
  end
    
  specify "should handle subselects" do
    @ds.filter(:$b).filter(:num=>@ds.select(:num).filter(:num=>:$n)).filter(:$c).call(:select, :n=>1, :b=>0, :c=>2)
    @db.sqls.should == ['SELECT * FROM items WHERE ((0 AND (num IN (SELECT num FROM items WHERE (num = 1)))) AND 2)']
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

context Sequel::Dataset::UnnumberedArgumentMapper do
  before do
    @db = Sequel::Database.new
    @db.meta_def(:sqls){@sqls||=[]}
    def @db.execute(sql, opts={})
      sqls << [sql, *opts[:arguments]]
    end
    @ds = @db[:items].filter(:num=>:$n)
    def @ds.fetch_rows(sql, &block)
      execute(sql)
    end
    def @ds.execute(sql, opts={}, &block)
      @db.execute(sql, {:arguments=>bind_arguments}.merge(opts))
    end
    def @ds.execute_dui(*args, &block)
      execute(*args, &block)
    end
    def @ds.execute_insert(*args, &block)
      execute(*args, &block)
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
    @ps.first.inspect.should == '<Sequel::Dataset/PreparedStatement "SELECT * FROM items WHERE (num = ?)">'
  end
  
  specify "should submitted the SQL to the database with placeholders and bind variables" do
    @ps.each{|p| p.call(:n=>1)}
    @db.sqls.should == [["SELECT * FROM items WHERE (num = ?)", 1],
      ["SELECT * FROM items WHERE (num = ?)", 1],
      ["SELECT * FROM items WHERE (num = ?) LIMIT 1", 1],
      ["DELETE FROM items WHERE (num = ?)", 1],
      ["INSERT INTO items (num) VALUES (?)", 1],
      ["UPDATE items SET num = ? WHERE (num = ?)", 1, 1],
      ]
  end
end

context "Sequel::Dataset#server" do
  specify "should set the server to use for the dataset" do
    @db = Sequel::Database.new
    @ds = @db[:items].server(:s)
    sqls = []
    @db.meta_def(:execute) do |sql, opts|
      sqls << [sql, opts[:server]]
    end
    def @ds.fetch_rows(sql, &block)
      execute(sql)
    end
    @ds.all
    @ds.server(:i).insert(:a=>1)
    @ds.server(:d).delete
    @ds.server(:u).update(:a=>:a+1)
    sqls.should == [['SELECT * FROM items', :s],
      ['INSERT INTO items (a) VALUES (1)', :i],
      ['DELETE FROM items', :d],
      ['UPDATE items SET a = (a + 1)', :u]]
  end
end

context "Sequel::Dataset #set_defaults" do
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

context "Sequel::Dataset #set_overrides" do
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

context "Sequel::Dataset#qualify" do
  specify "should qualify to the given table" do
    MockDatabase.new[:t].filter{a<b}.qualify(:e).sql.should == 'SELECT e.* FROM t WHERE (e.a < e.b)'
  end

  specify "should qualify to the first source if no table if given" do
    MockDatabase.new[:t].filter{a<b}.qualify.sql.should == 'SELECT t.* FROM t WHERE (t.a < t.b)'
  end
end

context "Sequel::Dataset#qualify_to" do
  specify "should qualify to the given table" do
    MockDatabase.new[:t].filter{a<b}.qualify_to(:e).sql.should == 'SELECT e.* FROM t WHERE (e.a < e.b)'
  end
end

context "Sequel::Dataset#qualify_to_first_source" do
  before do
    @ds = MockDatabase.new[:t]
  end

  specify "should qualify_to the first source" do
    @ds.qualify_to_first_source.sql.should == 'SELECT t.* FROM t'
    @ds.should_receive(:qualify_to).with(:t).once
    @ds.qualify_to_first_source
  end

  specify "should handle the select, order, where, having, and group options/clauses" do
    @ds.select(:a).filter(:a=>1).order(:a).group(:a).having(:a).qualify_to_first_source.sql.should == \
      'SELECT t.a FROM t WHERE (t.a = 1) GROUP BY t.a HAVING t.a ORDER BY t.a'
  end

  specify "should handle the select using a table.* if all columns are currently selected" do
    @ds.filter(:a=>1).order(:a).group(:a).having(:a).qualify_to_first_source.sql.should == \
      'SELECT t.* FROM t WHERE (t.a = 1) GROUP BY t.a HAVING t.a ORDER BY t.a'
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
    @ds.select({:b=>{:c=>1}}.case(false)).qualify_to_first_source.sql.should == "SELECT (CASE WHEN t.b THEN (t.c = 1) ELSE 'f' END) FROM t"
  end

  specify "should handle SQL::Identifiers" do
    @ds.select{a}.qualify_to_first_source.sql.should == 'SELECT t.a FROM t'
  end

  specify "should handle SQL::OrderedExpressions" do
    @ds.order(:a.desc, :b.asc).qualify_to_first_source.sql.should == 'SELECT t.* FROM t ORDER BY t.a DESC, t.b ASC'
  end

  specify "should handle SQL::AliasedExpressions" do
    @ds.select(:a.as(:b)).qualify_to_first_source.sql.should == 'SELECT t.a AS b FROM t'
  end

  specify "should handle SQL::CaseExpressions" do
    @ds.filter{{a=>b}.case(c, d)}.qualify_to_first_source.sql.should == 'SELECT t.* FROM t WHERE (CASE t.d WHEN t.a THEN t.b ELSE t.c END)'
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

  specify "should handle SQL::SQLArrays" do
    @ds.filter(:a=>[:b, :c].sql_array).qualify_to_first_source.sql.should == 'SELECT t.* FROM t WHERE (t.a IN (t.b, t.c))'
  end

  specify "should handle SQL::Subscripts" do
    @ds.filter{a.sql_subscript(b,3)}.qualify_to_first_source.sql.should == 'SELECT t.* FROM t WHERE t.a[t.b, 3]'
  end

  specify "should handle SQL::PlaceholderLiteralStrings" do
    @ds.filter('? > ?', :a, 1).qualify_to_first_source.sql.should == 'SELECT t.* FROM t WHERE (t.a > 1)'
  end

  specify "should handle SQL::WindowFunctions" do
    @ds.meta_def(:supports_window_functions?){true}
    @ds.select{sum(:over, :args=>:a, :partition=>:b, :order=>:c){}}.qualify_to_first_source.sql.should == 'SELECT sum(t.a) OVER (PARTITION BY t.b ORDER BY t.c) FROM t'
  end

  specify "should handle all other objects by returning them unchanged" do
    @ds.select("a").filter{a(3)}.filter('blah').order('true'.lit).group('a > ?'.lit(1)).having(false).qualify_to_first_source.sql.should == \
      "SELECT 'a' FROM t WHERE (a(3) AND (blah)) GROUP BY a > 1 HAVING 'f' ORDER BY true"
  end
end

context "Sequel::Dataset #with and #with_recursive" do
  before do
    @db = MockDatabase.new
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
  
  specify "#with_recursive should take an :union_all=>false option" do
    @ds.with_recursive(:t, @db[:x], @db[:t], :union_all=>false).sql.should == 'WITH t AS (SELECT * FROM x UNION SELECT * FROM t) SELECT * FROM t'
  end

  specify "#with and #with_recursive should raise an error unless the dataset supports CTEs" do
    @ds.meta_def(:supports_cte?){false}
    proc{@ds.with(:t, @db[:x], :args=>[:b])}.should raise_error(Sequel::Error)
    proc{@ds.with_recursive(:t, @db[:x], @db[:t], :args=>[:b, :c])}.should raise_error(Sequel::Error)
  end
end

describe Sequel::SQL::Constants do
  before do
    @db = MockDatabase.new
  end
  
  it "should have CURRENT_DATE" do
    @db.literal(Sequel::SQL::Constants::CURRENT_DATE) == 'CURRENT_DATE'
    @db.literal(Sequel::CURRENT_DATE) == 'CURRENT_DATE'
  end

  it "should have CURRENT_TIME" do
    @db.literal(Sequel::SQL::Constants::CURRENT_TIME) == 'CURRENT_TIME'
    @db.literal(Sequel::CURRENT_TIME) == 'CURRENT_TIME'
  end

  it "should have CURRENT_TIMESTAMP" do
    @db.literal(Sequel::SQL::Constants::CURRENT_TIMESTAMP) == 'CURRENT_TIMESTAMP'
    @db.literal(Sequel::CURRENT_TIMESTAMP) == 'CURRENT_TIMESTAMP'
  end
end

describe "Sequel timezone support" do
  before do
    @db = MockDatabase.new
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
    s = t.new_offset(Sequel::LOCAL_DATETIME_OFFSET).strftime("'%Y-%m-%d %H:%M:%S")
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
