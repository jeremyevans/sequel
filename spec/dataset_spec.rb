require File.join(File.dirname(__FILE__), '../lib/sequel')

context "Dataset" do
  setup do
    @dataset = Sequel::Dataset.new("db")
  end
  
  specify "should accept database and opts in initialize" do
    db = 'db'
    opts = {:from => :test}
    d = Sequel::Dataset.new(db, opts)
    d.db.should be(db)
    d.opts.should be(opts)
    
    d = Sequel::Dataset.new(db)
    d.db.should be(db)
    d.opts.should be_a_kind_of(Hash)
    d.opts.should == {}
  end
  
  specify "should provide clone_merge for chainability." do
    d1 = @dataset.clone_merge(:from => :test)
    d1.class.should == @dataset.class
    d1.should_not == @dataset
    d1.db.should be(@dataset.db)
    d1.opts[:from].should == :test
    @dataset.opts[:from].should be_nil
    
    d2 = d1.clone_merge(:order => :name)
    d2.class.should == @dataset.class
    d2.should_not == d1
    d2.should_not == @dataset
    d2.db.should be(@dataset.db)
    d2.opts[:from].should == :test
    d2.opts[:order].should == :name
    d1.opts[:order].should be_nil
  end
  
  specify "should include Enumerable" do
    Sequel::Dataset.included_modules.should include(Enumerable)
  end
  
  specify "should raise NotImplementedError for the dataset interface methods" do
    proc {@dataset.fetch_rows('abc')}.should raise_error(NotImplementedError)
    proc {@dataset.insert(1, 2, 3)}.should raise_error(NotImplementedError)
    proc {@dataset.update(:name => 'abc')}.should raise_error(NotImplementedError)
    proc {@dataset.delete}.should raise_error(NotImplementedError)
  end
end

context "Dataset#clone_merge" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:items)
  end
  
  specify "should return a clone self" do
    clone = @dataset.clone_merge({})
    clone.class.should == @dataset.class
    clone.db.should == @dataset.db
    clone.opts.should == @dataset.opts
  end
  
  specify "should merge the specified options" do
    clone = @dataset.clone_merge(1 => 2)
    clone.opts.should == {1 => 2, :from => [:items]}
  end
  
  specify "should overwrite existing options" do
    clone = @dataset.clone_merge(:from => [:other])
    clone.opts.should == {:from => [:other]}
  end
  
  specify "should create a clone with a deep copy of options" do
    clone = @dataset.clone_merge(:from => [:other])
    @dataset.opts[:from].should == [:items]
    clone.opts[:from].should == [:other]
  end
  
  specify "should return an object with the same modules included" do
    m = Module.new do
      def __xyz__; "xyz"; end
    end
    @dataset.extend(m)
    @dataset.clone_merge({}).should respond_to(:__xyz__)
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
    @dataset.insert_sql.should == 'INSERT INTO test DEFAULT VALUES;'
    @dataset.insert_sql(:name => 'wxyz', :price => 342).
      should match(/INSERT INTO test \(name, price\) VALUES \('wxyz', 342\)|INSERT INTO test \(price, name\) VALUES \(342, 'wxyz'\)/)
    @dataset.insert_sql('a', 2, 6.5).should ==
      "INSERT INTO test VALUES ('a', 2, 6.5);"
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
    proc {@dataset.update_sql(:a=>1)}.should raise_error
  end

  specify "should raise on #delete_sql" do
    proc {@dataset.delete_sql}.should raise_error
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
      should match(/WHERE \(name = 'xyz'\) AND \(price = 342\)|WHERE \(price = 342\) AND \(name = 'xyz'\)/)
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
    @d1.where('(GDP > ?)', 1000).select_sql.should == 
      "SELECT * FROM test WHERE (region = 'Asia') AND (GDP > 1000)"
    
    # array and array
    @d2.where('(GDP > ?)', 1000).select_sql.should ==
      "SELECT * FROM test WHERE (region = 'Asia') AND (GDP > 1000)"
    
    # array and hash
    @d2.where(:name => ['Japan', 'China']).select_sql.should ==
      "SELECT * FROM test WHERE (region = 'Asia') AND (name IN ('Japan', 'China'))"
      
    # array and string
    @d2.where('GDP > ?').select_sql.should ==
      "SELECT * FROM test WHERE (region = 'Asia') AND (GDP > ?)"
    
    # string and string
    @d3.where('b = 2').select_sql.should ==
      "SELECT * FROM test WHERE (a = 1) AND (b = 2)"
    
    # string and hash
    @d3.where(:c => 3).select_sql.should == 
      "SELECT * FROM test WHERE (a = 1) AND (c = 3)"
      
    # string and array
    @d3.where('(d = ?)', 4).select_sql.should ==
      "SELECT * FROM test WHERE (a = 1) AND (d = 4)"
      
    # string and proc expr
    @d3.where {e < 5}.select_sql.should ==
      "SELECT * FROM test WHERE (a = 1) AND (e < 5)"
  end
  
  specify "should raise if the dataset is grouped" do
    proc {@dataset.group(:t).where(:a => 1)}.should raise_error
  end
  
  specify "should accept ranges" do
    @dataset.filter(:id => 4..7).sql.should ==
      'SELECT * FROM test WHERE (id >= 4 AND id <= 7)'
    @dataset.filter(:id => 4...7).sql.should ==
      'SELECT * FROM test WHERE (id >= 4 AND id < 7)'

    @dataset.filter {id == (4..7)}.sql.should ==
      'SELECT * FROM test WHERE (id >= 4 AND id <= 7)'

    @dataset.filter {id.in 4..7}.sql.should ==
      'SELECT * FROM test WHERE (id >= 4 AND id <= 7)'
  end
  
  specify "should accept nil" do
    @dataset.filter(:owner_id => nil).sql.should ==
      'SELECT * FROM test WHERE (owner_id IS NULL)'

    @dataset.filter{owner_id.nil?}.sql.should ==
      'SELECT * FROM test WHERE (owner_id IS NULL)'
  end
  
  specify "should accept a subquery" do
    # select all countries that have GDP greater than the average for Asia
    @dataset.filter('gdp > ?', @d1.select(:gdp.AVG)).sql.should ==
      "SELECT * FROM test WHERE gdp > (SELECT avg(gdp) FROM test WHERE (region = 'Asia'))"

    @dataset.filter(:id => @d1.select(:id)).sql.should ==
      "SELECT * FROM test WHERE (id IN (SELECT id FROM test WHERE (region = 'Asia')))"
  end
  
  specify "should accept a subquery for an EXISTS clause" do
    a = @dataset.filter {price < 100}
    @dataset.filter(a.exists).sql.should ==
      'SELECT * FROM test WHERE EXISTS (SELECT 1 FROM test WHERE (price < 100))'
  end
  
  specify "should accept proc expressions (nice!)" do
    d = @d1.select(:gdp.AVG)
    @dataset.filter {gdp > d}.sql.should ==
      "SELECT * FROM test WHERE (gdp > (SELECT avg(gdp) FROM test WHERE (region = 'Asia')))"
    
    @dataset.filter {id.in 4..7}.sql.should ==
      'SELECT * FROM test WHERE (id >= 4 AND id <= 7)'
    
    @dataset.filter {c == 3}.sql.should ==
      'SELECT * FROM test WHERE (c = 3)'
      
    @dataset.filter {id == :items__id}.sql.should ==
      'SELECT * FROM test WHERE (id = items.id)'
      
    @dataset.filter {a < 1}.sql.should ==
      'SELECT * FROM test WHERE (a < 1)'

    @dataset.filter {a <=> 1}.sql.should ==
      'SELECT * FROM test WHERE NOT (a = 1)'
      
    @dataset.filter {a >= 1 && b <= 2}.sql.should ==
      'SELECT * FROM test WHERE (a >= 1) AND (b <= 2)'
      
    @dataset.filter {c =~ 'ABC%'}.sql.should ==
      "SELECT * FROM test WHERE (c LIKE 'ABC%')"

    @dataset.filter {test.ccc =~ 'ABC%'}.sql.should ==
      "SELECT * FROM test WHERE (test.ccc LIKE 'ABC%')"
  end
  
  specify "should raise SequelError for invalid proc expressions" do
    proc {@dataset.filter {Object.czxczxcz}}.should raise_error(SequelError)
    proc {@dataset.filter {a.bcvxv}}.should raise_error(SequelError)
    proc {@dataset.filter {x}}.should raise_error(SequelError)
  end
end

context "Dataset#or" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test)
    @d1 = @dataset.where(:x => 1)
  end
  
  specify "should raise if no filter exists" do
    proc {@dataset.or(:a => 1)}.should raise_error(SequelError)
  end
  
  specify "should add an alternative expression to the where clause" do
    @d1.or(:y => 2).sql.should == 
      'SELECT * FROM test WHERE (x = 1) OR (y = 2)'
  end
  
  specify "should accept all forms of filters" do
    # probably not exhaustive, but good enough
    @d1.or('(y > ?)', 2).sql.should ==
      'SELECT * FROM test WHERE (x = 1) OR (y > 2)'
      
    (@d1.or {yy > 3}).sql.should ==
      'SELECT * FROM test WHERE (x = 1) OR (yy > 3)'
  end
  
  specify "should correctly add parens to give predictable results" do
    @d1.filter(:y => 2).or(:z => 3).sql.should == 
      'SELECT * FROM test WHERE ((x = 1) AND (y = 2)) OR (z = 3)'

    @d1.or(:y => 2).filter(:z => 3).sql.should == 
      'SELECT * FROM test WHERE ((x = 1) OR (y = 2)) AND (z = 3)'
  end
end

context "Dataset#and" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test)
    @d1 = @dataset.where(:x => 1)
  end
  
  specify "should raise if no filter exists" do
    proc {@dataset.and(:a => 1)}.should raise_error(SequelError)
  end
  
  specify "should add an alternative expression to the where clause" do
    @d1.and(:y => 2).sql.should == 
      'SELECT * FROM test WHERE (x = 1) AND (y = 2)'
  end
  
  specify "should accept all forms of filters" do
    # probably not exhaustive, but good enough
    @d1.and('(y > ?)', 2).sql.should ==
      'SELECT * FROM test WHERE (x = 1) AND (y > 2)'
      
    (@d1.and {yy > 3}).sql.should ==
      'SELECT * FROM test WHERE (x = 1) AND (yy > 3)'
  end
  
  specify "should correctly add parens to give predictable results" do
    @d1.or(:y => 2).and(:z => 3).sql.should == 
      'SELECT * FROM test WHERE ((x = 1) OR (y = 2)) AND (z = 3)'

    @d1.and(:y => 2).or(:z => 3).sql.should == 
      'SELECT * FROM test WHERE ((x = 1) AND (y = 2)) OR (z = 3)'
  end
end

context "Dataset#exclude" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end

  specify "should correctly include the NOT operator when one condition is given" do
    @dataset.exclude(:region=>'Asia').select_sql.should ==
      "SELECT * FROM test WHERE NOT (region = 'Asia')"
  end

  specify "should take multiple conditions as a hash and express the logic correctly in SQL" do
    @dataset.exclude(:region => 'Asia', :name => 'Japan').select_sql.
      should match(Regexp.union(/WHERE NOT \(\(region = 'Asia'\) AND \(name = 'Japan'\)\)/,
                                /WHERE NOT \(\(name = 'Japan'\) AND \(region = 'Asia'\)\)/))
  end

  specify "should parenthesize a single string condition correctly" do
    @dataset.exclude("region = 'Asia' AND name = 'Japan'").select_sql.should ==
      "SELECT * FROM test WHERE NOT (region = 'Asia' AND name = 'Japan')"
  end

  specify "should parenthesize an array condition correctly" do
    @dataset.exclude('region = ? AND name = ?', 'Asia', 'Japan').select_sql.should ==
      "SELECT * FROM test WHERE NOT (region = 'Asia' AND name = 'Japan')"
  end

  specify "should corrently parenthesize when it is used twice" do
    @dataset.exclude(:region => 'Asia').exclude(:name => 'Japan').select_sql.should ==
      "SELECT * FROM test WHERE NOT (region = 'Asia') AND NOT (name = 'Japan')"
  end
  
  specify "should support proc expressions" do
    @dataset.exclude {id == (6...12)}.sql.should == 
      'SELECT * FROM test WHERE NOT ((id >= 6 AND id < 12))'
  end
end

context "Dataset#having" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test)
    @grouped = @dataset.group(:region).select(:region, :population.SUM, :gdp.AVG)
    @d1 = @grouped.having('sum(population) > 10')
    @d2 = @grouped.having(:region => 'Asia')
    @fields = "region, sum(population), avg(gdp)"
  end

  specify "should raise if the dataset is not grouped" do
    proc {@dataset.having('avg(gdp) > 10')}.should raise_error
  end

  specify "should affect select statements" do
    @d1.select_sql.should ==
      "SELECT #{@fields} FROM test GROUP BY region HAVING sum(population) > 10"
  end

  specify "should support proc expressions" do
    @grouped.having {SUM(:population) > 10}.sql.should == 
      "SELECT #{@fields} FROM test GROUP BY region HAVING (sum(population) > 10)"
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
      "SELECT * FROM (SELECT * FROM a WHERE (a = 1))"
  end

  specify "should use the relevant table name if given a simple dataset" do
    @dataset.from(@dataset.from(:a)).select_sql.should ==
      "SELECT * FROM a"
  end
  
  specify "should raise if no source is given" do
    proc {@dataset.from(@dataset.from).select_sql}.should raise_error(SequelError)
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
  
  specify "should accept mixed types (strings and symbols)" do
    @d.select('aaa').sql.should == 'SELECT aaa FROM test'
    @d.select(:a, 'b').sql.should == 'SELECT a, b FROM test'
    @d.select(:test__cc, 'test.d AS e').sql.should == 
      'SELECT test.cc, test.d AS e FROM test'
    @d.select('test.d AS e', :test__cc).sql.should == 
      'SELECT test.d AS e, test.cc FROM test'

    # symbol helpers      
    @d.select(:test.ALL).sql.should ==
      'SELECT test.* FROM test'
    @d.select(:test__name.AS(:n)).sql.should ==
      'SELECT test.name AS n FROM test'
    @d.select(:test__name___n).sql.should ==
      'SELECT test.name AS n FROM test'
  end

  specify "should use the wildcard if no arguments are given" do
    @d.select.sql.should == 'SELECT * FROM test'
  end
  
  specify "should accept a hash for AS values" do
    @d.select(:name => 'My Name', :__ggh => 'Age').sql.should == \
      "SELECT name AS 'My Name', __ggh AS 'Age' FROM test"
  end

  specify "should overrun the previous select option" do
    @d.select(:a, :b, :c).select.sql.should == 'SELECT * FROM test'
    @d.select(:price).select(:name).sql.should == 'SELECT name FROM test'
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
    @dataset.order(:name, :price.DESC).sql.should ==
      'SELECT * FROM test ORDER BY name, price DESC'
  end
  
  specify "should overrun a previous ordering" do
    @dataset.order(:name).order(:stamp).sql.should ==
      'SELECT * FROM test ORDER BY stamp'
  end
  
  specify "should accept a string" do
    @dataset.order('dada ASC').sql.should ==
      'SELECT * FROM test ORDER BY dada ASC'
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
    @dataset.reverse_order(:name.DESC).sql.should ==
      'SELECT * FROM test ORDER BY name'
  end
  
  specify "should accept multiple arguments" do
    @dataset.reverse_order(:name, :price.DESC).sql.should ==
      'SELECT * FROM test ORDER BY name DESC, price'
  end

  specify "should reverse a previous ordering if no arguments are given" do
    @dataset.order(:name).reverse_order.sql.should ==
      'SELECT * FROM test ORDER BY name DESC'
    @dataset.order('clumsy DESC, fool').reverse_order.sql.should ==
      'SELECT * FROM test ORDER BY clumsy, fool DESC'
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

context "Dataset#qualified_field_name" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should return the same if already qualified" do
    @dataset.qualified_field_name('test.a', :items).should == 'test.a'
    @dataset.qualified_field_name(:ccc__b, :items).should == 'ccc.b'
  end
  
  specify "should qualify the field with the supplied table name" do
    @dataset.qualified_field_name('a', :items).should == 'items.a'
    @dataset.qualified_field_name(:b1, :items).should == 'items.b1'
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
  
  specify "should map using #[fieldname] if fieldname is given" do
    @d.map(:a).should == [1, 3, 5]
  end
  
  specify "should return the complete dataset values if nothing is given" do
    @d.map.should == DummyDataset::VALUES
  end
end

context "Dataset#to_hash" do
  setup do
    @d = DummyDataset.new(nil).from(:items)
  end
  
  specify "should provide a hash with the first field as key and the second as value" do
    @d.to_hash(:a, :b).should == {1 => 2, 3 => 4, 5 => 6}
    @d.to_hash(:b, :a).should == {2 => 1, 4 => 3, 6 => 5}
  end
end

context "Dataset#uniq" do
  setup do
    @dataset = Sequel::Dataset.new(nil).from(:test).select(:name)
  end
  
  specify "should include DISTINCT clause in statement" do
    @dataset.uniq.sql.should == 'SELECT DISTINCT name FROM test'
  end
  
  specify "should be aliased by Dataset#distinct" do
    @dataset.distinct.sql.should == 'SELECT DISTINCT name FROM test'
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
  
  specify "should format SQL propertly" do
    @dataset.count.should == 1
    @c.sql.should == 'SELECT COUNT(*) FROM test'
  end
  
  specify "should be aliased by #size" do
    @dataset.size.should == 1
  end
  
  specify "should include the where clause if it's there" do
    @dataset.filter {abc < 30}.count.should == 1
    @c.sql.should == 'SELECT COUNT(*) FROM test WHERE (abc < 30)'
  end
end

context "Dataset#join_table" do
  setup do
    @d = Sequel::Dataset.new(nil).from(:items)
  end
  
  specify "should format the JOIN clause properly" do
    @d.join_table(:left_outer, :categories, :category_id => :id).sql.should ==
      'SELECT * FROM items LEFT OUTER JOIN categories ON (categories.category_id = items.id)'
  end
  
  specify "should include WHERE clause if applicable" do
    @d.filter {price < 100}.join_table(:right_outer, :categories, :category_id => :id).sql.should ==
      'SELECT * FROM items RIGHT OUTER JOIN categories ON (categories.category_id = items.id) WHERE (price < 100)'
  end
  
  specify "should include ORDER BY clause if applicable" do
    @d.order(:stamp).join_table(:full_outer, :categories, :category_id => :id).sql.should ==
      'SELECT * FROM items FULL OUTER JOIN categories ON (categories.category_id = items.id) ORDER BY stamp'
  end
  
  specify "should support multiple joins" do
    @d.join_table(:inner, :b, :items_id).join_table(:left_outer, :c, :b_id => :b__id).sql.should ==
      'SELECT * FROM items INNER JOIN b ON (b.items_id = items.id) LEFT OUTER JOIN c ON (c.b_id = b.id)'
  end
  
  specify "should use id as implicit relation primary key if ommited" do
    @d.join_table(:left_outer, :categories, :category_id).sql.should ==
      @d.join_table(:left_outer, :categories, :category_id => :id).sql

    # when doing multiple joins, id should be qualified using the last joined table
    @d.join_table(:right_outer, :b, :items_id).join_table(:full_outer, :c, :b_id).sql.should ==
      'SELECT * FROM items RIGHT OUTER JOIN b ON (b.items_id = items.id) FULL OUTER JOIN c ON (c.b_id = b.id)'
  end
  
  specify "should support left outer joins" do
    @d.join_table(:left_outer, :categories, :category_id).sql.should ==
      'SELECT * FROM items LEFT OUTER JOIN categories ON (categories.category_id = items.id)'

    @d.left_outer_join(:categories, :category_id).sql.should ==
      'SELECT * FROM items LEFT OUTER JOIN categories ON (categories.category_id = items.id)'
  end

  specify "should support right outer joins" do
    @d.join_table(:right_outer, :categories, :category_id).sql.should ==
      'SELECT * FROM items RIGHT OUTER JOIN categories ON (categories.category_id = items.id)'

    @d.right_outer_join(:categories, :category_id).sql.should ==
      'SELECT * FROM items RIGHT OUTER JOIN categories ON (categories.category_id = items.id)'
  end

  specify "should support full outer joins" do
    @d.join_table(:full_outer, :categories, :category_id).sql.should ==
      'SELECT * FROM items FULL OUTER JOIN categories ON (categories.category_id = items.id)'

    @d.full_outer_join(:categories, :category_id).sql.should ==
      'SELECT * FROM items FULL OUTER JOIN categories ON (categories.category_id = items.id)'
  end

  specify "should support inner joins" do
    @d.join_table(:inner, :categories, :category_id).sql.should ==
      'SELECT * FROM items INNER JOIN categories ON (categories.category_id = items.id)'

    @d.inner_join(:categories, :category_id).sql.should ==
      'SELECT * FROM items INNER JOIN categories ON (categories.category_id = items.id)'
  end
  
  specify "should default to an inner join" do
    @d.join_table(nil, :categories, :category_id).sql.should ==
      'SELECT * FROM items INNER JOIN categories ON (categories.category_id = items.id)'

    @d.join(:categories, :category_id).sql.should ==
      'SELECT * FROM items INNER JOIN categories ON (categories.category_id = items.id)'
  end
  
  specify "should raise if an invalid join type is specified" do
    proc {@d.join_table(:invalid, :a, :b)}.should raise_error(SequelError)
  end
  
  specify "should treat aliased tables correctly" do
    @d.from('stats s').join('players p', :id => :player_id).sql.should ==
      'SELECT * FROM stats s INNER JOIN players p ON (p.id = s.player_id)'
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
    @d.min(:a).should == 'SELECT min(a) AS v FROM test'
  end
  
  specify "should include max" do
    @d.max(:b).should == 'SELECT max(b) AS v FROM test'
  end
  
  specify "should include sum" do
    @d.sum(:c).should == 'SELECT sum(c) AS v FROM test'
  end
  
  specify "should include avg" do
    @d.avg(:d).should == 'SELECT avg(d) AS v FROM test'
  end
  
  specify "should accept qualified fields" do
    @d.avg(:test__bc).should == 'SELECT avg(test.bc) AS v FROM test'
  end
end

context "Dataset#first" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      @@last_dataset = nil
      @@last_opts = nil
      
      def self.last_dataset
        @@last_dataset
      end
      
      def self.last_opts
        @@last_opts
      end

      def single_record(opts = nil)
        @@last_opts = @opts.merge(opts || {})
        {:a => 1, :b => 2}
      end
      
      def all
        @@last_dataset = self
        [{:a => 1, :b => 2}] * @opts[:limit]
      end
    end
    @d = @c.new(nil).from(:test)
  end
  
  specify "should return the first matching record if a hash is specified" do
    @d.first(:z => 26).should == {:a => 1, :b => 2}
    @c.last_opts[:where].should == ('(z = 26)')

    @d.first('z = ?', 15)
    @c.last_opts[:where].should == ('z = 15')
  end
  
  specify "should return a single record if no argument is given" do
    @d.first.should == {:a => 1, :b => 2}
  end
  
  specify "should set the limit according to the given number" do
    @d.first
    @c.last_opts[:limit].should == 1
    
    i = rand(10) + 10
    @d.first(i)
    @c.last_dataset.opts[:limit].should == i
  end
  
  specify "should return an array with the records if argument is greater than 1" do
    i = rand(10) + 10
    r = @d.first(i)
    r.should be_a_kind_of(Array)
    r.size.should == i
    r.each {|row| row.should == {:a => 1, :b => 2}}
  end
end

context "Dataset#last" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      @@last_dataset = nil
      
      def self.last_dataset
        @@last_dataset
      end

      def single_record(opts = nil)
        @@last_dataset = clone_merge(opts) if opts
        {:a => 1, :b => 2}
      end
      
      def all
        @@last_dataset = self
        [{:a => 1, :b => 2}] * @opts[:limit]
      end
    end
    @d = @c.new(nil).from(:test)
  end
  
  specify "should raise if no order is given" do
    proc {@d.last}.should raise_error(SequelError)
    proc {@d.last(2)}.should raise_error(SequelError)
    proc {@d.order(:a).last}.should_not raise_error
    proc {@d.order(:a).last(2)}.should_not raise_error
  end
  
  specify "should invert the order" do
    @d.order(:a).last
    @c.last_dataset.opts[:order].should == ['a DESC']
    
    @d.order(:b.DESC).last
    @c.last_dataset.opts[:order].should == ['b']
    
    @d.order(:c, :d).last
    @c.last_dataset.opts[:order].should == ['c DESC', 'd DESC']
    
    @d.order(:e.DESC, :f).last
    @c.last_dataset.opts[:order].should == ['e', 'f DESC']
  end
  
  specify "should return the first matching record if a hash is specified" do
    @d.order(:a).last(:z => 26).should == {:a => 1, :b => 2}
    @c.last_dataset.opts[:where].should == ('(z = 26)')

    @d.order(:a).last('z = ?', 15)
    @c.last_dataset.opts[:where].should == ('z = 15')
  end
  
  specify "should return a single record if no argument is given" do
    @d.order(:a).last.should == {:a => 1, :b => 2}
  end
  
  specify "should set the limit according to the given number" do
    i = rand(10) + 10
    r = @d.order(:a).last(i)
    @c.last_dataset.opts[:limit].should == i
  end
  
  specify "should return an array with the records if argument is greater than 1" do
    i = rand(10) + 10
    r = @d.order(:a).last(i)
    r.should be_a_kind_of(Array)
    r.size.should == i
    r.each {|row| row.should == {:a => 1, :b => 2}}
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
        @@last_dataset = opts ? clone_merge(opts) : self
        {1 => 2, 3 => 4}
      end
    end
    @d = @c.new(nil).from(:test)
  end
  
  specify "should return a single record filtered according to the given conditions" do
    @d[:name => 'didi'].should == {1 => 2, 3 => 4}
    @c.last_dataset.opts[:where].should == "(name = 'didi')"

    @d[:id => 5..45].should == {1 => 2, 3 => 4}
    @c.last_dataset.opts[:where].should == "(id >= 5 AND id <= 45)"
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
  
  specify "should call each and return the first record" do
    @d.single_record.should == 'SELECT * FROM test'
  end
  
  specify "should pass opts to each" do
    @d.single_record(:limit => 3).should == 'SELECT * FROM test LIMIT 3'
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
    @d = @c.new(nil).from(:test)
  end
  
  specify "should call each and return the first value of the first record" do
    @d.single_value.should == 'SELECT * FROM test'
  end
  
  specify "should pass opts to each" do
    @d.single_value(:limit => 3).should == 'SELECT * FROM test LIMIT 3'
  end
end

context "Dataset#set_model" do
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
  end
  
  specify "should clear the models hash and restore the stock #each if nil is specified" do
    @dataset.set_model(@m)
    @dataset.set_model(nil)
    @dataset.first.should == 1
    @dataset.model_classes.should be_nil
  end
  
  specify "should clear the models hash and restore the stock #each if nothing is specified" do
    @dataset.set_model(@m)
    @dataset.set_model
    @dataset.first.should == 1
    @dataset.model_classes.should be_nil
  end
  
  specify "should alter #each to provide model instances" do
    @dataset.first.should == 1
    @dataset.set_model(@m)
    @dataset.first.should == @m.new(1)
  end
  
  specify "should extend the dataset with a #destroy method" do
    @dataset.should_not respond_to(:destroy)
    @dataset.set_model(@m)
    @dataset.should respond_to(:destroy)
  end
  
  specify "should set opts[:naked] to nil" do
    @dataset.opts[:naked] = true
    @dataset.set_model(@m)
    @dataset.opts[:naked].should be_nil
  end
  
  specify "should provide support for polymorphic model instantiation" do
    @m1 = Class.new(@m)
    @m2 = Class.new(@m)
    @dataset.set_model(0, 0 => @m1, 1 => @m2)
    all = @dataset.all
    all[0].class.should == @m2
    all[1].class.should == @m1
    all[2].class.should == @m2
    all[3].class.should == @m1
    #...
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
    @dataset.set_model(0, 0 => @m1, 1 => @m2)
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
        (1..10).each(&block)
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
    @dataset.set_model(0, nil => @m, 1 => @m2)
    all = @dataset.all
    all[0].class.should == @m2
    all[1].class.should == @m
    all[2].class.should == @m2
    all[3].class.should == @m
    #...
  end
  
  specify "should raise SequelError if no suitable class is found in the polymorphic hash" do
    @m2 = Class.new(@m)
    @dataset.set_model(0, 1 => @m2)
    proc {@dataset.all}.should raise_error(SequelError)
  end

  specify "should supply naked records if the naked option is specified" do
    @dataset.set_model(0, nil => @m)
    @dataset.each(:naked => true) {|r| r.class.should == Fixnum}
  end
end

context "Dataset#destroy" do
  setup do
    db = Object.new
    m = Module.new do
      def transaction; yield; end
    end
    db.extend(m)
    
    DESTROYED = []
    
    @m = Class.new do
      def initialize(c)
        @c = c
      end
      
      attr_accessor :c
      
      def ==(o)
        @c == o.c
      end
      
      def destroy
        DESTROYED << self
      end
    end
    MODELS = [@m.new(12), @m.new(13)]

    c = Class.new(Sequel::Dataset) do
      def fetch_rows(sql, &block)
        (12..13).each(&block)
      end
    end

    @d = c.new(db).from(:test)
    @d.set_model(@m)
  end
  
  specify "should destroy raise for every model in the dataset" do
    count = @d.destroy
    count.should == 2
    DESTROYED.should == MODELS
  end 
end

context "Dataset#<<" do
  setup do
    @d = Sequel::Dataset.new(nil)
    @d.meta_def(:insert) do
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
end

context "Dataset#columns" do
  setup do
    @dataset = DummyDataset.new(nil).from(:items)
    @dataset.meta_def(:columns=) {|c| @columns = c}
    @dataset.meta_def(:first) {@columns = select_sql(nil)}
  end
  
  specify "should return the value of @columns" do
    @dataset.columns = [:a, :b, :c]
    @dataset.columns.should == [:a, :b, :c]
  end
  
  specify "should call first if @columns is nil" do
    @dataset.columns = nil
    @dataset.columns.should == 'SELECT * FROM items'
    @dataset.opts[:from] = [:nana]
    @dataset.columns.should == 'SELECT * FROM items'
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
        @sqls << 'BEGIN;'
        yield
        @sqls << 'COMMIT;'
      end
    end
    @db = @dbc.new
    
    @ds = Sequel::Dataset.new(@db).from(:items)
    
    @list = [{:name => 'abc'}, {:name => 'def'}, {:name => 'ghi'}]
  end
  
  specify "should join all inserts into a single SQL string" do
    @ds.multi_insert(@list)
    @db.sqls.should == [
      'BEGIN;',
      "INSERT INTO items (name) VALUES ('abc');",
      "INSERT INTO items (name) VALUES ('def');",
      "INSERT INTO items (name) VALUES ('ghi');",
      'COMMIT;'
    ]
  end
  
  specify "should accept the commit_every option for commiting every x records" do
    @ds.multi_insert(@list, :commit_every => 2)
    @db.sqls.should == [
      'BEGIN;',
      "INSERT INTO items (name) VALUES ('abc');",
      "INSERT INTO items (name) VALUES ('def');",
      'COMMIT;',
      'BEGIN;',
      "INSERT INTO items (name) VALUES ('ghi');",
      'COMMIT;'
    ]
  end
end