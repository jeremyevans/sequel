require File.join(File.dirname(__FILE__), '../lib/sequel')

context "Dataset" do
  setup do
    @dataset = Sequel::Dataset.new("db")
  end
  
  specify "should accept database, opts and model_class in initialize" do
    db = 'db'
    opts = {:from => :test}
    a_class = Class.new
    d = Sequel::Dataset.new(db, opts, a_class)
    d.db.should be(db)
    d.opts.should be(opts)
    d.model_class.should be(a_class)
    
    d = Sequel::Dataset.new(db)
    d.db.should be(db)
    d.opts.should be_a_kind_of(Hash)
    d.opts.should == {}
    d.model_class.should be_nil
  end
  
  specify "should provide dup_merge for chainability." do
    d1 = @dataset.dup_merge(:from => :test)
    d1.class.should == @dataset.class
    d1.should_not == @dataset
    d1.db.should be(@dataset.db)
    d1.opts[:from].should == :test
    @dataset.opts[:from].should be_nil
    
    d2 = d1.dup_merge(:order => :name)
    d2.class.should == @dataset.class
    d2.should_not == d1
    d2.should_not == @dataset
    d2.db.should be(@dataset.db)
    d2.opts[:from].should == :test
    d2.opts[:order].should == :name
    d1.opts[:order].should be_nil
    
    # dup_merge should preserve @model_class
    a_class = Class.new
    d3 = Sequel::Dataset.new("db", nil, a_class)
    d3.db.should_not be(@dataset.db)
    d4 = @dataset.dup_merge({})
    d4.db.should be(@dataset.db)
    d3.model_class.should be(a_class)
    d4.model_class.should be_nil
    d5 = d3.dup_merge(:from => :test)
    d5.db.should be(d3.db)
    d5.model_class.should == a_class
  end
  
  specify "should include Enumerable" do
    Sequel::Dataset.included_modules.should include(Enumerable)
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
      should match(/INSERT INTO test \(name, price\) VALUES \('wxyz', 342\)|INSERT INTO test \(price, name\) VALUES \(342, 'wxyz'\)/)
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
    @d1 = Sequel::Dataset.new(nil, {1 => 2, 3 => 4}, Class.new)
    @d2 = Sequel::Dataset.new(nil, {5 => 6, 7 => 8})
  end
  
  specify "should return self if already naked (no record class)" do
    @d2.naked.should be(@d2)
    @d1.naked.should_not be(@d1)
  end
  
  specify "should return a naked copy of self (no record class)" do
    naked = @d1.naked
    naked.should_not be(@d1)
    naked.model_class.should be_nil
    naked.opts.should == @d1.opts
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
  def each(&block)
    VALUES.each(&block)
  end
end

context "Dataset#map" do
  setup do
    @d = DummyDataset.new(nil)
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

context "Dataset#hash_column" do
  setup do
    @d = DummyDataset.new(nil)
  end
  
  specify "should provide a hash with the first field as key and the second as value" do
    @d.hash_column(:a, :b).should == {1 => 2, 3 => 4, 5 => 6}
    @d.hash_column(:b, :a).should == {2 => 1, 4 => 3, 6 => 5}
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
      
      def each(opts = nil)
        @@sql = select_sql(opts)
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

context "Dataset#join" do
  setup do
    @d = Sequel::Dataset.new(nil).from(:items)
  end
  
  specify "should format the JOIN clause properly" do
    @d.join(:categories, :category_id => :id).sql.should ==
      'SELECT * FROM items LEFT OUTER JOIN categories ON (categories.category_id = items.id)'
  end
  
  specify "should include WHERE clause if applicable" do
    @d.filter {price < 100}.join(:categories, :category_id => :id).sql.should ==
      'SELECT * FROM items LEFT OUTER JOIN categories ON (categories.category_id = items.id) WHERE (price < 100)'
  end

  specify "should include ORDER BY clause if applicable" do
    @d.order(:stamp).join(:categories, :category_id => :id).sql.should ==
      'SELECT * FROM items LEFT OUTER JOIN categories ON (categories.category_id = items.id) ORDER BY stamp'
  end
  
  specify "should use id as implicit relation primary key if ommited" do
    @d.join(:categories, :category_id).sql.should ==
      @d.join(:categories, :category_id => :id).sql
  end
end

context "Dataset#<<" do
  setup do
    c = Class.new(Sequel::Dataset) do
      def insert(*args)
        args
      end
    end
    
    @d = c.new(nil)
  end
  
  specify "should call Dataset#insert" do
    (@d << {:a => 1, :b => 2}).should == [{:a => 1, :b => 2}]
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
      def each(opts = nil)
        yield({1 => opts[:select].first})
      end
    end
    @d = c.new(nil).from(:test)
  end
  
  specify "should include min" do
    @d.min(:a).should == 'min(a)'
  end
  
  specify "should include max" do
    @d.max(:b).should == 'max(b)'
  end
  
  specify "should include sum" do
    @d.sum(:c).should == 'sum(c)'
  end
  
  specify "should include avg" do
    @d.avg(:d).should == 'avg(d)'
  end
  
  specify "should accept qualified fields" do
    @d.avg(:test__bc).should == 'avg(test.bc)'
  end
end

context "Dataset#first" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      @@last_dataset = nil
      
      def self.last_dataset
        @@last_dataset
      end

      def single_record
        {:a => 1, :b => 2}
      end
      
      def all
        @@last_dataset = self
        [{:a => 1, :b => 2}] * @opts[:limit]
      end
    end
    @d = @c.new(nil).from(:test)
  end
  
  specify "should return a single record if no argument is given" do
    @d.first.should == {:a => 1, :b => 2}
  end
  
  specify "should set the limit according to the given number" do
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
        @@last_dataset = dup_merge(opts) if opts
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

context "Dataset#[]" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      @@last_dataset = nil
      
      def self.last_dataset
        @@last_dataset
      end

      def single_record(opts = nil)
        @@last_dataset = opts ? dup_merge(opts) : self
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
      def each(opts = nil)
        opts = opts ? @opts.merge(opts) : @opts
        yield opts
        yield 2
      end
    end
    @cc = Class.new(@c) do
      def each(opts = nil); end
    end
    @d = @c.new(nil).from(:test)
    @e = @cc.new(nil).from(:test)
  end
  
  specify "should call each and return the first record" do
    @d.single_record.should == @d.opts
  end
  
  specify "should pass opts to each" do
    @d.single_record(:a => :b).should == @d.opts.merge(:a => :b)
  end
  
  specify "should return nil if no record is present" do
    @e.single_record.should be_nil
  end
end

context "Dataset#single_value" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      def each(opts = nil)
        yield({1 => (opts && opts[:a]) || 2})
      end
    end
    @d = @c.new(nil).from(:test)
  end
  
  specify "should call each and return the first value of the first record" do
    @d.single_value.should == 2
  end
  
  specify "should pass opts to each" do
    @d.single_value(:a => :b).should == :b
  end
end

context "Dataset#destroy" do
  setup do
    @d = Sequel::Dataset.new(nil).from(:test)
  end
  
  specify "should raise exception if no model is associated with the dataset" do
    proc {@d.destroy}.should raise_error(SequelError)
  end
  
  specify "should call raise for every model in the dataset" do
    DESTROYED = []
    m = Class.new do
      def destroy
        DESTROYED << self
      end
    end
    MODELS = [m.new, m.new]
    c = Class.new(Sequel::Dataset) do
      def each(opts = nil)
        MODELS.each {|i| yield i}
      end
    end
    db = Class.new do
      def transaction
        yield
      end
    end
    
    count = c.new(db.new, {}, m).destroy
    count.should == 2
    DESTROYED.should == MODELS
  end 
end