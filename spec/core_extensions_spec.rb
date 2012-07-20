require 'rubygems'

unless Object.const_defined?('Sequel') && Sequel.const_defined?('Model')
  $:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../../lib/"))
  require 'sequel/no_core_ext'
end

Sequel.quote_identifiers = false
Sequel.identifier_input_method = nil
Sequel.identifier_output_method = nil

Regexp.send(:include, Sequel::SQL::StringMethods)
String.send(:include, Sequel::SQL::StringMethods)
Sequel.extension :core_extensions

describe "Sequel core extensions" do
  specify "should have Sequel.core_extensions? be true if enabled" do
    Sequel.core_extensions?.should be_true
  end
end

describe "Core extensions" do
  before do
    db = Sequel::Database.new
    @d = db[:items]
    def @d.l(*args, &block)
      literal(filter_expr(*args, &block))
    end
    def @d.lit(*args)
      literal(*args)
    end
  end
  
  if RUBY_VERSION < '1.9.0'
    it "should not allow inequality operations on true, false, or nil" do
      @d.lit(:x > 1).should == "(x > 1)"
      @d.lit(:x < true).should == "(x < 't')"
      @d.lit(:x >= false).should == "(x >= 'f')"
      @d.lit(:x <= nil).should == "(x <= NULL)"
    end

    it "should not allow inequality operations on boolean complex expressions" do
      @d.lit(:x > (:y > 5)).should == "(x > (y > 5))"
      @d.lit(:x < (:y < 5)).should == "(x < (y < 5))"
      @d.lit(:x >= (:y >= 5)).should == "(x >= (y >= 5))"
      @d.lit(:x <= (:y <= 5)).should == "(x <= (y <= 5))"
      @d.lit(:x > {:y => nil}).should == "(x > (y IS NULL))"
      @d.lit(:x < ~{:y => nil}).should == "(x < (y IS NOT NULL))"
      @d.lit(:x >= {:y => 5}).should == "(x >= (y = 5))"
      @d.lit(:x <= ~{:y => 5}).should == "(x <= (y != 5))"
      @d.lit(:x >= {:y => [1,2,3]}).should == "(x >= (y IN (1, 2, 3)))"
      @d.lit(:x <= ~{:y => [1,2,3]}).should == "(x <= (y NOT IN (1, 2, 3)))"
    end
    
    it "should support >, <, >=, and <= via Symbol#>,<,>=,<=" do
      @d.l(:x > 100).should == '(x > 100)'
      @d.l(:x < 100.01).should == '(x < 100.01)'
      @d.l(:x >= 100000000000000000000000000000000000).should == '(x >= 100000000000000000000000000000000000)'
      @d.l(:x <= 100).should == '(x <= 100)'
    end
    
    it "should support negation of >, <, >=, and <= via Symbol#~" do
      @d.l(~(:x > 100)).should == '(x <= 100)'
      @d.l(~(:x < 100.01)).should == '(x >= 100.01)'
      @d.l(~(:x >= 100000000000000000000000000000000000)).should == '(x < 100000000000000000000000000000000000)'
      @d.l(~(:x <= 100)).should == '(x > 100)'
    end
    
    it "should support double negation via ~" do
      @d.l(~~(:x > 100)).should == '(x > 100)'
    end
  end
  it "should support NOT via Symbol#~" do
    @d.l(~:x).should == 'NOT x'
    @d.l(~:x__y).should == 'NOT x.y'
  end
  
  it "should support + - * / via Symbol#+,-,*,/" do
    @d.l(:x + 1 > 100).should == '((x + 1) > 100)'
    @d.l((:x * :y) < 100.01).should == '((x * y) < 100.01)'
    @d.l((:x - :y/2) >= 100000000000000000000000000000000000).should == '((x - (y / 2)) >= 100000000000000000000000000000000000)'
    @d.l((((:x - :y)/(:x + :y))*:z) <= 100).should == '((((x - y) / (x + y)) * z) <= 100)'
    @d.l(~((((:x - :y)/(:x + :y))*:z) <= 100)).should == '((((x - y) / (x + y)) * z) > 100)'
  end
  
  it "should support LIKE via Symbol#like" do
    @d.l(:x.like('a')).should == '(x LIKE \'a\')'
    @d.l(:x.like(/a/)).should == '(x ~ \'a\')'
    @d.l(:x.like('a', 'b')).should == '((x LIKE \'a\') OR (x LIKE \'b\'))'
    @d.l(:x.like(/a/, /b/i)).should == '((x ~ \'a\') OR (x ~* \'b\'))'
    @d.l(:x.like('a', /b/)).should == '((x LIKE \'a\') OR (x ~ \'b\'))'

    @d.l('a'.like(:x)).should == "('a' LIKE x)"
    @d.l('a'.like(:x, 'b')).should == "(('a' LIKE x) OR ('a' LIKE 'b'))"
    @d.l('a'.like(:x, /b/)).should == "(('a' LIKE x) OR ('a' ~ 'b'))"
    @d.l('a'.like(:x, /b/i)).should == "(('a' LIKE x) OR ('a' ~* 'b'))"

    @d.l(/a/.like(:x)).should == "('a' ~ x)"
    @d.l(/a/.like(:x, 'b')).should == "(('a' ~ x) OR ('a' ~ 'b'))"
    @d.l(/a/.like(:x, /b/)).should == "(('a' ~ x) OR ('a' ~ 'b'))"
    @d.l(/a/.like(:x, /b/i)).should == "(('a' ~ x) OR ('a' ~* 'b'))"

    @d.l(/a/i.like(:x)).should == "('a' ~* x)"
    @d.l(/a/i.like(:x, 'b')).should == "(('a' ~* x) OR ('a' ~* 'b'))"
    @d.l(/a/i.like(:x, /b/)).should == "(('a' ~* x) OR ('a' ~* 'b'))"
    @d.l(/a/i.like(:x, /b/i)).should == "(('a' ~* x) OR ('a' ~* 'b'))"
  end

  it "should support NOT LIKE via Symbol#like and Symbol#~" do
    @d.l(~:x.like('a')).should == '(x NOT LIKE \'a\')'
    @d.l(~:x.like(/a/)).should == '(x !~ \'a\')'
    @d.l(~:x.like('a', 'b')).should == '((x NOT LIKE \'a\') AND (x NOT LIKE \'b\'))'
    @d.l(~:x.like(/a/, /b/i)).should == '((x !~ \'a\') AND (x !~* \'b\'))'
    @d.l(~:x.like('a', /b/)).should == '((x NOT LIKE \'a\') AND (x !~ \'b\'))'

    @d.l(~'a'.like(:x)).should == "('a' NOT LIKE x)"
    @d.l(~'a'.like(:x, 'b')).should == "(('a' NOT LIKE x) AND ('a' NOT LIKE 'b'))"
    @d.l(~'a'.like(:x, /b/)).should == "(('a' NOT LIKE x) AND ('a' !~ 'b'))"
    @d.l(~'a'.like(:x, /b/i)).should == "(('a' NOT LIKE x) AND ('a' !~* 'b'))"

    @d.l(~/a/.like(:x)).should == "('a' !~ x)"
    @d.l(~/a/.like(:x, 'b')).should == "(('a' !~ x) AND ('a' !~ 'b'))"
    @d.l(~/a/.like(:x, /b/)).should == "(('a' !~ x) AND ('a' !~ 'b'))"
    @d.l(~/a/.like(:x, /b/i)).should == "(('a' !~ x) AND ('a' !~* 'b'))"

    @d.l(~/a/i.like(:x)).should == "('a' !~* x)"
    @d.l(~/a/i.like(:x, 'b')).should == "(('a' !~* x) AND ('a' !~* 'b'))"
    @d.l(~/a/i.like(:x, /b/)).should == "(('a' !~* x) AND ('a' !~* 'b'))"
    @d.l(~/a/i.like(:x, /b/i)).should == "(('a' !~* x) AND ('a' !~* 'b'))"
  end

  it "should support ILIKE via Symbol#ilike" do
    @d.l(:x.ilike('a')).should == '(x ILIKE \'a\')'
    @d.l(:x.ilike(/a/)).should == '(x ~* \'a\')'
    @d.l(:x.ilike('a', 'b')).should == '((x ILIKE \'a\') OR (x ILIKE \'b\'))'
    @d.l(:x.ilike(/a/, /b/i)).should == '((x ~* \'a\') OR (x ~* \'b\'))'
    @d.l(:x.ilike('a', /b/)).should == '((x ILIKE \'a\') OR (x ~* \'b\'))'

    @d.l('a'.ilike(:x)).should == "('a' ILIKE x)"
    @d.l('a'.ilike(:x, 'b')).should == "(('a' ILIKE x) OR ('a' ILIKE 'b'))"
    @d.l('a'.ilike(:x, /b/)).should == "(('a' ILIKE x) OR ('a' ~* 'b'))"
    @d.l('a'.ilike(:x, /b/i)).should == "(('a' ILIKE x) OR ('a' ~* 'b'))"

    @d.l(/a/.ilike(:x)).should == "('a' ~* x)"
    @d.l(/a/.ilike(:x, 'b')).should == "(('a' ~* x) OR ('a' ~* 'b'))"
    @d.l(/a/.ilike(:x, /b/)).should == "(('a' ~* x) OR ('a' ~* 'b'))"
    @d.l(/a/.ilike(:x, /b/i)).should == "(('a' ~* x) OR ('a' ~* 'b'))"

    @d.l(/a/i.ilike(:x)).should == "('a' ~* x)"
    @d.l(/a/i.ilike(:x, 'b')).should == "(('a' ~* x) OR ('a' ~* 'b'))"
    @d.l(/a/i.ilike(:x, /b/)).should == "(('a' ~* x) OR ('a' ~* 'b'))"
    @d.l(/a/i.ilike(:x, /b/i)).should == "(('a' ~* x) OR ('a' ~* 'b'))"
  end

  it "should support NOT ILIKE via Symbol#ilike and Symbol#~" do
    @d.l(~:x.ilike('a')).should == '(x NOT ILIKE \'a\')'
    @d.l(~:x.ilike(/a/)).should == '(x !~* \'a\')'
    @d.l(~:x.ilike('a', 'b')).should == '((x NOT ILIKE \'a\') AND (x NOT ILIKE \'b\'))'
    @d.l(~:x.ilike(/a/, /b/i)).should == '((x !~* \'a\') AND (x !~* \'b\'))'
    @d.l(~:x.ilike('a', /b/)).should == '((x NOT ILIKE \'a\') AND (x !~* \'b\'))'

    @d.l(~'a'.ilike(:x)).should == "('a' NOT ILIKE x)"
    @d.l(~'a'.ilike(:x, 'b')).should == "(('a' NOT ILIKE x) AND ('a' NOT ILIKE 'b'))"
    @d.l(~'a'.ilike(:x, /b/)).should == "(('a' NOT ILIKE x) AND ('a' !~* 'b'))"
    @d.l(~'a'.ilike(:x, /b/i)).should == "(('a' NOT ILIKE x) AND ('a' !~* 'b'))"

    @d.l(~/a/.ilike(:x)).should == "('a' !~* x)"
    @d.l(~/a/.ilike(:x, 'b')).should == "(('a' !~* x) AND ('a' !~* 'b'))"
    @d.l(~/a/.ilike(:x, /b/)).should == "(('a' !~* x) AND ('a' !~* 'b'))"
    @d.l(~/a/.ilike(:x, /b/i)).should == "(('a' !~* x) AND ('a' !~* 'b'))"

    @d.l(~/a/i.ilike(:x)).should == "('a' !~* x)"
    @d.l(~/a/i.ilike(:x, 'b')).should == "(('a' !~* x) AND ('a' !~* 'b'))"
    @d.l(~/a/i.ilike(:x, /b/)).should == "(('a' !~* x) AND ('a' !~* 'b'))"
    @d.l(~/a/i.ilike(:x, /b/i)).should == "(('a' !~* x) AND ('a' !~* 'b'))"
  end

  it "should support sql_expr on arrays with all two pairs" do
    @d.l([[:x, 100],[:y, 'a']].sql_expr).should == '((x = 100) AND (y = \'a\'))'
    @d.l([[:x, true], [:y, false]].sql_expr).should == '((x IS TRUE) AND (y IS FALSE))'
    @d.l([[:x, nil], [:y, [1,2,3]]].sql_expr).should == '((x IS NULL) AND (y IN (1, 2, 3)))'
  end
  
  it "should support sql_negate on arrays with all two pairs" do
    @d.l([[:x, 100],[:y, 'a']].sql_negate).should == '((x != 100) AND (y != \'a\'))'
    @d.l([[:x, true], [:y, false]].sql_negate).should == '((x IS NOT TRUE) AND (y IS NOT FALSE))'
    @d.l([[:x, nil], [:y, [1,2,3]]].sql_negate).should == '((x IS NOT NULL) AND (y NOT IN (1, 2, 3)))'
  end
  
  it "should support ~ on arrays with all two pairs" do
    @d.l(~[[:x, 100],[:y, 'a']]).should == '((x != 100) OR (y != \'a\'))'
    @d.l(~[[:x, true], [:y, false]]).should == '((x IS NOT TRUE) OR (y IS NOT FALSE))'
    @d.l(~[[:x, nil], [:y, [1,2,3]]]).should == '((x IS NOT NULL) OR (y NOT IN (1, 2, 3)))'
  end
  
  it "should support sql_or on arrays with all two pairs" do
    @d.l([[:x, 100],[:y, 'a']].sql_or).should == '((x = 100) OR (y = \'a\'))'
    @d.l([[:x, true], [:y, false]].sql_or).should == '((x IS TRUE) OR (y IS FALSE))'
    @d.l([[:x, nil], [:y, [1,2,3]]].sql_or).should == '((x IS NULL) OR (y IN (1, 2, 3)))'
  end
  
  it "should support Array#sql_string_join for concatenation of SQL strings" do
    @d.lit([:x].sql_string_join).should == '(x)'
    @d.lit([:x].sql_string_join(', ')).should == '(x)'
    @d.lit([:x, :y].sql_string_join).should == '(x || y)'
    @d.lit([:x, :y].sql_string_join(', ')).should == "(x || ', ' || y)"
    @d.lit([:x.sql_function(1), :y.sql_subscript(1)].sql_string_join).should == '(x(1) || y[1])'
    @d.lit([:x.sql_function(1), 'y.z'.lit].sql_string_join(', ')).should == "(x(1) || ', ' || y.z)"
    @d.lit([:x, 1, :y].sql_string_join).should == "(x || '1' || y)"
    @d.lit([:x, 1, :y].sql_string_join(', ')).should == "(x || ', ' || '1' || ', ' || y)"
    @d.lit([:x, 1, :y].sql_string_join(:y__z)).should == "(x || y.z || '1' || y.z || y)"
    @d.lit([:x, 1, :y].sql_string_join(1)).should == "(x || '1' || '1' || '1' || y)"
    @d.lit([:x, :y].sql_string_join('y.x || x.y'.lit)).should == "(x || y.x || x.y || y)"
    @d.lit([[:x, :y].sql_string_join, [:a, :b].sql_string_join].sql_string_join).should == "(x || y || a || b)"
  end

  it "should support sql_expr on hashes" do
    @d.l({:x => 100, :y => 'a'}.sql_expr)[1...-1].split(' AND ').sort.should == ['(x = 100)', '(y = \'a\')']
    @d.l({:x => true, :y => false}.sql_expr)[1...-1].split(' AND ').sort.should == ['(x IS TRUE)', '(y IS FALSE)']
    @d.l({:x => nil, :y => [1,2,3]}.sql_expr)[1...-1].split(' AND ').sort.should == ['(x IS NULL)', '(y IN (1, 2, 3))']
  end
  
  it "should support sql_negate on hashes" do
    @d.l({:x => 100, :y => 'a'}.sql_negate)[1...-1].split(' AND ').sort.should == ['(x != 100)', '(y != \'a\')']
    @d.l({:x => true, :y => false}.sql_negate)[1...-1].split(' AND ').sort.should == ['(x IS NOT TRUE)', '(y IS NOT FALSE)']
    @d.l({:x => nil, :y => [1,2,3]}.sql_negate)[1...-1].split(' AND ').sort.should == ['(x IS NOT NULL)', '(y NOT IN (1, 2, 3))']
  end
  
  it "should support ~ on hashes" do
    @d.l(~{:x => 100, :y => 'a'})[1...-1].split(' OR ').sort.should == ['(x != 100)', '(y != \'a\')']
    @d.l(~{:x => true, :y => false})[1...-1].split(' OR ').sort.should == ['(x IS NOT TRUE)', '(y IS NOT FALSE)']
    @d.l(~{:x => nil, :y => [1,2,3]})[1...-1].split(' OR ').sort.should == ['(x IS NOT NULL)', '(y NOT IN (1, 2, 3))']
  end
  
  it "should support sql_or on hashes" do
    @d.l({:x => 100, :y => 'a'}.sql_or)[1...-1].split(' OR ').sort.should == ['(x = 100)', '(y = \'a\')']
    @d.l({:x => true, :y => false}.sql_or)[1...-1].split(' OR ').sort.should == ['(x IS TRUE)', '(y IS FALSE)']
    @d.l({:x => nil, :y => [1,2,3]}.sql_or)[1...-1].split(' OR ').sort.should == ['(x IS NULL)', '(y IN (1, 2, 3))']
  end
  
  it "should Hash#& and Hash#|" do
    @d.l({:y => :z} & :x).should == '((y = z) AND x)'
    @d.l({:x => :a} & {:y => :z}).should == '((x = a) AND (y = z))'
    @d.l({:y => :z} | :x).should == '((y = z) OR x)'
    @d.l({:x => :a} | {:y => :z}).should == '((x = a) OR (y = z))'
  end
end

describe "Array#case and Hash#case" do
  before do
    @d = Sequel::Dataset.new(nil)
  end

  specify "should return SQL CASE expression" do
    @d.literal({:x=>:y}.case(:z)).should == '(CASE WHEN x THEN y ELSE z END)'
    @d.literal({:x=>:y}.case(:z, :exp)).should == '(CASE exp WHEN x THEN y ELSE z END)'
    ['(CASE WHEN x THEN y WHEN a THEN b ELSE z END)',
     '(CASE WHEN a THEN b WHEN x THEN y ELSE z END)'].should(include(@d.literal({:x=>:y, :a=>:b}.case(:z))))
    @d.literal([[:x, :y]].case(:z)).should == '(CASE WHEN x THEN y ELSE z END)'
    @d.literal([[:x, :y], [:a, :b]].case(:z)).should == '(CASE WHEN x THEN y WHEN a THEN b ELSE z END)'
    @d.literal([[:x, :y], [:a, :b]].case(:z, :exp)).should == '(CASE exp WHEN x THEN y WHEN a THEN b ELSE z END)'
    @d.literal([[:x, :y], [:a, :b]].case(:z, :exp__w)).should == '(CASE exp.w WHEN x THEN y WHEN a THEN b ELSE z END)'
  end

  specify "should return SQL CASE expression with expression even if nil" do
    @d.literal({:x=>:y}.case(:z, nil)).should == '(CASE NULL WHEN x THEN y ELSE z END)'
  end

  specify "should raise an error if an array that isn't all two pairs is used" do
    proc{[:b].case(:a)}.should raise_error(Sequel::Error)
    proc{[:b, :c].case(:a)}.should raise_error(Sequel::Error)
    proc{[[:b, :c], :d].case(:a)}.should raise_error(Sequel::Error)
  end

  specify "should raise an error if an empty array/hash is used" do
    proc{[].case(:a)}.should raise_error(Sequel::Error)
    proc{{}.case(:a)}.should raise_error(Sequel::Error)
  end
end

describe "Array#sql_value_list and #sql_array" do
  before do
    @d = Sequel::Dataset.new(nil)
  end

  specify "should treat the array as an SQL value list instead of conditions when used as a placeholder value" do
    @d.filter("(a, b) IN ?", [[:x, 1], [:y, 2]]).sql.should == 'SELECT * WHERE ((a, b) IN ((x = 1) AND (y = 2)))'
    @d.filter("(a, b) IN ?", [[:x, 1], [:y, 2]].sql_value_list).sql.should == 'SELECT * WHERE ((a, b) IN ((x, 1), (y, 2)))'
    @d.filter("(a, b) IN ?", [[:x, 1], [:y, 2]].sql_array).sql.should == 'SELECT * WHERE ((a, b) IN ((x, 1), (y, 2)))'
  end

  specify "should be no difference when used as a hash value" do
    @d.filter([:a, :b]=>[[:x, 1], [:y, 2]]).sql.should == 'SELECT * WHERE ((a, b) IN ((x, 1), (y, 2)))'
    @d.filter([:a, :b]=>[[:x, 1], [:y, 2]].sql_value_list).sql.should == 'SELECT * WHERE ((a, b) IN ((x, 1), (y, 2)))'
    @d.filter([:a, :b]=>[[:x, 1], [:y, 2]].sql_array).sql.should == 'SELECT * WHERE ((a, b) IN ((x, 1), (y, 2)))'
  end
end

describe "String#lit" do
  before do
    @ds = ds = Sequel::Database.new[:t]
  end

  specify "should return an LiteralString object" do
    'xyz'.lit.should be_a_kind_of(Sequel::LiteralString)
    'xyz'.lit.to_s.should == 'xyz'
  end
  
  specify "should inhibit string literalization" do
    @ds.update_sql(:stamp => "NOW()".lit).should == "UPDATE t SET stamp = NOW()"
  end

  specify "should return a PlaceholderLiteralString object if args are given" do
    a = 'DISTINCT ?'.lit(:a)
    a.should be_a_kind_of(Sequel::SQL::PlaceholderLiteralString)
    @ds.literal(a).should == 'DISTINCT a'
    @ds.quote_identifiers = true
    @ds.literal(a).should == 'DISTINCT "a"'
  end
  
  specify "should handle named placeholders if given a single argument hash" do
    a = 'DISTINCT :b'.lit(:b=>:a)
    a.should be_a_kind_of(Sequel::SQL::PlaceholderLiteralString)
    @ds.literal(a).should == 'DISTINCT a'
    @ds.quote_identifiers = true
    @ds.literal(a).should == 'DISTINCT "a"'
  end

  specify "should treat placeholder literal strings as generic expressions" do
    a = ':b'.lit(:b=>:a)
    @ds.literal(a + 1).should == "(a + 1)"
    @ds.literal(a & :b).should == "(a AND b)"
    @ds.literal(a.sql_string + :b).should == "(a || b)"
  end
end

describe "String#to_sequel_blob" do
  specify "should return a Blob object" do
    'xyz'.to_sequel_blob.should be_a_kind_of(::Sequel::SQL::Blob)
    'xyz'.to_sequel_blob.should == 'xyz'
  end

  specify "should retain binary data" do
    "\1\2\3\4".to_sequel_blob.should == "\1\2\3\4"
  end
end

describe "#desc" do
  before do
    @ds = Sequel::Dataset.new(nil)
  end
  
  specify "should format a DESC clause for a column ref" do
    :test.desc.to_s(@ds).should == 'test DESC'
    
    :items__price.desc.to_s(@ds).should == 'items.price DESC'
  end

  specify "should format a DESC clause for a function" do
    :avg.sql_function(:test).desc.to_s(@ds).should == 'avg(test) DESC'
  end
end

describe "#asc" do
  before do
    @ds = Sequel::Dataset.new(nil)
  end
  
  specify "should format a ASC clause for a column ref" do
    :test.asc.to_s(@ds).should == 'test ASC'
    
    :items__price.asc.to_s(@ds).should == 'items.price ASC'
  end

  specify "should format a ASC clause for a function" do
    :avg.sql_function(:test).asc.to_s(@ds).should == 'avg(test) ASC'
  end
end

describe "#as" do
  before do
    @ds = Sequel::Dataset.new(nil)
  end
  
  specify "should format a AS clause for a column ref" do
    :test.as(:t).to_s(@ds).should == 'test AS t'
    
    :items__price.as(:p).to_s(@ds).should == 'items.price AS p'
  end

  specify "should format a AS clause for a function" do
    :avg.sql_function(:test).as(:avg).to_s(@ds).should == 'avg(test) AS avg'
  end
  
  specify "should format a AS clause for a literal value" do
    'abc'.as(:abc).to_s(@ds).should == "'abc' AS abc"
  end
end

describe "Column references" do
  before do
    @ds = Sequel::Database.new.dataset
    def @ds.quoted_identifier_append(sql, c)
      sql << "`#{c}`"
    end
    @ds.quote_identifiers = true
  end
  
  specify "should be quoted properly" do
    @ds.literal(:xyz).should == "`xyz`"
    @ds.literal(:xyz__abc).should == "`xyz`.`abc`"

    @ds.literal(:xyz.as(:x)).should == "`xyz` AS `x`"
    @ds.literal(:xyz__abc.as(:x)).should == "`xyz`.`abc` AS `x`"

    @ds.literal(:xyz___x).should == "`xyz` AS `x`"
    @ds.literal(:xyz__abc___x).should == "`xyz`.`abc` AS `x`"
  end
  
  specify "should be quoted properly in SQL functions" do
    @ds.literal(:avg.sql_function(:xyz)).should == "avg(`xyz`)"
    @ds.literal(:avg.sql_function(:xyz, 1)).should == "avg(`xyz`, 1)"
    @ds.literal(:avg.sql_function(:xyz).as(:a)).should == "avg(`xyz`) AS `a`"
  end

  specify "should be quoted properly in ASC/DESC clauses" do
    @ds.literal(:xyz.asc).should == "`xyz` ASC"
    @ds.literal(:avg.sql_function(:xyz, 1).desc).should == "avg(`xyz`, 1) DESC"
  end
  
  specify "should be quoted properly in a cast function" do
    @ds.literal(:x.cast(:integer)).should == "CAST(`x` AS integer)"
    @ds.literal(:x__y.cast('varchar(20)')).should == "CAST(`x`.`y` AS varchar(20))"
  end
end

describe "Blob" do
  specify "#to_sequel_blob should return self" do
    blob = "x".to_sequel_blob
    blob.to_sequel_blob.object_id.should == blob.object_id
  end
end

if RUBY_VERSION < '1.9.0'
  describe "Symbol#[]" do
    specify "should format an SQL Function" do
      ds = Sequel::Dataset.new(nil)
      ds.literal(:xyz[]).should == 'xyz()'
      ds.literal(:xyz[1]).should == 'xyz(1)'
      ds.literal(:xyz[1, 2, :abc[3]]).should == 'xyz(1, 2, abc(3))'
    end
  end
end

describe "Symbol#*" do
  before do
    @ds = Sequel::Dataset.new(nil)
  end
  
  specify "should format a qualified wildcard if no argument" do
    :xyz.*.to_s(@ds).should == 'xyz.*'
    :abc.*.to_s(@ds).should == 'abc.*'
  end

  specify "should format a filter expression if an argument" do
    :xyz.*(3).to_s(@ds).should == '(xyz * 3)'
    :abc.*(5).to_s(@ds).should == '(abc * 5)'
  end

  specify "should support qualified symbols if no argument" do
    :xyz__abc.*.to_s(@ds).should == 'xyz.abc.*'
  end
end

describe "Symbol" do
  before do
    @ds = Sequel::Dataset.new(nil)
    @ds.quote_identifiers = true
    @ds.identifier_input_method = :upcase
  end

  specify "#identifier should format an identifier" do
    @ds.literal(:xyz__abc.identifier).should == '"XYZ__ABC"'
  end

  specify "#qualify should format a qualified column" do
    @ds.literal(:xyz.qualify(:abc)).should == '"ABC"."XYZ"'
  end

  specify "#qualify should work on QualifiedIdentifiers" do
    @ds.literal(:xyz.qualify(:abc).qualify(:def)).should == '"DEF"."ABC"."XYZ"'
  end

  specify "should be able to qualify an identifier" do
    @ds.literal(:xyz.identifier.qualify(:xyz__abc)).should == '"XYZ"."ABC"."XYZ"'
  end

  specify "should be able to specify a schema.table.column" do
    @ds.literal(:column.qualify(:table.qualify(:schema))).should == '"SCHEMA"."TABLE"."COLUMN"'
    @ds.literal(:column.qualify(:table__name.identifier.qualify(:schema))).should == '"SCHEMA"."TABLE__NAME"."COLUMN"'
  end

  specify "should be able to specify order" do
    @oe = :xyz.desc
    @oe.class.should == Sequel::SQL::OrderedExpression
    @oe.descending.should == true
    @oe = :xyz.asc
    @oe.class.should == Sequel::SQL::OrderedExpression
    @oe.descending.should == false
  end

  specify "should work correctly with objects" do
    o = Object.new
    def o.sql_literal(ds) "(foo)" end
    @ds.literal(:column.qualify(o)).should == '(foo)."COLUMN"'
  end
end

describe "Symbol" do
  before do
    @ds = Sequel::Database.new.dataset
  end
  
  specify "should support sql_function method" do
    :COUNT.sql_function('1').to_s(@ds).should == "COUNT('1')"
    @ds.select(:COUNT.sql_function('1')).sql.should == "SELECT COUNT('1')"
  end
  
  specify "should support cast method" do
    :abc.cast(:integer).to_s(@ds).should == "CAST(abc AS integer)"
  end

  specify "should support sql array accesses via sql_subscript" do
    @ds.literal(:abc.sql_subscript(1)).should == "abc[1]"
    @ds.literal(:abc__def.sql_subscript(1)).should == "abc.def[1]"
    @ds.literal(:abc.sql_subscript(1)|2).should == "abc[1, 2]"
    @ds.literal(:abc.sql_subscript(1)[2]).should == "abc[1][2]"
  end

  specify "should support cast_numeric and cast_string" do
    x = :abc.cast_numeric
    x.should be_a_kind_of(Sequel::SQL::NumericExpression)
    x.to_s(@ds).should == "CAST(abc AS integer)"

    x = :abc.cast_numeric(:real)
    x.should be_a_kind_of(Sequel::SQL::NumericExpression)
    x.to_s(@ds).should == "CAST(abc AS real)"

    x = :abc.cast_string
    x.should be_a_kind_of(Sequel::SQL::StringExpression)
    x.to_s(@ds).should == "CAST(abc AS varchar(255))"

    x = :abc.cast_string(:varchar)
    x.should be_a_kind_of(Sequel::SQL::StringExpression)
    x.to_s(@ds).should == "CAST(abc AS varchar(255))"
  end
  
  specify "should allow database independent types when casting" do
    db = @ds.db
    def db.cast_type_literal(type)
      return :foo if type == Integer
      return :bar if type == String
      type
    end
    :abc.cast(String).to_s(@ds).should == "CAST(abc AS bar)"
    :abc.cast(String).to_s(@ds).should == "CAST(abc AS bar)"
    :abc.cast_string.to_s(@ds).should == "CAST(abc AS bar)"
    :abc.cast_string(Integer).to_s(@ds).should == "CAST(abc AS foo)"
    :abc.cast_numeric.to_s(@ds).should == "CAST(abc AS foo)"
    :abc.cast_numeric(String).to_s(@ds).should == "CAST(abc AS bar)"
  end

  specify "should support SQL EXTRACT function via #extract " do
    :abc.extract(:year).to_s(@ds).should == "extract(year FROM abc)"
  end
end

describe "Postgres extensions integration" do
  before do
    @db = Sequel.mock
    Sequel.extension(:pg_array, :pg_array_ops, :pg_hstore, :pg_hstore_ops, :pg_json, :pg_range, :pg_range_ops, :pg_row, :pg_row_ops)
  end

  it "Symbol#pg_array should return an ArrayOp" do
    @db.literal(:a.pg_array.unnest).should == "unnest(a)"
  end

  it "Symbol#pg_row should return a PGRowOp" do
    @db.literal(:a.pg_row[:a]).should == "(a).a"
  end

  it "Symbol#hstore should return an HStoreOp" do
    @db.literal(:a.hstore['a']).should == "(a -> 'a')"
  end

  it "Symbol#pg_range should return a RangeOp" do
    @db.literal(:a.pg_range.lower).should == "lower(a)"
  end

  it "Array#pg_array should return a PGArray" do
    @db.literal([1].pg_array.op.unnest).should == "unnest(ARRAY[1])"
    @db.literal([1].pg_array(:int4).op.unnest).should == "unnest(ARRAY[1]::int4[])"
  end

  it "Array#pg_json should return a JSONArray" do
    @db.literal([1].pg_json).should == "'[1]'::json"
  end

  it "Array#pg_row should return a ArrayRow" do
    @db.literal([1].pg_row).should == "ROW(1)"
  end

  it "Hash#hstore should return an HStore" do
    @db.literal({'a'=>1}.hstore.op['a']).should == '(\'"a"=>"1"\'::hstore -> \'a\')'
  end

  it "Hash#pg_json should return an JSONHash" do
    @db.literal({'a'=>'b'}.pg_json).should == "'{\"a\":\"b\"}'::json"
  end

  it "Range#pg_range should return an PGRange" do
    @db.literal((1..2).pg_range).should == "'[1,2]'"
    @db.literal((1..2).pg_range(:int4range)).should == "'[1,2]'::int4range"
  end
end
