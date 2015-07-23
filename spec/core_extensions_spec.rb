require 'rubygems'

if ENV['COVERAGE']
  require File.join(File.dirname(File.expand_path(__FILE__)), "sequel_coverage")
  SimpleCov.sequel_coverage(:filter=>%r{lib/sequel/extensions/core_extensions\.rb\z})
end

unless Object.const_defined?('Sequel') && Sequel.const_defined?('Model')
  $:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../../lib/"))
  require 'sequel'
  Sequel::Deprecation.backtrace_filter = true
end

Sequel.quote_identifiers = false
Sequel.identifier_input_method = nil
Sequel.identifier_output_method = nil

Regexp.send(:include, Sequel::SQL::StringMethods)
String.send(:include, Sequel::SQL::StringMethods)
Sequel.extension :core_extensions
if RUBY_VERSION < '1.9.0'
  Sequel.extension :ruby18_symbol_extensions
end

require 'minitest/autorun'

describe "Sequel core extensions" do
  it "should have Sequel.core_extensions? be true if enabled" do
    Sequel.core_extensions?.must_equal true
  end
end

describe "Core extensions" do
  before do
    db = Sequel::Database.new
    @d = db[:items]
    def @d.supports_regexp?; true end
    def @d.l(*args, &block)
      literal(filter_expr(*args, &block))
    end
    def @d.lit(*args)
      literal(*args)
    end
  end
  
  if RUBY_VERSION < '1.9.0'
    it "should not allow inequality operations on true, false, or nil" do
      @d.lit(:x > 1).must_equal "(x > 1)"
      @d.lit(:x < true).must_equal "(x < 't')"
      @d.lit(:x >= false).must_equal "(x >= 'f')"
      @d.lit(:x <= nil).must_equal "(x <= NULL)"
    end

    it "should not allow inequality operations on boolean complex expressions" do
      @d.lit(:x > (:y > 5)).must_equal "(x > (y > 5))"
      @d.lit(:x < (:y < 5)).must_equal "(x < (y < 5))"
      @d.lit(:x >= (:y >= 5)).must_equal "(x >= (y >= 5))"
      @d.lit(:x <= (:y <= 5)).must_equal "(x <= (y <= 5))"
      @d.lit(:x > {:y => nil}).must_equal "(x > (y IS NULL))"
      @d.lit(:x < ~{:y => nil}).must_equal "(x < (y IS NOT NULL))"
      @d.lit(:x >= {:y => 5}).must_equal "(x >= (y = 5))"
      @d.lit(:x <= ~{:y => 5}).must_equal "(x <= (y != 5))"
      @d.lit(:x >= {:y => [1,2,3]}).must_equal "(x >= (y IN (1, 2, 3)))"
      @d.lit(:x <= ~{:y => [1,2,3]}).must_equal "(x <= (y NOT IN (1, 2, 3)))"
    end
    
    it "should support >, <, >=, and <= via Symbol#>,<,>=,<=" do
      @d.l(:x > 100).must_equal '(x > 100)'
      @d.l(:x < 100.01).must_equal '(x < 100.01)'
      @d.l(:x >= 100000000000000000000000000000000000).must_equal '(x >= 100000000000000000000000000000000000)'
      @d.l(:x <= 100).must_equal '(x <= 100)'
    end
    
    it "should support negation of >, <, >=, and <= via Symbol#~" do
      @d.l(~(:x > 100)).must_equal '(x <= 100)'
      @d.l(~(:x < 100.01)).must_equal '(x >= 100.01)'
      @d.l(~(:x >= 100000000000000000000000000000000000)).must_equal '(x < 100000000000000000000000000000000000)'
      @d.l(~(:x <= 100)).must_equal '(x > 100)'
    end
    
    it "should support double negation via ~" do
      @d.l(~~(:x > 100)).must_equal '(x > 100)'
    end
  end
  it "should support NOT via Symbol#~" do
    @d.l(~:x).must_equal 'NOT x'
    @d.l(~:x__y).must_equal 'NOT x.y'
  end
  
  it "should support + - * / via Symbol#+,-,*,/" do
    @d.l(:x + 1 > 100).must_equal '((x + 1) > 100)'
    @d.l((:x * :y) < 100.01).must_equal '((x * y) < 100.01)'
    @d.l((:x - :y/2) >= 100000000000000000000000000000000000).must_equal '((x - (y / 2)) >= 100000000000000000000000000000000000)'
    @d.l((((:x - :y)/(:x + :y))*:z) <= 100).must_equal '((((x - y) / (x + y)) * z) <= 100)'
    @d.l(~((((:x - :y)/(:x + :y))*:z) <= 100)).must_equal '((((x - y) / (x + y)) * z) > 100)'
  end
  
  it "should support LIKE via Symbol#like" do
    @d.l(:x.like('a')).must_equal '(x LIKE \'a\' ESCAPE \'\\\')'
    @d.l(:x.like(/a/)).must_equal '(x ~ \'a\')'
    @d.l(:x.like('a', 'b')).must_equal '((x LIKE \'a\' ESCAPE \'\\\') OR (x LIKE \'b\' ESCAPE \'\\\'))'
    @d.l(:x.like(/a/, /b/i)).must_equal '((x ~ \'a\') OR (x ~* \'b\'))'
    @d.l(:x.like('a', /b/)).must_equal '((x LIKE \'a\' ESCAPE \'\\\') OR (x ~ \'b\'))'

    @d.l('a'.like(:x)).must_equal "('a' LIKE x ESCAPE '\\')"
    @d.l('a'.like(:x, 'b')).must_equal "(('a' LIKE x ESCAPE '\\') OR ('a' LIKE 'b' ESCAPE '\\'))"
    @d.l('a'.like(:x, /b/)).must_equal "(('a' LIKE x ESCAPE '\\') OR ('a' ~ 'b'))"
    @d.l('a'.like(:x, /b/i)).must_equal "(('a' LIKE x ESCAPE '\\') OR ('a' ~* 'b'))"

    @d.l(/a/.like(:x)).must_equal "('a' ~ x)"
    @d.l(/a/.like(:x, 'b')).must_equal "(('a' ~ x) OR ('a' ~ 'b'))"
    @d.l(/a/.like(:x, /b/)).must_equal "(('a' ~ x) OR ('a' ~ 'b'))"
    @d.l(/a/.like(:x, /b/i)).must_equal "(('a' ~ x) OR ('a' ~* 'b'))"

    @d.l(/a/i.like(:x)).must_equal "('a' ~* x)"
    @d.l(/a/i.like(:x, 'b')).must_equal "(('a' ~* x) OR ('a' ~* 'b'))"
    @d.l(/a/i.like(:x, /b/)).must_equal "(('a' ~* x) OR ('a' ~* 'b'))"
    @d.l(/a/i.like(:x, /b/i)).must_equal "(('a' ~* x) OR ('a' ~* 'b'))"
  end

  it "should support NOT LIKE via Symbol#like and Symbol#~" do
    @d.l(~:x.like('a')).must_equal '(x NOT LIKE \'a\' ESCAPE \'\\\')'
    @d.l(~:x.like(/a/)).must_equal '(x !~ \'a\')'
    @d.l(~:x.like('a', 'b')).must_equal '((x NOT LIKE \'a\' ESCAPE \'\\\') AND (x NOT LIKE \'b\' ESCAPE \'\\\'))'
    @d.l(~:x.like(/a/, /b/i)).must_equal '((x !~ \'a\') AND (x !~* \'b\'))'
    @d.l(~:x.like('a', /b/)).must_equal '((x NOT LIKE \'a\' ESCAPE \'\\\') AND (x !~ \'b\'))'

    @d.l(~'a'.like(:x)).must_equal "('a' NOT LIKE x ESCAPE '\\')"
    @d.l(~'a'.like(:x, 'b')).must_equal "(('a' NOT LIKE x ESCAPE '\\') AND ('a' NOT LIKE 'b' ESCAPE '\\'))"
    @d.l(~'a'.like(:x, /b/)).must_equal "(('a' NOT LIKE x ESCAPE '\\') AND ('a' !~ 'b'))"
    @d.l(~'a'.like(:x, /b/i)).must_equal "(('a' NOT LIKE x ESCAPE '\\') AND ('a' !~* 'b'))"

    @d.l(~/a/.like(:x)).must_equal "('a' !~ x)"
    @d.l(~/a/.like(:x, 'b')).must_equal "(('a' !~ x) AND ('a' !~ 'b'))"
    @d.l(~/a/.like(:x, /b/)).must_equal "(('a' !~ x) AND ('a' !~ 'b'))"
    @d.l(~/a/.like(:x, /b/i)).must_equal "(('a' !~ x) AND ('a' !~* 'b'))"

    @d.l(~/a/i.like(:x)).must_equal "('a' !~* x)"
    @d.l(~/a/i.like(:x, 'b')).must_equal "(('a' !~* x) AND ('a' !~* 'b'))"
    @d.l(~/a/i.like(:x, /b/)).must_equal "(('a' !~* x) AND ('a' !~* 'b'))"
    @d.l(~/a/i.like(:x, /b/i)).must_equal "(('a' !~* x) AND ('a' !~* 'b'))"
  end

  it "should support ILIKE via Symbol#ilike" do
    @d.l(:x.ilike('a')).must_equal '(UPPER(x) LIKE UPPER(\'a\') ESCAPE \'\\\')'
    @d.l(:x.ilike(/a/)).must_equal '(x ~* \'a\')'
    @d.l(:x.ilike('a', 'b')).must_equal '((UPPER(x) LIKE UPPER(\'a\') ESCAPE \'\\\') OR (UPPER(x) LIKE UPPER(\'b\') ESCAPE \'\\\'))'
    @d.l(:x.ilike(/a/, /b/i)).must_equal '((x ~* \'a\') OR (x ~* \'b\'))'
    @d.l(:x.ilike('a', /b/)).must_equal '((UPPER(x) LIKE UPPER(\'a\') ESCAPE \'\\\') OR (x ~* \'b\'))'

    @d.l('a'.ilike(:x)).must_equal "(UPPER('a') LIKE UPPER(x) ESCAPE '\\')"
    @d.l('a'.ilike(:x, 'b')).must_equal "((UPPER('a') LIKE UPPER(x) ESCAPE '\\') OR (UPPER('a') LIKE UPPER('b') ESCAPE '\\'))"
    @d.l('a'.ilike(:x, /b/)).must_equal "((UPPER('a') LIKE UPPER(x) ESCAPE '\\') OR ('a' ~* 'b'))"
    @d.l('a'.ilike(:x, /b/i)).must_equal "((UPPER('a') LIKE UPPER(x) ESCAPE '\\') OR ('a' ~* 'b'))"

    @d.l(/a/.ilike(:x)).must_equal "('a' ~* x)"
    @d.l(/a/.ilike(:x, 'b')).must_equal "(('a' ~* x) OR ('a' ~* 'b'))"
    @d.l(/a/.ilike(:x, /b/)).must_equal "(('a' ~* x) OR ('a' ~* 'b'))"
    @d.l(/a/.ilike(:x, /b/i)).must_equal "(('a' ~* x) OR ('a' ~* 'b'))"

    @d.l(/a/i.ilike(:x)).must_equal "('a' ~* x)"
    @d.l(/a/i.ilike(:x, 'b')).must_equal "(('a' ~* x) OR ('a' ~* 'b'))"
    @d.l(/a/i.ilike(:x, /b/)).must_equal "(('a' ~* x) OR ('a' ~* 'b'))"
    @d.l(/a/i.ilike(:x, /b/i)).must_equal "(('a' ~* x) OR ('a' ~* 'b'))"
  end

  it "should support NOT ILIKE via Symbol#ilike and Symbol#~" do
    @d.l(~:x.ilike('a')).must_equal '(UPPER(x) NOT LIKE UPPER(\'a\') ESCAPE \'\\\')'
    @d.l(~:x.ilike(/a/)).must_equal '(x !~* \'a\')'
    @d.l(~:x.ilike('a', 'b')).must_equal '((UPPER(x) NOT LIKE UPPER(\'a\') ESCAPE \'\\\') AND (UPPER(x) NOT LIKE UPPER(\'b\') ESCAPE \'\\\'))'
    @d.l(~:x.ilike(/a/, /b/i)).must_equal '((x !~* \'a\') AND (x !~* \'b\'))'
    @d.l(~:x.ilike('a', /b/)).must_equal '((UPPER(x) NOT LIKE UPPER(\'a\') ESCAPE \'\\\') AND (x !~* \'b\'))'

    @d.l(~'a'.ilike(:x)).must_equal "(UPPER('a') NOT LIKE UPPER(x) ESCAPE '\\')"
    @d.l(~'a'.ilike(:x, 'b')).must_equal "((UPPER('a') NOT LIKE UPPER(x) ESCAPE '\\') AND (UPPER('a') NOT LIKE UPPER('b') ESCAPE '\\'))"
    @d.l(~'a'.ilike(:x, /b/)).must_equal "((UPPER('a') NOT LIKE UPPER(x) ESCAPE '\\') AND ('a' !~* 'b'))"
    @d.l(~'a'.ilike(:x, /b/i)).must_equal "((UPPER('a') NOT LIKE UPPER(x) ESCAPE '\\') AND ('a' !~* 'b'))"

    @d.l(~/a/.ilike(:x)).must_equal "('a' !~* x)"
    @d.l(~/a/.ilike(:x, 'b')).must_equal "(('a' !~* x) AND ('a' !~* 'b'))"
    @d.l(~/a/.ilike(:x, /b/)).must_equal "(('a' !~* x) AND ('a' !~* 'b'))"
    @d.l(~/a/.ilike(:x, /b/i)).must_equal "(('a' !~* x) AND ('a' !~* 'b'))"

    @d.l(~/a/i.ilike(:x)).must_equal "('a' !~* x)"
    @d.l(~/a/i.ilike(:x, 'b')).must_equal "(('a' !~* x) AND ('a' !~* 'b'))"
    @d.l(~/a/i.ilike(:x, /b/)).must_equal "(('a' !~* x) AND ('a' !~* 'b'))"
    @d.l(~/a/i.ilike(:x, /b/i)).must_equal "(('a' !~* x) AND ('a' !~* 'b'))"
  end

  it "should support sql_expr on arrays with all two pairs" do
    @d.l([[:x, 100],[:y, 'a']].sql_expr).must_equal '((x = 100) AND (y = \'a\'))'
    @d.l([[:x, true], [:y, false]].sql_expr).must_equal '((x IS TRUE) AND (y IS FALSE))'
    @d.l([[:x, nil], [:y, [1,2,3]]].sql_expr).must_equal '((x IS NULL) AND (y IN (1, 2, 3)))'
  end
  
  it "should support sql_negate on arrays with all two pairs" do
    @d.l([[:x, 100],[:y, 'a']].sql_negate).must_equal '((x != 100) AND (y != \'a\'))'
    @d.l([[:x, true], [:y, false]].sql_negate).must_equal '((x IS NOT TRUE) AND (y IS NOT FALSE))'
    @d.l([[:x, nil], [:y, [1,2,3]]].sql_negate).must_equal '((x IS NOT NULL) AND (y NOT IN (1, 2, 3)))'
  end
  
  it "should support ~ on arrays with all two pairs" do
    @d.l(~[[:x, 100],[:y, 'a']]).must_equal '((x != 100) OR (y != \'a\'))'
    @d.l(~[[:x, true], [:y, false]]).must_equal '((x IS NOT TRUE) OR (y IS NOT FALSE))'
    @d.l(~[[:x, nil], [:y, [1,2,3]]]).must_equal '((x IS NOT NULL) OR (y NOT IN (1, 2, 3)))'
  end
  
  it "should support sql_or on arrays with all two pairs" do
    @d.l([[:x, 100],[:y, 'a']].sql_or).must_equal '((x = 100) OR (y = \'a\'))'
    @d.l([[:x, true], [:y, false]].sql_or).must_equal '((x IS TRUE) OR (y IS FALSE))'
    @d.l([[:x, nil], [:y, [1,2,3]]].sql_or).must_equal '((x IS NULL) OR (y IN (1, 2, 3)))'
  end
  
  it "should support Array#sql_string_join for concatenation of SQL strings" do
    @d.lit([:x].sql_string_join).must_equal '(x)'
    @d.lit([:x].sql_string_join(', ')).must_equal '(x)'
    @d.lit([:x, :y].sql_string_join).must_equal '(x || y)'
    @d.lit([:x, :y].sql_string_join(', ')).must_equal "(x || ', ' || y)"
    @d.lit([:x.sql_function(1), :y.sql_subscript(1)].sql_string_join).must_equal '(x(1) || y[1])'
    @d.lit([:x.sql_function(1), 'y.z'.lit].sql_string_join(', ')).must_equal "(x(1) || ', ' || y.z)"
    @d.lit([:x, 1, :y].sql_string_join).must_equal "(x || '1' || y)"
    @d.lit([:x, 1, :y].sql_string_join(', ')).must_equal "(x || ', ' || '1' || ', ' || y)"
    @d.lit([:x, 1, :y].sql_string_join(:y__z)).must_equal "(x || y.z || '1' || y.z || y)"
    @d.lit([:x, 1, :y].sql_string_join(1)).must_equal "(x || '1' || '1' || '1' || y)"
    @d.lit([:x, :y].sql_string_join('y.x || x.y'.lit)).must_equal "(x || y.x || x.y || y)"
    @d.lit([[:x, :y].sql_string_join, [:a, :b].sql_string_join].sql_string_join).must_equal "(x || y || a || b)"
  end

  it "should support sql_expr on hashes" do
    @d.l({:x => 100, :y => 'a'}.sql_expr)[1...-1].split(' AND ').sort.must_equal ['(x = 100)', '(y = \'a\')']
    @d.l({:x => true, :y => false}.sql_expr)[1...-1].split(' AND ').sort.must_equal ['(x IS TRUE)', '(y IS FALSE)']
    @d.l({:x => nil, :y => [1,2,3]}.sql_expr)[1...-1].split(' AND ').sort.must_equal ['(x IS NULL)', '(y IN (1, 2, 3))']
  end
  
  it "should support sql_negate on hashes" do
    @d.l({:x => 100, :y => 'a'}.sql_negate)[1...-1].split(' AND ').sort.must_equal ['(x != 100)', '(y != \'a\')']
    @d.l({:x => true, :y => false}.sql_negate)[1...-1].split(' AND ').sort.must_equal ['(x IS NOT TRUE)', '(y IS NOT FALSE)']
    @d.l({:x => nil, :y => [1,2,3]}.sql_negate)[1...-1].split(' AND ').sort.must_equal ['(x IS NOT NULL)', '(y NOT IN (1, 2, 3))']
  end
  
  it "should support ~ on hashes" do
    @d.l(~{:x => 100, :y => 'a'})[1...-1].split(' OR ').sort.must_equal ['(x != 100)', '(y != \'a\')']
    @d.l(~{:x => true, :y => false})[1...-1].split(' OR ').sort.must_equal ['(x IS NOT TRUE)', '(y IS NOT FALSE)']
    @d.l(~{:x => nil, :y => [1,2,3]})[1...-1].split(' OR ').sort.must_equal ['(x IS NOT NULL)', '(y NOT IN (1, 2, 3))']
  end
  
  it "should support sql_or on hashes" do
    @d.l({:x => 100, :y => 'a'}.sql_or)[1...-1].split(' OR ').sort.must_equal ['(x = 100)', '(y = \'a\')']
    @d.l({:x => true, :y => false}.sql_or)[1...-1].split(' OR ').sort.must_equal ['(x IS TRUE)', '(y IS FALSE)']
    @d.l({:x => nil, :y => [1,2,3]}.sql_or)[1...-1].split(' OR ').sort.must_equal ['(x IS NULL)', '(y IN (1, 2, 3))']
  end
  
  it "should Hash#& and Hash#|" do
    @d.l({:y => :z} & :x).must_equal '((y = z) AND x)'
    @d.l({:x => :a} & {:y => :z}).must_equal '((x = a) AND (y = z))'
    @d.l({:y => :z} | :x).must_equal '((y = z) OR x)'
    @d.l({:x => :a} | {:y => :z}).must_equal '((x = a) OR (y = z))'
  end
end

describe "Array#case and Hash#case" do
  before do
    @d = Sequel.mock.dataset
  end

  it "should return SQL CASE expression" do
    @d.literal({:x=>:y}.case(:z)).must_equal '(CASE WHEN x THEN y ELSE z END)'
    @d.literal({:x=>:y}.case(:z, :exp)).must_equal '(CASE exp WHEN x THEN y ELSE z END)'
    ['(CASE WHEN x THEN y WHEN a THEN b ELSE z END)',
     '(CASE WHEN a THEN b WHEN x THEN y ELSE z END)'].must_include(@d.literal({:x=>:y, :a=>:b}.case(:z)))
    @d.literal([[:x, :y]].case(:z)).must_equal '(CASE WHEN x THEN y ELSE z END)'
    @d.literal([[:x, :y], [:a, :b]].case(:z)).must_equal '(CASE WHEN x THEN y WHEN a THEN b ELSE z END)'
    @d.literal([[:x, :y], [:a, :b]].case(:z, :exp)).must_equal '(CASE exp WHEN x THEN y WHEN a THEN b ELSE z END)'
    @d.literal([[:x, :y], [:a, :b]].case(:z, :exp__w)).must_equal '(CASE exp.w WHEN x THEN y WHEN a THEN b ELSE z END)'
  end

  it "should return SQL CASE expression with expression even if nil" do
    @d.literal({:x=>:y}.case(:z, nil)).must_equal '(CASE NULL WHEN x THEN y ELSE z END)'
  end

  it "should raise an error if an array that isn't all two pairs is used" do
    proc{[:b].case(:a)}.must_raise(Sequel::Error)
    proc{[:b, :c].case(:a)}.must_raise(Sequel::Error)
    proc{[[:b, :c], :d].case(:a)}.must_raise(Sequel::Error)
  end

  it "should raise an error if an empty array/hash is used" do
    proc{[].case(:a)}.must_raise(Sequel::Error)
    proc{{}.case(:a)}.must_raise(Sequel::Error)
  end
end

describe "Array#sql_value_list and #sql_array" do
  before do
    @d = Sequel.mock.dataset
  end

  it "should treat the array as an SQL value list instead of conditions when used as a placeholder value" do
    @d.filter("(a, b) IN ?", [[:x, 1], [:y, 2]]).sql.must_equal 'SELECT * WHERE ((a, b) IN ((x = 1) AND (y = 2)))'
    @d.filter("(a, b) IN ?", [[:x, 1], [:y, 2]].sql_value_list).sql.must_equal 'SELECT * WHERE ((a, b) IN ((x, 1), (y, 2)))'
    @d.filter("(a, b) IN ?", [[:x, 1], [:y, 2]].sql_array).sql.must_equal 'SELECT * WHERE ((a, b) IN ((x, 1), (y, 2)))'
  end

  it "should be no difference when used as a hash value" do
    @d.filter([:a, :b]=>[[:x, 1], [:y, 2]]).sql.must_equal 'SELECT * WHERE ((a, b) IN ((x, 1), (y, 2)))'
    @d.filter([:a, :b]=>[[:x, 1], [:y, 2]].sql_value_list).sql.must_equal 'SELECT * WHERE ((a, b) IN ((x, 1), (y, 2)))'
    @d.filter([:a, :b]=>[[:x, 1], [:y, 2]].sql_array).sql.must_equal 'SELECT * WHERE ((a, b) IN ((x, 1), (y, 2)))'
  end
end

describe "String#lit" do
  before do
    @ds = Sequel::Database.new[:t]
  end

  it "should return an LiteralString object" do
    'xyz'.lit.must_be_kind_of(Sequel::LiteralString)
    'xyz'.lit.to_s.must_equal 'xyz'
  end
  
  it "should inhibit string literalization" do
    @ds.update_sql(:stamp => "NOW()".lit).must_equal "UPDATE t SET stamp = NOW()"
  end

  it "should return a PlaceholderLiteralString object if args are given" do
    a = 'DISTINCT ?'.lit(:a)
    a.must_be_kind_of(Sequel::SQL::PlaceholderLiteralString)
    @ds.literal(a).must_equal 'DISTINCT a'
    @ds.quote_identifiers = true
    @ds.literal(a).must_equal 'DISTINCT "a"'
  end
  
  it "should handle named placeholders if given a single argument hash" do
    a = 'DISTINCT :b'.lit(:b=>:a)
    a.must_be_kind_of(Sequel::SQL::PlaceholderLiteralString)
    @ds.literal(a).must_equal 'DISTINCT a'
    @ds.quote_identifiers = true
    @ds.literal(a).must_equal 'DISTINCT "a"'
  end

  it "should treat placeholder literal strings as generic expressions" do
    a = ':b'.lit(:b=>:a)
    @ds.literal(a + 1).must_equal "(a + 1)"
    @ds.literal(a & :b).must_equal "(a AND b)"
    @ds.literal(a.sql_string + :b).must_equal "(a || b)"
  end
end

describe "String#to_sequel_blob" do
  it "should return a Blob object" do
    'xyz'.to_sequel_blob.must_be_kind_of(::Sequel::SQL::Blob)
    'xyz'.to_sequel_blob.must_equal 'xyz'
  end

  it "should retain binary data" do
    "\1\2\3\4".to_sequel_blob.must_equal "\1\2\3\4"
  end
end

describe "String cast methods" do
  before do
    @ds = Sequel.mock.dataset
  end

  it "should support cast method" do
    @ds.literal('abc'.cast(:integer)).must_equal "CAST('abc' AS integer)"
  end

  it "should support cast_numeric and cast_string" do
    x = 'abc'.cast_numeric
    x.must_be_kind_of(Sequel::SQL::NumericExpression)
    @ds.literal(x).must_equal "CAST('abc' AS integer)"

    x = 'abc'.cast_numeric(:real)
    x.must_be_kind_of(Sequel::SQL::NumericExpression)
    @ds.literal(x).must_equal "CAST('abc' AS real)"

    x = 'abc'.cast_string
    x.must_be_kind_of(Sequel::SQL::StringExpression)
    @ds.literal(x).must_equal "CAST('abc' AS varchar(255))"

    x = 'abc'.cast_string(:varchar)
    x.must_be_kind_of(Sequel::SQL::StringExpression)
    @ds.literal(x).must_equal "CAST('abc' AS varchar(255))"
  end
end
  
describe "#desc" do
  before do
    @ds = Sequel.mock.dataset
  end
  
  it "should format a DESC clause for a column ref" do
    @ds.literal(:test.desc).must_equal 'test DESC'
    
    @ds.literal(:items__price.desc).must_equal 'items.price DESC'
  end

  it "should format a DESC clause for a function" do
    @ds.literal(:avg.sql_function(:test).desc).must_equal 'avg(test) DESC'
  end
end

describe "#asc" do
  before do
    @ds = Sequel.mock.dataset
  end
  
  it "should format a ASC clause for a column ref" do
    @ds.literal(:test.asc).must_equal 'test ASC'
    
    @ds.literal(:items__price.asc).must_equal 'items.price ASC'
  end

  it "should format a ASC clause for a function" do
    @ds.literal(:avg.sql_function(:test).asc).must_equal 'avg(test) ASC'
  end
end

describe "#as" do
  before do
    @ds = Sequel.mock.dataset
  end
  
  it "should format a AS clause for a column ref" do
    @ds.literal(:test.as(:t)).must_equal 'test AS t'
    
    @ds.literal(:items__price.as(:p)).must_equal 'items.price AS p'
  end

  it "should format a AS clause for a function" do
    @ds.literal(:avg.sql_function(:test).as(:avg)).must_equal 'avg(test) AS avg'
  end
  
  it "should format a AS clause for a literal value" do
    @ds.literal('abc'.as(:abc)).must_equal "'abc' AS abc"
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
  
  it "should be quoted properly" do
    @ds.literal(:xyz).must_equal "`xyz`"
    @ds.literal(:xyz__abc).must_equal "`xyz`.`abc`"

    @ds.literal(:xyz.as(:x)).must_equal "`xyz` AS `x`"
    @ds.literal(:xyz__abc.as(:x)).must_equal "`xyz`.`abc` AS `x`"

    @ds.literal(:xyz___x).must_equal "`xyz` AS `x`"
    @ds.literal(:xyz__abc___x).must_equal "`xyz`.`abc` AS `x`"
  end
  
  it "should be quoted properly in SQL functions" do
    @ds.literal(:avg.sql_function(:xyz)).must_equal "avg(`xyz`)"
    @ds.literal(:avg.sql_function(:xyz, 1)).must_equal "avg(`xyz`, 1)"
    @ds.literal(:avg.sql_function(:xyz).as(:a)).must_equal "avg(`xyz`) AS `a`"
  end

  it "should be quoted properly in ASC/DESC clauses" do
    @ds.literal(:xyz.asc).must_equal "`xyz` ASC"
    @ds.literal(:avg.sql_function(:xyz, 1).desc).must_equal "avg(`xyz`, 1) DESC"
  end
  
  it "should be quoted properly in a cast function" do
    @ds.literal(:x.cast(:integer)).must_equal "CAST(`x` AS integer)"
    @ds.literal(:x__y.cast('varchar(20)')).must_equal "CAST(`x`.`y` AS varchar(20))"
  end
end

describe "Blob" do
  it "#to_sequel_blob should return self" do
    blob = "x".to_sequel_blob
    blob.to_sequel_blob.object_id.must_equal blob.object_id
  end
end

if RUBY_VERSION < '1.9.0'
  describe "Symbol#[]" do
    it "should format an SQL Function" do
      ds = Sequel.mock.dataset
      ds.literal(:xyz[]).must_equal 'xyz()'
      ds.literal(:xyz[1]).must_equal 'xyz(1)'
      ds.literal(:xyz[1, 2, :abc[3]]).must_equal 'xyz(1, 2, abc(3))'
    end
  end
end

describe "Symbol#*" do
  before do
    @ds = Sequel.mock.dataset
  end
  
  it "should format a qualified wildcard if no argument" do
    @ds.literal(:xyz.*).must_equal 'xyz.*'
    @ds.literal(:abc.*).must_equal 'abc.*'
  end

  it "should format a filter expression if an argument" do
    @ds.literal(:xyz.*(3)).must_equal '(xyz * 3)'
    @ds.literal(:abc.*(5)).must_equal '(abc * 5)'
  end

  it "should support qualified symbols if no argument" do
    @ds.literal(:xyz__abc.*).must_equal 'xyz.abc.*'
  end
end

describe "Symbol" do
  before do
    @ds = Sequel.mock.dataset
    @ds.quote_identifiers = true
    @ds.identifier_input_method = :upcase
  end

  it "#identifier should format an identifier" do
    @ds.literal(:xyz__abc.identifier).must_equal '"XYZ__ABC"'
  end

  it "#qualify should format a qualified column" do
    @ds.literal(:xyz.qualify(:abc)).must_equal '"ABC"."XYZ"'
  end

  it "#qualify should work on QualifiedIdentifiers" do
    @ds.literal(:xyz.qualify(:abc).qualify(:def)).must_equal '"DEF"."ABC"."XYZ"'
  end

  it "should be able to qualify an identifier" do
    @ds.literal(:xyz.identifier.qualify(:xyz__abc)).must_equal '"XYZ"."ABC"."XYZ"'
  end

  it "should be able to specify a schema.table.column" do
    @ds.literal(:column.qualify(:table.qualify(:schema))).must_equal '"SCHEMA"."TABLE"."COLUMN"'
    @ds.literal(:column.qualify(:table__name.identifier.qualify(:schema))).must_equal '"SCHEMA"."TABLE__NAME"."COLUMN"'
  end

  it "should be able to specify order" do
    @oe = :xyz.desc
    @oe.class.must_equal Sequel::SQL::OrderedExpression
    @oe.descending.must_equal true
    @oe = :xyz.asc
    @oe.class.must_equal Sequel::SQL::OrderedExpression
    @oe.descending.must_equal false
  end

  it "should work correctly with objects" do
    o = Object.new
    def o.sql_literal(ds) "(foo)" end
    @ds.literal(:column.qualify(o)).must_equal '(foo)."COLUMN"'
  end
end

describe "Symbol" do
  before do
    @ds = Sequel::Database.new.dataset
  end
  
  it "should support sql_function method" do
    @ds.literal(:COUNT.sql_function('1')).must_equal "COUNT('1')"
    @ds.select(:COUNT.sql_function('1')).sql.must_equal "SELECT COUNT('1')"
  end
  
  it "should support cast method" do
    @ds.literal(:abc.cast(:integer)).must_equal "CAST(abc AS integer)"
  end

  it "should support sql array accesses via sql_subscript" do
    @ds.literal(:abc.sql_subscript(1)).must_equal "abc[1]"
    @ds.literal(:abc__def.sql_subscript(1)).must_equal "abc.def[1]"
    @ds.literal(:abc.sql_subscript(1)|2).must_equal "abc[1, 2]"
    @ds.literal(:abc.sql_subscript(1)[2]).must_equal "abc[1][2]"
  end

  it "should support cast_numeric and cast_string" do
    x = :abc.cast_numeric
    x.must_be_kind_of(Sequel::SQL::NumericExpression)
    @ds.literal(x).must_equal "CAST(abc AS integer)"

    x = :abc.cast_numeric(:real)
    x.must_be_kind_of(Sequel::SQL::NumericExpression)
    @ds.literal(x).must_equal "CAST(abc AS real)"

    x = :abc.cast_string
    x.must_be_kind_of(Sequel::SQL::StringExpression)
    @ds.literal(x).must_equal "CAST(abc AS varchar(255))"

    x = :abc.cast_string(:varchar)
    x.must_be_kind_of(Sequel::SQL::StringExpression)
    @ds.literal(x).must_equal "CAST(abc AS varchar(255))"
  end
  
  it "should support boolean methods" do
    @ds.literal(~:x).must_equal "NOT x"
    @ds.literal(:x & :y).must_equal "(x AND y)"
    @ds.literal(:x | :y).must_equal "(x OR y)"
  end

  it "should support complex expression methods" do
    @ds.literal(:x.sql_boolean & 1).must_equal "(x AND 1)"
    @ds.literal(:x.sql_number & :y).must_equal "(x & y)"
    @ds.literal(:x.sql_string + :y).must_equal "(x || y)"
  end

  it "should allow database independent types when casting" do
    db = @ds.db
    def db.cast_type_literal(type)
      return :foo if type == Integer
      return :bar if type == String
      type
    end
    @ds.literal(:abc.cast(String)).must_equal "CAST(abc AS bar)"
    @ds.literal(:abc.cast(String)).must_equal "CAST(abc AS bar)"
    @ds.literal(:abc.cast_string).must_equal "CAST(abc AS bar)"
    @ds.literal(:abc.cast_string(Integer)).must_equal "CAST(abc AS foo)"
    @ds.literal(:abc.cast_numeric).must_equal "CAST(abc AS foo)"
    @ds.literal(:abc.cast_numeric(String)).must_equal "CAST(abc AS bar)"
  end

  it "should support SQL EXTRACT function via #extract " do
    @ds.literal(:abc.extract(:year)).must_equal "extract(year FROM abc)"
  end
end

describe "Postgres extensions integration" do
  before do
    @db = Sequel.mock
    Sequel.extension(:pg_array, :pg_array_ops, :pg_hstore, :pg_hstore_ops, :pg_json, :pg_json_ops, :pg_range, :pg_range_ops, :pg_row, :pg_row_ops, :pg_inet_ops)
  end

  it "Symbol#pg_array should return an ArrayOp" do
    @db.literal(:a.pg_array.unnest).must_equal "unnest(a)"
  end

  it "Symbol#pg_row should return a PGRowOp" do
    @db.literal(:a.pg_row[:a]).must_equal "(a).a"
  end

  it "Symbol#hstore should return an HStoreOp" do
    @db.literal(:a.hstore['a']).must_equal "(a -> 'a')"
  end

  it "Symbol#pg_inet should return an InetOp" do
    @db.literal(:a.pg_inet.contains(:b)).must_equal "(a >> b)"
  end

  it "Symbol#pg_json should return an JSONOp" do
    @db.literal(:a.pg_json[%w'a b']).must_equal "(a #> ARRAY['a','b'])"
    @db.literal(:a.pg_json.extract('a')).must_equal "json_extract_path(a, 'a')"
  end

  it "Symbol#pg_jsonb should return an JSONBOp" do
    @db.literal(:a.pg_jsonb[%w'a b']).must_equal "(a #> ARRAY['a','b'])"
    @db.literal(:a.pg_jsonb.extract('a')).must_equal "jsonb_extract_path(a, 'a')"
  end

  it "Symbol#pg_range should return a RangeOp" do
    @db.literal(:a.pg_range.lower).must_equal "lower(a)"
  end

  it "Array#pg_array should return a PGArray" do
    @db.literal([1].pg_array.op.unnest).must_equal "unnest(ARRAY[1])"
    @db.literal([1].pg_array(:int4).op.unnest).must_equal "unnest(ARRAY[1]::int4[])"
  end

  it "Array#pg_json should return a JSONArray" do
    @db.literal([1].pg_json).must_equal "'[1]'::json"
  end

  it "Array#pg_jsonb should return a JSONBArray" do
    @db.literal([1].pg_jsonb).must_equal "'[1]'::jsonb"
  end

  it "Array#pg_row should return a ArrayRow" do
    @db.literal([1].pg_row).must_equal "ROW(1)"
  end

  it "Hash#hstore should return an HStore" do
    @db.literal({'a'=>1}.hstore.op['a']).must_equal '(\'"a"=>"1"\'::hstore -> \'a\')'
  end

  it "Hash#pg_json should return an JSONHash" do
    @db.literal({'a'=>'b'}.pg_json).must_equal "'{\"a\":\"b\"}'::json"
  end

  it "Hash#pg_jsonb should return an JSONBHash" do
    @db.literal({'a'=>'b'}.pg_jsonb).must_equal "'{\"a\":\"b\"}'::jsonb"
  end

  it "Range#pg_range should return an PGRange" do
    @db.literal((1..2).pg_range).must_equal "'[1,2]'"
    @db.literal((1..2).pg_range(:int4range)).must_equal "'[1,2]'::int4range"
  end
end
