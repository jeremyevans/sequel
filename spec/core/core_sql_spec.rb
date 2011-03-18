require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')

describe "Array#all_two_pairs?" do
  specify "should return false if empty" do
    [].all_two_pairs?.should == false
  end

  specify "should return false if any of the elements is not an array" do
    [1].all_two_pairs?.should == false
    [[1,2],1].all_two_pairs?.should == false
  end

  specify "should return false if any of the elements has a length other than two" do
    [[1,2],[]].all_two_pairs?.should == false
    [[1,2],[1]].all_two_pairs?.should == false
    [[1,2],[1,2,3]].all_two_pairs?.should == false
  end

  specify "should return true if all of the elements are arrays with a length of two" do
    [[1,2]].all_two_pairs?.should == true
    [[1,2],[1,2]].all_two_pairs?.should == true
    [[1,2],[1,2],[1,2]].all_two_pairs?.should == true
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
    @ds = ds = MockDatabase.new.dataset
  end
  specify "should return an LiteralString object" do
    'xyz'.lit.should be_a_kind_of(Sequel::LiteralString)
    'xyz'.lit.to_s.should == 'xyz'
  end
  
  specify "should inhibit string literalization" do
    Sequel::Database.new[:t].update_sql(:stamp => "NOW()".lit).should == \
      "UPDATE t SET stamp = NOW()"
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
    @c = Class.new(Sequel::Dataset) do
      def quoted_identifier(c); "`#{c}`"; end
    end
    @ds = @c.new(MockDatabase.new)
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
end

describe "Dataset#literal" do
  before do
    @ds = MockDataset.new(nil)
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

describe "Symbol" do
  before do
    @ds = Sequel::Dataset.new(MockDatabase.new)
  end
  
  specify "should support upper case outer functions" do
    :COUNT.sql_function('1').to_s(@ds).should == "COUNT('1')"
  end
  
  specify "should inhibit string literalization" do
    db = Sequel::Database.new
    ds = db[:t]
    ds.select(:COUNT.sql_function('1')).sql.should == "SELECT COUNT('1') FROM t"
  end
  
  specify "should support cast method" do
    :abc.cast(:integer).to_s(@ds).should == "CAST(abc AS integer)"
  end

  specify "should support sql array accesses via sql_subscript" do
    @ds.literal(:abc.sql_subscript(1)).should == "abc[1]"
    @ds.literal(:abc__def.sql_subscript(1)).should == "abc.def[1]"
    @ds.literal(:abc.sql_subscript(1)|2).should == "abc[1, 2]"
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
    m = MockDatabase.new
    m.instance_eval do
       def cast_type_literal(type)
         return :foo if type == Integer
         return :bar if type == String
         type
       end
    end
    @ds2 = Sequel::Dataset.new(m)
    :abc.cast(String).to_s(@ds).should == "CAST(abc AS varchar(255))"
    :abc.cast(String).to_s(@ds2).should == "CAST(abc AS bar)"
    :abc.cast(String).to_s(@ds2).should == "CAST(abc AS bar)"
    :abc.cast_string.to_s(@ds2).should == "CAST(abc AS bar)"
    :abc.cast_string(Integer).to_s(@ds2).should == "CAST(abc AS foo)"
    :abc.cast_numeric.to_s(@ds2).should == "CAST(abc AS foo)"
    :abc.cast_numeric(String).to_s(@ds2).should == "CAST(abc AS bar)"
  end

  specify "should support SQL EXTRACT function via #extract " do
    :abc.extract(:year).to_s(@ds).should == "extract(year FROM abc)"
  end
end

describe "Sequel::SQL::Function#==" do
  specify "should be true for functions with the same name and arguments, false otherwise" do
    a = :date.sql_function(:t)
    b = :date.sql_function(:t)
    a.should == b
    (a == b).should == true
    c = :date.sql_function(:c)
    a.should_not == c
    (a == c).should == false
    d = :time.sql_function(:c)
    a.should_not == d
    c.should_not == d
    (a == d).should == false
    (c == d).should == false
  end
end

describe "Sequel::SQL::OrderedExpression" do
  specify "should #desc" do
    @oe = :column.asc
    @oe.descending.should == false
    @oe.desc.descending.should == true
  end

  specify "should #asc" do
    @oe = :column.desc
    @oe.descending.should == true
    @oe.asc.descending.should == false
  end

  specify "should #invert" do
    @oe = :column.desc
    @oe.invert.descending.should == false
    @oe.invert.invert.descending.should == true
  end
end

describe "Expression" do
  specify "should consider objects == only if they have the same attributes" do
    :column.qualify(:table).cast(:type).*(:numeric_column).asc.should == :column.qualify(:table).cast(:type).*(:numeric_column).asc
    :other_column.qualify(:table).cast(:type).*(:numeric_column).asc.should_not == :column.qualify(:table).cast(:type).*(:numeric_column).asc

    :column.qualify(:table).cast(:type).*(:numeric_column).asc.should eql(:column.qualify(:table).cast(:type).*(:numeric_column).asc)
    :other_column.qualify(:table).cast(:type).*(:numeric_column).asc.should_not eql(:column.qualify(:table).cast(:type).*(:numeric_column).asc)
  end

  specify "should use the same hash value for objects that have the same attributes" do
    :column.qualify(:table).cast(:type).*(:numeric_column).asc.hash.should == :column.qualify(:table).cast(:type).*(:numeric_column).asc.hash
    :other_column.qualify(:table).cast(:type).*(:numeric_column).asc.hash.should_not == :column.qualify(:table).cast(:type).*(:numeric_column).asc.hash

    h = {}
    a = :column.qualify(:table).cast(:type).*(:numeric_column).asc
    b = :column.qualify(:table).cast(:type).*(:numeric_column).asc
    h[a] = 1
    h[b] = 2
    h[a].should == 2
    h[b].should == 2
  end
end
