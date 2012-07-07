require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')

describe "Sequel core extensions" do
  specify "should not be used inside Sequel, to insure Sequel works without them" do
    usage = []
    match_re = /(\.(sql_value_list|sql_array|sql_expr|sql_negate|sql_or|sql_string_join|lit|to_sequel_blob|case)\W)|:\w+\.(qualify|identifier|as|cast|asc|desc|sql_subscript|\*|sql_function)\W/
    comment_re = /^\s*#|# core_sql/
    Dir['lib/sequel/**/*.rb'].each do |f|
      lines = File.read(f).split("\n").grep(match_re).delete_if{|l| l =~ comment_re}
      usage << [f, lines] unless lines.empty?
    end
    puts usage unless usage.empty?
    usage.should be_empty
  end

  specify "should have Sequel.core_extensions? be true if enabled" do
    Sequel.core_extensions?.should be_true
  end
end

describe "Array and Hash extensions" do
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
