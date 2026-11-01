# frozen_string_literal: true
if ENV['COVERAGE']
  require_relative "sequel_coverage"
  SimpleCov.sequel_coverage(:filter=>%r{lib/sequel/extensions/core_extensions\.rb\z})
end

$:.unshift(File.join(File.dirname(File.expand_path(__FILE__)), "../lib/"))
require_relative '../lib/sequel'

ENV['MT_NO_PLUGINS'] = '1' # Work around stupid autoloading of plugins
gem 'minitest'
require 'minitest/global_expectations/autorun'
require 'minitest/hooks/default'

require_relative "visibility_checking_after_hook" if ENV['CHECK_METHOD_VISIBILITY']

require_relative "deprecation_helper.rb"

if RUBY_VERSION >= "4.1"
Sequel.extension :virtual_row_core_refinements, :pg_array, :pg_hstore, :pg_row, :pg_range, :pg_multirange, :pg_row_ops, :pg_range_ops, :pg_array_ops, :pg_hstore_ops, :pg_json, :pg_json_ops, :sqlite_json_ops, :is_distinct_from

describe "Core refinements" do
  before do
    db = Sequel.mock
    @d = db[:items].with_extend do
      def supports_regexp?; true end
      def l(&block)
        literal(select(&block).opts[:select][0])
      end
    end
  end
  
  it "should support NOT via Symbol#~" do
    @d.l{~:x}.must_equal 'NOT x'
  end

  with_symbol_splitting "should support NOT via Symbol#~ for splittable symbols" do
    @d.l{~:x__y}.must_equal 'NOT x.y'
  end
  
  it "should support + - * / power via Symbol#+,-,*,/,**" do
    @d.l{:x + 1 > 100}.must_equal '((x + 1) > 100)'
    @d.l{(:x * :y) < 100.01}.must_equal '((x * y) < 100.01)'
    @d.l{(:x - :y/2) >= 100000000000000000000000000000000000}.must_equal '((x - (y / 2)) >= 100000000000000000000000000000000000)'
    @d.l{(((:x - :y)/(:x + :y))*:z) <= 100}.must_equal '((((x - y) / (x + y)) * z) <= 100)'
    @d.l{~((((:x - :y)/(:x + :y))*:z) <= 100)}.must_equal '((((x - y) / (x + y)) * z) > 100)'
    @d.l{~((((:x ** :y)/(:x + :y))*:z) <= 100)}.must_equal '(((power(x, y) / (x + y)) * z) > 100)'
  end
  
  it "should support LIKE via Symbol#like" do
    @d.l{:x.like('a')}.must_equal '(x LIKE \'a\' ESCAPE \'\\\')'
    @d.l{:x.like(/a/)}.must_equal '(x ~ \'a\')'
    @d.l{:x.like('a', 'b')}.must_equal '((x LIKE \'a\' ESCAPE \'\\\') OR (x LIKE \'b\' ESCAPE \'\\\'))'
    @d.l{:x.like(/a/, /b/i)}.must_equal '((x ~ \'a\') OR (x ~* \'b\'))'
    @d.l{:x.like('a', /b/)}.must_equal '((x LIKE \'a\' ESCAPE \'\\\') OR (x ~ \'b\'))'
  end

  it "should support NOT LIKE via Symbol#like and Symbol#~" do
    @d.l{~:x.like('a')}.must_equal '(x NOT LIKE \'a\' ESCAPE \'\\\')'
    @d.l{~:x.like(/a/)}.must_equal '(x !~ \'a\')'
    @d.l{~:x.like('a', 'b')}.must_equal '((x NOT LIKE \'a\' ESCAPE \'\\\') AND (x NOT LIKE \'b\' ESCAPE \'\\\'))'
    @d.l{~:x.like(/a/, /b/i)}.must_equal '((x !~ \'a\') AND (x !~* \'b\'))'
    @d.l{~:x.like('a', /b/)}.must_equal '((x NOT LIKE \'a\' ESCAPE \'\\\') AND (x !~ \'b\'))'
  end

  it "should support ILIKE via Symbol#ilike" do
    @d.l{:x.ilike('a')}.must_equal '(UPPER(x) LIKE UPPER(\'a\') ESCAPE \'\\\')'
    @d.l{:x.ilike(/a/)}.must_equal '(x ~* \'a\')'
    @d.l{:x.ilike('a', 'b')}.must_equal '((UPPER(x) LIKE UPPER(\'a\') ESCAPE \'\\\') OR (UPPER(x) LIKE UPPER(\'b\') ESCAPE \'\\\'))'
    @d.l{:x.ilike(/a/, /b/i)}.must_equal '((x ~* \'a\') OR (x ~* \'b\'))'
    @d.l{:x.ilike('a', /b/)}.must_equal '((UPPER(x) LIKE UPPER(\'a\') ESCAPE \'\\\') OR (x ~* \'b\'))'
  end

  it "should support NOT ILIKE via Symbol#ilike and Symbol#~" do
    @d.l{~:x.ilike('a')}.must_equal '(UPPER(x) NOT LIKE UPPER(\'a\') ESCAPE \'\\\')'
    @d.l{~:x.ilike(/a/)}.must_equal '(x !~* \'a\')'
    @d.l{~:x.ilike('a', 'b')}.must_equal '((UPPER(x) NOT LIKE UPPER(\'a\') ESCAPE \'\\\') AND (UPPER(x) NOT LIKE UPPER(\'b\') ESCAPE \'\\\'))'
    @d.l{~:x.ilike(/a/, /b/i)}.must_equal '((x !~* \'a\') AND (x !~* \'b\'))'
    @d.l{~:x.ilike('a', /b/)}.must_equal '((UPPER(x) NOT LIKE UPPER(\'a\') ESCAPE \'\\\') AND (x !~* \'b\'))'
  end

  it "should support sql_expr on arrays with all two pairs" do
    @d.l{[[:x, 100],[:y, 'a']].sql_expr}.must_equal '((x = 100) AND (y = \'a\'))'
    @d.l{[[:x, true], [:y, false]].sql_expr}.must_equal '((x IS TRUE) AND (y IS FALSE))'
    @d.l{[[:x, nil], [:y, [1,2,3]]].sql_expr}.must_equal '((x IS NULL) AND (y IN (1, 2, 3)))'
  end
  
  it "should support sql_negate on arrays with all two pairs" do
    @d.l{[[:x, 100],[:y, 'a']].sql_negate}.must_equal '((x != 100) AND (y != \'a\'))'
    @d.l{[[:x, true], [:y, false]].sql_negate}.must_equal '((x IS NOT TRUE) AND (y IS NOT FALSE))'
    @d.l{[[:x, nil], [:y, [1,2,3]]].sql_negate}.must_equal '((x IS NOT NULL) AND (y NOT IN (1, 2, 3)))'
  end
  
  it "should support ~ on arrays with all two pairs" do
    @d.l{~[[:x, 100],[:y, 'a']]}.must_equal '((x != 100) OR (y != \'a\'))'
    @d.l{~[[:x, true], [:y, false]]}.must_equal '((x IS NOT TRUE) OR (y IS NOT FALSE))'
    @d.l{~[[:x, nil], [:y, [1,2,3]]]}.must_equal '((x IS NOT NULL) OR (y NOT IN (1, 2, 3)))'
  end
  
  it "should support sql_or on arrays with all two pairs" do
    @d.l{[[:x, 100],[:y, 'a']].sql_or}.must_equal '((x = 100) OR (y = \'a\'))'
    @d.l{[[:x, true], [:y, false]].sql_or}.must_equal '((x IS TRUE) OR (y IS FALSE))'
    @d.l{[[:x, nil], [:y, [1,2,3]]].sql_or}.must_equal '((x IS NULL) OR (y IN (1, 2, 3)))'
  end
  
  it "should support Array#sql_string_join for concatenation of SQL strings" do
    @d.l{[:x].sql_string_join}.must_equal '(x)'
    @d.l{[:x].sql_string_join(', ')}.must_equal '(x)'
    @d.l{[:x, :y].sql_string_join}.must_equal '(x || y)'
    @d.l{[:x, :y].sql_string_join(', ')}.must_equal "(x || ', ' || y)"
    @d.l{[:x.sql_function(1), :y.sql_subscript(1)].sql_string_join}.must_equal '(x(1) || y[1])'
    @d.l{[:x.sql_function(1), 'y.z'.lit].sql_string_join(', ')}.must_equal "(x(1) || ', ' || y.z)"
    @d.l{[:x, 1, :y].sql_string_join}.must_equal "(x || '1' || y)"
    @d.l{[:x, 1, :y].sql_string_join(', ')}.must_equal "(x || ', ' || '1' || ', ' || y)"
    @d.l{[:x, 1, :y].sql_string_join(Sequel[:y][:z])}.must_equal "(x || y.z || '1' || y.z || y)"
    @d.l{[:x, 1, :y].sql_string_join(1)}.must_equal "(x || '1' || '1' || '1' || y)"
    @d.l{[:x, :y].sql_string_join('y.x || x.y'.lit)}.must_equal "(x || y.x || x.y || y)"
    @d.l{[[:x, :y].sql_string_join, [:a, :b].sql_string_join].sql_string_join}.must_equal "(x || y || a || b)"
  end

  it "should support sql_expr on hashes" do
    @d.l{{:x => 100, :y => 'a'}.sql_expr}[1...-1].split(' AND ').sort.must_equal ['(x = 100)', '(y = \'a\')']
    @d.l{{:x => true, :y => false}.sql_expr}[1...-1].split(' AND ').sort.must_equal ['(x IS TRUE)', '(y IS FALSE)']
    @d.l{{:x => nil, :y => [1,2,3]}.sql_expr}[1...-1].split(' AND ').sort.must_equal ['(x IS NULL)', '(y IN (1, 2, 3))']
  end
  
  it "should support sql_negate on hashes" do
    @d.l{{:x => 100, :y => 'a'}.sql_negate}[1...-1].split(' AND ').sort.must_equal ['(x != 100)', '(y != \'a\')']
    @d.l{{:x => true, :y => false}.sql_negate}[1...-1].split(' AND ').sort.must_equal ['(x IS NOT TRUE)', '(y IS NOT FALSE)']
    @d.l{{:x => nil, :y => [1,2,3]}.sql_negate}[1...-1].split(' AND ').sort.must_equal ['(x IS NOT NULL)', '(y NOT IN (1, 2, 3))']
  end
  
  it "should support ~ on hashes" do
    @d.l{~{:x => 100, :y => 'a'}}[1...-1].split(' OR ').sort.must_equal ['(x != 100)', '(y != \'a\')']
    @d.l{~{:x => true, :y => false}}[1...-1].split(' OR ').sort.must_equal ['(x IS NOT TRUE)', '(y IS NOT FALSE)']
    @d.l{~{:x => nil, :y => [1,2,3]}}[1...-1].split(' OR ').sort.must_equal ['(x IS NOT NULL)', '(y NOT IN (1, 2, 3))']
  end
  
  it "should support sql_or on hashes" do
    @d.l{{:x => 100, :y => 'a'}.sql_or}[1...-1].split(' OR ').sort.must_equal ['(x = 100)', '(y = \'a\')']
    @d.l{{:x => true, :y => false}.sql_or}[1...-1].split(' OR ').sort.must_equal ['(x IS TRUE)', '(y IS FALSE)']
    @d.l{{:x => nil, :y => [1,2,3]}.sql_or}[1...-1].split(' OR ').sort.must_equal ['(x IS NULL)', '(y IN (1, 2, 3))']
  end
  
  it "should Hash#& and Hash#|" do
    @d.l{{:y => :z} & :x}.must_equal '((y = z) AND x)'
    @d.l{{:x => :a} & {:y => :z}}.must_equal '((x = a) AND (y = z))'
    @d.l{{:y => :z} | :x}.must_equal '((y = z) OR x)'
    @d.l{{:x => :a} | {:y => :z}}.must_equal '((x = a) OR (y = z))'
  end
end

describe "Array#case and Hash#case" do
  before do
    @d = Sequel.mock.dataset
  end

  def literal(&block)
    @d.literal(@d.where(&block).opts[:where])
  end
  
  it "should return SQL CASE expression" do
    literal{{:x=>:y}.case(:z)}.must_equal '(CASE WHEN x THEN y ELSE z END)'
    literal{{:x=>:y}.case(:z, :exp)}.must_equal '(CASE exp WHEN x THEN y ELSE z END)'
    ['(CASE WHEN x THEN y WHEN a THEN b ELSE z END)',
     '(CASE WHEN a THEN b WHEN x THEN y ELSE z END)'].must_include(literal{{:x=>:y, :a=>:b}.case(:z)})
    literal{[[:x, :y]].case(:z)}.must_equal '(CASE WHEN x THEN y ELSE z END)'
    literal{[[:x, :y], [:a, :b]].case(:z)}.must_equal '(CASE WHEN x THEN y WHEN a THEN b ELSE z END)'
    literal{[[:x, :y], [:a, :b]].case(:z, :exp)}.must_equal '(CASE exp WHEN x THEN y WHEN a THEN b ELSE z END)'
    literal{[[:x, :y], [:a, :b]].case(:z, Sequel[:exp][:w])}.must_equal '(CASE exp.w WHEN x THEN y WHEN a THEN b ELSE z END)'
  end

  it "should return SQL CASE expression with expression even if nil" do
    literal{{:x=>:y}.case(:z, nil)}.must_equal '(CASE NULL WHEN x THEN y ELSE z END)'
  end
end

describe "Array#sql_value_list and #sql_array" do
  before do
    @d = Sequel.mock.dataset
  end

  it "should treat the array as an SQL value list instead of conditions when used as a placeholder value" do
    @d.filter{Sequel.lit("(a, b) IN ?", [[:x, 1], [:y, 2]])}.sql.must_equal 'SELECT * WHERE ((a, b) IN ((x = 1) AND (y = 2)))'
    @d.filter{Sequel.lit("(a, b) IN ?", [[:x, 1], [:y, 2]].sql_value_list)}.sql.must_equal 'SELECT * WHERE ((a, b) IN ((x, 1), (y, 2)))'
  end

  it "should be no difference when used as a hash value" do
    @d.filter{{[:a, :b]=>[[:x, 1], [:y, 2]]}}.sql.must_equal 'SELECT * WHERE ((a, b) IN ((x, 1), (y, 2)))'
    @d.filter{{[:a, :b]=>[[:x, 1], [:y, 2]].sql_value_list}}.sql.must_equal 'SELECT * WHERE ((a, b) IN ((x, 1), (y, 2)))'
  end
end

describe "#desc" do
  before do
    @ds = Sequel.mock.dataset
  end
  
  def literal(&block)
    @ds.literal(@ds.where(&block).opts[:where])
  end
  
  it "should format a DESC clause for a column ref" do
    literal{:test.desc}.must_equal 'test DESC'
  end
    
  with_symbol_splitting "should format a DESC clause for a column ref with splittable symbol" do
    literal{:items__price.desc}.must_equal 'items.price DESC'
  end

  it "should format a DESC clause for a function" do
    literal{:avg.sql_function(:test).desc}.must_equal 'avg(test) DESC'
  end
end

describe "#asc" do
  before do
    @ds = Sequel.mock.dataset
  end
  
  def literal(&block)
    @ds.literal(@ds.where(&block).opts[:where])
  end
  
  it "should format a ASC clause for a column ref" do
    literal{:test.asc}.must_equal 'test ASC'
  end
    
  with_symbol_splitting "should format a ASC clause for a column ref with splittable symbol" do
    literal{:items__price.asc}.must_equal 'items.price ASC'
  end

  it "should format a ASC clause for a function" do
    literal{:avg.sql_function(:test).asc}.must_equal 'avg(test) ASC'
  end
end

describe "#as" do
  before do
    @ds = Sequel.mock.dataset
  end
  
  def literal(&block)
    @ds.literal(@ds.where(&block).opts[:where])
  end
  
  it "should format a AS clause for a column ref" do
    literal{:test.as(:t)}.must_equal 'test AS t'
  end
    
  with_symbol_splitting "should format a AS clause for a column ref with splittable symbols" do
    literal{:items__price.as(:p)}.must_equal 'items.price AS p'
  end

  it "should format a AS clause for a function" do
    literal{:avg.sql_function(:test).as(:avg)}.must_equal 'avg(test) AS avg'
  end
  
  it "should format a AS clause for a literal value" do
    literal{'abc'.as(:abc)}.must_equal "'abc' AS abc"
  end
end

describe "Column references" do
  before do
    @ds = Sequel.mock.dataset.with_quote_identifiers(true).with_extend{def quoted_identifier_append(sql, c) sql << "`#{c}`" end}
  end
  
  def literal(&block)
    @ds.literal(@ds.where(&block).opts[:where])
  end
  
  it "should be quoted properly" do
    literal{:xyz}.must_equal "`xyz`"
    literal{:xyz.as(:x)}.must_equal "`xyz` AS `x`"
  end
  
  it "should be quoted properly in SQL functions" do
    literal{:avg.sql_function(:xyz)}.must_equal "avg(`xyz`)"
    literal{:avg.sql_function(:xyz, 1)}.must_equal "avg(`xyz`, 1)"
    literal{:avg.sql_function(:xyz).as(:a)}.must_equal "avg(`xyz`) AS `a`"
  end

  it "should be quoted properly in ASC/DESC clauses" do
    literal{:xyz.asc}.must_equal "`xyz` ASC"
    literal{:avg.sql_function(:xyz, 1).desc}.must_equal "avg(`xyz`, 1) DESC"
  end
  
  it "should be quoted properly in a cast function" do
    literal{:x.cast(:integer)}.must_equal "CAST(`x` AS integer)"
  end

  with_symbol_splitting "should be quoted properly when using splittable symbols" do
    literal{:xyz__abc}.must_equal "`xyz`.`abc`"
    literal{:xyz__abc.as(:x)}.must_equal "`xyz`.`abc` AS `x`"
    literal{:xyz___x}.must_equal "`xyz` AS `x`"
    literal{:xyz__abc___x}.must_equal "`xyz`.`abc` AS `x`"
    literal{:x__y.cast('varchar(20)')}.must_equal "CAST(`x`.`y` AS varchar(20))"
  end
end

describe "Symbol#*" do
  before do
    @ds = Sequel.mock.dataset
  end
  
  def literal(&block)
    @ds.literal(@ds.select(&block).opts[:select][0])
  end
  
  it "should format a qualified wildcard if no argument" do
    literal{:xyz.*}.must_equal 'xyz.*'
    literal{:abc.*}.must_equal 'abc.*'
  end

  it "should format a filter expression if an argument" do
    literal{:xyz.*(3)}.must_equal '(xyz * 3)'
    literal{:abc.*(5)}.must_equal '(abc * 5)'
  end

  with_symbol_splitting "should support qualified symbols if no argument" do
    literal{:xyz__abc.*}.must_equal 'xyz.abc.*'
  end
end

describe "Symbol" do
  before do
    @ds = Sequel.mock.dataset.with_quote_identifiers(true)
  end

  def literal(&block)
    @ds.literal(@ds.where(&block).opts[:where])
  end
  
  it "#identifier should format an identifier" do
    literal{:xyz__abc.identifier}.must_equal '"xyz__abc"'
  end

  it "#qualify should format a qualified column" do
    literal{:xyz.qualify(:abc)}.must_equal '"abc"."xyz"'
  end

  it "#qualify should work on QualifiedIdentifiers" do
    literal{:xyz.qualify(:abc).qualify(:def)}.must_equal '"def"."abc"."xyz"'
  end

  it "should be able to qualify an identifier" do
    literal{:xyz.identifier.qualify(Sequel[:xyz][:abc])}.must_equal '"xyz"."abc"."xyz"'
  end

  with_symbol_splitting "should be able to qualify an identifier with qualified symbol" do
    literal{:xyz.identifier.qualify(:xyz__abc)}.must_equal '"xyz"."abc"."xyz"'
  end

  it "should be able to specify a schema.table.column" do
    literal{:column.qualify(:table.qualify(:schema))}.must_equal '"schema"."table"."column"'
    literal{:column.qualify(:table__name.identifier.qualify(:schema))}.must_equal '"schema"."table__name"."column"'
  end

  it "should work correctly with objects" do
    o = Object.new
    def o.sql_literal(ds) "(foo)" end
    literal{:column.qualify(o)}.must_equal '(foo)."column"'
  end
end

describe "Symbol" do
  before do
    @ds = Sequel.mock.dataset
  end

  def literal(&block)
    @ds.literal(@ds.select(&block).opts[:select][0])
  end
  
  it "should support sql_function method" do
    literal{:COUNT.sql_function('1')}.must_equal "COUNT('1')"
    @ds.select{:COUNT.sql_function('1')}.sql.must_equal "SELECT COUNT('1')"
  end
  
  it "should support cast method" do
    literal{:abc.cast(:integer)}.must_equal "CAST(abc AS integer)"
  end

  with_symbol_splitting "should support sql array accesses via sql_subscript for splittable symbols" do
    literal{:abc__def.sql_subscript(1)}.must_equal "abc.def[1]"
  end

  it "should support sql array accesses via sql_subscript" do
    literal{:abc.sql_subscript(1)}.must_equal "abc[1]"
    literal{:abc.sql_subscript(1)|2}.must_equal "abc[1, 2]"
    literal{:abc.sql_subscript(1)[2]}.must_equal "abc[1][2]"
  end

  it "should support cast_numeric and cast_string" do
    literal{:abc.cast_numeric}.must_equal "CAST(abc AS integer)"
    literal{:abc.cast_numeric(:real)}.must_equal "CAST(abc AS real)"
    literal{:abc.cast_string}.must_equal "CAST(abc AS varchar(255))"
    literal{:abc.cast_string(:varchar)}.must_equal "CAST(abc AS varchar(255))"
  end
  
  it "should allow database independent types when casting" do
    db = @ds.db
    def db.cast_type_literal(type)
      return :foo if type == Integer
      return :bar if type == String
      type
    end
    literal{:abc.cast(String)}.must_equal "CAST(abc AS bar)"
    literal{:abc.cast(String)}.must_equal "CAST(abc AS bar)"
    literal{:abc.cast_string}.must_equal "CAST(abc AS bar)"
    literal{:abc.cast_string(Integer)}.must_equal "CAST(abc AS foo)"
    literal{:abc.cast_numeric}.must_equal "CAST(abc AS foo)"
    literal{:abc.cast_numeric(String)}.must_equal "CAST(abc AS bar)"
  end

  it "should support SQL EXTRACT function via #extract " do
    literal{:abc.extract(:year)}.must_equal "extract(year FROM abc)"
  end
end

describe "is_distinct_from extension integration" do
  it "Symbol#is_distinct_from should return an IsDistinctFrom" do
    db = Sequel.connect("mock://postgres")
    db.extension :is_distinct_from
    db.literal(db.dataset.where{:a.is_distinct_from(:b)}.opts[:where]).must_equal '("a" IS DISTINCT FROM "b")'
  end
end

describe "Postgres/SQLite extensions integration" do
  before do
    @db = Sequel.mock
  end

  def literal(&block)
    @db.literal(@db.dataset.select(&block).opts[:select][0])
  end

  it "Symbol#pg_array should return an ArrayOp" do
    literal{:a.pg_array.unnest}.must_equal "unnest(a)"
  end

  it "Symbol#pg_row should return a PGRowOp" do
    literal{:a.pg_row[:a]}.must_equal "(a).a"
  end

  it "Symbol#hstore should return an HStoreOp" do
    literal{:a.hstore['a']}.must_equal "(a -> 'a')"
  end

  it "Symbol#pg_json should return an JSONOp" do
    literal{:a.pg_json[%w'a b']}.must_equal "(a #> ARRAY['a','b'])"
    literal{:a.pg_json.extract('a')}.must_equal "json_extract_path(a, 'a')"
  end

  it "Symbol#pg_jsonb should return an JSONBOp" do
    literal{:a.pg_jsonb[%w'a b']}.must_equal "(a #> ARRAY['a','b'])"
    literal{:a.pg_jsonb.extract('a')}.must_equal "jsonb_extract_path(a, 'a')"
  end

  it "Symbol#pg_range should return a RangeOp" do
    literal{:a.pg_range.lower}.must_equal "lower(a)"
  end

  it "Array#pg_array should return a PGArray" do
    literal{[1].pg_array.op.unnest}.must_equal "unnest(ARRAY[1])"
    literal{[1].pg_array(:int4).op.unnest}.must_equal "unnest(ARRAY[1]::int4[])"
  end

  it "Array#pg_json should return a JSONArray" do
    literal{[1].pg_json}.must_equal "'[1]'::json"
  end

  it "Array#pg_jsonb should return a JSONBArray" do
    literal{[1].pg_jsonb}.must_equal "'[1]'::jsonb"
  end

  it "Array#pg_row should return a ArrayRow" do
    literal{[1].pg_row}.must_equal "ROW(1)"
  end

  it "Hash#hstore should return an HStore" do
    literal{{'a'=>1}.hstore.op['a']}.must_equal '(\'"a"=>"1"\'::hstore -> \'a\')'
  end

  it "Hash#pg_json should return an JSONHash" do
    literal{{'a'=>'b'}.pg_json}.must_equal "'{\"a\":\"b\"}'::json"
  end

  it "Hash#pg_jsonb should return an JSONBHash" do
    literal{{'a'=>'b'}.pg_jsonb}.must_equal "'{\"a\":\"b\"}'::jsonb"
  end

  it "Range#pg_range should return an PGRange" do
    literal{(1..2).pg_range}.must_equal "'[1,2]'"
    literal{(1..2).pg_range(:int4range)}.must_equal "int4range(1,2,'[]')"
  end

  it "Symbol#sqlite_json_opt should return an SQLite::JSONOp" do
    literal{:a.sqlite_json_op[1]}.must_equal "(a ->> 1)"
    literal{:a.sqlite_json_op.minify}.must_equal "json(a)"
  end
end
end
