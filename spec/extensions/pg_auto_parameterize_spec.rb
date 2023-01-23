require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "pg_auto_parameterize extension" do
  before do
    @db = Sequel.connect('mock://postgres')
    @db.synchronize{|c| def c.escape_bytea(v) v*2 end}
    @db.extend_datasets{def use_cursor(*) self end}
    @db.extend(Module.new do
      def copy_table(*a) run(copy_table_sql(*a)) end
      private
      def copy_table_sql(ds, *) "COPY TABLE #{ds.is_a?(Sequel::Dataset) ? ds.sql : ds}" end
    end)
    @db.extension :pg_auto_parameterize
  end

  it "should parameterize select, insert, update, delete, and merge statements" do
    @db.fetch = {:a=>1}
    @db.numrows = 1
    @db.autoid = 1

    @db[:table].all.must_equal [{:a=>1}]
    @db.sqls.must_equal ['SELECT * FROM "table"']

    @db[:table].filter(:a=>1).all.must_equal [{:a=>1}]
    @db.sqls.must_equal ['SELECT * FROM "table" WHERE ("a" = $1::int4) -- args: [1]']

    @db[:table].filter(:a=>1).update(:b=>'a').must_equal 1
    @db.sqls.must_equal ['UPDATE "table" SET "b" = $1 WHERE ("a" = $2::int4) -- args: ["a", 1]']

    @db[:table].filter(:a=>1).delete.must_equal 1
    @db.sqls.must_equal ['DELETE FROM "table" WHERE ("a" = $1::int4) -- args: [1]']

    @db[:table].insert(:a=>1).must_equal 1
    @db.sqls.must_equal ['INSERT INTO "table" ("a") VALUES ($1::int4) RETURNING "id" -- args: [1]']

    @db[:table].
      merge_using(:m2, :i1=>:i2).
      merge_do_nothing_when_not_matched{b > 50}.
      merge_insert(:i1=>Sequel[:i2], :a=>Sequel[:b]+11).
      merge_do_nothing_when_matched{a > 50}.
      merge_delete{a > 30}.
      merge_update(:i1=>Sequel[:i1]+:i2+10, :a=>Sequel[:a]+:b+20).
      merge
    sqls = @db.sqls
    sqls.must_equal ['MERGE INTO "table" USING "m2" ON ("i1" = "i2") WHEN NOT MATCHED AND ("b" > $1::int4) THEN DO NOTHING WHEN NOT MATCHED THEN INSERT  ("i1", "a") VALUES ("i2", ("b" + $2::int4)) WHEN MATCHED AND ("a" > $1::int4) THEN DO NOTHING WHEN MATCHED AND ("a" > $3::int4) THEN DELETE WHEN MATCHED THEN UPDATE SET "i1" = ("i1" + "i2" + $4::int4), "a" = ("a" + "b" + $5::int4)']
    sqls[0].args.must_equal [50, 11, 30, 10, 20]
  end

  it "should parameterize insert of multiple rows" do
    args = (1...40).to_a
    @db[:table].import([:a], args)
    sqls = @db.sqls
    sqls.size.must_equal 1
    sqls[0].must_equal 'INSERT INTO "table" ("a") VALUES ' + args.map{|i| "($#{i}::int4)"}.join(', ') + " -- args: #{args.inspect}"
  end

  it "should default to splitting inserts of multiple rows to 40 at a time" do
    args = (1...81).to_a
    @db[:table].import([:a], args)
    sqls = @db.sqls
    sqls.size.must_equal 2
    sqls[0].must_equal 'INSERT INTO "table" ("a") VALUES ' + args[0...40].map{|i| "($#{i}::int4)"}.join(', ') + " -- args: #{args[0...40].inspect}"
    sqls[1].must_equal 'INSERT INTO "table" ("a") VALUES ' + args[0...40].map{|i| "($#{i}::int4)"}.join(', ') + " -- args: #{args[40...80].inspect}"
  end

  it "should automatically parameterize queries strings, blobs, numerics, dates, and times" do
    ds = @db[:table]
    pr = proc do |sql, *args|
      arg = args[0]
      parg = args[1] || arg
      s = ds.filter(:a=>arg).sql
      s.must_equal sql
      if parg == :nil
        s.args.must_be_nil
      else
        s.args.must_equal [parg]
      end
    end
    pr.call('SELECT * FROM "table" WHERE ("a" = $1::int4)', 1)
    pr.call('SELECT * FROM "table" WHERE ("a" = $1::int8)', 18446744073709551616)
    pr.call('SELECT * FROM "table" WHERE ("a" = $1::numeric)', 1.1)
    pr.call('SELECT * FROM "table" WHERE ("a" = $1::double precision)', (1.0/0.0))
    pr.call('SELECT * FROM "table" WHERE ("a" = $1::numeric)', BigDecimal('1.01'))
    pr.call('SELECT * FROM "table" WHERE ("a" = $1)', "a")
    pr.call('SELECT * FROM "table" WHERE ("a" = $1::bytea)', Sequel.blob("a\0b"))
    pr.call('SELECT * FROM "table" WHERE ("a" = $1::time)', Sequel::SQLTime.create(1, 2, 3, 500000))
    pr.call('SELECT * FROM "table" WHERE ("a" = $1::date)', Date.today)
    pr.call('SELECT * FROM "table" WHERE ("a" = $1::timestamp)', DateTime.new(2012, 1, 2, 3, 4, 5))
    pr.call('SELECT * FROM "table" WHERE ("a" = $1::timestamp)', Time.utc(2012, 1, 2, 3, 4, 5))
    pr.call('SELECT * FROM "table" WHERE ("a" = 1)', Sequel.lit('1'), :nil)
    pr.call('SELECT * FROM "table" WHERE ("a" = "b")', :b, :nil)
  end

  it "should automatically parameterize and not typecast Sequel::SQL::Cast values" do
    ds = @db[:table]
    pr = proc do |*args|
      arg = args[0]
      parg = args[1] || arg
      s = ds.filter(:a=>Sequel.cast(arg, :foo)).sql
      s.must_equal 'SELECT * FROM "table" WHERE ("a" = CAST($1 AS foo))'
      if parg == :nil
        s.args.must_be_nil
      else
        s.args.must_equal [parg]
      end
    end
    pr.call(1)
    pr.call(18446744073709551616)
    pr.call(1.1)
    pr.call(BigDecimal('1.01'))
    pr.call("a")
    pr.call(Sequel.blob("a\0b"))
    pr.call(Sequel::SQLTime.create(1, 2, 3, 500000))
    pr.call(Date.today)
    pr.call(DateTime.new(2012, 1, 2, 3, 4, 5))
    pr.call(Time.utc(2012, 1, 2, 3, 4, 5))

    sql = ds.where(:a=>Sequel.cast(Sequel.lit('1'), :foo)).sql
    sql.must_equal 'SELECT * FROM "table" WHERE ("a" = CAST(1 AS foo))'
    sql.args.must_be_nil
  end

  it "should parameterize model pk lookup and delete queries" do
    m = Sequel::Model(@db[:table].with_fetch(:id=>1).with_numrows(1))
    @db.sqls

    m[1].must_equal m.load(:id=>1)
    @db.sqls.must_equal ['SELECT * FROM "table" WHERE ("id" = $1::int4) LIMIT 1 -- args: [1]']

    o = m.load(:id=>1)
    o.delete.must_be_same_as o
    @db.sqls.must_equal ['DELETE FROM "table" WHERE ("id" = $1::int4) -- args: [1]']
  end

  it "should use same parameters when using select_group" do
    sql = @db[:a].select_group{foo(1).as(:f)}.sql
    sql.must_equal 'SELECT foo($1::int4) AS "f" FROM "a" GROUP BY foo($1::int4)'
    sql.args.must_equal [1]
  end

  it "should use same parameters when same objects" do
    expr = Sequel.function(:foo, 1)
    sql = @db[:a].select(expr.as(:f)).group(expr).sql
    sql.must_equal 'SELECT foo($1::int4) AS "f" FROM "a" GROUP BY foo($1::int4)'
    sql.args.must_equal [1]

    expr = Sequel.function(:foo, 'a')
    sql = @db[:a].select(expr.as(:f)).group(expr).sql
    sql.must_equal 'SELECT foo($1) AS "f" FROM "a" GROUP BY foo($1)'
    sql.args.must_equal ['a']
  end

  it "should use different parameters for different but equal objects" do
    sql = @db[:a].select{foo("a").as("f")}.group{foo("a")}.sql
    sql.must_equal 'SELECT foo($1) AS "f" FROM "a" GROUP BY foo($2)'
    sql.args.must_equal ['a', 'a']
  end

  it "should parameterize ORDER BY if possible" do
    sql = @db[:a].order{foo(1)}.sql
    sql.must_equal 'SELECT * FROM "a" ORDER BY foo($1::int4)'
    sql.args.must_equal [1]
  end

  it "should not parameterize ORDER BY if it contains integers or ordered integers" do
    sql = @db[:a].order(1).sql
    sql.must_equal 'SELECT * FROM "a" ORDER BY 1'
    sql.args.must_be_nil

    sql = @db[:a].reverse(1).sql
    sql.must_equal 'SELECT * FROM "a" ORDER BY 1 DESC'
    sql.args.must_be_nil
  end

  it "should not parameterize LIMIT or OFFSET" do
    sql = @db[:a].limit(1).sql
    sql.must_equal 'SELECT * FROM "a" LIMIT 1'
    sql.args.must_be_nil

    sql = @db[:a].offset(1).sql
    sql.must_equal 'SELECT * FROM "a" OFFSET 1'
    sql.args.must_be_nil

    sql = @db[:a].limit(1, 1).sql
    sql.must_equal 'SELECT * FROM "a" LIMIT 1 OFFSET 1'
    sql.args.must_be_nil
  end

  it "should not parameterize in CTE CYCLE clauses" do
    ds = @db[:x]
    sql = @db[:t].with_recursive(:t, ds.filter(:id=>1), ds.join(:t, :id=>:parent_id).select_all(:i1),
      :cycle=>{:columns=>[:id, :parent_id], :path_column=>:pc, :cycle_column=>:cc, :cycle_value=>1, :noncycle_value=>0}).sql
    sql.must_equal 'WITH RECURSIVE "t" AS (SELECT * FROM "x" WHERE ("id" = $1::int4) UNION ALL (SELECT "i1".* FROM "x" INNER JOIN "t" ON ("t"."id" = "x"."parent_id"))) CYCLE "id", "parent_id" SET "cc" TO 1 DEFAULT 0 USING "pc" SELECT * FROM "t"'
    sql.args.must_equal [1]

    sql = @db[:t].with_recursive(:t, ds.filter(:parent_id=>nil), ds.join(:t, :id=>:parent_id).select_all(:i1), :search=>{:by=>:id}).sql
    sql.must_equal 'WITH RECURSIVE "t" AS (SELECT * FROM "x" WHERE ("parent_id" IS NULL) UNION ALL (SELECT "i1".* FROM "x" INNER JOIN "t" ON ("t"."id" = "x"."parent_id"))) SEARCH DEPTH FIRST BY "id" SET "ordercol" SELECT * FROM "t"'
    sql.args.must_be_nil
  end

  it "should parameterize datasets with static SQL using placeholders" do
    sql = @db.fetch("SELECT a FROM b WHERE c = ?", 2).sql
    sql.must_equal 'SELECT a FROM b WHERE c = $1::int4'
    sql.args.must_equal [2]
  end

  it "should parameterize datasets with static SQL using placeholders in subqueries" do
    sql = @db[:t].from(@db.fetch("SELECT a FROM b WHERE c = ?", 2)).sql
    sql.must_equal 'SELECT * FROM (SELECT a FROM b WHERE c = $1::int4) AS "t1"'
    sql.args.must_equal [2]
  end

  it "should automatically parameterize when using with_sql" do
    sql = @db[:table].filter(:a=>1, :b=>2).with_sql(:update_sql, :b=>3).sql
    sql.must_equal 'UPDATE "table" SET "b" = $1::int4 WHERE (("a" = $2::int4) AND ("b" = $3::int4))'
    sql.args.must_equal [3, 1, 2]
  end

  it "should automatically parameterize when using with_sql in subquery" do
    sql = @db.from(@db[:table].filter(:a=>1, :b=>2).with_sql(:delete_sql)).sql
    sql.must_equal 'SELECT * FROM (DELETE FROM "table" WHERE (("a" = $1::int4) AND ("b" = $2::int4))) AS "t1"'
    sql.args.must_equal [1, 2]
  end

  it "should parameterize datasets with static SQL using placeholders in a subquery" do
    sql = @db.from(@db.fetch("SELECT a FROM b WHERE c = ?", 2)).sql
    sql.must_equal 'SELECT * FROM (SELECT a FROM b WHERE c = $1::int4) AS "t1"'
    sql.args.must_equal [2]
  end

  it "should automatically switch column IN (int, ...) to column = ANY($) with parameter" do
    sql = @db[:table].where(:a=>[1,2,3]).sql
    sql.must_equal 'SELECT * FROM "table" WHERE ("a" = ANY(CAST($1 AS int8[])))'
    sql.args.must_equal ['{1,2,3}']

    sql = @db[:table].where(:a=>[1,nil,3]).sql
    sql.must_equal 'SELECT * FROM "table" WHERE ("a" = ANY(CAST($1 AS int8[])))'
    sql.args.must_equal ['{1,NULL,3}']
  end

  it "should automatically switch column NOT IN (int, ...) to column != ALL($) with parameter" do
    sql = @db[:table].exclude(:a=>[1,2,3]).sql
    sql.must_equal 'SELECT * FROM "table" WHERE ("a" != ALL(CAST($1 AS int8[])))'
    sql.args.must_equal ['{1,2,3}']

    sql = @db[:table].exclude(:a=>[1,nil,3]).sql
    sql.must_equal 'SELECT * FROM "table" WHERE ("a" != ALL(CAST($1 AS int8[])))'
    sql.args.must_equal ['{1,NULL,3}']
  end

  it "should not convert IN/NOT IN expressions that don't use integers" do
    sql = @db[:table].where([:a, :b]=>%w[1 2]).sql
    sql.must_equal 'SELECT * FROM "table" WHERE (("a", "b") IN ($1, $2))'
    sql.args.must_equal %w[1 2]

    sql = @db[:table].exclude([:a, :b]=>%w[1 2]).sql
    sql.must_equal 'SELECT * FROM "table" WHERE (("a", "b") NOT IN ($1, $2))'
    sql.args.must_equal %w[1 2]
  end

  it "should not convert multiple column IN expressions" do
    sql = @db[:table].where([:a, :b]=>[[1,2]]).sql
    sql.must_equal 'SELECT * FROM "table" WHERE (("a", "b") IN (($1::int4, $2::int4)))'
    sql.args.must_equal [1, 2]

    sql = @db[:table].exclude([:a, :b]=>[[1,2]]).sql
    sql.must_equal 'SELECT * FROM "table" WHERE (("a", "b") NOT IN (($1::int4, $2::int4)))'
    sql.args.must_equal [1, 2]
  end

  it "should not convert single value expressions" do
    sql = @db[:table].where(:a=>[1]).sql
    sql.must_equal 'SELECT * FROM "table" WHERE ("a" IN ($1::int4))'
    sql.args.must_equal [1]

    sql = @db[:table].where(:a=>[1]).sql
    sql.must_equal 'SELECT * FROM "table" WHERE ("a" IN ($1::int4))'
    sql.args.must_equal [1]
  end

  it "should automatically parameterize pg_array with types correctly" do
    @db.extension :pg_array
    v = Sequel.pg_array([1], :int4)
    sql = @db[:table].insert_sql(v)
    sql.must_equal 'INSERT INTO "table" VALUES ($1::int4[])'
    sql.args.must_equal [v]

    v = Sequel.pg_array([1, nil], :int4)
    sql = @db[:table].insert_sql(v)
    sql.must_equal 'INSERT INTO "table" VALUES ($1::int4[])'
    sql.args.must_equal [v]
  end

  it "should not automatically parameterize pg_array with internal expressions" do
    @db.extension :pg_array
    v = Sequel.pg_array([Sequel.function(:foo)], :int4)
    sql = @db[:table].insert_sql(v)
    sql.must_equal 'INSERT INTO "table" VALUES (ARRAY[foo()]::int4[])'
    sql.args.must_be_nil
  end

  it "should not automatically parameterize pg_array without type" do
    @db.extension :pg_array
    v = Sequel.pg_array([1])
    sql = @db[:table].insert_sql(v)
    sql.must_equal 'INSERT INTO "table" VALUES (ARRAY[$1::int4])'
    sql.args.must_equal [1]
  end

  it "should automatically parameterize pg_hstore values" do
    @db.fetch = {:oid=>9999, :typname=>'hstore'}
    @db.extension :pg_hstore
    v = Sequel.hstore('a'=>'b')
    sql = @db[:table].insert_sql(v)
    sql.must_equal 'INSERT INTO "table" VALUES ($1::hstore)'
    sql.args.must_equal [v]
  end

  it "should automatically parameterize pg_inet values" do
    @db.extension :pg_inet
    v = IPAddr.new('127.0.0.1')
    sql = @db[:table].insert_sql(v)
    sql.must_equal 'INSERT INTO "table" VALUES ($1::inet)'
    sql.args.must_equal [v]
  end

  it "should automatically parameterize pg_inet values when loading pg_interval extension after" do
    @db.extension :pg_inet
    begin
      @db.extension :pg_interval
    rescue LoadError
      skip("cannot load pg_interval extension")
    else
      v = IPAddr.new('127.0.0.1')
      sql = @db[:table].insert_sql(v)
      sql.must_equal 'INSERT INTO "table" VALUES ($1::inet)'
      sql.args.must_equal [v]
    end
  end

  it "should automatically parameterize pg_json values" do
    @db.extension :pg_json
    v = Sequel.pg_json({})
    sql = @db[:table].insert_sql(v)
    sql.must_equal 'INSERT INTO "table" VALUES ($1::json)'
    sql.args.must_equal [v]

    v = Sequel.pg_jsonb({})
    sql = @db[:table].insert_sql(v)
    sql.must_equal 'INSERT INTO "table" VALUES ($1::jsonb)'
    sql.args.must_equal [v]
  end

  it "should automatically parameterize pg_multirange values" do
    @db.extension :pg_multirange
    v = Sequel.pg_multirange([1..2, 5..6], :int4multirange)
    sql = @db[:table].insert_sql(v)
    sql.must_equal 'INSERT INTO "table" VALUES ($1::int4multirange)'
    sql.args.length.must_equal 1
    sql.args.must_equal [v]
  end

  it "should automatically parameterize pg_range values" do
    @db.extension :pg_range
    v = Sequel.pg_range(1..2, :int4range)
    sql = @db[:table].insert_sql(v)
    sql.must_equal 'INSERT INTO "table" VALUES ($1::int4range)'
    sql.args.must_equal [v]

    v = Sequel.pg_range(1..2)
    sql = @db[:table].insert_sql(v)
    sql.must_equal 'INSERT INTO "table" VALUES ($1)'
    sql.args.must_equal ['[1,2]']
  end

  it "should automatically parameterize pg_row values if parts are automatically parameterizable" do
    @db.extension :pg_row
    aclass = Sequel::Postgres::PGRow::ArrayRow.subclass(:arow)
    hclass = Sequel::Postgres::PGRow::HashRow.subclass(:hrow, [:a, :b])

    v = aclass.new([1, nil, 3])
    sql = @db[:table].insert_sql(v)
    sql.must_equal 'INSERT INTO "table" VALUES ($1::"arow")'
    sql.args.must_equal [v]

    v = hclass.new(:a=>1, :b=>nil)
    sql = @db[:table].insert_sql(v)
    sql.must_equal 'INSERT INTO "table" VALUES ($1::"hrow")'
    sql.args.must_equal [v]

    sql = @db[:table].insert_sql(aclass.new([1, Sequel.function(:foo), 3]))
    sql.must_equal 'INSERT INTO "table" VALUES (ROW($1::int4, foo(), $2::int4)::"arow")'
    sql.args.must_equal [1, 3]

    sql = @db[:table].insert_sql(hclass.new(:a=>1, :b=>Sequel.function(:foo)))
    sql.must_equal 'INSERT INTO "table" VALUES (ROW($1::int4, foo())::"hrow")'
    sql.args.must_equal [1]
  end

  it "should show args with string when inspecting SQL if there are args" do
    @db[:table].sql.inspect.must_equal '"SELECT * FROM \\"table\\""'
    @db[:table].filter(:a=>1).sql.inspect.must_equal '"SELECT * FROM \\"table\\" WHERE (\\"a\\" = $1::int4); [1]"'
  end

  it "should keep args when adding to the SQL string" do
    (@db[:table].sql + ' -- foo').inspect.must_equal '"SELECT * FROM \\"table\\" -- foo"'
    (@db[:table].filter(:a=>1).sql + ' -- foo').inspect.must_equal '"SELECT * FROM \\"table\\" WHERE (\\"a\\" = $1::int4) -- foo; [1]"'
  end

  it "should freeze args when freezing" do
    sql = @db[:table].sql
    sql.freeze.must_be_same_as sql
    sql.args.must_be_nil

    sql = @db[:table].filter(:a=>1).sql
    sql.freeze.must_be_same_as sql
    sql.args.frozen?.must_equal true
  end

  it "should support placeholder literalizers with existing arguments when not auto parametizing" do
    ds = @db[:table].having(:a=>5).no_auto_parameterize
    3.times do |i|
      ds.first(:b=>i)
    end
    @db.sqls.must_equal ["SELECT * FROM \"table\" WHERE (\"b\" = 0) HAVING (\"a\" = 5) LIMIT 1",
      "SELECT * FROM \"table\" WHERE (\"b\" = 1) HAVING (\"a\" = 5) LIMIT 1",
      "SELECT * FROM \"table\" WHERE (\"b\" = 2) HAVING (\"a\" = 5) LIMIT 1"]
  end

  it "should support placeholder literalizers with existing arguments" do
    ds = @db[:table].having(:a=>5)
    3.times do |i|
      ds.first(:b=>i)
    end
    @db.sqls.must_equal ["SELECT * FROM \"table\" WHERE (\"b\" = $1::int4) HAVING (\"a\" = $2::int4) LIMIT 1 -- args: [0, 5]",
      "SELECT * FROM \"table\" WHERE (\"b\" = $1::int4) HAVING (\"a\" = $2::int4) LIMIT 1 -- args: [1, 5]",
      "SELECT * FROM \"table\" WHERE (\"b\" = $2::int4) HAVING (\"a\" = $1::int4) LIMIT 1 -- args: [5, 2]"]
  end

  it "should support placeholder literalizers without arguments" do
    ds = @db[:table]
    3.times do |i|
      ds.first(:b=>i)
    end
    @db.sqls.must_equal ["SELECT * FROM \"table\" WHERE (\"b\" = $1::int4) LIMIT 1 -- args: [0]",
      "SELECT * FROM \"table\" WHERE (\"b\" = $1::int4) LIMIT 1 -- args: [1]",
      "SELECT * FROM \"table\" WHERE (\"b\" = $1::int4) LIMIT 1 -- args: [2]"]
  end

  it "should not automatically parameterize if no_auto_parameterize is used" do
    ds = @db[:table].no_auto_parameterize
    ds.filter(:a=>1).sql.must_equal 'SELECT * FROM "table" WHERE ("a" = 1)'
    ds.filter(:a=>1).delete_sql.must_equal 'DELETE FROM "table" WHERE ("a" = 1)'
    ds.filter(:a=>1).update_sql(:a=>2).must_equal 'UPDATE "table" SET "a" = 2 WHERE ("a" = 1)'
    ds.insert_sql(:a=>1).must_equal 'INSERT INTO "table" ("a") VALUES (1)'

    @db.sqls
    ds.import([:a], [1])
    sqls = @db.sqls
    sqls.size.must_equal 1
    sqls[0].must_equal 'INSERT INTO "table" ("a") VALUES (1)'
  end

  it "should have no_auto_parameterize return self if automatic parameterization is already disabled" do
    ds = @db[:table].no_auto_parameterize
    ds.no_auto_parameterize.must_be_same_as ds
  end

  it "should not auto parameterize objects wrapped with Sequel.skip_auto_param" do
    @db[:table].filter(:a=>Sequel.skip_pg_auto_param(1)).sql.must_equal 'SELECT * FROM "table" WHERE ("a" = 1)'
    @db[:table].no_auto_parameterize.filter(:a=>Sequel.skip_pg_auto_param(1)).sql.must_equal 'SELECT * FROM "table" WHERE ("a" = 1)'
  end

  it "should not automatically parameterize prepared statements" do
    @db[:table].filter(:a=>1, :b=>:$b).prepare(:select, :foo).sql.must_equal 'SELECT * FROM "table" WHERE (("a" = 1) AND ("b" = $b))'
  end

  it "should not parameterize datasets with static SQL not using placeholders" do
    @db.fetch("SELECT a FROM b WHERE c = 2").sql.must_equal 'SELECT a FROM b WHERE c = 2'
  end

  it "should not parameterize datasets with static SQL using placeholders in a subselect if no_auto_parameterize is used" do
    @db.from(@db.fetch("SELECT a FROM b WHERE c = ?", 2)).no_auto_parameterize.sql.must_equal 'SELECT * FROM (SELECT a FROM b WHERE c = 2) AS "t1"'
  end

  it "should not auto parameterize when using cursors" do
    @db[:table].filter(:a=>1).use_cursor.opts[:no_auto_parameterize].must_equal true
  end

  it "should not attempt to parameterize create_view" do
    @db.create_view :foo, @db[:table].filter(:a=>1)
    @db.sqls.must_equal ['CREATE VIEW "foo" AS SELECT * FROM "table" WHERE ("a" = 1)']
  end

  it "should not attempt to parameterize create_table(:as=>ds)" do
    @db.create_table(:foo, :as=>@db[:table].filter(:a=>1))
    @db.sqls.must_equal ['CREATE TABLE "foo" AS SELECT * FROM "table" WHERE ("a" = 1)']
  end

  it "should not attempt to parameterize copy table" do
    @db.copy_table(@db[:table].where(:a=>1))
    @db.sqls.must_equal ['COPY TABLE SELECT * FROM "table" WHERE ("a" = 1)']
    @db.copy_table(:table)
    @db.sqls.must_equal ['COPY TABLE table']
  end

  it "should raise when trying to load the extension into an unsupported database" do
    proc{Sequel.mock.extension :pg_auto_parameterize}.must_raise Sequel::Error
  end
end
