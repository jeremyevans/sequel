require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "pg_auto_parameterize extension" do
  before do
    @db = Sequel.connect('mock://postgres', :quote_identifiers=>false)
    @db.synchronize{|c| def c.escape_bytea(v) v*2 end}
    @db.extend_datasets{def use_cursor(*) clone end}
    @db.extend Sequel::Postgres::AutoParameterize::DatabaseMethods
  end

  it "should automatically parameterize queries strings, blobs, numerics, dates, and times" do
    pr = proc do |ds, sql, *args|
      arg = args[0]
      parg = args[1] || arg
      s = ds.filter(:a=>arg).sql
      s.should == sql
      s.args.should == (parg == :nil ? nil : [parg])
    end
    pr.call(@db[:table], 'SELECT * FROM table WHERE (a = $1::int4)', 1)
    pr.call(@db[:table], 'SELECT * FROM table WHERE (a = $1::int8)', 18446744073709551616)
    pr.call(@db[:table], 'SELECT * FROM table WHERE (a = $1::double precision)', 1.1)
    pr.call(@db[:table], 'SELECT * FROM table WHERE (a = $1::numeric)', BigDecimal.new('1.01'))
    pr.call(@db[:table], 'SELECT * FROM table WHERE (a = $1::text)', "a")
    pr.call(@db[:table], 'SELECT * FROM table WHERE (a = $1::bytea)', "a\0b".to_sequel_blob)
    pr.call(@db[:table], 'SELECT * FROM table WHERE (a = 1)', '1'.lit, :nil)
    pr.call(@db[:table], 'SELECT * FROM table WHERE (a = $1::time)', Sequel::SQLTime.create(1, 2, 3, 500000))
    pr.call(@db[:table], 'SELECT * FROM table WHERE (a = $1::date)', Date.today)
    pr.call(@db[:table], 'SELECT * FROM table WHERE (a = $1::timestamp)', DateTime.new(2012, 1, 2, 3, 4, 5))
    pr.call(@db[:table], 'SELECT * FROM table WHERE (a = $1::timestamp)', Time.utc(2012, 1, 2, 3, 4, 5))
  end

  it "should extract parameters from query string when executing" do
    @db.fetch = {:a=>1}
    @db.numrows = 1
    @db.autoid = 1

    @db[:table].filter(:a=>1).all.should == [{:a=>1}]
    @db.sqls.should == ['SELECT * FROM table WHERE (a = $1::int4) -- args: [1]']

    @db[:table].filter(:a=>1).update(:b=>'a').should == 1
    @db.sqls.should == ['UPDATE table SET b = $1::text WHERE (a = $2::int4) -- args: ["a", 1]']

    @db[:table].filter(:a=>1).delete.should == 1
    @db.sqls.should == ['DELETE FROM table WHERE (a = $1::int4) -- args: [1]']

    @db[:table].insert(:a=>1).should == 1
    @db.sqls.should == ['INSERT INTO table (a) VALUES ($1::int4) RETURNING id -- args: [1]']
  end

  it "should not automatically paramiterize if no_auto_parameterize is used" do
    @db[:table].no_auto_parameterize.filter(:a=>1).sql.should == 'SELECT * FROM table WHERE (a = 1)'
  end

  it "should not automatically parameterize prepared statements" do
    @db[:table].filter(:a=>1, :b=>:$b).prepare(:select).sql.should =~ /SELECT \* FROM table WHERE \(\((a = 1|b = \$b)\) AND \((a = 1|b = \$b)\)\)/
  end

  it "should show args with string when inspecting SQL " do
    @db[:table].filter(:a=>1).sql.inspect.should == '"SELECT * FROM table WHERE (a = $1::int4); [1]"'
  end

  it "should not auto parameterize when using cursors" do
    @db[:table].filter(:a=>1).use_cursor.opts[:no_auto_parameterize].should be_true
  end
end
