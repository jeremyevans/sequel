require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

Sequel::Postgres::StatementCache::PGError = Sequel::Error

describe "pg_statement_cache and pg_auto_parameterize extensions" do
  before do
    @dbp = proc do |opts|
      @db = Sequel.connect 'mock://postgres', :quote_identifiers=>false, :statement_cache_opts=>{:max_size=>4}.merge(opts),
        :after_connect=>(proc do |c|
          c.extend(Module.new do
            def execute_query(sql, args)
              raise Sequel::Postgres::StatementCache::PGError if @db.exec_raise
              @db.execute(sql, :arguments=>args, :no_eq=>true)
            end
            def prepare(name, sql)
              raise Sequel::Postgres::StatementCache::PGError if sql =~ /prepare_raise/
              @ps ||= {}
              @ps[name] = sql
              @db._sqls << "PREPARE #{name} AS #{sql}"
            end
            def exec_prepared(name, args=nil)
              @db._sqls << "EXECUTE #{name} (#{@ps[name]})#{" -- args: #{args.inspect}" if args}"
            end
          end)
        end)
      @db.extension :pg_auto_parameterize, :pg_statement_cache
      @db.extend(Module.new do
        attr_accessor :exec_raise
        def _execute(c, sql, opts={})
          opts[:no_eq] ? super : c.send(:execute_query, sql, opts[:arguments])
        end
        def _sqls
          @sqls
        end
        def statement_cache
          synchronize{|c| c.statement_cache}
        end
      end)
      @db
    end
    @db = @dbp.call({})
  end

  it "should automatically prepare statements executed multiple times" do
    @db[:table].filter(:b=>2).all
    3.times{|i| @db[:table].filter(:a=>i).all}
    @db.sqls.should == ["SELECT * FROM table WHERE (b = $1::int4) -- args: [2]",
      "SELECT * FROM table WHERE (a = $1::int4) -- args: [0]",
      "PREPARE sequel_pgap_2 AS SELECT * FROM table WHERE (a = $1::int4)",
      "EXECUTE sequel_pgap_2 (SELECT * FROM table WHERE (a = $1::int4)) -- args: [1]",
      "EXECUTE sequel_pgap_2 (SELECT * FROM table WHERE (a = $1::int4)) -- args: [2]"]
  end

  it "should work correctly for queries without parameters" do
    @db[:table].filter(:b).all
    3.times{|i| @db[:table].filter(:a).all}
    @db.sqls.should == ["SELECT * FROM table WHERE b",
      "SELECT * FROM table WHERE a",
      "PREPARE sequel_pgap_2 AS SELECT * FROM table WHERE a",
      "EXECUTE sequel_pgap_2 (SELECT * FROM table WHERE a)",
      "EXECUTE sequel_pgap_2 (SELECT * FROM table WHERE a)"]
  end

  it "should correctly return the size of the cache" do
    sc = @db.statement_cache
    sc.size.should == 0
    @db[:table].filter(:b=>2).all
    sc.size.should == 1
    3.times{|i| @db[:table].filter(:a=>i).all}
    sc.size.should == 2
  end

  it "should correctly clear the cache" do
    sc = @db.statement_cache
    sc.size.should == 0
    @db[:table].filter(:b=>2).all
    sc.size.should == 1
    3.times{|i| @db[:table].filter(:a=>i).all}
    sc.size.should == 2
    sc.clear
    sc.size.should == 0
    3.times{|i| @db[:table].filter(:a=>i).all}
    sc.size.should == 1
  end

  it "should correctly yield each entry in the cache" do
    @db[:table].filter(:b=>2).all
    3.times{|i| @db[:table].filter(:a=>i).all}
    a = []
    @db.statement_cache.each{|k, v| a << [k, v]}
    a.sort!
    a[0][0].should == "SELECT * FROM table WHERE (a = $1::int4)"
    a[1][0].should == "SELECT * FROM table WHERE (b = $1::int4)"
    s1 = a[1][1]
    s1.cache_id.should == 1
    s1.num_executes.should == 1
    s1 = a[0][1]
    s1.cache_id.should == 2
    s1.num_executes.should == 3
  end

  it "should automatically cleanup the cache when it goes beyond its maximum size" do
    sc = @db.statement_cache
    4.times{|i| @db[:table].filter(:"a#{i}"=>1).all}
    sc.size.should == 4
    @db[:table].filter(:b=>1).all
    sc.size.should == 2
  end

  it "should clear statement caches when altering tables" do
    @db[:table].filter(:b=>2).all
    sc = @db.statement_cache
    @db.alter_table(:foo){drop_column :bar}
    sc.size.should == 0
  end

  it "should clear statement caches when dropping tables" do
    @db[:table].filter(:b=>2).all
    sc = @db.statement_cache
    @db.drop_table(:foo)
    sc.size.should == 0
  end

  it "should deallocate prepared statements when clearing the cache" do
    3.times{|i| @db[:table].filter(:a=>i).all}
    @db.sqls
    @db.statement_cache.clear
    @db.sqls.should == ["DEALLOCATE sequel_pgap_1"]
  end

  it "should deallocate prepared statements when cleaning up the cache" do
    @db = @dbp.call(:sorter=>proc{|t, s| -s.num_executes})
    4.times{|i| @db[:table].filter(:"a#{i}"=>1).all}
    @db[:table].filter(:a0=>1).all
    @db.sqls
    @db[:table].filter(:b=>1).all
    @db.sqls.should == ["DEALLOCATE sequel_pgap_1", "SELECT * FROM table WHERE (b = $1::int4) -- args: [1]"]
  end

  it "should not deallocate nonprepared statements when clearing the cache" do
    4.times{|i| @db[:table].filter(:"a#{i}"=>1).all}
    @db.sqls
    @db.statement_cache.clear
    @db.sqls.should == []
  end

  it "should not deallocate nonprepared statements when cleaning up the cache" do
    @db = @dbp.call(:sorter=>proc{|t, s| -s.num_executes})
    4.times{|i| @db[:table].filter(:"a#{i}"=>1).all}
    @db.sqls
    @db[:table].filter(:b=>1).all
    @db.sqls.should == ["SELECT * FROM table WHERE (b = $1::int4) -- args: [1]"]
  end

  it "should have a configurable max_size and min_size" do
    @db = @dbp.call(:max_size=>10, :min_size=>2)
    10.times{|i| @db[:table].filter(:"a#{i}"=>1).all}
    sc = @db.statement_cache
    sc.size.should == 10
    @db[:table].filter(:b=>1).all
    sc.size.should == 2
  end

  it "should have a configurable prepare_after" do
    @db = @dbp.call(:prepare_after=>3)
    4.times{|i| @db[:table].filter(:a=>i).all}
    @db.sqls.should == ["SELECT * FROM table WHERE (a = $1::int4) -- args: [0]",
      "SELECT * FROM table WHERE (a = $1::int4) -- args: [1]",
      "PREPARE sequel_pgap_1 AS SELECT * FROM table WHERE (a = $1::int4)",
      "EXECUTE sequel_pgap_1 (SELECT * FROM table WHERE (a = $1::int4)) -- args: [2]",
      "EXECUTE sequel_pgap_1 (SELECT * FROM table WHERE (a = $1::int4)) -- args: [3]"]
  end

  it "should have a configurable sorter" do
    @db = @dbp.call(:sorter=>proc{|t, s| s.num_executes})
    4.times{|i| (i+1).times{@db[:table].filter(:"a#{i}"=>1).all}}
    @db[:table].filter(:b=>1).all
    sc = @db.statement_cache
    a = []
    sc.each{|k, v| a << [k, v]}
    a.sort!
    a[0][0].should == "SELECT * FROM table WHERE (a3 = $1::int4)"
    a[1][0].should == "SELECT * FROM table WHERE (b = $1::int4)"
    s1 = a[1][1]
    s1.num_executes.should == 1
    s1 = a[0][1]
    s1.cache_id.should == 4
    s1.num_executes.should == 4
  end

  it "should ignore errors when preparing queries" do
    3.times{|i| @db[:table].filter(:prepare_raise=>1).all}
    @db.sqls.should == ["SELECT * FROM table WHERE (prepare_raise = $1::int4) -- args: [1]",
      "SELECT * FROM table WHERE (prepare_raise = $1::int4) -- args: [1]",
      "SELECT * FROM table WHERE (prepare_raise = $1::int4) -- args: [1]"]
  end

  it "should ignore errors when deallocating queries" do
    3.times{|i| @db[:table].filter(:a=>1).all}
    @db.exec_raise = true
    @db.statement_cache.clear
    @db.sqls.should == ["SELECT * FROM table WHERE (a = $1::int4) -- args: [1]",
      "PREPARE sequel_pgap_1 AS SELECT * FROM table WHERE (a = $1::int4)",
      "EXECUTE sequel_pgap_1 (SELECT * FROM table WHERE (a = $1::int4)) -- args: [1]",
      "EXECUTE sequel_pgap_1 (SELECT * FROM table WHERE (a = $1::int4)) -- args: [1]"]
  end

end
