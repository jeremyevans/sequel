require_relative "spec_helper"

describe "Sequel Mock Adapter" do
  it "should have an adapter method" do
    db = Sequel.mock
    db.must_be_kind_of(Sequel::Mock::Database)
    db.adapter_scheme.must_equal :mock
  end

  it "should support registering mock adapter type" do
    begin
      Module.new do
        Sequel::Database.set_shared_adapter_scheme(:foo, self)
        
        def self.mock_adapter_setup(db)
          db.instance_variable_set(:@foo, :foo)
        end

        module self::DatabaseMethods
          def foo
            @foo
          end
        end

        module self::DatasetMethods
          def foo
            db.foo
          end
        end
      end

      Sequel.connect('mock://foo') do |db|
        db.foo.must_equal :foo
        db.dataset.foo.must_equal :foo
      end
    ensure
      Sequel.synchronize{Sequel::SHARED_ADAPTER_MAP.delete(:foo)}
    end
  end

  it "should have constructor accept no arguments" do
    Sequel.require 'adapters/mock'
    Sequel::Mock::Database.new.must_be_kind_of(Sequel::Mock::Database)
    Sequel.require 'mock', 'adapters'
    Sequel::Mock::Database.new.must_be_kind_of(Sequel::Mock::Database)
  end

  it "should each not return any rows by default" do
    called = false
    Sequel.mock[:t].each{|r| called = true}
    called.must_equal false
  end

  it "should return 0 for update/delete/with_sql_delete/execute_dui by default" do
    Sequel.mock[:t].update(:a=>1).must_equal 0
    Sequel.mock[:t].delete.must_equal 0
    Sequel.mock[:t].with_sql_delete('DELETE FROM t').must_equal 0
    Sequel.mock.execute_dui('DELETE FROM t').must_equal 0
  end

  it "should handle return results for Database#execute without use of dataset" do
    a = []
    db = Sequel.mock(:fetch=>{:a=>1})
    db.execute('SELECT 1') do |r|
      a << r
    end
    a.must_equal [{:a=>1}]
    db.sqls.must_equal ["SELECT 1"]
  end

  it "should handle empty columns when fetching" do
    db = Sequel.mock(:columns=>[])
    ds = db[:t]
    ds.each do |r|
      raise
    end
    db.sqls.must_equal ["SELECT * FROM t"]
    ds.columns.must_equal []
  end

  it "should return nil for insert/execute_insert by default" do
    Sequel.mock[:t].insert(:a=>1).must_be_nil
    Sequel.mock.execute_insert('INSERT INTO a () DEFAULT VALUES').must_be_nil
  end

  it "should be able to set the rows returned by each using :fetch option with a single hash" do
    rs = []
    db = Sequel.mock(:fetch=>{:a=>1})
    db[:t].each{|r| rs << r}
    rs.must_equal [{:a=>1}]
    db[:t].each{|r| rs << r}
    rs.must_equal [{:a=>1}, {:a=>1}]
    db[:t].each{|r| r[:a] = 2; rs << r}
    rs.must_equal [{:a=>1}, {:a=>1}, {:a=>2}]
    db[:t].each{|r| rs << r}
    rs.must_equal [{:a=>1}, {:a=>1}, {:a=>2}, {:a=>1}]
  end

  it "should be able to set the rows returned by each using :fetch option with an array of hashes" do
    rs = []
    db = Sequel.mock(:fetch=>[{:a=>1}, {:a=>2}])
    db[:t].each{|r| rs << r}
    rs.must_equal [{:a=>1}, {:a=>2}]
    db[:t].each{|r| rs << r}
    rs.must_equal [{:a=>1}, {:a=>2}, {:a=>1}, {:a=>2}]
    db[:t].each{|r| r[:a] += 2; rs << r}
    rs.must_equal [{:a=>1}, {:a=>2}, {:a=>1}, {:a=>2}, {:a=>3}, {:a=>4}]
    db[:t].each{|r| rs << r}
    rs.must_equal [{:a=>1}, {:a=>2}, {:a=>1}, {:a=>2}, {:a=>3}, {:a=>4}, {:a=>1}, {:a=>2}]
  end

  it "should be able to set the rows returned by each using :fetch option with an array or arrays of hashes" do
    rs = []
    db = Sequel.mock(:fetch=>[[{:a=>1}, {:a=>2}], [{:a=>3}, {:a=>4}]])
    db[:t].each{|r| rs << r}
    rs.must_equal [{:a=>1}, {:a=>2}]
    db[:t].each{|r| rs << r}
    rs.must_equal [{:a=>1}, {:a=>2}, {:a=>3}, {:a=>4}]
    db[:t].each{|r| rs << r}
    rs.must_equal [{:a=>1}, {:a=>2}, {:a=>3}, {:a=>4}]
  end

  it "should be able to set the rows returned by each using :fetch option with a proc that takes sql" do
    rs = []
    db = Sequel.mock(:fetch=>proc{|sql| sql =~ /FROM t/ ? {:b=>1} : [{:a=>1}, {:a=>2}]})
    db[:t].each{|r| rs << r}
    rs.must_equal [{:b=>1}]
    db[:b].each{|r| rs << r}
    rs.must_equal [{:b=>1}, {:a=>1}, {:a=>2}]
    db[:t].each{|r| r[:b] += 1; rs << r}
    db[:b].each{|r| r[:a] += 2; rs << r}
    rs.must_equal [{:b=>1}, {:a=>1}, {:a=>2}, {:b=>2}, {:a=>3}, {:a=>4}]
    db[:t].each{|r| rs << r}
    db[:b].each{|r| rs << r}
    rs.must_equal [{:b=>1}, {:a=>1}, {:a=>2}, {:b=>2}, {:a=>3}, {:a=>4}, {:b=>1}, {:a=>1}, {:a=>2}]
  end

  it "should have a fetch= method for setting rows returned by each after the fact" do
    rs = []
    db = Sequel.mock
    db.fetch = {:a=>1}
    db[:t].each{|r| rs << r}
    rs.must_equal [{:a=>1}]
    db[:t].each{|r| rs << r}
    rs.must_equal [{:a=>1}] * 2
  end

  it "should be able to set an exception to raise by setting the :fetch option to an exception class" do
    db = Sequel.mock(:fetch=>ArgumentError)
    proc{db[:t].all}.must_raise(Sequel::DatabaseError)
    begin
      db[:t].all
    rescue => e
    end
    e.must_be_kind_of(Sequel::DatabaseError)
    e.wrapped_exception.must_be_kind_of(ArgumentError) 
  end

  it "should be able to set separate kinds of results for fetch using an array" do
    rs = []
    db = Sequel.mock(:fetch=>[{:a=>1}, [{:a=>2}, {:a=>3}], proc{|s| {:a=>4}}, proc{|s| }, nil, ArgumentError])
    db[:t].each{|r| rs << r}
    rs.must_equal [{:a=>1}]
    db[:t].each{|r| rs << r}
    rs.must_equal [{:a=>1}, {:a=>2}, {:a=>3}]
    db[:t].each{|r| rs << r}
    rs.must_equal [{:a=>1}, {:a=>2}, {:a=>3}, {:a=>4}]
    db[:t].each{|r| rs << r}
    rs.must_equal [{:a=>1}, {:a=>2}, {:a=>3}, {:a=>4}]
    db[:t].each{|r| rs << r}
    rs.must_equal [{:a=>1}, {:a=>2}, {:a=>3}, {:a=>4}]
    proc{db[:t].all}.must_raise(Sequel::DatabaseError)
  end

  it "should be able to set the rows returned by each on a per dataset basis using with_fetch" do
    rs = []
    db = Sequel.mock(:fetch=>{:a=>1})
    ds = db[:t]
    ds.each{|r| rs << r}
    rs.must_equal [{:a=>1}]
    ds = ds.with_fetch(:b=>2)
    ds.each{|r| rs << r}
    rs.must_equal [{:a=>1}, {:b=>2}]
  end

  it "should raise Error if given an invalid object to fetch" do
    proc{Sequel.mock(:fetch=>Class.new).get(:a)}.must_raise(Sequel::DatabaseError)
    proc{Sequel.mock(:fetch=>Object.new).get(:a)}.must_raise(Sequel::DatabaseError)
  end

  it "should be able to set the number of rows modified by update and delete using :numrows option as an integer" do
    db = Sequel.mock(:numrows=>2)
    db[:t].update(:a=>1).must_equal 2
    db[:t].delete.must_equal 2
    db[:t].update(:a=>1).must_equal 2
    db[:t].delete.must_equal 2
  end

  it "should be able to set the number of rows modified by update and delete using :numrows option as an array of integers" do
    db = Sequel.mock(:numrows=>[2, 1])
    db[:t].update(:a=>1).must_equal 2
    db[:t].delete.must_equal 1
    db[:t].update(:a=>1).must_equal 0
    db[:t].delete.must_equal 0
  end

  it "should be able to set the number of rows modified by update and delete using :numrows option as a proc" do
    db = Sequel.mock(:numrows=>proc{|sql| sql =~ / t/ ? 2 : 1})
    db[:t].update(:a=>1).must_equal 2
    db[:t].delete.must_equal 2
    db[:b].update(:a=>1).must_equal 1
    db[:b].delete.must_equal 1
  end

  it "should be able to set an exception to raise by setting the :numrows option to an exception class" do
    db = Sequel.mock(:numrows=>ArgumentError)
    proc{db[:t].update(:a=>1)}.must_raise(Sequel::DatabaseError)
    begin
      db[:t].delete
    rescue => e
    end
    e.must_be_kind_of(Sequel::DatabaseError)
    e.wrapped_exception.must_be_kind_of(ArgumentError) 
  end

  it "should be able to set separate kinds of results for numrows using an array" do
    db = Sequel.mock(:numrows=>[1, proc{|s| 2}, nil, ArgumentError])
    db[:t].delete.must_equal 1
    db[:t].update(:a=>1).must_equal 2
    db[:t].delete.must_equal 0
    proc{db[:t].delete}.must_raise(Sequel::DatabaseError)
  end

  it "should have a numrows= method to set the number of rows modified by update and delete after the fact" do
    db = Sequel.mock
    db.numrows = 2
    db[:t].update(:a=>1).must_equal 2
    db[:t].delete.must_equal 2
    db[:t].update(:a=>1).must_equal 2
    db[:t].delete.must_equal 2
  end

  it "should be able to set the number of rows modified by update and delete on a per dataset basis" do
    db = Sequel.mock(:numrows=>2)
    ds = db[:t]
    ds.update(:a=>1).must_equal 2
    ds.delete.must_equal 2
    ds = ds.with_numrows(3)
    ds.update(:a=>1).must_equal 3
    ds.delete.must_equal 3
  end

  it "should raise Error if given an invalid object for numrows or autoid" do
    proc{Sequel.mock(:numrows=>Class.new)[:a].delete}.must_raise(Sequel::DatabaseError)
    proc{Sequel.mock(:numrows=>Object.new)[:a].delete}.must_raise(Sequel::DatabaseError)
    proc{Sequel.mock(:autoid=>Class.new)[:a].insert}.must_raise(Sequel::DatabaseError)
    proc{Sequel.mock(:autoid=>Object.new)[:a].insert}.must_raise(Sequel::DatabaseError)
  end

  it "should be able to set the autogenerated primary key returned by insert using :autoid option as an integer" do
    db = Sequel.mock(:autoid=>1)
    db[:t].insert(:a=>1).must_equal 1
    db[:t].insert(:a=>1).must_equal 2
    db[:t].insert(:a=>1).must_equal 3
  end

  it "should be able to set the autogenerated primary key returned by insert using :autoid option as an array of integers" do
    db = Sequel.mock(:autoid=>[1, 3, 5])
    db[:t].insert(:a=>1).must_equal 1
    db[:t].insert(:a=>1).must_equal 3
    db[:t].insert(:a=>1).must_equal 5
    db[:t].insert(:a=>1).must_be_nil
  end

  it "should be able to set the autogenerated primary key returned by insert using :autoid option as a proc" do
    db = Sequel.mock(:autoid=>proc{|sql| sql =~ /INTO t / ? 2 : 1})
    db[:t].insert(:a=>1).must_equal 2
    db[:t].insert(:a=>1).must_equal 2
    db[:b].insert(:a=>1).must_equal 1
    db[:b].insert(:a=>1).must_equal 1
  end

  it "should be able to set an exception to raise by setting the :autoid option to an exception class" do
    db = Sequel.mock(:autoid=>ArgumentError)
    proc{db[:t].insert(:a=>1)}.must_raise(Sequel::DatabaseError)
    begin
      db[:t].insert
    rescue => e
    end
    e.must_be_kind_of(Sequel::DatabaseError)
    e.wrapped_exception.must_be_kind_of(ArgumentError) 
  end

  it "should be able to set separate kinds of results for autoid using an array" do
    db = Sequel.mock(:autoid=>[1, proc{|s| 2}, nil, ArgumentError])
    db[:t].insert.must_equal 1
    db[:t].insert.must_equal 2
    db[:t].insert.must_be_nil
    proc{db[:t].insert}.must_raise(Sequel::DatabaseError)
  end

  it "should have an autoid= method to set the autogenerated primary key returned by insert after the fact" do
    db = Sequel.mock
    db.autoid = 1
    db[:t].insert(:a=>1).must_equal 1
    db[:t].insert(:a=>1).must_equal 2
    db[:t].insert(:a=>1).must_equal 3
  end

  it "should be able to set the autogenerated primary key returned by insert on a per dataset basis" do
    db = Sequel.mock(:autoid=>1)
    ds = db[:t]
    ds.insert(:a=>1).must_equal 1
    ds = ds.with_autoid(5)
    ds.insert(:a=>1).must_equal 5
    ds.insert(:a=>1).must_equal 6
    db[:t].insert(:a=>1).must_equal 2
  end

  it "should be able to set the columns to set in the dataset as an array of symbols" do
    db = Sequel.mock(:columns=>[:a, :b])
    db[:t].columns.must_equal [:a, :b]
    db.sqls.must_equal ["SELECT * FROM t LIMIT 0"]
    ds = db[:t]
    ds.all
    db.sqls.must_equal ["SELECT * FROM t"]
    ds.columns.must_equal [:a, :b]
    db.sqls.must_equal []
    db[:t].columns.must_equal [:a, :b]
  end

  it "should be able to set the columns to set in the dataset as an array of arrays of symbols" do
    db = Sequel.mock(:columns=>[[:a, :b], [:c, :d]])
    db[:t].columns.must_equal [:a, :b]
    db[:x].columns.must_equal [:c, :d]
  end

  it "should be able to set the columns to set in the dataset as a proc" do
    db = Sequel.mock(:columns=>proc{|sql| (sql =~ / t/) ? [:a, :b] : [:c, :d]})
    db[:b].columns.must_equal [:c, :d]
    db[:t].columns.must_equal [:a, :b]
  end

  it "should have a columns= method to set the columns to set after the fact" do
    db = Sequel.mock
    db.columns = [[:a, :b], [:c, :d]]
    db[:t].columns.must_equal [:a, :b]
    db[:x].columns.must_equal [:c, :d]
  end

  it "should raise Error if given an invalid columns" do
    proc{Sequel.mock(:columns=>Object.new)[:a].columns}.must_raise(Sequel::DatabaseError)
  end

  it "should not quote identifiers by default" do
    Sequel.mock.send(:quote_identifiers_default).must_equal false
  end

  it "should allow overriding of server_version" do
    db = Sequel.mock
    db.server_version.must_be_nil
    db.server_version = 80102
    db.server_version.must_equal 80102
  end

  it "should not fold to uppercase by default" do
    Sequel.mock.send(:folds_unquoted_identifiers_to_uppercase?).must_equal false
  end

  it "should keep a record of all executed SQL in #sqls" do
    db = Sequel.mock
    db[:t].all
    db[:b].delete
    db[:c].insert(:a=>1)
    db[:d].update(:a=>1)
    db.sqls.must_equal ['SELECT * FROM t', 'DELETE FROM b', 'INSERT INTO c (a) VALUES (1)', 'UPDATE d SET a = 1']
  end

  it "should clear sqls on retrieval" do
    db = Sequel.mock
    db[:t].all
    db.sqls.must_equal ['SELECT * FROM t']
    db.sqls.must_equal []
  end

  it "should also log SQL executed to the given loggers" do
    a = []
    def a.method_missing(m, *x) push(*x) end
    db = Sequel.mock(:loggers=>[a])
    db[:t].all
    db[:b].delete
    db[:c].insert(:a=>1)
    db[:d].update(:a=>1)
    a.zip(['SELECT * FROM t', 'DELETE FROM b', 'INSERT INTO c (a) VALUES (1)', 'UPDATE d SET a = 1']).each do |is, should|
      is.must_match should
    end
  end

  it "should correctly handle transactions" do
    db = Sequel.mock
    db.transaction{db[:a].all}
    db.sqls.must_equal ['BEGIN', 'SELECT * FROM a', 'COMMIT']
    db.transaction{db[:a].all; raise Sequel::Rollback}
    db.sqls.must_equal ['BEGIN', 'SELECT * FROM a', 'ROLLBACK']
    proc{db.transaction{db[:a].all; raise ArgumentError}}.must_raise(ArgumentError)
    db.sqls.must_equal ['BEGIN', 'SELECT * FROM a', 'ROLLBACK']
    proc{db.transaction(:rollback=>:reraise){db[:a].all; raise Sequel::Rollback}}.must_raise(Sequel::Rollback)
    db.sqls.must_equal ['BEGIN', 'SELECT * FROM a', 'ROLLBACK']
    db.transaction(:rollback=>:always){db[:a].all}
    db.sqls.must_equal ['BEGIN', 'SELECT * FROM a', 'ROLLBACK']
    db.transaction{db.transaction{db[:a].all; raise Sequel::Rollback}}
    db.sqls.must_equal ['BEGIN', 'SELECT * FROM a', 'ROLLBACK']
    db.transaction{db.transaction(:savepoint=>true){db[:a].all; raise Sequel::Rollback}}
    db.sqls.must_equal ['BEGIN', 'SAVEPOINT autopoint_1', 'SELECT * FROM a', 'ROLLBACK TO SAVEPOINT autopoint_1', 'COMMIT']
    db.transaction{db.transaction(:savepoint=>true){db[:a].all}; raise Sequel::Rollback}
    db.sqls.must_equal ['BEGIN', 'SAVEPOINT autopoint_1', 'SELECT * FROM a', 'RELEASE SAVEPOINT autopoint_1', 'ROLLBACK']
  end

  it "should correctly handle transactions when sharding" do
    db = Sequel.mock(:servers=>{:test=>{}, 'test'=>{}})
    db.transaction{db.transaction(:server=>:test){db[:a].all; db[:t].server(:test).all}}
    db.sqls.must_equal ['BEGIN', 'BEGIN -- test', 'SELECT * FROM a', 'SELECT * FROM t -- test', 'COMMIT -- test', 'COMMIT']
    db.transaction{db.transaction(:server=>'test'){db[:a].all; db[:t].server("test").all}}
    db.sqls.must_equal ['BEGIN', 'BEGIN -- "test"', 'SELECT * FROM a', 'SELECT * FROM t -- "test"', 'COMMIT -- "test"', 'COMMIT']
  end

  it "should correctly handle with_advisory_lock when sharding" do
    db = Sequel.mock(:host=>'postgres', :servers=>{:test=>{}, 'test'=>{}}, :fetch=>{:v=>true})
    db.with_advisory_lock(1234, server: :test){}
    db.sqls.must_equal ['SELECT pg_try_advisory_lock(1234) LIMIT 1 -- test', 'SELECT pg_advisory_unlock(1234) LIMIT 1 -- test']
  end

  it "should yield a mock connection object from synchronize" do
    c = Sequel.mock.synchronize{|conn| conn}
    c.must_be_kind_of(Sequel::Mock::Connection)
  end

  it "should deal correctly with sharding" do
    db = Sequel.mock(:servers=>{:test=>{}})
    c1 = db.synchronize{|conn| conn}
    c2 = db.synchronize(:test){|conn| conn}
    c1.server.must_equal :default
    c2.server.must_equal :test
  end

  it "should disconnect correctly" do
    db = Sequel.mock
    db.test_connection
    db.disconnect
  end

  it "should accept :extend option for extending the object with a module" do
    Sequel.mock(:extend=>Module.new{def foo(v) v * 2 end}).foo(3).must_equal 6
  end

  it "should accept :sqls option for where to store the SQL queries" do
    a = []
    Sequel.mock(:sqls=>a)[:t].all
    a.must_equal ['SELECT * FROM t']
  end

  it "should include :append option in SQL if it is given" do
    db = Sequel.mock(:append=>'a')
    db[:t].all
    db.sqls.must_equal ['SELECT * FROM t -- a']
  end

  it "should append :arguments option to execute to the SQL if present" do
    db = Sequel.mock
    db.execute('SELECT * FROM t', :arguments=>[1, 2])
    db.sqls.must_equal ['SELECT * FROM t -- args: [1, 2]']
  end

  it "should have Dataset#columns take columns to set and return self" do
    db = Sequel.mock
    ds = db[:t].columns(:id, :a, :b)
    ds.must_be_kind_of(Sequel::Mock::Dataset)
    ds.columns.must_equal [:id, :a, :b]
  end

  it "should be able to load dialects based on the database name" do
    Sequel.mock(:host=>'access').select(Date.new(2011, 12, 13)).sql.must_equal 'SELECT #2011-12-13#'
    Sequel.mock(:host=>'db2').select(1).sql.must_equal 'SELECT 1 FROM "SYSIBM"."SYSDUMMY1"'
    Sequel.mock(:host=>'mssql')[:A].full_text_search(:B, 'C').with_quote_identifiers(false).sql.must_equal "SELECT * FROM A WHERE (CONTAINS (B, 'C'))"
    Sequel.mock(:host=>'mysql')[:a].full_text_search(:b, 'c').with_quote_identifiers(false).sql.must_equal "SELECT * FROM a WHERE (MATCH (b) AGAINST ('c'))"
    Sequel.mock(:host=>'oracle')[:a].limit(1).with_quote_identifiers(false).sql.upcase.must_equal 'SELECT * FROM (SELECT * FROM A) T1 WHERE (ROWNUM <= 1)'
    Sequel.mock(:host=>'postgres')[:a].full_text_search(:b, 'c').with_quote_identifiers(false).sql.must_equal "SELECT * FROM a WHERE (to_tsvector(CAST('simple' AS regconfig), (COALESCE(b, ''))) @@ to_tsquery(CAST('simple' AS regconfig), 'c'))"
    Sequel.mock(:host=>'sqlanywhere').from(:A).offset(1).with_quote_identifiers(false).sql.must_equal 'SELECT TOP 2147483647 START AT (1 + 1) * FROM A'
    Sequel.mock(:host=>'sqlite')[Sequel[:a].as(:b)].with_quote_identifiers(false).sql.must_equal "SELECT * FROM a AS 'b'"
  end

  it "should be able to mock schema calls" do
    Sequel.mock(:host=>'mysql', :fetch=>{:Field=>'a'}).schema(:a).first.first.must_equal :a
  end

  it "should automatically set version for adapters needing versions" do
    Sequel.mock(:host=>'postgres').server_version.must_be :>=, 90400
    Sequel.mock(:host=>'mssql').server_version.must_be :>=, 11000000
    Sequel.mock(:host=>'mysql').server_version.must_be :>=, 50617
    Sequel.mock(:host=>'sqlite').sqlite_version.must_be :>=, 30804
    Sequel.mock(:host=>'oracle').server_version.must_be :>=, 11000000
  end

  it "should stub out the primary_key method for postgres" do
    Sequel.mock(:host=>'postgres').primary_key(:t).must_equal :id
  end

  it "should stub out the bound_variable_arg method for postgres" do
    Sequel.mock(:host=>'postgres').bound_variable_arg(:t, nil).must_equal :t
  end

  it "should handle creating tables on oracle" do
    Sequel.mock(:host=>'oracle').create_table(:a){String :b}
  end
end

describe "PostgreSQL support" do
  before do
    @db = Sequel.mock(:host=>'postgres')
  end

  it "should support savepoints" do
    @db.transaction{@db.transaction(:savepoint=>true){}}
    @db.sqls.must_equal ["BEGIN", "SAVEPOINT autopoint_1", "RELEASE SAVEPOINT autopoint_1", "COMMIT"]
  end

  it "should create an unlogged table" do
    @db.create_table(:unlogged_dolls, :unlogged => true){text :name}
    @db.sqls.must_equal ['CREATE UNLOGGED TABLE "unlogged_dolls" ("name" text)']
  end

  it "should respect :unlogged_tables_default Database option when creating a table" do
    @db.opts[:unlogged_tables_default] = true
    @db.create_table(:unlogged_dolls, :unlogged => true){text :name}
    @db.sqls.must_equal ['CREATE UNLOGGED TABLE "unlogged_dolls" ("name" text)']
    @db.create_table(:unlogged_dolls){text :name}
    @db.sqls.must_equal ['CREATE UNLOGGED TABLE "unlogged_dolls" ("name" text)']
    @db.create_table(:unlogged_dolls, :unlogged => false){text :name}
    @db.sqls.must_equal ['CREATE TABLE "unlogged_dolls" ("name" text)']

    @db.opts[:unlogged_tables_default] = 't'
    @db.create_table(:unlogged_dolls){text :name}
    @db.sqls.must_equal ['CREATE UNLOGGED TABLE "unlogged_dolls" ("name" text)']

    @db.opts[:unlogged_tables_default] = 'f'
    @db.create_table(:unlogged_dolls){text :name}
    @db.sqls.must_equal ['CREATE TABLE "unlogged_dolls" ("name" text)']
  end

  it "should support spatial indexes" do
    @db.alter_table(:posts){add_spatial_index [:geom]}
    @db.sqls.must_equal ['CREATE INDEX "posts_geom_index" ON "posts" USING gist ("geom")']
  end

  it "should support indexes with index type" do
    @db.alter_table(:posts){add_index :p, :type => 'gist'}
    @db.sqls.must_equal ['CREATE INDEX "posts_p_index" ON "posts" USING gist ("p")']
  end

  it "should have #transaction support various types of synchronous options" do
    @db.transaction(:synchronous=>:on){}
    @db.transaction(:synchronous=>true){}
    @db.transaction(:synchronous=>:off){}
    @db.transaction(:synchronous=>false){}
    @db.sqls.grep(/synchronous/).must_equal ["SET LOCAL synchronous_commit = on", "SET LOCAL synchronous_commit = on", "SET LOCAL synchronous_commit = off", "SET LOCAL synchronous_commit = off"]

    @db.transaction(:synchronous=>nil){}
    @db.sqls.must_equal ['BEGIN', 'COMMIT']

    if @db.server_version >= 90100
      @db.transaction(:synchronous=>:local){}
      @db.sqls.grep(/synchronous/).must_equal ["SET LOCAL synchronous_commit = local"]

      if @db.server_version >= 90200
        @db.transaction(:synchronous=>:remote_write){}
        @db.sqls.grep(/synchronous/).must_equal ["SET LOCAL synchronous_commit = remote_write"]
      end
    end
  end

  it "should have #transaction support read only transactions" do
    @db.transaction(:read_only=>true){}
    @db.transaction(:read_only=>false){}
    @db.transaction(:isolation=>:serializable, :read_only=>true){}
    @db.transaction(:isolation=>:serializable, :read_only=>false){}
    @db.sqls.grep(/READ/).must_equal ["SET TRANSACTION READ ONLY", "SET TRANSACTION READ WRITE", "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY", "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE"]
  end

  it "should have #transaction support deferrable transactions" do
    @db.transaction(:deferrable=>true){}
    @db.transaction(:deferrable=>false){}
    @db.transaction(:deferrable=>true, :read_only=>true){}
    @db.transaction(:deferrable=>false, :read_only=>false){}
    @db.transaction(:isolation=>:serializable, :deferrable=>true, :read_only=>true){}
    @db.transaction(:isolation=>:serializable, :deferrable=>false, :read_only=>false){}
    @db.sqls.grep(/DEF/).must_equal ["SET TRANSACTION DEFERRABLE", "SET TRANSACTION NOT DEFERRABLE", "SET TRANSACTION READ ONLY DEFERRABLE", "SET TRANSACTION READ WRITE NOT DEFERRABLE",  "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE", "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE READ WRITE NOT DEFERRABLE"]
  end

  it "should support creating indexes concurrently" do
    @db.add_index :test, [:name, :value], :concurrently=>true
    @db.sqls.must_equal ['CREATE INDEX CONCURRENTLY "test_name_value_index" ON "test" ("name", "value")']
  end

  it "should support dropping indexes concurrently" do
    @db.drop_index :test, [:name, :value], :concurrently=>true, :name=>'tnv2'
    @db.sqls.must_equal ['DROP INDEX CONCURRENTLY "tnv2"']
  end

  it "should use INSERT RETURNING for inserts" do
    @db[:test5].insert(:value=>10)
    @db.sqls.must_equal ['INSERT INTO "test5" ("value") VALUES (10) RETURNING "id"']
  end

  it "should support opclass specification" do
    @db.alter_table(:posts){add_index(:user_id, :opclass => :int4_ops, :type => :btree)}
    @db.sqls.must_equal ['CREATE INDEX "posts_user_id_index" ON "posts" USING btree ("user_id" int4_ops)']
  end

  it 'should quote NaN' do
    nan = 0.0/0.0
    @db[:test5].insert_sql(:value => nan).must_equal %q{INSERT INTO "test5" ("value") VALUES ('NaN')}
  end

  it 'should quote +Infinity' do
    inf = 1.0/0.0
    @db[:test5].insert_sql(:value => inf).must_equal %q{INSERT INTO "test5" ("value") VALUES ('Infinity')}
  end

  it 'should quote -Infinity' do
    inf = -1.0/0.0
    @db[:test5].insert_sql(:value => inf).must_equal %q{INSERT INTO "test5" ("value") VALUES ('-Infinity')}
  end

  it "Dataset#insert_conflict should respect expressions in the target argument" do
    @ds = @db[:ic_test]
    @ds.insert_conflict(:target=>:a).insert_sql(1, 2, 3).must_equal "INSERT INTO \"ic_test\" VALUES (1, 2, 3) ON CONFLICT (\"a\") DO NOTHING"
    @ds.insert_conflict(:target=>:c, :conflict_where=>{:c_is_unique=>true}).insert_sql(1, 2, 3).must_equal "INSERT INTO \"ic_test\" VALUES (1, 2, 3) ON CONFLICT (\"c\") WHERE (\"c_is_unique\" IS TRUE) DO NOTHING"
    @ds.insert_conflict(:target=>[:b, :c]).insert_sql(1, 2, 3).must_equal "INSERT INTO \"ic_test\" VALUES (1, 2, 3) ON CONFLICT (\"b\", \"c\") DO NOTHING"
    @ds.insert_conflict(:target=>[:b, Sequel.function(:round, :c)]).insert_sql(1, 2, 3).must_equal "INSERT INTO \"ic_test\" VALUES (1, 2, 3) ON CONFLICT (\"b\", round(\"c\")) DO NOTHING"
    @ds.insert_conflict(:target=>[:b, Sequel.virtual_row{|o| o.round(:c)}]).insert_sql(1, 2, 3).must_equal "INSERT INTO \"ic_test\" VALUES (1, 2, 3) ON CONFLICT (\"b\", round(\"c\")) DO NOTHING"
  end

  it "should support creating and dropping foreign tables" do
    @db.create_table(:t, :foreign=>:f, :options=>{:o=>1}){Integer :a}
    @db.drop_table(:t, :foreign=>true)
    @db.sqls.must_equal ['CREATE FOREIGN TABLE "t" ("a" integer) SERVER "f" OPTIONS (o \'1\')',
      'DROP FOREIGN TABLE "t"']

    @db.create_table(:t, :foreign=>:f){Integer :a}
    @db.sqls.must_equal ['CREATE FOREIGN TABLE "t" ("a" integer) SERVER "f"']
  end

  it "should not support creating temporary foreign tables" do
    proc{@db.create_table(:t, :foreign=>:f, :temp=>true){Integer :a}}.must_raise Sequel::Error
  end

  it "should not support creating unlogged foreign tables" do
    proc{@db.create_table(:t, :foreign=>:f, :unlogged=>true){Integer :a}}.must_raise Sequel::Error
  end

  it "#create_function and #drop_function should create and drop functions" do
    @db.create_function('tf', 'SELECT 1', :returns=>:integer)
    @db.drop_function('tf')
    @db.sqls.map{|s| s.gsub(/\s+/, ' ').strip}.must_equal ["CREATE FUNCTION tf() RETURNS integer LANGUAGE SQL AS 'SELECT 1'",
      'DROP FUNCTION tf()']
  end

  it "#create_function and #drop_function should support options" do
    @db.create_function('tf', 'SELECT $1 + $2', :args=>[[:integer, :a], :integer], :replace=>true, :returns=>:integer, :language=>'SQL', :behavior=>:immutable, :strict=>true, :security_definer=>true, :cost=>2, :set=>{:search_path => 'public'})
    @db.drop_function('tf', :if_exists=>true, :cascade=>true, :args=>[[:integer, :a], :integer])
    @db.sqls.map{|s| s.gsub(/\s+/, ' ').strip}.must_equal ["CREATE OR REPLACE FUNCTION tf(a integer, integer) RETURNS integer LANGUAGE SQL IMMUTABLE STRICT SECURITY DEFINER COST 2 SET search_path = public AS 'SELECT $1 + $2'",
      'DROP FUNCTION IF EXISTS tf(a integer, integer) CASCADE']

    @db.create_function('tf', 'myfunc.so', :language=>'SQL', :rows=>20, :link_symbol=>:ls)
    @db.sqls.map{|s| s.gsub(/\s+/, ' ').strip}.must_equal ["CREATE FUNCTION tf() RETURNS void LANGUAGE SQL ROWS 20 AS 'myfunc.so', 'ls'"]

    @db.create_function('tf', 'SELECT $1 + $2', :args=>[[:integer, :a, 'OUT']], :language=>'SQL')
    @db.sqls.map{|s| s.gsub(/\s+/, ' ').strip}.must_equal ["CREATE FUNCTION tf(OUT a integer) LANGUAGE SQL AS 'SELECT $1 + $2'"]
  end

  it "#create_language and #drop_language should create and drop languages" do
    @db.create_language(:plpgsql)
    @db.create_language(:plpgsql, :replace=>true, :trusted=>true, :handler=>:a, :validator=>:b)
    @db.drop_language(:plpgsql)
    @db.drop_language(:plpgsql, :if_exists=>true, :cascade=>true)
    @db.sqls.map{|s| s.gsub(/\s+/, ' ').strip}.must_equal ['CREATE LANGUAGE plpgsql',
      'CREATE OR REPLACE TRUSTED LANGUAGE plpgsql HANDLER a VALIDATOR b',
      'DROP LANGUAGE plpgsql',
      'DROP LANGUAGE IF EXISTS plpgsql CASCADE']
  end

  it "#create_schema and #drop_schema should create and drop schemas" do
    @db.create_schema(:sequel)
    @db.create_schema(:sequel, :if_not_exists=>true, :owner=>:foo)
    @db.drop_schema(:sequel)
    @db.drop_schema(:sequel, :if_exists=>true, :cascade=>true)
    @db.sqls.must_equal ['CREATE SCHEMA "sequel"',
      'CREATE SCHEMA IF NOT EXISTS "sequel" AUTHORIZATION "foo"',
      'DROP SCHEMA "sequel"',
      'DROP SCHEMA IF EXISTS "sequel" CASCADE']
  end

  it "#create_trigger and #drop_trigger should create and drop triggers" do
    @db.create_trigger(:test, :identity, :tf, :each_row=>true)
    @db.create_trigger(:test, :identity, :tf, :after=>true, :events=>:insert, :args=>[1, 'a'])
    @db.create_trigger(:test, :identity, :tf, :each_row=>true, :when=> {Sequel[:new][:name] => 'b'})
    @db.drop_trigger(:test, :identity)
    @db.drop_trigger(:test, :identity, :if_exists=>true, :cascade=>true)
    @db.sqls.map{|s| s.gsub(/\s+/, ' ').strip}.must_equal ['CREATE TRIGGER identity BEFORE INSERT OR UPDATE OR DELETE ON "test" FOR EACH ROW EXECUTE PROCEDURE tf()',
      'CREATE TRIGGER identity AFTER INSERT ON "test" EXECUTE PROCEDURE tf(1, \'a\')',
      %q{CREATE TRIGGER identity BEFORE INSERT OR UPDATE OR DELETE ON "test" FOR EACH ROW WHEN ("new"."name" = 'b') EXECUTE PROCEDURE tf()},
      'DROP TRIGGER identity ON "test"',
      'DROP TRIGGER IF EXISTS identity ON "test" CASCADE']
  end

  it "#create_trigger should raise exception if using trigger conditions on an unsupported version" do
    def @db.server_version; 80400 end
    proc{@db.create_trigger(:test, :identity, :tf, :when=>true)}.must_raise Sequel::Error 
  end

  it "should attempt to remove aliases when inserting on PostgreSQL <9.5" do
    def @db.server_version(*); 90400 end
    @db.from{a.as(:b)}.returning.insert
    @db.sqls.must_equal ["INSERT INTO \"a\" DEFAULT VALUES RETURNING *"]
  end

  it "should support adding columns with COLLATE" do
    @db.alter_table(:t){add_column :x, String, :collate=>'s'}
    @db.sqls.must_equal ["ALTER TABLE \"t\" ADD COLUMN \"x\" text COLLATE s"]
    @db.alter_table(:t){add_column :x, String, :collate=>:s}
    @db.sqls.must_equal ["ALTER TABLE \"t\" ADD COLUMN \"x\" text COLLATE \"s\""]
  end

  it "should support dropping columns with CASCADE and IF EXISTS" do
    @db.alter_table(:t){drop_column :x, :cascade=>true, :if_exists=>true}
    @db.sqls.must_equal ["ALTER TABLE \"t\" DROP COLUMN IF EXISTS \"x\" CASCADE"]
  end

  it "should support transaction isolation levels" do
    @db.supports_transaction_isolation_levels?.must_equal true
  end

  it "should not support arbitrary window function frame options" do
    @db.dataset.supports_window_function_frame_option?(:bad).must_equal false
  end

  it "should support getting server version from specific server" do
    begin
      version = @db.server_version
      @db.instance_variable_set(:@server_version, nil)
      @db.fetch = {:v=>version}
      @db.server_version(:default)
      @db.sqls.must_equal ["SELECT CAST(current_setting('server_version_num') AS integer) AS v"]
    ensure
      @db.instance_variable_set(:@server_version, version)
    end
  end

  it "should support bytea unescaping" do
    bytea = @db.conversion_procs[17]
    bytea.call('\\x70').ord.must_equal 112
    bytea.call('\\070').ord.must_equal 56
    bytea.call('\\\\').ord.must_equal 92
  end

  it "should raise for integers outside of PostgreSQL bigint range" do
    proc{@db.literal(2**63)}.must_raise Sequel::InvalidValue
  end

  it "should support converting serial columns to identity" do
    @db.fetch= [[{:v=>'on'}], [{:attnum=>1}], [{:objid=>2345, :v=>false}]]
    def @db.regclass_oid(x) 1234 end
    def @db.schema(x) [[:id, {:primary_key=>true, :auto_increment=>true}]] end
    @db.convert_serial_to_identity(:table).must_be_nil
    @db.sqls.must_equal [
      "SELECT current_setting('is_superuser') LIMIT 1",
      "SELECT \"attnum\" FROM \"pg_attribute\" WHERE ((\"attrelid\" = 1234) AND (\"attname\" = 'id')) LIMIT 1",
      "SELECT \"objid\", (\"deptype\" = 'i') AS \"v\" FROM \"pg_depend\" WHERE ((\"refclassid\" = CAST('pg_class' AS regclass)) AND (\"refobjid\" = 1234) AND (\"refobjsubid\" = 1) AND (\"classid\" = CAST('pg_class' AS regclass)) AND (\"objsubid\" = 0) AND (\"deptype\" IN ('a', 'i')))",
       "BEGIN",
       "ALTER TABLE \"table\" ALTER COLUMN \"id\" DROP DEFAULT",
       "UPDATE \"pg_depend\" SET \"deptype\" = 'i' WHERE ((\"classid\" = CAST('pg_class' AS regclass)) AND (\"objid\" = 2345) AND (\"objsubid\" = 0) AND (\"deptype\" = 'a'))",
       "UPDATE \"pg_attribute\" SET \"attidentity\" = 'd' WHERE ((\"attrelid\" = 1234) AND (\"attname\" = 'id'))",
       "COMMIT"
    ]
  end

  it "should support :column option when converting serial columns to identity" do
    @db.fetch= [[{:v=>'on'}], [{:attnum=>1}], [{:objid=>2345, :v=>false}]]
    def @db.regclass_oid(x) 1234 end
    @db.convert_serial_to_identity(:table, :column=>'id').must_be_nil
    @db.sqls.must_equal [
      "SELECT current_setting('is_superuser') LIMIT 1",
      "SELECT \"attnum\" FROM \"pg_attribute\" WHERE ((\"attrelid\" = 1234) AND (\"attname\" = 'id')) LIMIT 1",
      "SELECT \"objid\", (\"deptype\" = 'i') AS \"v\" FROM \"pg_depend\" WHERE ((\"refclassid\" = CAST('pg_class' AS regclass)) AND (\"refobjid\" = 1234) AND (\"refobjsubid\" = 1) AND (\"classid\" = CAST('pg_class' AS regclass)) AND (\"objsubid\" = 0) AND (\"deptype\" IN ('a', 'i')))",
       "BEGIN",
       "ALTER TABLE \"table\" ALTER COLUMN \"id\" DROP DEFAULT",
       "UPDATE \"pg_depend\" SET \"deptype\" = 'i' WHERE ((\"classid\" = CAST('pg_class' AS regclass)) AND (\"objid\" = 2345) AND (\"objsubid\" = 0) AND (\"deptype\" = 'a'))",
       "UPDATE \"pg_attribute\" SET \"attidentity\" = 'd' WHERE ((\"attrelid\" = 1234) AND (\"attname\" = 'id'))",
       "COMMIT"
    ]
  end

  it "should support :server option converting serial columns to identity" do
    @db.fetch= [[{:v=>'on'}], [{:attnum=>1}], [{:objid=>2345, :v=>false}]]
    def @db.regclass_oid(x) 1234 end
    @db.convert_serial_to_identity(:table, :column=>'id', :server=>:default).must_be_nil
    @db.sqls.must_equal [
      "SELECT current_setting('is_superuser') LIMIT 1",
      "SELECT \"attnum\" FROM \"pg_attribute\" WHERE ((\"attrelid\" = 1234) AND (\"attname\" = 'id')) LIMIT 1",
      "SELECT \"objid\", (\"deptype\" = 'i') AS \"v\" FROM \"pg_depend\" WHERE ((\"refclassid\" = CAST('pg_class' AS regclass)) AND (\"refobjid\" = 1234) AND (\"refobjsubid\" = 1) AND (\"classid\" = CAST('pg_class' AS regclass)) AND (\"objsubid\" = 0) AND (\"deptype\" IN ('a', 'i')))",
       "BEGIN",
       "ALTER TABLE \"table\" ALTER COLUMN \"id\" DROP DEFAULT",
       "UPDATE \"pg_depend\" SET \"deptype\" = 'i' WHERE ((\"classid\" = CAST('pg_class' AS regclass)) AND (\"objid\" = 2345) AND (\"objsubid\" = 0) AND (\"deptype\" = 'a'))",
       "UPDATE \"pg_attribute\" SET \"attidentity\" = 'd' WHERE ((\"attrelid\" = 1234) AND (\"attname\" = 'id'))",
       "COMMIT"
    ]
  end

  it "should handle case where column is already identity when converting serial columns to identity" do
    @db.fetch= [[{:v=>'on'}], [{:attnum=>1}], [{:objid=>2345, :v=>true}]]
    def @db.regclass_oid(x) 1234 end
    @db.convert_serial_to_identity(:table, :column=>'id').must_be_nil
    @db.sqls.must_equal [
      "SELECT current_setting('is_superuser') LIMIT 1",
      "SELECT \"attnum\" FROM \"pg_attribute\" WHERE ((\"attrelid\" = 1234) AND (\"attname\" = 'id')) LIMIT 1",
      "SELECT \"objid\", (\"deptype\" = 'i') AS \"v\" FROM \"pg_depend\" WHERE ((\"refclassid\" = CAST('pg_class' AS regclass)) AND (\"refobjid\" = 1234) AND (\"refobjsubid\" = 1) AND (\"classid\" = CAST('pg_class' AS regclass)) AND (\"objsubid\" = 0) AND (\"deptype\" IN ('a', 'i')))",
    ]
  end

  it "should handle case where multiple serial columns exist when converting serial columns to identity" do
    @db.fetch= [[{:v=>'on'}], [{:attnum=>1}], [{:objid=>2345, :v=>false}, {:objid=>3456, :v=>false}]]
    def @db.regclass_oid(x) 1234 end
    proc{@db.convert_serial_to_identity(:table, :column=>'id')}.must_raise Sequel::Error
    @db.sqls.must_equal [
      "SELECT current_setting('is_superuser') LIMIT 1",
      "SELECT \"attnum\" FROM \"pg_attribute\" WHERE ((\"attrelid\" = 1234) AND (\"attname\" = 'id')) LIMIT 1",
      "SELECT \"objid\", (\"deptype\" = 'i') AS \"v\" FROM \"pg_depend\" WHERE ((\"refclassid\" = CAST('pg_class' AS regclass)) AND (\"refobjid\" = 1234) AND (\"refobjsubid\" = 1) AND (\"classid\" = CAST('pg_class' AS regclass)) AND (\"objsubid\" = 0) AND (\"deptype\" IN ('a', 'i')))",
    ]
  end

  it "should handle case where no serial columns exist when converting serial columns to identity" do
    @db.fetch= [[{:v=>'on'}], [{:attnum=>1}], []]
    def @db.regclass_oid(x) 1234 end
    proc{@db.convert_serial_to_identity(:table, :column=>'id')}.must_raise Sequel::Error
    @db.sqls.must_equal [
      "SELECT current_setting('is_superuser') LIMIT 1",
      "SELECT \"attnum\" FROM \"pg_attribute\" WHERE ((\"attrelid\" = 1234) AND (\"attname\" = 'id')) LIMIT 1",
      "SELECT \"objid\", (\"deptype\" = 'i') AS \"v\" FROM \"pg_depend\" WHERE ((\"refclassid\" = CAST('pg_class' AS regclass)) AND (\"refobjid\" = 1234) AND (\"refobjsubid\" = 1) AND (\"classid\" = CAST('pg_class' AS regclass)) AND (\"objsubid\" = 0) AND (\"deptype\" IN ('a', 'i')))",
    ]
  end

  it "should not support converting serial columns to identity on PostgreSQL <10.2" do
    def @db.server_version; 100000; end
    proc{@db.convert_serial_to_identity(:table)}.must_raise Sequel::Error
  end

  it "should not support converting serial columns to identity if not a superuser" do
    @db.fetch = {:v=>'off'}
    proc{@db.convert_serial_to_identity(:table)}.must_raise Sequel::Error
  end

  it "should not support converting serial columns to identity if column cannot be determined" do
    @db.fetch = {:v=>'on'}
    def @db.schema(x) [[:id, {:primary_key=>false, :auto_increment=>false}]] end
    proc{@db.convert_serial_to_identity(:table)}.must_raise Sequel::Error
  end

  it "should have connection_configuration_sqls return SQLs to use for new connections" do
    @db.send(:connection_configuration_sqls).must_equal [
      "SET standard_conforming_strings = ON",
      "SET client_min_messages = 'WARNING'"
    ]

    h = {:force_standard_strings=>false, :client_min_messages=>:notice}
    @db.send(:connection_configuration_sqls, h).must_equal ["SET client_min_messages = 'NOTICE'"]

    h[:client_min_messages] = :notice2
    proc{@db.send(:connection_configuration_sqls, h)}.must_raise Sequel::Error

    h[:client_min_messages] = nil
    @db.send(:connection_configuration_sqls, h).must_equal []

    h[:search_path] = Object.new
    proc{@db.send(:connection_configuration_sqls, h)}.must_raise Sequel::Error

    h[:search_path] = 'public,foo'
    @db.send(:connection_configuration_sqls, h).must_equal ["SET search_path = \"public\",\"foo\""]

    h[:search_path] = %w'public foo2'
    @db.send(:connection_configuration_sqls, h).must_equal ["SET search_path = \"public\",\"foo2\""]
  end

  it "should recognize 40P01 SQL state as a serialization failure" do
    @db.send(:database_specific_error_class_from_sqlstate, '40P01').must_equal Sequel::SerializationFailure
  end

  it "should use correct SQL for defer_constraints and immediate_constraints" do
    @db.defer_constraints
    @db.sqls.must_equal ['SET CONSTRAINTS ALL DEFERRED']
    @db.immediate_constraints
    @db.sqls.must_equal ['SET CONSTRAINTS ALL IMMEDIATE']

    @db.defer_constraints(:constraints=>:a)
    @db.sqls.must_equal ['SET CONSTRAINTS "a" DEFERRED']
    @db.immediate_constraints(:constraints=>[:a, :b])
    @db.sqls.must_equal ['SET CONSTRAINTS "a", "b" IMMEDIATE']
  end

  it "should correctly handle defer_constraints and immediate_constraints :server option" do
    db = Sequel.connect("mock://postgres", :servers=>{:test=>{}})
    db.defer_constraints(:server=>:test)
    db.sqls.must_equal ['SET CONSTRAINTS ALL DEFERRED -- test']
    db.immediate_constraints(:server=>:test)
    db.sqls.must_equal ['SET CONSTRAINTS ALL IMMEDIATE -- test']
  end

  it "should raise if an invalid number of elements is provided when using derived column lists with an array" do
    @db.from{c.function.as(:d, [[:a, :b], [:e, :f]])}.sql.must_equal 'SELECT * FROM c() AS "d"("a" b, "e" f)'
    proc{@db.from{c.function.as(:d, [[:a], [:e, :f]])}.sql}.must_raise Sequel::Error
    proc{@db.from{c.function.as(:d, [[:a, :b, :c], [:e, :f]])}.sql}.must_raise Sequel::Error
  end
end

describe "MySQL support" do
  before do
    @db = Sequel.mock(:host=>'mysql')
  end

  it "should support spatial indexes" do
    @db.alter_table(:posts){add_spatial_index [:geom]}
    @db.sqls.must_equal ['CREATE SPATIAL INDEX `posts_geom_index` ON `posts` (`geom`)']
  end

  it "should support fulltext indexes and full_text_search" do
    @db.alter_table(:posts){add_full_text_index :title; add_full_text_index [:title, :body]}
    @db.sqls.must_equal [ "CREATE FULLTEXT INDEX `posts_title_index` ON `posts` (`title`)", "CREATE FULLTEXT INDEX `posts_title_body_index` ON `posts` (`title`, `body`)" ]
  end

  it "should support indexes with index type" do
    @db.alter_table(:posts){add_index :id, :type => :btree}
    @db.sqls.must_equal ["CREATE INDEX `posts_id_index` USING btree ON `posts` (`id`)"]
  end
end

describe "SQLite support" do
  before do
    @db = Sequel.mock(:host=>'sqlite')
  end

  it "should use a string literal for Sequel[:col].as(:alias)" do
    @db.literal(Sequel[:c].as(:a)).must_equal "`c` AS 'a'"
  end

  it "should use a string literal for Sequel[:table][:col].as(:alias)" do
    @db.literal(Sequel[:t][:c].as(:a)).must_equal "`t`.`c` AS 'a'"
  end

  it "should use a string literal for :column.as(:alias)" do
    @db.literal(Sequel.as(:c, :a)).must_equal "`c` AS 'a'"
  end

  it "should use a string literal in the SELECT clause" do
    @db[:t].select(Sequel[:c].as(:a)).sql.must_equal "SELECT `c` AS 'a' FROM `t`"
  end

  it "should use a string literal in the FROM clause" do
    @db[Sequel[:t].as(:a)].sql.must_equal "SELECT * FROM `t` AS 'a'"
  end

  it "should use a string literal in the JOIN clause" do
    @db[:t].join_table(:natural, :j, nil, :table_alias=>:a).sql.must_equal "SELECT * FROM `t` NATURAL JOIN `j` AS 'a'"
  end

  it "should have support for various #transaction modes" do
    @db.transaction{}
    @db.transaction(:mode => :immediate){}
    @db.transaction(:mode => :exclusive){}
    @db.transaction(:mode => :deferred){}
    @db.sqls.must_equal ["BEGIN", "COMMIT", "BEGIN IMMEDIATE TRANSACTION", "COMMIT", "BEGIN EXCLUSIVE TRANSACTION", "COMMIT", "BEGIN DEFERRED TRANSACTION", "COMMIT"]

    @db.transaction_mode.must_be_nil
    @db.transaction_mode = :immediate
    @db.transaction_mode.must_equal :immediate
    @db.transaction{}
    @db.transaction(:mode => :exclusive){}
    @db.sqls.must_equal ["BEGIN IMMEDIATE TRANSACTION", "COMMIT", "BEGIN EXCLUSIVE TRANSACTION", "COMMIT"]
  end

  it "should choose a temporary table name that isn't already used when dropping or renaming columns" do
    exists = [true, true, false]
    @db.define_singleton_method(:table_exists?){|x| exists.shift}
    @db.drop_column(:test3, :i)
    @db.sqls.grep(/ALTER/).must_equal ["ALTER TABLE `test3` RENAME TO `test3_backup2`"]

    exists = [true, true, true, false]
    @db.rename_column(:test3, :h, :i)
    @db.sqls.grep(/ALTER/).must_equal ["ALTER TABLE `test3` RENAME TO `test3_backup3`"]
  end
end

describe "Oracle support" do
  before do
    @db = Sequel.mock(:host=>'oracle')
  end

  it "should not support savepoints" do
    proc{@db.transaction{@db.transaction(:savepoint=>true){}}}.must_raise Sequel::InvalidOperation
  end
end
