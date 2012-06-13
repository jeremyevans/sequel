require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

shared_examples_for "Database#with_server" do  
  specify "should set the default server to use in the block" do
    @db.with_server(:a){@db[:t].all}
    @db.sqls.should == ["SELECT * FROM t -- a"]
    @db.with_server(:b){@db[:t].all}
    @db.sqls.should == ["SELECT * FROM t -- b"]
  end

  specify "should have no affect after the block" do
    @db.with_server(:a){@db[:t].all}
    @db.sqls.should == ["SELECT * FROM t -- a"]
    @db[:t].all
    @db.sqls.should == ["SELECT * FROM t"]
  end

  specify "should not override specific server inside the block" do
    @db.with_server(:a){@db[:t].server(:b).all}
    @db.sqls.should == ["SELECT * FROM t -- b"]
  end

  specify "should work correctly when blocks are nested" do
    @db[:t].all
    @db.with_server(:a) do
      @db[:t].all
      @db.with_server(:b){@db[:t].all}
      @db[:t].all
    end
    @db[:t].all
    @db.sqls.should == ["SELECT * FROM t", "SELECT * FROM t -- a", "SELECT * FROM t -- b", "SELECT * FROM t -- a", "SELECT * FROM t"]
  end

  specify "should work correctly for inserts/updates/deletes" do
    @db.with_server(:a) do
      @db[:t].insert
      @db[:t].update(:a=>1)
      @db[:t].delete
    end
    @db.sqls.should == ["INSERT INTO t DEFAULT VALUES -- a", "UPDATE t SET a = 1 -- a", "DELETE FROM t -- a"]
  end
end

describe "Database#with_server single threaded" do
  before do
    @db = Sequel.mock(:single_threaded=>true, :servers=>{:a=>{}, :b=>{}})
    @db.extension :server_block
  end

  it_should_behave_like "Database#with_server"
end

describe "Database#with_server multi threaded" do
  before do
    @db = Sequel.mock(:servers=>{:a=>{}, :b=>{}, :c=>{}, :d=>{}})
    @db.extension :server_block
  end

  it_should_behave_like "Database#with_server"

  specify "should respect multithreaded access" do
    q, q1 = Queue.new, Queue.new
    
    t = nil
    @db[:t].all
    @db.with_server(:a) do
      @db[:t].all
      t = Thread.new do
        @db[:t].all
        @db.with_server(:c) do
          @db[:t].all
          @db.with_server(:d){@db[:t].all}
          q.push nil
          q1.pop
          @db[:t].all
        end
        @db[:t].all
      end
      q.pop
      @db.with_server(:b){@db[:t].all}
      @db[:t].all
    end
    @db[:t].all
    q1.push nil
    t.join
    @db.sqls.should == ["SELECT * FROM t", "SELECT * FROM t -- a", "SELECT * FROM t", "SELECT * FROM t -- c", "SELECT * FROM t -- d",
      "SELECT * FROM t -- b", "SELECT * FROM t -- a", "SELECT * FROM t", "SELECT * FROM t -- c", "SELECT * FROM t"]
  end
end

