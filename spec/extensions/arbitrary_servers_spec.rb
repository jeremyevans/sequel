require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "arbtirary servers" do
  before do
    @db = Sequel.mock(:servers=>{})
    @db.extension :arbitrary_servers
  end

  specify "should allow arbitrary server options using a hash" do
    @db.synchronize(:host=>'host1', :database=>'db1') do |c|
      c.opts[:host].should == 'host1'
      c.opts[:database].should == 'db1'
    end
  end

  specify "should not cache connections to arbitrary servers" do
    x = nil
    @db.synchronize(:host=>'host1', :database=>'db1') do |c|
      x = c
    end
    @db.synchronize(:host=>'host1', :database=>'db1') do |c2|
      c2.should_not equal(x)
    end
  end

  specify "should yield same connection correctly when nesting" do
    @db.synchronize(:host=>'host1', :database=>'db1') do |c|
      @db.synchronize(:host=>'host1', :database=>'db1') do |c2|
        c2.should equal(c)
      end
    end
  end

  specify "should disconnect when connection is finished" do
    x, x1 = nil, nil
    @db.meta_def(:disconnect_connection){|c| x = c}
    @db.synchronize(:host=>'host1', :database=>'db1') do |c|
      x1 = c
      @db.synchronize(:host=>'host1', :database=>'db1') do |c2|
        c2.should equal(c)
      end
      x.should equal(nil)
    end
    x.should equal(x1)
  end

  specify "should yield different connection correctly when nesting" do
    @db.synchronize(:host=>'host1', :database=>'db1') do |c|
      c.opts[:host].should == 'host1'
      @db.synchronize(:host=>'host2', :database=>'db1') do |c2|
        c2.opts[:host].should == 'host2'
        c2.should_not equal(c)
      end
    end
  end

  specify "should respect multithreaded access" do
    @db.synchronize(:host=>'host1', :database=>'db1') do |c|
      Thread.new do
        @db.synchronize(:host=>'host1', :database=>'db1') do |c2|
          c2.should_not equal(c)
        end
      end.join
    end
  end

  specify "should work correctly with server_block plugin" do
    @db.extend Sequel::ServerBlock
    @db.with_server(:host=>'host1', :database=>'db1') do
      @db.synchronize do |c|
        c.opts[:host].should == 'host1'
        c.opts[:database].should == 'db1'
        @db.synchronize do |c2|
          c2.should equal(c)
        end
      end
    end
  end


  specify "should respect multithreaded access with server block plugin" do
    @db.extend Sequel::ServerBlock
    q, q1 = Queue.new, Queue.new

    t = nil
    @db[:t].all
    @db.with_server(:host=>'a') do
      @db[:t].all
      t = Thread.new do
        @db[:t].all
        @db.with_server(:host=>'c') do
          @db[:t].all
          @db.with_server(:host=>'d'){@db[:t].all}
          q.push nil
          q1.pop
          @db[:t].all
        end
        @db[:t].all
      end
      q.pop
      @db.with_server(:host=>'b'){@db[:t].all}
      @db[:t].all
    end
    @db[:t].all
    q1.push nil
    t.join
    @db.sqls.should == ['SELECT * FROM t', 'SELECT * FROM t -- {:host=>"a"}', 'SELECT * FROM t', 'SELECT * FROM t -- {:host=>"c"}', 'SELECT * FROM t -- {:host=>"d"}',
      'SELECT * FROM t -- {:host=>"b"}', 'SELECT * FROM t -- {:host=>"a"}', 'SELECT * FROM t', 'SELECT * FROM t -- {:host=>"c"}', 'SELECT * FROM t']
  end
end
