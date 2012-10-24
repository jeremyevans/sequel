require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

shared_examples_for "Sequel::ConnectionValidator" do  
  before do
    @db.extend(Module.new do
      def disconnect_connection(conn)
        @sqls << 'disconnect'
      end
      def valid_connection?(conn)
        super
        conn.valid
      end
      def connect(server)
        conn = super
        conn.extend(Module.new do
          attr_accessor :valid
        end)
        conn.valid = true
        conn
      end
    end)
    @db.extension(:connection_validator)
  end

  it "should still allow new connections" do
    @db.synchronize{|c| c}.should be_a_kind_of(Sequel::Mock::Connection)
  end

  it "should only validate if connection idle longer than timeout" do
    c1 = @db.synchronize{|c| c}
    @db.sqls.should == []
    @db.synchronize{|c| c}.should equal(c1)
    @db.sqls.should == []
    @db.pool.connection_validation_timeout = -1
    @db.synchronize{|c| c}.should equal(c1)
    @db.sqls.should == ['SELECT NULL']
    @db.pool.connection_validation_timeout = 1
    @db.synchronize{|c| c}.should equal(c1)
    @db.sqls.should == []
    @db.synchronize{|c| c}.should equal(c1)
    @db.sqls.should == []
  end

  it "should disconnect connection if not valid" do
    c1 = @db.synchronize{|c| c}
    @db.sqls.should == []
    c1.valid = false
    @db.pool.connection_validation_timeout = -1
    c2 = @db.synchronize{|c| c}
    @db.sqls.should == ['SELECT NULL', 'disconnect']
    c2.should_not equal(c1)
  end

  it "should disconnect multiple connections repeatedly if they are not valid" do
    q, q1 = Queue.new, Queue.new
    c1 = nil
    c2 = nil
    @db.pool.connection_validation_timeout = -1
    @db.synchronize do |c|
      Thread.new do
        @db.synchronize do |cc|
          c2 = cc
        end
        q1.pop
        q.push nil
      end
      q1.push nil
      q.pop
      c1 = c
    end
    c1.valid = false
    c2.valid = false

    c3 = @db.synchronize{|c| c}
    @db.sqls.should == ['SELECT NULL', 'disconnect', 'SELECT NULL', 'disconnect']
    c3.should_not equal(c1)
    c3.should_not equal(c2)
  end

  it "should not leak connection references" do
    c1 = @db.synchronize do |c|
      @db.pool.instance_variable_get(:@connection_timestamps).should == {}
      c
    end
    @db.pool.instance_variable_get(:@connection_timestamps).should have_key(c1)

    c1.valid = false
    @db.pool.connection_validation_timeout = -1
    c2 = @db.synchronize do |c|
      @db.pool.instance_variable_get(:@connection_timestamps).should == {}
      c
    end
    c2.should_not equal(c1)
    @db.pool.instance_variable_get(:@connection_timestamps).should_not have_key(c1)
    @db.pool.instance_variable_get(:@connection_timestamps).should have_key(c2)
  end
end

describe "Sequel::ConnectionValidator with threaded pool" do
  before do
    @db = Sequel.mock
  end
  it_should_behave_like "Sequel::ConnectionValidator"
end
describe "Sequel::ConnectionValidator with sharded threaded pool" do
  before do
    @db = Sequel.mock(:servers=>{})
  end
  it_should_behave_like "Sequel::ConnectionValidator"
end

