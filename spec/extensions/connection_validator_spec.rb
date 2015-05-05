require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

connection_validator_specs = shared_description do
  describe "connection validator" do
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
      @db.synchronize{|c| c}.must_be_kind_of(Sequel::Mock::Connection)
    end

    it "should only validate if connection idle longer than timeout" do
      c1 = @db.synchronize{|c| c}
      @db.sqls.must_equal []
      @db.synchronize{|c| c}.must_be_same_as(c1)
      @db.sqls.must_equal []
      @db.pool.connection_validation_timeout = -1
      @db.synchronize{|c| c}.must_be_same_as(c1)
      @db.sqls.must_equal ['SELECT NULL']
      @db.pool.connection_validation_timeout = 1
      @db.synchronize{|c| c}.must_be_same_as(c1)
      @db.sqls.must_equal []
      @db.synchronize{|c| c}.must_be_same_as(c1)
      @db.sqls.must_equal []
    end

    it "should disconnect connection if not valid" do
      c1 = @db.synchronize{|c| c}
      @db.sqls.must_equal []
      c1.valid = false
      @db.pool.connection_validation_timeout = -1
      c2 = @db.synchronize{|c| c}
      @db.sqls.must_equal ['SELECT NULL', 'disconnect']
      c2.wont_be_same_as(c1)
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
      @db.sqls.must_equal ['SELECT NULL', 'disconnect', 'SELECT NULL', 'disconnect']
      c3.wont_be_same_as(c1)
      c3.wont_be_same_as(c2)
    end

    it "should not leak connection references" do
      c1 = @db.synchronize do |c|
        @db.pool.instance_variable_get(:@connection_timestamps).must_equal({})
        c
      end
      @db.pool.instance_variable_get(:@connection_timestamps).must_include(c1)

      c1.valid = false
      @db.pool.connection_validation_timeout = -1
      c2 = @db.synchronize do |c|
        @db.pool.instance_variable_get(:@connection_timestamps).must_equal({})
        c
      end
      c2.wont_be_same_as(c1)
      @db.pool.instance_variable_get(:@connection_timestamps).wont_include(c1)
      @db.pool.instance_variable_get(:@connection_timestamps).must_include(c2)
    end

    it "should handle case where determining validity requires a connection" do
      @db.meta_def(:valid_connection?){|c| synchronize{}; true}
      @db.pool.connection_validation_timeout = -1
      c1 = @db.synchronize{|c| c}
      @db.synchronize{|c| c}.must_be_same_as(c1)
    end
  end
end

describe "Sequel::ConnectionValidator with threaded pool" do
  before do
    @db = Sequel.mock
  end
  include connection_validator_specs
end
describe "Sequel::ConnectionValidator with sharded threaded pool" do
  before do
    @db = Sequel.mock(:servers=>{})
  end
  include connection_validator_specs
end

