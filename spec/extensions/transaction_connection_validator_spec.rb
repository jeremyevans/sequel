require_relative "spec_helper"

describe "transaction_connection_validator extension" do
  database_error = Class.new(StandardError)

  before do
    @db = Sequel.mock
    @m = Module.new do
      def post_execute(conn, sql); end
      def disconnect_connection(conn)
        @sqls << 'disconnect'
      end
      def connect(server)
        @sqls << 'connect'
        super
      end
      private
      define_method(:database_error_classes) do
        [database_error]
      end
      def disconnect_error?(e, opts)
        e.message.include? 'disconnect'
      end
      def log_connection_execute(conn, sql)
        res = super
        post_execute(conn, sql)
        res
      end
    end
    @db.extend @m
    @db.extension(:transaction_connection_validator)
  end

  it "should not affect transactions that do not raise exceptions" do
    @db.transaction{}
    @db.sqls.must_equal ['BEGIN', 'COMMIT']
  end

  it "should retry transactions for disconnects during BEGIN" do
    conns = []
    @db.define_singleton_method(:post_execute) do |conn, sql|
      conns << conn
      raise database_error, "disconnect error" if @sqls == ['BEGIN']
    end
    @db.transaction{}
    @db.sqls.must_equal ['BEGIN', 'ROLLBACK', 'disconnect', 'connect', 'BEGIN', 'COMMIT']
    conns.uniq.size.must_equal 2
  end

  it "should handle DatabaseDisconnectErrors as disconnects" do
    conns = []
    @db.define_singleton_method(:post_execute) do |conn, sql|
      conns << conn
      raise Sequel::DatabaseDisconnectError if @sqls == ['BEGIN']
    end
    @db.transaction{}
    @db.sqls.must_equal ['BEGIN', 'ROLLBACK', 'disconnect', 'connect', 'BEGIN', 'COMMIT']
    conns.uniq.size.must_equal 2
  end

  it "should not retry if a connection has already been checked out before calling transaction" do
    conns = []
    @db.define_singleton_method(:post_execute) do |conn, sql|
      conns << conn
      raise Sequel::DatabaseDisconnectError if @sqls == ['BEGIN']
    end

    c = nil
    proc do
      @db.synchronize do |c1|
        c = c1
        @db.transaction{}
      end
    end.must_raise(Sequel::DatabaseDisconnectError)
    @db.sqls.must_equal ['BEGIN', 'ROLLBACK', 'disconnect']
    conns.uniq.must_equal [c]
  end

  it "should not retry transaction setup more than 5 times" do
    conns = []
    @db.define_singleton_method(:post_execute) do |conn, sql|
      conns << conn
      raise database_error, "disconnect error"
    end
    proc do 
      @db.transaction{}
    end.must_raise(Sequel::DatabaseDisconnectError)
    @db.sqls.must_equal(['BEGIN', 'ROLLBACK', 'disconnect', 'connect'] * 5)
    conns.uniq.size.must_equal 5
  end

  it "should not retry on non-disconnect errors" do
    conns = []
    @db.define_singleton_method(:post_execute) do |conn, sql|
      conns << conn
      raise database_error, "normal error" if @sqls == ['BEGIN']
    end

    proc{@db.transaction{}}.must_raise(Sequel::DatabaseError)
    @db.sqls.must_equal ['BEGIN', 'ROLLBACK']
    conns.uniq.size.must_equal 1
  end
end
