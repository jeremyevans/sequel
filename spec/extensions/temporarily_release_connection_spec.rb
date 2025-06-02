require_relative "spec_helper"

pool_types = [ :threaded, :sharded_threaded]
pool_types += [ :timed_queue, :sharded_timed_queue] if RUBY_VERSION >= '3.2'

pool_types.each do |pool_type|
  describe "temporarily_release_connection extension with pool class #{pool_type}" do
    before do
      opts = {:max_connections=>1, :pool_class=>pool_type}
      if pool_type.to_s.start_with?('sharded')
        opts[:servers] = {:foo=>{}, :bar=>{}}
      end
      @db = Sequel.mock(opts).extension(:temporarily_release_connection)
    end

    it "should temporarily release connection during block so it can be acquired by other threads" do
      conns = []
      @db.transaction(:rollback=>:always) do |c|
        @db.temporarily_release_connection(c) do
          Array.new(4) do |i|
            Thread.new do
              @db.synchronize do |conn|
                conns << conn
              end
            end
          end.map(&:join)
        end
      end

      c = @db.synchronize{|conn| conn}
      conns.size.must_equal 4
      conns.each do |conn|
        conn.must_be_same_as c
      end

      @db.sqls.must_equal ['BEGIN', 'ROLLBACK']
    end

    it "should temporarily release connection for specific shard during block so it can be acquired by other threads" do
      conns = []
      @db.transaction(:rollback=>:always, :server=>:foo) do |c|
        @db.temporarily_release_connection(c, :foo) do
          @db.transaction(:rollback=>:always, :server=>:bar) do |c2|
            @db.temporarily_release_connection(c2, :bar) do
              Array.new(4) do |i|
                Thread.new do
                  @db.synchronize(:foo) do |conn|
                    @db.synchronize(:bar) do |conn2|
                      conns << [conn, conn2]
                    end
                  end
                end
              end.map(&:join)
            end
          end
        end
      end

      c = @db.synchronize(:foo){|conn| conn}
      c2 = @db.synchronize(:bar){|conn| conn}
      conns.size.must_equal 4
      conns.each do |conn, conn2|
        conn.must_be_same_as c
        conn2.must_be_same_as c2
      end

      @db.sqls.must_equal ["BEGIN -- foo", "BEGIN -- bar", "ROLLBACK -- bar", "ROLLBACK -- foo"]
    end if pool_type.to_s.start_with?('sharded')

    it "should raise UnableToReacquireConnectionError if unable to reacquire the same connection it released" do
      proc do
        @db.transaction(rollback: :always) do |conn|
          @db.temporarily_release_connection(conn) do
            @db.disconnect
          end
        end
      end.must_raise Sequel::UnableToReacquireConnectionError
      @db.sqls.must_equal ['BEGIN']
    end

    it "should raise if provided a connection that is not checked out" do
      proc do
        @db.temporarily_release_connection(@db.synchronize{|conn| conn})
      end.must_raise Sequel::Error
    end

    it "should raise if pool max_size is not 1" do
      db = Sequel.mock(:pool_type=>pool_type)
      proc do
        db.extension(:temporarily_release_connection)
      end.must_raise Sequel::Error
    end
  end
end

describe "temporarily_release_connection extension" do
  it "should raise if pool uses connection_handling: :disconnect option" do
    db = Sequel.mock(:connection_handling=>:disconnect, :pool_class=>:threaded)
    proc do
      db.extension(:temporarily_release_connection)
    end.must_raise Sequel::Error
  end

  it "should raise if pool uses unsupported pool type" do
    db = Sequel.mock(:pool_class=>:single)
    proc do
      db.extension(:temporarily_release_connection)
    end.must_raise Sequel::Error
  end
end
