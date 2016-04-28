require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')
CONNECTION_POOL_DEFAULTS = {:pool_timeout=>5, :pool_sleep_time=>0.001, :max_connections=>4}

mock_db = lambda do |*a, &b|
  db = Sequel.mock
  (class << db; self end).send(:define_method, :connect){|c| b.arity == 1 ? b.call(c) : b.call} if b
  if b2 = a.shift
    (class << db; self end).send(:define_method, :disconnect_connection){|c| b2.arity == 1 ? b2.call(c) : b2.call}
  end
  db
end

describe "An empty ConnectionPool" do
  before do
    @cpool = Sequel::ConnectionPool.get_pool(mock_db.call, CONNECTION_POOL_DEFAULTS)
  end

  it "should have no available connections" do
    @cpool.available_connections.must_equal []
  end

  it "should have no allocated connections" do
    @cpool.allocated.must_equal({})
  end

  it "should have a created_count of zero" do
    @cpool.created_count.must_equal 0
  end
end

describe "ConnectionPool options" do
  it "should support string option values" do
    cpool = Sequel::ConnectionPool.get_pool(mock_db.call, {:max_connections=>'5', :pool_timeout=>'3', :pool_sleep_time=>'0.01'})
    cpool.max_size.must_equal 5
    cpool.instance_variable_get(:@timeout).must_equal 3
    cpool.instance_variable_get(:@sleep_time).must_equal 0.01 unless cpool.class::USE_WAITER
  end

  it "should raise an error unless size is positive" do
    lambda{Sequel::ConnectionPool.get_pool(mock_db.call{1}, :max_connections=>0)}.must_raise(Sequel::Error)
    lambda{Sequel::ConnectionPool.get_pool(mock_db.call{1}, :max_connections=>-10)}.must_raise(Sequel::Error)
    lambda{Sequel::ConnectionPool.get_pool(mock_db.call{1}, :max_connections=>'-10')}.must_raise(Sequel::Error)
    lambda{Sequel::ConnectionPool.get_pool(mock_db.call{1}, :max_connections=>'0')}.must_raise(Sequel::Error)
  end

  it "should support an optional pool name" do
    cpool = Sequel::ConnectionPool.get_pool(mock_db.call, {:name => 'testing'})
    cpool.name.must_equal 'testing'
  end
end

describe "A connection pool handling connections" do
  before do
    @max_size = 2
    msp = proc{@max_size=3}
    @cpool = Sequel::ConnectionPool.get_pool(mock_db.call(proc{|c| msp.call}){:got_connection}, CONNECTION_POOL_DEFAULTS.merge(:max_connections=>@max_size))
  end

  it "#hold should increment #created_count" do
    @cpool.hold do
      @cpool.created_count.must_equal 1
      @cpool.hold {@cpool.hold {@cpool.created_count.must_equal 1}}
      Thread.new{@cpool.hold {_(@cpool.created_count).must_equal 2}}.join
    end
  end

  it "#hold should add the connection to the #allocated array" do
    @cpool.hold do
      @cpool.allocated.size.must_equal 1
  
      @cpool.allocated.must_equal(Thread.current=>:got_connection)
    end
  end

  it "#hold should yield a new connection" do
    @cpool.hold {|conn| conn.must_equal :got_connection}
  end

  it "a connection should be de-allocated after it has been used in #hold" do
    @cpool.hold {}
    @cpool.allocated.size.must_equal 0
  end

  it "#hold should return the value of its block" do
    @cpool.hold {:block_return}.must_equal :block_return
  end

  if RUBY_VERSION < '1.9.0' and !defined?(RUBY_ENGINE)
    it "#hold should remove dead threads from the pool if it reaches its max_size" do
      Thread.new{@cpool.hold{Thread.current.exit!}}.join
      @cpool.allocated.keys.map{|t| t.alive?}.must_equal [false]

      Thread.new{@cpool.hold{Thread.current.exit!}}.join
      @cpool.allocated.keys.map{|t| t.alive?}.must_equal [false, false]

      Thread.new{@cpool.hold{}}.join
      @cpool.allocated.must_equal({})
    end
  end

  it "#make_new should not make more than max_size connections" do
    q = Queue.new
    50.times{Thread.new{@cpool.hold{q.pop}}}
    50.times{q.push nil}
    @cpool.created_count.must_be :<=,  @max_size
  end

  it "database's disconnect connection method should be called when a disconnect is detected" do
    @max_size.must_equal 2
    proc{@cpool.hold{raise Sequel::DatabaseDisconnectError}}.must_raise(Sequel::DatabaseDisconnectError)
    @max_size.must_equal 3
  end

  it "#hold should remove the connection if a DatabaseDisconnectError is raised" do
    @cpool.created_count.must_equal 0
    q, q1 = Queue.new, Queue.new
    @cpool.hold{Thread.new{@cpool.hold{q1.pop; q.push nil}; q1.pop; q.push nil}; q1.push nil; q.pop; q1.push nil; q.pop}
    @cpool.created_count.must_equal 2
    proc{@cpool.hold{raise Sequel::DatabaseDisconnectError}}.must_raise(Sequel::DatabaseDisconnectError)
    @cpool.created_count.must_equal 1
    proc{@cpool.hold{raise Sequel::DatabaseDisconnectError}}.must_raise(Sequel::DatabaseDisconnectError)
    @cpool.created_count.must_equal 0
    proc{@cpool.hold{raise Sequel::DatabaseDisconnectError}}.must_raise(Sequel::DatabaseDisconnectError)
    @cpool.created_count.must_equal 0
  end
end

describe "A connection pool handling connection errors" do 
  it "#hold should raise a Sequel::DatabaseConnectionError if an exception is raised by the connection_proc" do
    cpool = Sequel::ConnectionPool.get_pool(CONNECTION_POOL_DEFAULTS){raise Interrupt}
    proc{cpool.hold{:block_return}}.must_raise(Sequel::DatabaseConnectionError)
    cpool.created_count.must_equal 0
  end

  it "#hold should raise a Sequel::DatabaseConnectionError if nil is returned by the connection_proc" do
    cpool = Sequel::ConnectionPool.get_pool(CONNECTION_POOL_DEFAULTS){nil}
    proc{cpool.hold{:block_return}}.must_raise(Sequel::DatabaseConnectionError)
    cpool.created_count.must_equal 0
  end
end

describe "ConnectionPool#hold" do
  before do
    value = 0
    c = @c = Class.new do
      define_method(:initialize){value += 1}
      define_method(:value){value}
    end
    @pool = Sequel::ConnectionPool.get_pool(mock_db.call{c.new}, CONNECTION_POOL_DEFAULTS)
  end
  
  it "shoulda use the database's connect method to get new connections" do
    res = nil
    @pool.hold {|c| res = c}
    res.must_be_kind_of(@c)
    res.value.must_equal 1
    @pool.hold {|c| res = c}
    res.must_be_kind_of(@c)
    res.value.must_equal 1 # the connection maker is invoked only once
  end
  
  it "should be re-entrant by the same thread" do
    cc = nil
    @pool.hold {|c| @pool.hold {|c1| @pool.hold {|c2| cc = c2}}}
    cc.must_be_kind_of(@c)
  end
  
  it "should catch exceptions and reraise them" do
    proc {@pool.hold {|c| c.foobar}}.must_raise(NoMethodError)
  end
end

describe "A connection pool with a max size of 1" do
  before do
    @invoked_count = 0
    icp = proc{@invoked_count += 1}
    @pool = Sequel::ConnectionPool.get_pool(mock_db.call{icp.call; 'herro'.dup}, CONNECTION_POOL_DEFAULTS.merge(:max_connections=>1))
  end
  
  it "should let only one thread access the connection at any time" do
    cc,c1, c2 = nil
    q, q1 = Queue.new, Queue.new
    
    t1 = Thread.new {@pool.hold {|c| cc = c; c1 = c.dup; q1.push nil; q.pop}}
    q1.pop
    cc.must_equal 'herro'
    c1.must_equal 'herro'
    
    t2 = Thread.new {@pool.hold {|c| c2 = c.dup; q1.push nil; q.pop;}}
    
    # connection held by t1
    t1.must_be :alive?
    t2.must_be :alive?
    
    cc.must_equal 'herro'
    c1.must_equal 'herro'
    c2.must_equal nil
    
    @pool.available_connections.must_be :empty?
    @pool.allocated.must_equal(t1=>cc)

    cc.gsub!('rr', 'll')
    q.push nil
    q1.pop

    t1.join
    t2.must_be :alive?
    
    c2.must_equal 'hello'

    @pool.available_connections.must_be :empty?
    @pool.allocated.must_equal(t2=>cc)
    
    #connection released
    q.push nil
    t2.join
    
    @invoked_count.must_equal 1
    @pool.size.must_equal 1
    @pool.available_connections.must_equal [cc]
    @pool.allocated.must_be :empty?
  end
  
  it "should let the same thread reenter #hold" do
    c1, c2, c3 = nil
    @pool.hold do |c|
      c1 = c
      @pool.hold do |cc2|
        c2 = cc2
        @pool.hold do |cc3|
          c3 = cc3
        end
      end
    end
    c1.must_equal 'herro'
    c2.must_equal 'herro'
    c3.must_equal 'herro'
    
    @invoked_count.must_equal 1
    @pool.size.must_equal 1
    @pool.available_connections.size.must_equal 1
    @pool.allocated.must_be :empty?
  end
end

ThreadedConnectionPoolSpecs = shared_description do
  it "should not have all_connections yield connections allocated to other threads" do
    pool = Sequel::ConnectionPool.get_pool(mock_db.call(&@icpp), @cp_opts.merge(:max_connections=>2, :pool_timeout=>0))
    q, q1 = Queue.new, Queue.new
    t = Thread.new do
      pool.hold do |c1|
        q1.push nil
        q.pop
      end
    end
    pool.hold do |c1|
      q1.pop
      pool.all_connections{|c| c.must_equal c1}
      q.push nil
    end
    t.join
  end

  it "should wait until a connection is available if all are checked out" do
    pool = Sequel::ConnectionPool.get_pool(mock_db.call(&@icpp), @cp_opts.merge(:max_connections=>1, :pool_timeout=>0.1, :pool_sleep_time=>0))
    q, q1 = Queue.new, Queue.new
    t = Thread.new do
      pool.hold do |c|
        q1.push nil
        3.times{Thread.pass}
        q.pop
      end
    end
    q1.pop
    proc{pool.hold{}}.must_raise(Sequel::PoolTimeout)
    q.push nil
    t.join
  end

  it "should not have all_connections yield all available connections" do
    pool = Sequel::ConnectionPool.get_pool(mock_db.call(&@icpp), @cp_opts.merge(:max_connections=>2, :pool_timeout=>0))
    q, q1 = Queue.new, Queue.new
    b = []
    t = Thread.new do
      pool.hold do |c1|
        b << c1
        q1.push nil
        q.pop
      end
    end
    pool.hold do |c1|
      q1.pop
      b << c1
      q.push nil
    end
    t.join
    a = []
    pool.all_connections{|c| a << c}
    a.sort.must_equal b.sort
  end

  it "should raise a PoolTimeout error if a connection couldn't be acquired before timeout" do
    q, q1 = Queue.new, Queue.new
    pool = Sequel::ConnectionPool.get_pool(mock_db.call(&@icpp), @cp_opts.merge(:max_connections=>1, :pool_timeout=>0, :name => "testing"))
    t = Thread.new{pool.hold{|c| q1.push nil; q.pop}}
    q1.pop
    proc{pool.hold{|c|}}.must_raise(Sequel::PoolTimeout).message.must_match "name: testing"
    q.push nil
    t.join
  end
  
  it "should not add a disconnected connection back to the pool if the disconnection_proc raises an error" do
    pool = Sequel::ConnectionPool.get_pool(mock_db.call(proc{|c| raise Sequel::Error}, &@icpp), @cp_opts.merge(:max_connections=>1, :pool_timeout=>0))
    proc{pool.hold{raise Sequel::DatabaseDisconnectError}}.must_raise(Sequel::Error)
    pool.available_connections.length.must_equal 0
  end

  it "should let five threads simultaneously access separate connections" do
    cc = {}
    threads = []
    q, q1, q2 = Queue.new, Queue.new, Queue.new
    
    5.times{|i| threads << Thread.new{@pool.hold{|c| q.pop; cc[i] = c; q1.push nil; q2.pop}}; q.push nil; q1.pop}
    threads.each {|t| t.must_be :alive?}
    cc.size.must_equal 5
    @invoked_count.must_equal 5
    @pool.size.must_equal 5
    @pool.available_connections.must_be :empty?

    h = {}
    i = 0
    threads.each{|t| h[t] = (i+=1)}
    @pool.allocated.must_equal h
    @pool.available_connections.must_equal []
    5.times{q2.push nil}
    threads.each{|t| t.join}
    
    @pool.available_connections.size.must_equal 5
    @pool.allocated.must_be :empty?
  end
  
  it "should block threads until a connection becomes available" do
    cc = {}
    threads = []
    q, q1 = Queue.new, Queue.new
    
    5.times{|i| threads << Thread.new{@pool.hold{|c| cc[i] = c; q1.push nil; q.pop}}}
    5.times{q1.pop}
    threads.each {|t| t.must_be :alive?}
    @pool.available_connections.must_be :empty?

    3.times {|i| threads << Thread.new {@pool.hold {|c| cc[i + 5] = c; q1.push nil}}}
    
    threads[5].must_be :alive?
    threads[6].must_be :alive?
    threads[7].must_be :alive?
    cc.size.must_equal 5
    cc[5].must_equal nil
    cc[6].must_equal nil
    cc[7].must_equal nil
    
    5.times{q.push nil}
    5.times{|i| threads[i].join}
    3.times{q1.pop}
    3.times{|i| threads[i+5].join}
    
    threads.each {|t| t.wont_be :alive?}
    
    @pool.size.must_equal 5
    @invoked_count.must_equal 5
    @pool.available_connections.size.must_equal 5
    @pool.allocated.must_be :empty?
  end

  it "should store connections in a stack if :connection_handling=>:stack" do
    @pool = Sequel::ConnectionPool.get_pool(mock_db.call(&@icpp), @cp_opts.merge(:connection_handling=>:stack))
    c2 = nil
    c = @pool.hold{|cc| Thread.new{@pool.hold{|cc2| c2 = cc2}}.join; cc}
    @pool.size.must_equal 2
    @pool.hold{|cc| cc.must_equal c}
    @pool.hold{|cc| cc.must_equal c}
    @pool.hold do |cc|
      cc.must_equal c
      Thread.new{@pool.hold{|cc2| cc2.must_equal c2}}
    end
  end

  it "should store connections in a queue if :connection_handling=>:queue" do
    @pool = Sequel::ConnectionPool.get_pool(mock_db.call(&@icpp), @cp_opts.merge(:connection_handling=>:queue))
    c2 = nil
    c = @pool.hold{|cc| Thread.new{@pool.hold{|cc2| c2 = cc2}}.join; cc}
    @pool.size.must_equal 2
    @pool.hold{|cc| cc.must_equal c2}
    @pool.hold{|cc| cc.must_equal c}
    @pool.hold do |cc|
      cc.must_equal c2
      Thread.new{@pool.hold{|cc2| cc2.must_equal c}}
    end
  end

  it "should not store connections if :connection_handling=>:disconnect" do
    @pool = Sequel::ConnectionPool.get_pool(mock_db.call(&@icpp), @cp_opts.merge(:connection_handling=>:disconnect))
    d = []
    meta_def(@pool.db, :disconnect_connection){|c| d << c}
    @pool.hold do |cc|
      cc.must_equal 1
      Thread.new{@pool.hold{|cc2| _(cc2).must_equal 2}}.join
      d.must_equal [2]
      @pool.hold{|cc3| cc3.must_equal 1}
    end
    @pool.size.must_equal 0
    d.must_equal [2, 1]

    @pool.hold{|cc| cc.must_equal 3}
    @pool.size.must_equal 0
    d.must_equal [2, 1, 3]

    @pool.hold{|cc| cc.must_equal 4}
    @pool.size.must_equal 0
    d.must_equal [2, 1, 3, 4]
  end
end

describe "Threaded Unsharded Connection Pool" do
  before do
    @invoked_count = 0
    @icpp = proc{@invoked_count += 1}
    @cp_opts = CONNECTION_POOL_DEFAULTS.merge(:max_connections=>5)
    @pool = Sequel::ConnectionPool.get_pool(mock_db.call(&@icpp), @cp_opts)
  end
  
  include ThreadedConnectionPoolSpecs
end

describe "Threaded Sharded Connection Pool" do
  before do
    @invoked_count = 0
    @icpp = proc{@invoked_count += 1}
    @cp_opts = CONNECTION_POOL_DEFAULTS.merge(:max_connections=>5, :servers=>{})
    @pool = Sequel::ConnectionPool.get_pool(mock_db.call(&@icpp), @cp_opts)
  end

  include ThreadedConnectionPoolSpecs
end

describe "ConnectionPool#disconnect" do
  before do
    @count = 0
    cp = proc{@count += 1}
    @pool = Sequel::ConnectionPool.get_pool(mock_db.call{{:id => cp.call}}, CONNECTION_POOL_DEFAULTS.merge(:max_connections=>5, :servers=>{}))
    threads = []
    q, q1 = Queue.new, Queue.new
    5.times {|i| threads << Thread.new {@pool.hold {|c| q1.push nil; q.pop}}}
    5.times{q1.pop}
    5.times{q.push nil}
    threads.each {|t| t.join}
  end
  
  it "should invoke the given block for each available connection" do
    @pool.size.must_equal 5
    @pool.available_connections.size.must_equal 5
    @pool.available_connections.each {|c| c[:id].wont_equal nil}
    conns = []
    meta_def(@pool.db, :disconnect_connection){|c| conns << c}
    @pool.disconnect
    conns.size.must_equal 5
  end
  
  it "should remove all available connections" do
    @pool.size.must_equal 5
    @pool.disconnect
    @pool.size.must_equal 0
  end

  it "should disconnect connections in use as soon as they are no longer in use" do
    @pool.size.must_equal 5
    @pool.hold do |conn|
      @pool.available_connections.size.must_equal 4
      @pool.available_connections.each {|c| c.wont_be_same_as(conn)}
      conns = []
      meta_def(@pool.db, :disconnect_connection){|c| conns << c}
      @pool.disconnect
      conns.size.must_equal 4
      @pool.size.must_equal 1
    end
    @pool.size.must_equal 0
  end
end

describe "A connection pool with multiple servers" do
  before do
    ic = @invoked_counts = Hash.new(0)
    @pool = Sequel::ConnectionPool.get_pool(mock_db.call{|server| "#{server}#{ic[server] += 1}"}, CONNECTION_POOL_DEFAULTS.merge(:servers=>{:read_only=>{}}))
  end
  
  it "should support preconnect method that immediately creates the maximum number of connections" do
    @pool.send(:preconnect)
    i = 0
    @pool.all_connections{|c1| i+=1}
    i.must_equal @pool.max_size * 2
  end

  it "should support preconnect method that immediately creates the maximum number of connections concurrently" do
    @pool.send(:preconnect, true)
    i = 0
    @pool.all_connections{|c1| i+=1}
    i.must_equal @pool.max_size * 2
  end

  it "#all_connections should return connections for all servers" do
    @pool.hold{}
    @pool.all_connections{|c1| c1.must_equal "default1"}
    a = []
    @pool.hold(:read_only) do |c|
      @pool.all_connections{|c1| a << c1}
    end
    a.sort_by{|c| c.to_s}.must_equal ["default1", "read_only1"]
  end
  
  it "#servers should return symbols for all servers" do
    @pool.servers.sort_by{|s| s.to_s}.must_equal [:default, :read_only]
  end

  it "should use the :default server by default" do
    @pool.size.must_equal 0
    @pool.hold do |c|
      c.must_equal "default1"
      @pool.allocated.must_equal(Thread.current=>"default1")
    end
    @pool.available_connections.must_equal ["default1"]
    @pool.size.must_equal 1
    @invoked_counts.must_equal(:default=>1)
  end
  
  it "should use the :default server an invalid server is used" do
    @pool.hold do |c1|
      c1.must_equal "default1"
      @pool.hold(:blah) do |c2|
        c2.must_equal c1
        @pool.hold(:blah2) do |c3|
          c2.must_equal c3
        end
      end
    end
  end

  it "should support a :servers_hash option used for converting the server argument" do
    ic = @invoked_counts
    @pool = Sequel::ConnectionPool.get_pool(mock_db.call{|server| "#{server}#{ic[server] += 1}"}, CONNECTION_POOL_DEFAULTS.merge(:servers_hash=>Hash.new(:read_only), :servers=>{:read_only=>{}}))
    @pool.hold(:blah) do |c1|
      c1.must_equal "read_only1"
      @pool.hold(:blah) do |c2|
        c2.must_equal c1
        @pool.hold(:blah2) do |c3|
          c2.must_equal c3
        end
      end
    end

    @pool = Sequel::ConnectionPool.get_pool(mock_db.call{|server| "#{server}#{ic[server] += 1}"}, CONNECTION_POOL_DEFAULTS.merge(:servers_hash=>Hash.new{|h,k| raise Sequel::Error}, :servers=>{:read_only=>{}}))
    proc{@pool.hold(:blah){|c1|}}.must_raise(Sequel::Error)
  end

  it "should use the requested server if server is given" do
    @pool.size(:read_only).must_equal 0
    @pool.hold(:read_only) do |c|
      c.must_equal "read_only1"
      @pool.allocated(:read_only).must_equal(Thread.current=>"read_only1")
    end
    @pool.available_connections(:read_only).must_equal ["read_only1"]
    @pool.size(:read_only).must_equal 1
    @invoked_counts.must_equal(:read_only=>1)
  end
  
  it "#hold should only yield connections for the server requested" do
    @pool.hold(:read_only) do |c|
      c.must_equal "read_only1"
      @pool.allocated(:read_only).must_equal(Thread.current=>"read_only1")
      @pool.hold do |d|
        d.must_equal "default1"
        @pool.hold do |e|
          e.must_equal d
          @pool.hold(:read_only){|b| b.must_equal c}
        end
        @pool.allocated.must_equal(Thread.current=>"default1")
      end
    end
    @invoked_counts.must_equal(:read_only=>1, :default=>1)
  end
  
  it "#disconnect should disconnect from all servers" do
    @pool.hold(:read_only){}
    @pool.hold{}
    conns = []
    @pool.size.must_equal 1
    @pool.size(:read_only).must_equal 1
    meta_def(@pool.db, :disconnect_connection){|c| conns << c}
    @pool.disconnect
    conns.sort.must_equal %w'default1 read_only1'
    @pool.size.must_equal 0
    @pool.size(:read_only).must_equal 0
    @pool.hold(:read_only){|c| c.must_equal 'read_only2'}
    @pool.hold{|c| c.must_equal 'default2'}
  end
  
  it "#add_servers should add new servers to the pool" do
    pool = Sequel::ConnectionPool.get_pool(mock_db.call{|s| s}, :servers=>{:server1=>{}})
    
    pool.hold{}
    pool.hold(:server2){}
    pool.hold(:server3){}
    pool.hold(:server1) do
      pool.allocated.length.must_equal 0
      pool.allocated(:server1).length.must_equal 1
      pool.allocated(:server2).must_equal nil
      pool.allocated(:server3).must_equal nil
      pool.available_connections.length.must_equal 1
      pool.available_connections(:server1).length.must_equal 0
      pool.available_connections(:server2).must_equal nil
      pool.available_connections(:server3).must_equal nil

      pool.add_servers([:server2, :server3])
      pool.hold(:server2){}
      pool.hold(:server3) do 
        pool.allocated.length.must_equal 0
        pool.allocated(:server1).length.must_equal 1
        pool.allocated(:server2).length.must_equal 0
        pool.allocated(:server3).length.must_equal 1
        pool.available_connections.length.must_equal 1
        pool.available_connections(:server1).length.must_equal 0
        pool.available_connections(:server2).length.must_equal 1
        pool.available_connections(:server3).length.must_equal 0
      end
    end
  end

  it "#add_servers should ignore existing keys" do
    pool = Sequel::ConnectionPool.get_pool(mock_db.call{|s| s}, :servers=>{:server1=>{}})
    
    pool.allocated.length.must_equal 0
    pool.allocated(:server1).length.must_equal 0
    pool.available_connections.length.must_equal 0
    pool.available_connections(:server1).length.must_equal 0
    pool.hold do |c1| 
      c1.must_equal :default
      pool.allocated.length.must_equal 1
      pool.allocated(:server1).length.must_equal 0
      pool.available_connections.length.must_equal 0
      pool.available_connections(:server1).length.must_equal 0
      pool.hold(:server1) do |c2| 
        c2.must_equal :server1
        pool.allocated.length.must_equal 1
        pool.allocated(:server1).length.must_equal 1
        pool.available_connections.length.must_equal 0
        pool.available_connections(:server1).length.must_equal 0
        pool.add_servers([:default, :server1])
        pool.allocated.length.must_equal 1
        pool.allocated(:server1).length.must_equal 1
        pool.available_connections.length.must_equal 0
        pool.available_connections(:server1).length.must_equal 0
      end
      pool.allocated.length.must_equal 1
      pool.allocated(:server1).length.must_equal 0
      pool.available_connections.length.must_equal 0
      pool.available_connections(:server1).length.must_equal 1
      pool.add_servers([:default, :server1])
      pool.allocated.length.must_equal 1
      pool.allocated(:server1).length.must_equal 0
      pool.available_connections.length.must_equal 0
      pool.available_connections(:server1).length.must_equal 1
    end
    pool.allocated.length.must_equal 0
    pool.allocated(:server1).length.must_equal 0
    pool.available_connections.length.must_equal 1
    pool.available_connections(:server1).length.must_equal 1
    pool.add_servers([:default, :server1])
    pool.allocated.length.must_equal 0
    pool.allocated(:server1).length.must_equal 0
    pool.available_connections.length.must_equal 1
    pool.available_connections(:server1).length.must_equal 1
  end
  
  it "#remove_servers should disconnect available connections immediately" do
    pool = Sequel::ConnectionPool.get_pool(mock_db.call{|s| s}, :max_connections=>5, :servers=>{:server1=>{}})
    threads = []
    q, q1 = Queue.new, Queue.new
    5.times {|i| threads << Thread.new {pool.hold(:server1){|c| q1.push nil; q.pop}}}
    5.times{q1.pop}
    5.times{q.push nil}
    threads.each {|t| t.join}
    
    pool.size(:server1).must_equal 5
    pool.remove_servers([:server1])
    pool.size(:server1).must_equal 0
  end
  
  it "#remove_servers should disconnect connections in use as soon as they are returned to the pool" do
    dc = []
    pool = Sequel::ConnectionPool.get_pool(mock_db.call(proc{|c| dc << c}){|c| c}, :servers=>{:server1=>{}})
    c1 = nil
    pool.hold(:server1) do |c|
      pool.size(:server1).must_equal 1
      dc.must_equal []
      pool.remove_servers([:server1])
      pool.size(:server1).must_equal 0
      dc.must_equal []
      c1 = c
    end
    pool.size(:server1).must_equal 0
    dc.must_equal [c1]
  end
  
  it "#remove_servers should remove server related data structures immediately" do
    pool = Sequel::ConnectionPool.get_pool(mock_db.call{|s| s}, :servers=>{:server1=>{}})
    pool.available_connections(:server1).must_equal []
    pool.allocated(:server1).must_equal({})
    pool.remove_servers([:server1])
    pool.available_connections(:server1).must_equal nil
    pool.allocated(:server1).must_equal nil
  end
  
  it "#remove_servers should not allow the removal of the default server" do
    pool = Sequel::ConnectionPool.get_pool(mock_db.call{|s| s}, :servers=>{:server1=>{}})
    pool.remove_servers([:server1])
    proc{pool.remove_servers([:default])}.must_raise(Sequel::Error)
  end
  
  it "#remove_servers should ignore servers that have already been removed" do
    dc = []
    pool = Sequel::ConnectionPool.get_pool(mock_db.call(proc{|c| dc << c}){|c| c}, :servers=>{:server1=>{}})
    c1 = nil
    pool.hold(:server1) do |c|
      pool.size(:server1).must_equal 1
      dc.must_equal []
      pool.remove_servers([:server1])
      pool.remove_servers([:server1])
      pool.size(:server1).must_equal 0
      dc.must_equal []
      c1 = c
    end
    pool.size(:server1).must_equal 0
    dc.must_equal [c1]
  end
end

ST_CONNECTION_POOL_DEFAULTS = CONNECTION_POOL_DEFAULTS.merge(:single_threaded=>true)

describe "SingleConnectionPool" do
  before do
    @pool = Sequel::ConnectionPool.get_pool(mock_db.call{1234}, ST_CONNECTION_POOL_DEFAULTS)
  end
  
  it "should provide a #hold method" do
    conn = nil
    @pool.hold{|c| conn = c}
    conn.must_equal 1234
  end
  
  it "should provide a #disconnect method" do
    conn = nil
    x = nil
    pool = Sequel::ConnectionPool.get_pool(mock_db.call(proc{|c| conn = c}){1234}, ST_CONNECTION_POOL_DEFAULTS)
    pool.hold{|c| x = c}
    x.must_equal 1234
    pool.disconnect
    conn.must_equal 1234
  end
end

describe "A single threaded pool with multiple servers" do
  before do
    @max_size=2
    msp = proc{@max_size += 1}
    @pool = Sequel::ConnectionPool.get_pool(mock_db.call(proc{|c| msp.call}){|c| c}, ST_CONNECTION_POOL_DEFAULTS.merge(:servers=>{:read_only=>{}}))
  end
  
  it "should support preconnect method that immediately creates the maximum number of connections" do
    @pool.send(:preconnect)
    i = 0
    @pool.all_connections{|c1| i+=1}
    i.must_equal 2
  end

  it "should support preconnect method that immediately creates the maximum number of connections, ignoring concurrent param" do
    @pool.send(:preconnect, true)
    i = 0
    @pool.all_connections{|c1| i+=1}
    i.must_equal 2
  end

  it "#all_connections should return connections for all servers" do
    @pool.hold{}
    @pool.all_connections{|c1| c1.must_equal :default}
    a = []
    @pool.hold(:read_only) do
      @pool.all_connections{|c1| a << c1}
    end
    a.sort_by{|c| c.to_s}.must_equal [:default, :read_only]
  end
  
  it "#servers should return symbols for all servers" do
    @pool.servers.sort_by{|s| s.to_s}.must_equal [:default, :read_only]
  end
  
  it "#add_servers should add new servers to the pool" do
    @pool.hold(:blah){|c| c.must_equal :default}
    @pool.add_servers([:blah])
    @pool.hold(:blah){|c| c.must_equal :blah}
  end
  
  it "#add_servers should ignore keys already existing" do
    @pool.hold{|c| c.must_equal :default}
    @pool.hold(:read_only){|c| c.must_equal :read_only}
    @pool.add_servers([:default, :read_only])
    @pool.conn.must_equal :default
    @pool.conn(:read_only).must_equal :read_only
  end
  
  it "#remove_servers should remove servers from the pool" do
    @pool.hold(:read_only){|c| c.must_equal :read_only}
    @pool.remove_servers([:read_only])
    @pool.hold(:read_only){|c| c.must_equal :default}
  end
  
  it "#remove_servers should not allow the removal of the default server" do
    proc{@pool.remove_servers([:default])}.must_raise(Sequel::Error)
  end
  
  it "#remove_servers should disconnect connection immediately" do
    @pool.hold(:read_only){|c| c.must_equal :read_only}
    @pool.conn(:read_only).must_equal :read_only
    @pool.remove_servers([:read_only])
    @pool.conn(:read_only).must_equal nil
    @pool.hold{}
    @pool.conn(:read_only).must_equal :default
  end
  
  it "#remove_servers should ignore keys that do not exist" do
    @pool.remove_servers([:blah])
  end
  
  it "should use the :default server by default" do
    @pool.hold{|c| c.must_equal :default}
    @pool.conn.must_equal :default
  end
  
  it "should use the :default server an invalid server is used" do
    @pool.hold do |c1|
      c1.must_equal :default
      @pool.hold(:blah) do |c2|
        c2.must_equal c1
        @pool.hold(:blah2) do |c3|
          c2.must_equal c3
        end
      end
    end
  end

  it "should use the requested server if server is given" do
    @pool.hold(:read_only){|c| c.must_equal :read_only}
    @pool.conn(:read_only).must_equal :read_only
  end
  
  it "#hold should only yield connections for the server requested" do
    @pool.hold(:read_only) do |c|
      c.must_equal :read_only
      @pool.hold do |d|
        d.must_equal :default
        @pool.hold do |e|
          e.must_equal d
          @pool.hold(:read_only){|b| b.must_equal c}
        end
      end
    end
    @pool.conn.must_equal :default
    @pool.conn(:read_only).must_equal :read_only
  end
  
  it "#disconnect should disconnect from all servers" do
    @pool.hold(:read_only){}
    @pool.hold{}
    @pool.conn.must_equal :default
    @pool.conn(:read_only).must_equal :read_only
    @pool.disconnect
    @max_size.must_equal 4
    @pool.conn.must_equal nil
    @pool.conn(:read_only).must_equal nil
  end

  it ":disconnection_proc option should set the disconnection proc to use" do
    @max_size.must_equal 2
    proc{@pool.hold{raise Sequel::DatabaseDisconnectError}}.must_raise(Sequel::DatabaseDisconnectError)
    @max_size.must_equal 3
  end

  it "#hold should remove the connection if a DatabaseDisconnectError is raised" do
    @pool.instance_variable_get(:@conns).length.must_equal 0
    @pool.hold{}
    @pool.instance_variable_get(:@conns).length.must_equal 1
    proc{@pool.hold{raise Sequel::DatabaseDisconnectError}}.must_raise(Sequel::DatabaseDisconnectError)
    @pool.instance_variable_get(:@conns).length.must_equal 0
  end
end

AllConnectionPoolClassesSpecs = shared_description do
  it "should have pool_type return a symbol" do
    @class.new(mock_db.call{123}, {}).pool_type.must_be_kind_of(Symbol)
  end

  it "should have all_connections yield current and available connections" do
    p = @class.new(mock_db.call{123}, {})
    p.hold{|c| p.all_connections{|c1| c.must_equal c1}}
  end

  it "should have a size method that gives the current size of the pool" do
    p = @class.new(mock_db.call{123}, {})
    p.size.must_equal 0
    p.hold{}
    p.size.must_equal 1
  end

  it "should have a max_size method that gives the maximum size of the pool" do
    @class.new(mock_db.call{123}, {}).max_size.must_be :>=,  1
  end

  it "should support preconnect method that immediately creates the maximum number of connections" do
    p = @class.new(mock_db.call{123}, {})
    p.send(:preconnect)
    i = 0
    p.all_connections{|c1| i+=1}
    i.must_equal p.max_size
  end

  it "should support preconnect method that immediately creates the maximum number of connections concurrently" do
    p = @class.new(mock_db.call{123}, {})
    p.send(:preconnect, true)
    i = 0
    p.all_connections{|c1| i+=1}
    i.must_equal p.max_size
  end

  it "should be able to modify after_connect proc after the pool is created" do
    a = []
    p = @class.new(mock_db.call{123}, {})
    p.after_connect = pr = proc{|c| a << c}
    p.after_connect.must_equal pr
    a.must_equal []
    p.hold{}
    a.must_equal [123]
  end

  it "should not raise an error when disconnecting twice" do
    c = @class.new(mock_db.call{123}, {})
    c.disconnect
    c.disconnect
  end
  
  it "should yield a connection created by the initialize block to hold" do
    x = nil
    @class.new(mock_db.call{123}, {}).hold{|c| x = c}
    x.must_equal 123
  end
  
  it "should have the initialize block accept a shard/server argument" do
    x = nil
    @class.new(mock_db.call{|c| [c, c]}, {}).hold{|c| x = c}
    x.must_equal [:default, :default]
  end
  
  it "should have respect an :after_connect proc that is called with each newly created connection" do
    x = nil
    @class.new(mock_db.call{123}, :after_connect=>proc{|c| x = [c, c]}).hold{}
    x.must_equal [123, 123]
    @class.new(mock_db.call{123}, :after_connect=>lambda{|c| x = [c, c]}).hold{}
    x.must_equal [123, 123]
    @class.new(mock_db.call{123}, :after_connect=>proc{|c, s| x = [c, s]}).hold{}
    x.must_equal [123, :default]
    @class.new(mock_db.call{123}, :after_connect=>lambda{|c, s| x = [c, s]}).hold{}
    x.must_equal [123, :default]
  end
  
  it "should raise a DatabaseConnectionError if the connection raises an exception" do
    proc{@class.new(mock_db.call{|c| raise Exception}, {}).hold{}}.must_raise(Sequel::DatabaseConnectionError)
  end
  
  it "should raise a DatabaseConnectionError if the initialize block returns nil" do
    proc{@class.new(mock_db.call{}, {}).hold{}}.must_raise(Sequel::DatabaseConnectionError)
  end
  
  it "should call the disconnection_proc option if the hold block raises a DatabaseDisconnectError" do
    x = nil
    proc{@class.new(mock_db.call(proc{|c| x = c}){123}).hold{raise Sequel::DatabaseDisconnectError}}.must_raise(Sequel::DatabaseDisconnectError)
    x.must_equal 123
  end
  
  it "should have a disconnect method that disconnects the connection" do
    x = nil
    c = @class.new(mock_db.call(proc{|c1| x = c1}){123})
    c.hold{}
    x.must_equal nil
    c.disconnect
    x.must_equal 123
  end
  
  it "should have a reentrent hold method" do
    o = Object.new
    c = @class.new(mock_db.call{o}, {})
    c.hold do |x|
      x.must_equal o
      c.hold do |x1|
        x1.must_equal o
        c.hold do |x2|
          x2.must_equal o
        end
      end
    end
  end
  
  it "should have a servers method that returns an array of shard/server symbols" do
    @class.new(mock_db.call{123}, {}).servers.must_equal [:default]
  end
  
  it "should have a servers method that returns an array of shard/server symbols" do
    c = @class.new(mock_db.call{123}, {})
    c.size.must_equal 0
    c.hold{}
    c.size.must_equal 1
  end
end

Sequel::ConnectionPool::CONNECTION_POOL_MAP.keys.each do |k, v|
  opts = {:single_threaded=>k, :servers=>(v ? {} : nil)}
  describe "Connection pool with #{opts.inspect}" do
    before(:all) do
      Sequel::ConnectionPool.send(:get_pool, mock_db.call, opts)
    end
    before do
      @class = Sequel::ConnectionPool.send(:connection_pool_class, opts)
    end

    include AllConnectionPoolClassesSpecs
  end
end
