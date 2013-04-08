require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')
CONNECTION_POOL_DEFAULTS = {:pool_timeout=>5, :pool_sleep_time=>0.001, :max_connections=>4}

mock_db = lambda do |*a, &b|
  db = Sequel.mock
  db.meta_def(:connect){|c| b.arity == 1 ? b.call(c) : b.call} if b
  if b2 = a.shift
    db.meta_def(:disconnect_connection){|c| b2.arity == 1 ? b2.call(c) : b2.call}
  end
  db
end

describe "An empty ConnectionPool" do
  before do
    @cpool = Sequel::ConnectionPool.get_pool(mock_db.call, CONNECTION_POOL_DEFAULTS)
  end

  specify "should have no available connections" do
    @cpool.available_connections.should == []
  end

  specify "should have no allocated connections" do
    @cpool.allocated.should == {}
  end

  specify "should have a created_count of zero" do
    @cpool.created_count.should == 0
  end
end

describe "ConnectionPool options" do
  specify "should support string option values" do
    cpool = Sequel::ConnectionPool.get_pool(mock_db.call, {:max_connections=>'5', :pool_timeout=>'3', :pool_sleep_time=>'0.01'})
    cpool.max_size.should == 5
    cpool.instance_variable_get(:@timeout).should == 3
    cpool.instance_variable_get(:@sleep_time).should == 0.01
  end

  specify "should raise an error unless size is positive" do
    lambda{Sequel::ConnectionPool.get_pool(mock_db.call{1}, :max_connections=>0)}.should raise_error(Sequel::Error)
    lambda{Sequel::ConnectionPool.get_pool(mock_db.call{1}, :max_connections=>-10)}.should raise_error(Sequel::Error)
    lambda{Sequel::ConnectionPool.get_pool(mock_db.call{1}, :max_connections=>'-10')}.should raise_error(Sequel::Error)
    lambda{Sequel::ConnectionPool.get_pool(mock_db.call{1}, :max_connections=>'0')}.should raise_error(Sequel::Error)
  end
end

describe "A connection pool handling connections" do
  before do
    @max_size = 2
    msp = proc{@max_size=3}
    @cpool = Sequel::ConnectionPool.get_pool(mock_db.call(proc{|c| msp.call}){:got_connection}, CONNECTION_POOL_DEFAULTS.merge(:max_connections=>@max_size))
  end

  specify "#hold should increment #created_count" do
    @cpool.hold do
      @cpool.created_count.should == 1
      @cpool.hold {@cpool.hold {@cpool.created_count.should == 1}}
      Thread.new{@cpool.hold {@cpool.created_count.should == 2}}.join
    end
  end

  specify "#hold should add the connection to the #allocated array" do
    @cpool.hold do
      @cpool.allocated.size.should == 1
  
      @cpool.allocated.should == {Thread.current=>:got_connection}
    end
  end

  specify "#hold should yield a new connection" do
    @cpool.hold {|conn| conn.should == :got_connection}
  end

  specify "a connection should be de-allocated after it has been used in #hold" do
    @cpool.hold {}
    @cpool.allocated.size.should == 0
  end

  specify "#hold should return the value of its block" do
    @cpool.hold {:block_return}.should == :block_return
  end

  if RUBY_VERSION < '1.9.0' and !defined?(RUBY_ENGINE)
    specify "#hold should remove dead threads from the pool if it reaches its max_size" do
      Thread.new{@cpool.hold{Thread.current.exit!}}.join
      @cpool.allocated.keys.map{|t| t.alive?}.should == [false]

      Thread.new{@cpool.hold{Thread.current.exit!}}.join
      @cpool.allocated.keys.map{|t| t.alive?}.should == [false, false]

      Thread.new{@cpool.hold{}}.join
      @cpool.allocated.should == {}
    end
  end

  specify "#make_new should not make more than max_size connections" do
    q = Queue.new
    50.times{Thread.new{@cpool.hold{q.pop}}}
    50.times{q.push nil}
    @cpool.created_count.should <= @max_size
  end

  specify ":disconnection_proc option should set the disconnection proc to use" do
    @max_size.should == 2
    proc{@cpool.hold{raise Sequel::DatabaseDisconnectError}}.should raise_error(Sequel::DatabaseDisconnectError)
    @max_size.should == 3
  end

  specify "#hold should remove the connection if a DatabaseDisconnectError is raised" do
    @cpool.created_count.should == 0
    q, q1 = Queue.new, Queue.new
    @cpool.hold{Thread.new{@cpool.hold{q1.pop; q.push nil}; q1.pop; q.push nil}; q1.push nil; q.pop; q1.push nil; q.pop}
    @cpool.created_count.should == 2
    proc{@cpool.hold{raise Sequel::DatabaseDisconnectError}}.should raise_error(Sequel::DatabaseDisconnectError)
    @cpool.created_count.should == 1
    proc{@cpool.hold{raise Sequel::DatabaseDisconnectError}}.should raise_error(Sequel::DatabaseDisconnectError)
    @cpool.created_count.should == 0
    proc{@cpool.hold{raise Sequel::DatabaseDisconnectError}}.should raise_error(Sequel::DatabaseDisconnectError)
    @cpool.created_count.should == 0
  end
end

describe "A connection pool handling connection errors" do 
  specify "#hold should raise a Sequel::DatabaseConnectionError if an exception is raised by the connection_proc" do
    cpool = Sequel::ConnectionPool.get_pool(CONNECTION_POOL_DEFAULTS){raise Interrupt}
    proc{cpool.hold{:block_return}}.should raise_error(Sequel::DatabaseConnectionError)
    cpool.created_count.should == 0
  end

  specify "#hold should raise a Sequel::DatabaseConnectionError if nil is returned by the connection_proc" do
    cpool = Sequel::ConnectionPool.get_pool(CONNECTION_POOL_DEFAULTS){nil}
    proc{cpool.hold{:block_return}}.should raise_error(Sequel::DatabaseConnectionError)
    cpool.created_count.should == 0
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
  
  specify "should pass the result of the connection maker proc to the supplied block" do
    res = nil
    @pool.hold {|c| res = c}
    res.should be_a_kind_of(@c)
    res.value.should == 1
    @pool.hold {|c| res = c}
    res.should be_a_kind_of(@c)
    res.value.should == 1 # the connection maker is invoked only once
  end
  
  specify "should be re-entrant by the same thread" do
    cc = nil
    @pool.hold {|c| @pool.hold {|c1| @pool.hold {|c2| cc = c2}}}
    cc.should be_a_kind_of(@c)
  end
  
  specify "should catch exceptions and reraise them" do
    proc {@pool.hold {|c| c.foobar}}.should raise_error(NoMethodError)
  end
end

describe "A connection pool with a max size of 1" do
  before do
    @invoked_count = 0
    icp = proc{@invoked_count += 1}
    @pool = Sequel::ConnectionPool.get_pool(mock_db.call{icp.call; 'herro'}, CONNECTION_POOL_DEFAULTS.merge(:max_connections=>1))
  end
  
  specify "should let only one thread access the connection at any time" do
    cc,c1, c2 = nil
    q, q1 = Queue.new, Queue.new
    
    t1 = Thread.new {@pool.hold {|c| cc = c; c1 = c.dup; q1.push nil; q.pop}}
    q1.pop
    cc.should == 'herro'
    c1.should == 'herro'
    
    t2 = Thread.new {@pool.hold {|c| c2 = c.dup; q1.push nil; q.pop;}}
    
    # connection held by t1
    t1.should be_alive
    t2.should be_alive
    
    cc.should == 'herro'
    c1.should == 'herro'
    c2.should be_nil
    
    @pool.available_connections.should be_empty
    @pool.allocated.should == {t1=>cc}

    cc.gsub!('rr', 'll')
    q.push nil
    q1.pop

    t1.join
    t2.should be_alive
    
    c2.should == 'hello'

    @pool.available_connections.should be_empty
    @pool.allocated.should == {t2=>cc}
    
    #connection released
    q.push nil
    t2.join
    
    @invoked_count.should == 1
    @pool.size.should == 1
    @pool.available_connections.should == [cc]
    @pool.allocated.should be_empty
  end
  
  specify "should let the same thread reenter #hold" do
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
    c1.should == 'herro'
    c2.should == 'herro'
    c3.should == 'herro'
    
    @invoked_count.should == 1
    @pool.size.should == 1
    @pool.available_connections.size.should == 1
    @pool.allocated.should be_empty
  end
end

shared_examples_for "A threaded connection pool" do  
  specify "should not have all_connections yield connections allocated to other threads" do
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
      pool.all_connections{|c| c.should == c1}
      q.push nil
    end
    t.join
  end

  specify "should wait until a connection is available if all are checked out" do
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
    proc{pool.hold{}}.should raise_error(Sequel::PoolTimeout)
    q.push nil
    t.join
  end

  specify "should not have all_connections yield all available connections" do
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
    a.sort.should == b.sort
  end

  specify "should raise a PoolTimeout error if a connection couldn't be acquired before timeout" do
    q, q1 = Queue.new, Queue.new
    pool = Sequel::ConnectionPool.get_pool(mock_db.call(&@icpp), @cp_opts.merge(:max_connections=>1, :pool_timeout=>0))
    t = Thread.new{pool.hold{|c| q1.push nil; q.pop}}
    q1.pop
    proc{pool.hold{|c|}}.should raise_error(Sequel::PoolTimeout)
    q.push nil
    t.join
  end
  
  it "should not add a disconnected connection back to the pool if the disconnection_proc raises an error" do
    pool = Sequel::ConnectionPool.get_pool(mock_db.call(proc{|c| raise Sequel::Error}, &@icpp), @cp_opts.merge(:max_connections=>1, :pool_timeout=>0))
    proc{pool.hold{raise Sequel::DatabaseDisconnectError}}.should raise_error(Sequel::Error)
    pool.available_connections.length.should == 0
  end

  specify "should let five threads simultaneously access separate connections" do
    cc = {}
    threads = []
    q, q1, q2 = Queue.new, Queue.new, Queue.new
    
    5.times{|i| threads << Thread.new{@pool.hold{|c| q.pop; cc[i] = c; q1.push nil; q2.pop}}; q.push nil; q1.pop}
    threads.each {|t| t.should be_alive}
    cc.size.should == 5
    @invoked_count.should == 5
    @pool.size.should == 5
    @pool.available_connections.should be_empty

    h = {}
    i = 0
    threads.each{|t| h[t] = (i+=1)}
    @pool.allocated.should == h
    @pool.available_connections.should == []
    5.times{q2.push nil}
    threads.each{|t| t.join}
    
    @pool.available_connections.size.should == 5
    @pool.allocated.should be_empty
  end
  
  specify "should block threads until a connection becomes available" do
    cc = {}
    threads = []
    q, q1 = Queue.new, Queue.new
    
    5.times{|i| threads << Thread.new{@pool.hold{|c| cc[i] = c; q1.push nil; q.pop}}}
    5.times{q1.pop}
    threads.each {|t| t.should be_alive}
    @pool.available_connections.should be_empty

    3.times {|i| threads << Thread.new {@pool.hold {|c| cc[i + 5] = c; q1.push nil}}}
    
    threads[5].should be_alive
    threads[6].should be_alive
    threads[7].should be_alive
    cc.size.should == 5
    cc[5].should be_nil
    cc[6].should be_nil
    cc[7].should be_nil
    
    5.times{q.push nil}
    5.times{|i| threads[i].join}
    3.times{q1.pop}
    3.times{|i| threads[i+5].join}
    
    threads.each {|t| t.should_not be_alive}
    
    @pool.size.should == 5
    @invoked_count.should == 5
    @pool.available_connections.size.should == 5
    @pool.allocated.should be_empty
  end

  specify "should store connections in a stack by default" do
    c2 = nil
    c = @pool.hold{|cc| Thread.new{@pool.hold{|cc2| c2 = cc2}}.join; cc}
    @pool.size.should == 2
    @pool.hold{|cc| cc.should == c}
    @pool.hold{|cc| cc.should == c}
    @pool.hold do |cc|
      cc.should == c
      Thread.new{@pool.hold{|cc2| cc2.should == c2}}
    end
  end

  specify "should store connections in a queue if :connection_handling=>:queue" do
    @pool = Sequel::ConnectionPool.get_pool(mock_db.call(&@icpp), @cp_opts.merge(:connection_handling=>:queue))
    c2 = nil
    c = @pool.hold{|cc| Thread.new{@pool.hold{|cc2| c2 = cc2}}.join; cc}
    @pool.size.should == 2
    @pool.hold{|cc| cc.should == c2}
    @pool.hold{|cc| cc.should == c}
    @pool.hold do |cc|
      cc.should == c2
      Thread.new{@pool.hold{|cc2| cc2.should == c}}
    end
  end

  specify "should not store connections if :connection_handling=>:disconnect" do
    @pool = Sequel::ConnectionPool.get_pool(mock_db.call(&@icpp), @cp_opts.merge(:connection_handling=>:disconnect))
    d = []
    @pool.db.meta_def(:disconnect_connection){|c| d << c}
    @pool.hold do |cc|
      cc.should == 1
      Thread.new{@pool.hold{|cc2| cc2.should == 2}}.join
      d.should == [2]
      @pool.hold{|cc3| cc3.should == 1}
    end
    @pool.size.should == 0
    d.should == [2, 1]

    @pool.hold{|cc| cc.should == 3}
    @pool.size.should == 0
    d.should == [2, 1, 3]

    @pool.hold{|cc| cc.should == 4}
    @pool.size.should == 0
    d.should == [2, 1, 3, 4]
  end
end

describe "Threaded Unsharded Connection Pool" do
  before do
    @invoked_count = 0
    @icpp = proc{@invoked_count += 1}
    @cp_opts = CONNECTION_POOL_DEFAULTS.merge(:max_connections=>5)
    @pool = Sequel::ConnectionPool.get_pool(mock_db.call(&@icpp), @cp_opts)
  end
  
  it_should_behave_like "A threaded connection pool"
end

describe "Threaded Sharded Connection Pool" do
  before do
    @invoked_count = 0
    @icpp = proc{@invoked_count += 1}
    @cp_opts = CONNECTION_POOL_DEFAULTS.merge(:max_connections=>5, :servers=>{})
    @pool = Sequel::ConnectionPool.get_pool(mock_db.call(&@icpp), @cp_opts)
  end

  it_should_behave_like "A threaded connection pool"
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
  
  specify "should invoke the given block for each available connection" do
    @pool.size.should == 5
    @pool.available_connections.size.should == 5
    @pool.available_connections.each {|c| c[:id].should_not be_nil}
    conns = []
    @pool.db.meta_def(:disconnect_connection){|c| conns << c}
    @pool.disconnect
    conns.size.should == 5
  end
  
  specify "should remove all available connections" do
    @pool.size.should == 5
    @pool.disconnect
    @pool.size.should == 0
  end

  specify "should disconnect connections in use as soon as they are no longer in use" do
    @pool.size.should == 5
    @pool.hold do |conn|
      @pool.available_connections.size.should == 4
      @pool.available_connections.each {|c| c.should_not be(conn)}
      conns = []
      @pool.db.meta_def(:disconnect_connection){|c| conns << c}
      @pool.disconnect
      conns.size.should == 4
      @pool.size.should == 1
    end
    @pool.size.should == 0
  end
end

describe "A connection pool with multiple servers" do
  before do
    ic = @invoked_counts = Hash.new(0)
    @pool = Sequel::ConnectionPool.get_pool(mock_db.call{|server| "#{server}#{ic[server] += 1}"}, CONNECTION_POOL_DEFAULTS.merge(:servers=>{:read_only=>{}}))
  end
  
  specify "#all_connections should return connections for all servers" do
    @pool.hold{}
    @pool.all_connections{|c1| c1.should == "default1"}
    a = []
    @pool.hold(:read_only) do |c|
      @pool.all_connections{|c1| a << c1}
    end
    a.sort_by{|c| c.to_s}.should == ["default1", "read_only1"]
  end
  
  specify "#servers should return symbols for all servers" do
    @pool.servers.sort_by{|s| s.to_s}.should == [:default, :read_only]
  end

  specify "should use the :default server by default" do
    @pool.size.should == 0
    @pool.hold do |c|
      c.should == "default1"
      @pool.allocated.should == {Thread.current=>"default1"}
    end
    @pool.available_connections.should == ["default1"]
    @pool.size.should == 1
    @invoked_counts.should == {:default=>1}
  end
  
  specify "should use the :default server an invalid server is used" do
    @pool.hold do |c1|
      c1.should == "default1"
      @pool.hold(:blah) do |c2|
        c2.should == c1
        @pool.hold(:blah2) do |c3|
          c2.should == c3
        end
      end
    end
  end

  specify "should support a :servers_hash option used for converting the server argument" do
    ic = @invoked_counts
    @pool = Sequel::ConnectionPool.get_pool(mock_db.call{|server| "#{server}#{ic[server] += 1}"}, CONNECTION_POOL_DEFAULTS.merge(:servers_hash=>Hash.new(:read_only), :servers=>{:read_only=>{}}))
    @pool.hold(:blah) do |c1|
      c1.should == "read_only1"
      @pool.hold(:blah) do |c2|
        c2.should == c1
        @pool.hold(:blah2) do |c3|
          c2.should == c3
        end
      end
    end

    @pool = Sequel::ConnectionPool.get_pool(mock_db.call{|server| "#{server}#{ic[server] += 1}"}, CONNECTION_POOL_DEFAULTS.merge(:servers_hash=>Hash.new{|h,k| raise Sequel::Error}, :servers=>{:read_only=>{}}))
    proc{@pool.hold(:blah){|c1|}}.should raise_error(Sequel::Error)
  end

  specify "should use the requested server if server is given" do
    @pool.size(:read_only).should == 0
    @pool.hold(:read_only) do |c|
      c.should == "read_only1"
      @pool.allocated(:read_only).should == {Thread.current=>"read_only1"}
    end
    @pool.available_connections(:read_only).should == ["read_only1"]
    @pool.size(:read_only).should == 1
    @invoked_counts.should == {:read_only=>1}
  end
  
  specify "#hold should only yield connections for the server requested" do
    @pool.hold(:read_only) do |c|
      c.should == "read_only1"
      @pool.allocated(:read_only).should == {Thread.current=>"read_only1"}
      @pool.hold do |d|
        d.should == "default1"
        @pool.hold do |e|
          e.should == d
          @pool.hold(:read_only){|b| b.should == c}
        end
        @pool.allocated.should == {Thread.current=>"default1"}
      end
    end
    @invoked_counts.should == {:read_only=>1, :default=>1}
  end
  
  specify "#disconnect should disconnect from all servers" do
    @pool.hold(:read_only){}
    @pool.hold{}
    conns = []
    @pool.size.should == 1
    @pool.size(:read_only).should == 1
    @pool.db.meta_def(:disconnect_connection){|c| conns << c}
    @pool.disconnect
    conns.sort.should == %w'default1 read_only1'
    @pool.size.should == 0
    @pool.size(:read_only).should == 0
    @pool.hold(:read_only){|c| c.should == 'read_only2'}
    @pool.hold{|c| c.should == 'default2'}
  end
  
  specify "#add_servers should add new servers to the pool" do
    pool = Sequel::ConnectionPool.get_pool(mock_db.call{|s| s}, :servers=>{:server1=>{}})
    
    pool.hold{}
    pool.hold(:server2){}
    pool.hold(:server3){}
    pool.hold(:server1) do
      pool.allocated.length.should == 0
      pool.allocated(:server1).length.should == 1
      pool.allocated(:server2).should == nil
      pool.allocated(:server3).should == nil
      pool.available_connections.length.should == 1
      pool.available_connections(:server1).length.should == 0
      pool.available_connections(:server2).should == nil
      pool.available_connections(:server3).should == nil

      pool.add_servers([:server2, :server3])
      pool.hold(:server2){}
      pool.hold(:server3) do 
        pool.allocated.length.should == 0
        pool.allocated(:server1).length.should == 1
        pool.allocated(:server2).length.should == 0
        pool.allocated(:server3).length.should == 1
        pool.available_connections.length.should == 1
        pool.available_connections(:server1).length.should == 0
        pool.available_connections(:server2).length.should == 1
        pool.available_connections(:server3).length.should == 0
      end
    end
  end

  specify "#add_servers should ignore existing keys" do
    pool = Sequel::ConnectionPool.get_pool(mock_db.call{|s| s}, :servers=>{:server1=>{}})
    
    pool.allocated.length.should == 0
    pool.allocated(:server1).length.should == 0
    pool.available_connections.length.should == 0
    pool.available_connections(:server1).length.should == 0
    pool.hold do |c1| 
      c1.should == :default
      pool.allocated.length.should == 1
      pool.allocated(:server1).length.should == 0
      pool.available_connections.length.should == 0
      pool.available_connections(:server1).length.should == 0
      pool.hold(:server1) do |c2| 
        c2.should == :server1
        pool.allocated.length.should == 1
        pool.allocated(:server1).length.should == 1
        pool.available_connections.length.should == 0
        pool.available_connections(:server1).length.should == 0
        pool.add_servers([:default, :server1])
        pool.allocated.length.should == 1
        pool.allocated(:server1).length.should == 1
        pool.available_connections.length.should == 0
        pool.available_connections(:server1).length.should == 0
      end
      pool.allocated.length.should == 1
      pool.allocated(:server1).length.should == 0
      pool.available_connections.length.should == 0
      pool.available_connections(:server1).length.should == 1
      pool.add_servers([:default, :server1])
      pool.allocated.length.should == 1
      pool.allocated(:server1).length.should == 0
      pool.available_connections.length.should == 0
      pool.available_connections(:server1).length.should == 1
    end
    pool.allocated.length.should == 0
    pool.allocated(:server1).length.should == 0
    pool.available_connections.length.should == 1
    pool.available_connections(:server1).length.should == 1
    pool.add_servers([:default, :server1])
    pool.allocated.length.should == 0
    pool.allocated(:server1).length.should == 0
    pool.available_connections.length.should == 1
    pool.available_connections(:server1).length.should == 1
  end
  
  specify "#remove_servers should disconnect available connections immediately" do
    pool = Sequel::ConnectionPool.get_pool(mock_db.call{|s| s}, :max_connections=>5, :servers=>{:server1=>{}})
    threads = []
    q, q1 = Queue.new, Queue.new
    5.times {|i| threads << Thread.new {pool.hold(:server1){|c| q1.push nil; q.pop}}}
    5.times{q1.pop}
    5.times{q.push nil}
    threads.each {|t| t.join}
    
    pool.size(:server1).should == 5
    pool.remove_servers([:server1])
    pool.size(:server1).should == 0
  end
  
  specify "#remove_servers should disconnect connections in use as soon as they are returned to the pool" do
    dc = []
    pool = Sequel::ConnectionPool.get_pool(mock_db.call(proc{|c| dc << c}){|c| c}, :servers=>{:server1=>{}})
    c1 = nil
    pool.hold(:server1) do |c|
      pool.size(:server1).should == 1
      dc.should == []
      pool.remove_servers([:server1])
      pool.size(:server1).should == 0
      dc.should == []
      c1 = c
    end
    pool.size(:server1).should == 0
    dc.should == [c1]
  end
  
  specify "#remove_servers should remove server related data structures immediately" do
    pool = Sequel::ConnectionPool.get_pool(mock_db.call{|s| s}, :servers=>{:server1=>{}})
    pool.available_connections(:server1).should == []
    pool.allocated(:server1).should == {}
    pool.remove_servers([:server1])
    pool.available_connections(:server1).should == nil
    pool.allocated(:server1).should == nil
  end
  
  specify "#remove_servers should not allow the removal of the default server" do
    pool = Sequel::ConnectionPool.get_pool(mock_db.call{|s| s}, :servers=>{:server1=>{}})
    proc{pool.remove_servers([:server1])}.should_not raise_error
    proc{pool.remove_servers([:default])}.should raise_error(Sequel::Error)
  end
  
  specify "#remove_servers should ignore servers that have already been removed" do
    dc = []
    pool = Sequel::ConnectionPool.get_pool(mock_db.call(proc{|c| dc << c}){|c| c}, :servers=>{:server1=>{}})
    c1 = nil
    pool.hold(:server1) do |c|
      pool.size(:server1).should == 1
      dc.should == []
      pool.remove_servers([:server1])
      pool.remove_servers([:server1])
      pool.size(:server1).should == 0
      dc.should == []
      c1 = c
    end
    pool.size(:server1).should == 0
    dc.should == [c1]
  end
end

ST_CONNECTION_POOL_DEFAULTS = CONNECTION_POOL_DEFAULTS.merge(:single_threaded=>true)

describe "SingleConnectionPool" do
  before do
    @pool = Sequel::ConnectionPool.get_pool(mock_db.call{1234}, ST_CONNECTION_POOL_DEFAULTS)
  end
  
  specify "should provide a #hold method" do
    conn = nil
    @pool.hold{|c| conn = c}
    conn.should == 1234
  end
  
  specify "should provide a #disconnect method" do
    conn = nil
    x = nil
    pool = Sequel::ConnectionPool.get_pool(mock_db.call(proc{|c| conn = c}){1234}, ST_CONNECTION_POOL_DEFAULTS)
    pool.hold{|c| x = c}
    x.should == 1234
    pool.disconnect
    conn.should == 1234
  end
end

describe "A single threaded pool with multiple servers" do
  before do
    @max_size=2
    msp = proc{@max_size += 1}
    @pool = Sequel::ConnectionPool.get_pool(mock_db.call(proc{|c| msp.call}){|c| c}, ST_CONNECTION_POOL_DEFAULTS.merge(:servers=>{:read_only=>{}}))
  end
  
  specify "#all_connections should return connections for all servers" do
    @pool.hold{}
    @pool.all_connections{|c1| c1.should == :default}
    a = []
    @pool.hold(:read_only) do
      @pool.all_connections{|c1| a << c1}
    end
    a.sort_by{|c| c.to_s}.should == [:default, :read_only]
  end
  
  specify "#servers should return symbols for all servers" do
    @pool.servers.sort_by{|s| s.to_s}.should == [:default, :read_only]
  end
  
  specify "#add_servers should add new servers to the pool" do
    @pool.hold(:blah){|c| c.should == :default}
    @pool.add_servers([:blah])
    @pool.hold(:blah){|c| c.should == :blah}
  end
  
  specify "#add_servers should ignore keys already existing" do
    @pool.hold{|c| c.should == :default}
    @pool.hold(:read_only){|c| c.should == :read_only}
    @pool.add_servers([:default, :read_only])
    @pool.conn.should == :default
    @pool.conn(:read_only).should == :read_only
  end
  
  specify "#remove_servers should remove servers from the pool" do
    @pool.hold(:read_only){|c| c.should == :read_only}
    @pool.remove_servers([:read_only])
    @pool.hold(:read_only){|c| c.should == :default}
  end
  
  specify "#remove_servers should not allow the removal of the default server" do
    proc{@pool.remove_servers([:default])}.should raise_error(Sequel::Error)
  end
  
  specify "#remove_servers should disconnect connection immediately" do
    @pool.hold(:read_only){|c| c.should == :read_only}
    @pool.conn(:read_only).should == :read_only
    @pool.remove_servers([:read_only])
    @pool.conn(:read_only).should == nil
    @pool.hold{}
    @pool.conn(:read_only).should == :default
  end
  
  specify "#remove_servers should ignore keys that do not exist" do
    proc{@pool.remove_servers([:blah])}.should_not raise_error
  end
  
  specify "should use the :default server by default" do
    @pool.hold{|c| c.should == :default}
    @pool.conn.should == :default
  end
  
  specify "should use the :default server an invalid server is used" do
    @pool.hold do |c1|
      c1.should == :default
      @pool.hold(:blah) do |c2|
        c2.should == c1
        @pool.hold(:blah2) do |c3|
          c2.should == c3
        end
      end
    end
  end

  specify "should use the requested server if server is given" do
    @pool.hold(:read_only){|c| c.should == :read_only}
    @pool.conn(:read_only).should == :read_only
  end
  
  specify "#hold should only yield connections for the server requested" do
    @pool.hold(:read_only) do |c|
      c.should == :read_only
      @pool.hold do |d|
        d.should == :default
        @pool.hold do |e|
          e.should == d
          @pool.hold(:read_only){|b| b.should == c}
        end
      end
    end
    @pool.conn.should == :default
    @pool.conn(:read_only).should == :read_only
  end
  
  specify "#disconnect should disconnect from all servers" do
    @pool.hold(:read_only){}
    @pool.hold{}
    @pool.conn.should == :default
    @pool.conn(:read_only).should == :read_only
    @pool.disconnect
    @max_size.should == 4
    @pool.conn.should == nil
    @pool.conn(:read_only).should == nil
  end

  specify ":disconnection_proc option should set the disconnection proc to use" do
    @max_size.should == 2
    proc{@pool.hold{raise Sequel::DatabaseDisconnectError}}.should raise_error(Sequel::DatabaseDisconnectError)
    @max_size.should == 3
  end

  specify "#hold should remove the connection if a DatabaseDisconnectError is raised" do
    @pool.instance_variable_get(:@conns).length.should == 0
    @pool.hold{}
    @pool.instance_variable_get(:@conns).length.should == 1
    proc{@pool.hold{raise Sequel::DatabaseDisconnectError}}.should raise_error(Sequel::DatabaseDisconnectError)
    @pool.instance_variable_get(:@conns).length.should == 0
  end
end

shared_examples_for "All connection pools classes" do
  specify "should have pool_type return a symbol" do
    @class.new(mock_db.call{123}, {}).pool_type.should be_a_kind_of(Symbol)
  end

  specify "should have all_connections yield current and available connections" do
    p = @class.new(mock_db.call{123}, {})
    p.hold{|c| p.all_connections{|c1| c.should == c1}}
  end

  specify "should be able to modify after_connect proc after the pool is created" do
    a = []
    p = @class.new(mock_db.call{123}, {})
    p.after_connect = pr = proc{|c| a << c}
    p.after_connect.should == pr
    a.should == []
    p.hold{}
    a.should == [123]
  end

  specify "should not raise an error when disconnecting twice" do
    c = @class.new(mock_db.call{123}, {})
    proc{c.disconnect}.should_not raise_error
    proc{c.disconnect}.should_not raise_error
  end
  
  specify "should yield a connection created by the initialize block to hold" do
    x = nil
    @class.new(mock_db.call{123}, {}).hold{|c| x = c}
    x.should == 123
  end
  
  specify "should have the initialize block accept a shard/server argument" do
    x = nil
    @class.new(mock_db.call{|c| [c, c]}, {}).hold{|c| x = c}
    x.should == [:default, :default]
  end
  
  specify "should have respect an :after_connect proc that is called with each newly created connection" do
    x = nil
    @class.new(mock_db.call{123}, :after_connect=>proc{|c| x = [c, c]}).hold{}
    x.should == [123, 123]
  end
  
  specify "should raise a DatabaseConnectionError if the connection raises an exception" do
    proc{@class.new(mock_db.call{|c| raise Exception}, {}).hold{}}.should raise_error(Sequel::DatabaseConnectionError)
  end
  
  specify "should raise a DatabaseConnectionError if the initialize block returns nil" do
    proc{@class.new(mock_db.call{}, {}).hold{}}.should raise_error(Sequel::DatabaseConnectionError)
  end
  
  specify "should call the disconnection_proc option if the hold block raises a DatabaseDisconnectError" do
    x = nil
    proc{@class.new(mock_db.call(proc{|c| x = c}){123}).hold{raise Sequel::DatabaseDisconnectError}}.should raise_error(Sequel::DatabaseDisconnectError)
    x.should == 123
  end
  
  specify "should have a disconnect method that disconnects the connection" do
    x = nil
    c = @class.new(mock_db.call(proc{|c1| x = c1}){123})
    c.hold{}
    x.should == nil
    c.disconnect
    x.should == 123
  end
  
  specify "should have a reentrent hold method" do
    o = Object.new
    c = @class.new(mock_db.call{o}, {})
    c.hold do |x|
      x.should == o
      c.hold do |x1|
        x1.should == o
        c.hold do |x2|
          x2.should == o
        end
      end
    end
  end
  
  specify "should have a servers method that returns an array of shard/server symbols" do
    @class.new(mock_db.call{123}, {}).servers.should == [:default]
  end
  
  specify "should have a servers method that returns an array of shard/server symbols" do
    c = @class.new(mock_db.call{123}, {})
    c.size.should == 0
    c.hold{}
    c.size.should == 1
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
    it_should_behave_like "All connection pools classes"
  end
end
