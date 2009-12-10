require File.join(File.dirname(__FILE__), 'spec_helper')
CONNECTION_POOL_DEFAULTS = {:pool_timeout=>5, :pool_sleep_time=>0.001,
  :pool_reuse_connections=>:allow, :pool_convert_exceptions=>true, :max_connections=>4}

context "An empty ConnectionPool" do
  before do
    @cpool = Sequel::ConnectionPool.new(CONNECTION_POOL_DEFAULTS)
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

context "ConnectionPool options" do
  specify "should support string option values" do
    cpool = Sequel::ConnectionPool.new({:max_connections=>'5', :pool_timeout=>'3', :pool_sleep_time=>'0.01'})
    cpool.max_size.should == 5
    cpool.instance_variable_get(:@timeout).should == 3
    cpool.instance_variable_get(:@sleep_time).should == 0.01
  end

  specify "should raise an error unless size is positive" do
    lambda{Sequel::ConnectionPool.new(:max_connections=>0)}.should raise_error(Sequel::Error)
    lambda{Sequel::ConnectionPool.new(:max_connections=>-10)}.should raise_error(Sequel::Error)
    lambda{Sequel::ConnectionPool.new(:max_connections=>'-10')}.should raise_error(Sequel::Error)
    lambda{Sequel::ConnectionPool.new(:max_connections=>'0')}.should raise_error(Sequel::Error)
  end
end

context "A connection pool handling connections" do
  before do
    @max_size = 2
    @cpool = Sequel::ConnectionPool.new(CONNECTION_POOL_DEFAULTS.merge(:disconnection_proc=>proc{|c| @max_size=3},  :max_connections=>@max_size)) {:got_connection}
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

  if RUBY_VERSION < '1.9.0' and (!defined?(RUBY_ENGINE) or RUBY_ENGINE != 'jruby')
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
    50.times{Thread.new{@cpool.hold{sleep 0.001}}}
    @cpool.created_count.should <= @max_size
  end

  specify ":disconnection_proc option should set the disconnection proc to use" do
    @max_size.should == 2
    proc{@cpool.hold{raise Sequel::DatabaseDisconnectError}}.should raise_error(Sequel::DatabaseDisconnectError)
    @max_size.should == 3
  end

  specify "#disconnection_proc= should set the disconnection proc to use" do
    a = 1
    @cpool.disconnection_proc = proc{|c| a += 1}
    proc{@cpool.hold{raise Sequel::DatabaseDisconnectError}}.should raise_error(Sequel::DatabaseDisconnectError)
    a.should == 2
  end

  specify "#hold should remove the connection if a DatabaseDisconnectError is raised" do
    @cpool.created_count.should == 0
    @cpool.hold{Thread.new{@cpool.hold{}}; sleep 0.01}
    @cpool.created_count.should == 2
    proc{@cpool.hold{raise Sequel::DatabaseDisconnectError}}.should raise_error(Sequel::DatabaseDisconnectError)
    @cpool.created_count.should == 1
    proc{@cpool.hold{raise Sequel::DatabaseDisconnectError}}.should raise_error(Sequel::DatabaseDisconnectError)
    @cpool.created_count.should == 0
    proc{@cpool.hold{raise Sequel::DatabaseDisconnectError}}.should raise_error(Sequel::DatabaseDisconnectError)
    @cpool.created_count.should == 0
  end
end

context "A connection pool handling connection errors" do 
  specify "#hold should raise a Sequel::DatabaseConnectionError if an exception is raised by the connection_proc" do
    cpool = Sequel::ConnectionPool.new(CONNECTION_POOL_DEFAULTS){raise Interrupt}
    proc{cpool.hold{:block_return}}.should raise_error(Sequel::DatabaseConnectionError)
    cpool.created_count.should == 0
  end

  specify "#hold should raise a Sequel::DatabaseConnectionError if nil is returned by the connection_proc" do
    cpool = Sequel::ConnectionPool.new(CONNECTION_POOL_DEFAULTS){nil}
    proc{cpool.hold{:block_return}}.should raise_error(Sequel::DatabaseConnectionError)
    cpool.created_count.should == 0
  end
end

class DummyConnection
  @@value = 0
  def initialize
    @@value += 1
  end
  
  def value
    @@value
  end
end

context "ConnectionPool#hold" do
  before do
    @pool = Sequel::ConnectionPool.new(CONNECTION_POOL_DEFAULTS) {DummyConnection.new}
  end
  
  specify "should pass the result of the connection maker proc to the supplied block" do
    res = nil
    @pool.hold {|c| res = c}
    res.should be_a_kind_of(DummyConnection)
    res.value.should == 1
    @pool.hold {|c| res = c}
    res.should be_a_kind_of(DummyConnection)
    res.value.should == 1 # the connection maker is invoked only once
  end
  
  specify "should be re-entrant by the same thread" do
    cc = nil
    @pool.hold {|c| @pool.hold {|c| @pool.hold {|c| cc = c}}}
    cc.should be_a_kind_of(DummyConnection)
  end
  
  specify "should catch exceptions and reraise them" do
    proc {@pool.hold {|c| c.foobar}}.should raise_error(NoMethodError)
  end
  
  specify "should handle Exception errors (normally not caught by rescue)" do
    err = nil
    begin
      @pool.hold {raise Exception}
    rescue => e
      err = e
    end
    err.should be_a_kind_of(RuntimeError)
  end
end

context "ConnectionPool#connection_proc" do
  before do
    @pool = Sequel::ConnectionPool.new(CONNECTION_POOL_DEFAULTS)
  end
  
  specify "should be nil if no block is supplied to the pool" do
    @pool.connection_proc.should be_nil
    proc {@pool.hold {}}.should raise_error
  end
  
  specify "should be mutable" do
    @pool.connection_proc = proc {'herro'}
    res = nil
    proc {@pool.hold {|c| res = c}}.should_not raise_error
    res.should == 'herro'
  end
end

context "A connection pool with a max size of 1" do
  before do
    @invoked_count = 0
    @pool = Sequel::ConnectionPool.new(CONNECTION_POOL_DEFAULTS.merge(:max_connections=>1)) {@invoked_count += 1; 'herro'}
  end
  
  specify "should let only one thread access the connection at any time" do
    cc,c1, c2 = nil
    
    t1 = Thread.new {@pool.hold {|c| cc = c; c1 = c.dup; while c == 'herro';sleep 0.01;end}}
    sleep 0.02
    cc.should == 'herro'
    c1.should == 'herro'
    
    t2 = Thread.new {@pool.hold {|c| c2 = c.dup; while c == 'hello';sleep 0.01;end}}
    sleep 0.02
    
    # connection held by t1
    t1.should be_alive
    t2.should be_alive
    
    cc.should == 'herro'
    c1.should == 'herro'
    c2.should be_nil
    
    @pool.available_connections.should be_empty
    @pool.allocated.should == {t1=>cc}
    
    cc.gsub!('rr', 'll')
    sleep 0.05
    
    # connection held by t2
    t1.should_not be_alive
    t2.should be_alive

    c2.should == 'hello'

    @pool.available_connections.should be_empty
    @pool.allocated.should == {t2=>cc}
    
    cc.gsub!('ll', 'rr')
    sleep 0.05
    
    #connection released
    t2.should_not be_alive
    
    cc.should == 'herro'
    
    @invoked_count.should == 1
    @pool.size.should == 1
    @pool.available_connections.should == [cc]
    @pool.allocated.should be_empty
  end
  
  specify "should let the same thread reenter #hold" do
    c1, c2, c3 = nil
    @pool.hold do |c|
      c1 = c
      @pool.hold do |c|
        c2 = c
        @pool.hold do |c|
          c3 = c
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

context "A connection pool with a max size of 5" do
  before do
    @invoked_count = 0
    @pool = Sequel::ConnectionPool.new(CONNECTION_POOL_DEFAULTS.merge(:max_connections=>5)) {@invoked_count += 1}
  end
  
  specify "should let five threads simultaneously access separate connections" do
    cc = {}
    threads = []
    stop = nil
    
    5.times {|i| threads << Thread.new {@pool.hold {|c| cc[i] = c; while !stop;sleep 0.01;end}}; sleep 0.01}
    sleep 0.02
    threads.each {|t| t.should be_alive}
    cc.size.should == 5
    @invoked_count.should == 5
    @pool.size.should == 5
    @pool.available_connections.should be_empty
    i = 0
    h = {}
    threads.each{|t| h[t] = (i+=1)}
    @pool.allocated.should == h
    
    threads[0].raise "your'e dead"
    sleep 0.01
    threads[3].raise "your'e dead too"
    
    sleep 0.01
    
    @pool.available_connections.should == [1, 4]
    @pool.allocated.should == {threads[1]=>2, threads[2]=>3, threads[4]=>5}
    
    stop = true
    sleep 0.02
    
    @pool.available_connections.size.should == 5
    @pool.allocated.should be_empty
  end
  
  specify "should block threads until a connection becomes available" do
    cc = {}
    threads = []
    stop = nil
    
    5.times {|i| threads << Thread.new {@pool.hold {|c| cc[i] = c; while !stop;sleep 0.01;end}}; sleep 0.01}
    sleep 0.02
    threads.each {|t| t.should be_alive}
    @pool.available_connections.should be_empty

    3.times {|i| threads << Thread.new {@pool.hold {|c| cc[i + 5] = c}}}
    
    sleep 0.02
    threads[5].should be_alive
    threads[6].should be_alive
    threads[7].should be_alive
    cc.size.should == 5
    cc[5].should be_nil
    cc[6].should be_nil
    cc[7].should be_nil
    
    stop = true
    sleep 0.05
    
    threads.each {|t| t.should_not be_alive}
    
    @pool.size.should == 5
    @invoked_count.should == 5
    @pool.available_connections.size.should == 5
    @pool.allocated.should be_empty
  end
end

context "ConnectionPool#disconnect" do
  before do
    @count = 0
    @pool = Sequel::ConnectionPool.new(CONNECTION_POOL_DEFAULTS.merge(:max_connections=>5)) {{:id => @count += 1}}
  end
  
  specify "should invoke the given block for each available connection" do
    threads = []
    stop = nil
    5.times {|i| threads << Thread.new {@pool.hold {|c| while !stop;sleep 0.01;end}}; sleep 0.01}
    while @pool.size < 5
      sleep 0.02
    end
    stop = true
    sleep 0.1
    threads.each {|t| t.join}
    
    @pool.size.should == 5
    @pool.available_connections.size.should == 5
    @pool.available_connections.each {|c| c[:id].should_not be_nil}
    conns = []
    @pool.disconnect {|c| conns << c}
    conns.size.should == 5
  end
  
  specify "should remove all available connections" do
    threads = []
    stop = nil
    5.times {|i| threads << Thread.new {@pool.hold {|c| while !stop;sleep 0.01;end}}; sleep 0.01}
    while @pool.size < 5
      sleep 0.02
    end
    stop = true
    sleep 0.1
    threads.each {|t| t.join}
    
    @pool.size.should == 5
    @pool.disconnect
    @pool.size.should == 0
  end

  specify "should disconnect connections in use as soon as they are no longer in use" do
    threads = []
    stop = nil
    5.times {|i| threads << Thread.new {@pool.hold {|c| while !stop;sleep 0.01;end}}; sleep 0.01}
    while @pool.size < 5
      sleep 0.02
    end
    stop = true
    sleep 0.1
    threads.each {|t| t.join}
    
    @pool.size.should == 5
    
    @pool.hold do |conn|
      @pool.available_connections.size.should == 4
      @pool.available_connections.each {|c| c.should_not be(conn)}
      conns = []
      @pool.disconnect {|c| conns << c}
      conns.size.should == 4
      @pool.size.should == 1
    end
    @pool.size.should == 0
  end
end

context "A connection pool with multiple servers" do
  before do
    @invoked_counts = Hash.new(0)
    @pool = Sequel::ConnectionPool.new(CONNECTION_POOL_DEFAULTS.merge(:servers=>{:read_only=>{}})){|server| "#{server}#{@invoked_counts[server] += 1}"}
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
    @pool.disconnect{|c| conns << c}
    conns.sort.should == %w'default1 read_only1'
    @pool.size.should == 0
    @pool.size(:read_only).should == 0
    @pool.hold(:read_only){|c| c.should == 'read_only2'}
    @pool.hold{|c| c.should == 'default2'}
  end
  
  specify "#add_servers should add new servers to the pool" do
    pool = Sequel::ConnectionPool.new(:servers=>{:server1=>{}}){|s| s}
    
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
    pool = Sequel::ConnectionPool.new(:servers=>{:server1=>{}}){|s| s}
    
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
    pool = Sequel::ConnectionPool.new(:max_connections=>5, :servers=>{:server1=>{}}){|s| s}
    threads = []
    stop = nil
    5.times {|i| threads << Thread.new{pool.hold(:server1){|c| sleep 0.05}}}
    sleep 0.1
    threads.each {|t| t.join}
    
    pool.size(:server1).should == 5
    pool.remove_servers([:server1])
    pool.size(:server1).should == 0
  end
  
  specify "#remove_servers should disconnect connections in use as soon as they are returned to the pool" do
    dc = []
    pool = Sequel::ConnectionPool.new(:servers=>{:server1=>{}}, :disconnection_proc=>proc{|c| dc << c}){|s| s}
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
    pool = Sequel::ConnectionPool.new(:servers=>{:server1=>{}}){|s| s}
    pool.available_connections(:server1).should == []
    pool.allocated(:server1).should == {}
    pool.remove_servers([:server1])
    pool.available_connections(:server1).should == nil
    pool.allocated(:server1).should == nil
  end
  
  specify "#remove_servers should not allow the removal of the default server" do
    pool = Sequel::ConnectionPool.new(:servers=>{:server1=>{}}){|s| s}
    proc{pool.remove_servers([:server1])}.should_not raise_error
    proc{pool.remove_servers([:default])}.should raise_error(Sequel::Error)
  end
  
  specify "#remove_servers should ignore servers that have already been removed" do
    dc = []
    pool = Sequel::ConnectionPool.new(:servers=>{:server1=>{}}, :disconnection_proc=>proc{|c| dc << c}){|s| s}
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

context "SingleThreadedPool" do
  before do
    @pool = Sequel::SingleThreadedPool.new(CONNECTION_POOL_DEFAULTS){1234}
  end
  
  specify "should provide a #hold method" do
    conn = nil
    @pool.hold {|c| conn = c}
    conn.should == 1234
  end
  
  specify "should provide a #disconnect method" do
    @pool.hold {|c|}
    @pool.conn.should == 1234
    conn = nil
    @pool.disconnect {|c| conn = c}
    conn.should == 1234
    @pool.conn.should be_nil
  end
end

context "A single threaded pool with multiple servers" do
  before do
    @max_size=2
    @pool = Sequel::SingleThreadedPool.new(CONNECTION_POOL_DEFAULTS.merge(:disconnection_proc=>proc{|c| @max_size=3}, :servers=>{:read_only=>{}})){|server| server}
  end
  
  specify "should use the :default server by default" do
    @pool.hold{|c| c.should == :default}
    @pool.conn.should == :default
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
    conns = []
    @pool.conn.should == :default
    @pool.conn(:read_only).should == :read_only
    @pool.disconnect{|c| conns << c}
    conns.sort_by{|x| x.to_s}.should == [:default, :read_only]
    @pool.conn.should == nil
    @pool.conn(:read_only).should == nil
  end

  specify ":disconnection_proc option should set the disconnection proc to use" do
    @max_size.should == 2
    proc{@pool.hold{raise Sequel::DatabaseDisconnectError}}.should raise_error(Sequel::DatabaseDisconnectError)
    @max_size.should == 3
  end

  specify "#disconnection_proc= should set the disconnection proc to use" do
    a = 1
    @pool.disconnection_proc = proc{|c| a += 1}
    proc{@pool.hold{raise Sequel::DatabaseDisconnectError}}.should raise_error(Sequel::DatabaseDisconnectError)
    a.should == 2
  end

  specify "#hold should remove the connection if a DatabaseDisconnectError is raised" do
    @pool.instance_variable_get(:@conns).length.should == 0
    @pool.hold{}
    @pool.instance_variable_get(:@conns).length.should == 1
    proc{@pool.hold{raise Sequel::DatabaseDisconnectError}}.should raise_error(Sequel::DatabaseDisconnectError)
    @pool.instance_variable_get(:@conns).length.should == 0
  end
end
