require File.join(File.dirname(__FILE__), '../lib/sequel')

context "A new Database" do
  setup do
    @db = Sequel::Database.new(1 => 2, :logger => 3)
  end
  
  specify "should receive options" do
    @db.opts.should == {1 => 2, :logger => 3}  
  end
  
  specify "should set the logger from opts[:logger]" do
    @db.logger.should == 3
  end
  
  specify "should create a connection pool" do
    @db.pool.should be_a_kind_of(Sequel::ConnectionPool)
    @db.pool.max_size.should == 4
    
    Sequel::Database.new(:max_connections => 10).pool.max_size.should == 10
  end
  
  specify "should pass the supplied block to the connection pool" do
    cc = nil
    d = Sequel::Database.new {1234}
    d.synchronize {|c| cc = c}
    cc.should == 1234
  end
end

context "Database#connect" do
  specify "should raise NotImplementedError" do
    proc {Sequel::Database.new.connect}.should raise_error(NotImplementedError)
  end
end

context "Database#uri" do
  setup do
    @c = Class.new(Sequel::Database) do
      set_adapter_scheme :mau
    end
    
    @db = Sequel('mau://user:pass@localhost:9876/maumau')
  end
  
  specify "should return the connection URI for the database" do
    @db.uri.should == 'mau://user:pass@localhost:9876/maumau'
  end
end

context "Database.adapter_scheme" do
  specify "should return the database schema" do
    Sequel::Database.adapter_scheme.should be_nil

    @c = Class.new(Sequel::Database) do
      set_adapter_scheme :mau
    end
    
    @c.adapter_scheme.should == :mau
  end
end

context "Database#dataset" do
  setup do
    @db = Sequel::Database.new
    @ds = @db.dataset
  end
  
  specify "should provide a blank dataset through #dataset" do
    @ds.should be_a_kind_of(Sequel::Dataset)
    @ds.opts.should == {}
    @ds.db.should be(@db)
  end
  
  specify "should provide a #from dataset" do
    d = @db.from(:mau)
    d.should be_a_kind_of(Sequel::Dataset)
    d.sql.should == 'SELECT * FROM mau'
    
    e = @db[:miu]
    e.should be_a_kind_of(Sequel::Dataset)
    e.sql.should == 'SELECT * FROM miu'
  end
  
  specify "should provide a #select dataset" do
    d = @db.select(:a, :b, :c).from(:mau)
    d.should be_a_kind_of(Sequel::Dataset)
    d.sql.should == 'SELECT a, b, c FROM mau'
  end
end

context "Database#execute" do
  specify "should raise NotImplementedError" do
    proc {Sequel::Database.new.execute('blah blah')}.should raise_error(NotImplementedError)
    proc {Sequel::Database.new << 'blah blah'}.should raise_error(NotImplementedError)
  end
end

context "Database#<<" do
  setup do
    @c = Class.new(Sequel::Database) do
      define_method(:execute) {|sql| sql}
    end
    @db = @c.new({})
  end
  
  specify "should pass the supplied sql to #execute" do
    (@db << "DELETE FROM items").should == "DELETE FROM items"
  end
  
  specify "should accept an array and convert it to SQL" do
    a = %[
      --
      CREATE TABLE items (a integer, /*b integer*/
        b text, c integer);
      DROP TABLE old_items;
    ].split($/)
    (@db << a).should == 
      "CREATE TABLE items (a integer, b text, c integer); DROP TABLE old_items;"
  end
  
  specify "should remove comments and whitespace from strings as well" do
    s = %[
      --
      CREATE TABLE items (a integer, /*b integer*/
        b text, c integer); \r\n
      DROP TABLE old_items;
    ]
    (@db << s).should == 
      "CREATE TABLE items (a integer, b text, c integer); DROP TABLE old_items;"
  end
end

context "Database#synchronize" do
  setup do
    @db = Sequel::Database.new(:max_connections => 1)
    @db.pool.connection_proc = proc {12345}
  end
  
  specify "should wrap the supplied block in pool.hold" do
    stop = false
    c1, c2 = nil
    t1 = Thread.new {@db.synchronize {|c| c1 = c; while !stop;sleep 0.1;end}}
    while !c1;end
    c1.should == 12345
    t2 = Thread.new {@db.synchronize {|c| c2 = c}}
    sleep 0.2
    @db.pool.available_connections.should be_empty
    c2.should be_nil
    stop = true
    t1.join
    sleep 0.1
    c2.should == 12345
    t2.join
  end
end

context "Database#test_connection" do
  setup do
    @db = Sequel::Database.new
    @test = nil
    @db.pool.connection_proc = proc {@test = rand(100)}
  end
  
  specify "should call pool#hold" do
    @db.test_connection
    @test.should_not be_nil
  end
  
  specify "should return true if successful" do
    @db.test_connection.should be_true
  end
end

class DummyDataset < Sequel::Dataset
  def first
    raise if @opts[:from] == [:a]
    true
  end
end

class DummyDatabase < Sequel::Database
  attr_reader :sql
  def execute(sql); @sql = sql; end
  def transaction; yield; end

  def dataset
    DummyDataset.new(self)
  end
end

context "Database#create_table" do
  setup do
    @db = DummyDatabase.new
  end
  
  specify "should construct proper SQL" do
    @db.create_table :test do
      primary_key :id, :integer, :null => false
      column :name, :text
      index :name, :unique => true
    end
    @db.sql.should == 
      'CREATE TABLE test (id integer NOT NULL PRIMARY KEY AUTOINCREMENT, name text);CREATE UNIQUE INDEX test_name_index ON test (name);'
  end
end

class Dummy2Database < Sequel::Database
  attr_reader :sql
  def execute(sql); @sql = sql; end
  def transaction; yield; end
end

context "Database#drop_table" do
  setup do
    @db = Dummy2Database.new
  end
  
  specify "should construct proper SQL" do
    @db.drop_table :test
    @db.sql.should == 
      'DROP TABLE test;'
  end
  
  specify "should accept multiple table names" do
    @db.drop_table :a, :bb, :ccc
    @db.sql.should ==
      'DROP TABLE a;DROP TABLE bb;DROP TABLE ccc;'
  end
end

context "Database#table_exists?" do
  setup do
    @db = DummyDatabase.new
    @db.stub!(:tables).and_return([:a, :b])
    @db2 = DummyDatabase.new
    Sequel::Dataset.stub!(:first).and_return(nil)
  end
  
  specify "should use Database#tables if available" do
    @db.table_exists?(:a).should be_true
    @db.table_exists?(:b).should be_true
    @db.table_exists?(:c).should be_false
  end
  
  specify "should otherise try to select the first record from the table's dataset" do
    @db2.table_exists?(:a).should be_false
    @db2.table_exists?(:b).should be_true
  end
end


class Dummy3Database < Sequel::Database
  attr_reader :sql, :transactions
  def execute(sql); @sql ||= []; @sql << sql; end

  class DummyConnection
    def initialize(db); @db = db; end
    def execute(sql); @db.execute(sql); end
  end
end

context "Database#transaction" do
  setup do
    @db = Dummy3Database.new
    @db.pool.connection_proc = proc {Dummy3Database::DummyConnection.new(@db)}
  end
  
  specify "should wrap the supplied block with BEGIN + COMMIT statements" do
    @db.transaction {@db.execute 'DROP TABLE test;'}
    @db.sql.should == ['BEGIN', 'DROP TABLE test;', 'COMMIT']
  end
  
  specify "should issue ROLLBACK if an exception is raised, and re-raise" do
    @db.transaction {@db.execute 'DROP TABLE test;'; raise RuntimeError} rescue nil
    @db.sql.should == ['BEGIN', 'DROP TABLE test;', 'ROLLBACK']
    
    proc {@db.transaction {raise RuntimeError}}.should raise_error(RuntimeError)
  end
  
  specify "should be re-entrant" do
    stop = false
    cc = nil
    t = Thread.new do
      @db.transaction {@db.transaction {@db.transaction {|c|
        cc = c
        while !stop; sleep 0.1; end
      }}}
    end
    while cc.nil?; sleep 0.1; end
    cc.should be_a_kind_of(Dummy3Database::DummyConnection)
    @db.transactions.should == [t]
    stop = true
    t.join
    @db.transactions.should be_empty
  end
end

class Sequel::Database
  def self.get_adapters; @@adapters; end
end

context "A Database adapter with a scheme" do
  setup do
    class CCC < Sequel::Database
      set_adapter_scheme :ccc
    end
  end

  specify "should be registered in adapters" do
    Sequel::Database.get_adapters[:ccc].should == CCC
  end
  
  specify "should be instantiated when its scheme is specified" do
    c = Sequel::Database.connect('ccc://localhost/db')
    c.should be_a_kind_of(CCC)
    c.opts[:host].should == 'localhost'
    c.opts[:database].should == 'db'
  end
  
  specify "should be accessible through Sequel.connect" do
    c = Sequel.connect 'ccc://localhost/db'
    c.should be_a_kind_of(CCC)
    c.opts[:host].should == 'localhost'
    c.opts[:database].should == 'db'
  end

  specify "should be accessible through Sequel.open" do
    c = Sequel.open 'ccc://localhost/db'
    c.should be_a_kind_of(CCC)
    c.opts[:host].should == 'localhost'
    c.opts[:database].should == 'db'
  end

  specify "should be accessible through Sequel()" do
    c = Sequel('ccc://localhost/db')
    c.should be_a_kind_of(CCC)
    c.opts[:host].should == 'localhost'
    c.opts[:database].should == 'db'
  end

end

context "An unknown database scheme" do
  specify "should raise an exception in Sequel::Database.connect" do
    proc {Sequel::Database.connect('ddd://localhost/db')}.should raise_error(SequelError)
  end

  specify "should raise an exception in Sequel.connect" do
    proc {Sequel.connect('ddd://localhost/db')}.should raise_error(SequelError)
  end

  specify "should raise an exception in Sequel.open" do
    proc {Sequel.open('ddd://localhost/db')}.should raise_error(SequelError)
  end
end

context "Database#uri_to_options" do
  specify "should convert a URI to an options hash" do
    h = Sequel::Database.uri_to_options(URI.parse('ttt://uuu:ppp@192.168.60.1:1234/blah'))
    h[:user].should == 'uuu'
    h[:password].should == 'ppp'
    h[:host].should == '192.168.60.1'
    h[:port].should == 1234
    h[:database].should == 'blah'
  end
end

context "A single threaded database" do
  teardown do
    Sequel::Database.single_threaded = false
  end
  
  specify "should use a SingleThreadedPool instead of a ConnectionPool" do
    db = Sequel::Database.new(:single_threaded => true)
    db.pool.should be_a_kind_of(Sequel::SingleThreadedPool)
  end
  
  specify "should be constructable using :single_threaded => true option" do
    db = Sequel::Database.new(:single_threaded => true)
    db.pool.should be_a_kind_of(Sequel::SingleThreadedPool)
  end
  
  specify "should be constructable using Database.single_threaded = true" do
    Sequel::Database.single_threaded = true
    db = Sequel::Database.new
    db.pool.should be_a_kind_of(Sequel::SingleThreadedPool)
  end

  specify "should be constructable using Sequel.single_threaded = true" do
    Sequel.single_threaded = true
    db = Sequel::Database.new
    db.pool.should be_a_kind_of(Sequel::SingleThreadedPool)
  end
end

context "A single threaded database" do
  setup do
    conn = 1234567
    @db = Sequel::Database.new(:single_threaded => true) do
      conn += 1
    end
  end
  
  specify "should invoke connection_proc only once" do
    @db.pool.hold {|c| c.should == 1234568}
    @db.pool.hold {|c| c.should == 1234568}
  end
  
  specify "should convert an Exception into a RuntimeError" do
    db = Sequel::Database.new(:single_threaded => true) do
      raise Exception
    end
    
    proc {db.pool.hold {|c|}}.should raise_error(RuntimeError)
  end
end

context "A database" do
  setup do
    Sequel::Database.single_threaded = false
  end
  
  teardown do
    Sequel::Database.single_threaded = false
  end
  
  specify "should be either single_threaded? or multi_threaded?" do
    db = Sequel::Database.new(:single_threaded => true)
    db.should be_single_threaded
    db.should_not be_multi_threaded
    
    db = Sequel::Database.new(:max_options => 1)
    db.should_not be_single_threaded
    db.should be_multi_threaded
    
    db = Sequel::Database.new
    db.should_not be_single_threaded
    db.should be_multi_threaded
    
    Sequel::Database.single_threaded = true
    
    db = Sequel::Database.new
    db.should be_single_threaded
    db.should_not be_multi_threaded
    
    db = Sequel::Database.new(:max_options => 4)
    db.should be_single_threaded
    db.should_not be_multi_threaded
    
  end
end