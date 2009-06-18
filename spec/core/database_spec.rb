require File.join(File.dirname(__FILE__), 'spec_helper')

context "A new Database" do
  before do
    @db = Sequel::Database.new(1 => 2, :logger => 3)
  end
  after do
    Sequel.quote_identifiers = false
    Sequel.identifier_input_method = nil
    Sequel.identifier_output_method = nil
  end
  
  specify "should receive options" do
    @db.opts.should == {1 => 2, :logger => 3}  
  end
  
  specify "should set the logger from opts[:logger] and opts[:loggers]" do
    @db.loggers.should == [3]
    Sequel::Database.new(1 => 2, :loggers => 3).loggers.should == [3]
    Sequel::Database.new(1 => 2, :loggers => [3]).loggers.should == [3]
    Sequel::Database.new(1 => 2, :logger => 4, :loggers => 3).loggers.should == [4,3]
    Sequel::Database.new(1 => 2, :logger => [4], :loggers => [3]).loggers.should == [4,3]
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

  specify "should respect the :single_threaded option" do
    db = Sequel::Database.new(:single_threaded=>true)
    db.pool.should be_a_kind_of(Sequel::SingleThreadedPool)
    db = Sequel::Database.new(:single_threaded=>'t')
    db.pool.should be_a_kind_of(Sequel::SingleThreadedPool)
    db = Sequel::Database.new(:single_threaded=>'1')
    db.pool.should be_a_kind_of(Sequel::SingleThreadedPool)
    db = Sequel::Database.new(:single_threaded=>false)
    db.pool.should be_a_kind_of(Sequel::ConnectionPool)
    db = Sequel::Database.new(:single_threaded=>'f')
    db.pool.should be_a_kind_of(Sequel::ConnectionPool)
    db = Sequel::Database.new(:single_threaded=>'0')
    db.pool.should be_a_kind_of(Sequel::ConnectionPool)
  end

  specify "should respect the :quote_identifiers option" do
    db = Sequel::Database.new(:quote_identifiers=>false)
    db.quote_identifiers?.should == false
    db = Sequel::Database.new(:quote_identifiers=>true)
    db.quote_identifiers?.should == true
  end

  specify "should upcase on input and downcase on output by default" do
    db = Sequel::Database.new
    db.send(:identifier_input_method_default).should == :upcase
    db.send(:identifier_output_method_default).should == :downcase
  end

  specify "should respect the :identifier_input_method option" do
    Sequel.identifier_input_method = nil
    Sequel::Database.identifier_input_method.should == ""
    db = Sequel::Database.new(:identifier_input_method=>nil)
    db.identifier_input_method.should == nil
    db.identifier_input_method = :downcase
    db.identifier_input_method.should == :downcase
    db = Sequel::Database.new(:identifier_input_method=>:upcase)
    db.identifier_input_method.should == :upcase
    db.identifier_input_method = nil
    db.identifier_input_method.should == nil
    Sequel.identifier_input_method = :downcase
    Sequel::Database.identifier_input_method.should == :downcase
    db = Sequel::Database.new(:identifier_input_method=>nil)
    db.identifier_input_method.should == nil
    db.identifier_input_method = :upcase
    db.identifier_input_method.should == :upcase
    db = Sequel::Database.new(:identifier_input_method=>:upcase)
    db.identifier_input_method.should == :upcase
    db.identifier_input_method = nil
    db.identifier_input_method.should == nil
  end
  
  specify "should respect the :identifier_output_method option" do
    Sequel.identifier_output_method = nil
    Sequel::Database.identifier_output_method.should == ""
    db = Sequel::Database.new(:identifier_output_method=>nil)
    db.identifier_output_method.should == nil
    db.identifier_output_method = :downcase
    db.identifier_output_method.should == :downcase
    db = Sequel::Database.new(:identifier_output_method=>:upcase)
    db.identifier_output_method.should == :upcase
    db.identifier_output_method = nil
    db.identifier_output_method.should == nil
    Sequel.identifier_output_method = :downcase
    Sequel::Database.identifier_output_method.should == :downcase
    db = Sequel::Database.new(:identifier_output_method=>nil)
    db.identifier_output_method.should == nil
    db.identifier_output_method = :upcase
    db.identifier_output_method.should == :upcase
    db = Sequel::Database.new(:identifier_output_method=>:upcase)
    db.identifier_output_method.should == :upcase
    db.identifier_output_method = nil
    db.identifier_output_method.should == nil
  end

  specify "should use the default Sequel.quote_identifiers value" do
    Sequel.quote_identifiers = true
    Sequel::Database.new({}).quote_identifiers?.should == true
    Sequel.quote_identifiers = false
    Sequel::Database.new({}).quote_identifiers?.should == false
    Sequel::Database.quote_identifiers = true
    Sequel::Database.new({}).quote_identifiers?.should == true
    Sequel::Database.quote_identifiers = false
    Sequel::Database.new({}).quote_identifiers?.should == false
  end

  specify "should use the default Sequel.identifier_input_method value" do
    Sequel.identifier_input_method = :downcase
    Sequel::Database.new({}).identifier_input_method.should == :downcase
    Sequel.identifier_input_method = :upcase
    Sequel::Database.new({}).identifier_input_method.should == :upcase
    Sequel::Database.identifier_input_method = :downcase
    Sequel::Database.new({}).identifier_input_method.should == :downcase
    Sequel::Database.identifier_input_method = :upcase
    Sequel::Database.new({}).identifier_input_method.should == :upcase
  end
  
  specify "should use the default Sequel.identifier_output_method value" do
    Sequel.identifier_output_method = :downcase
    Sequel::Database.new({}).identifier_output_method.should == :downcase
    Sequel.identifier_output_method = :upcase
    Sequel::Database.new({}).identifier_output_method.should == :upcase
    Sequel::Database.identifier_output_method = :downcase
    Sequel::Database.new({}).identifier_output_method.should == :downcase
    Sequel::Database.identifier_output_method = :upcase
    Sequel::Database.new({}).identifier_output_method.should == :upcase
  end

  specify "should respect the quote_indentifiers_default method if Sequel.quote_identifiers = nil" do
    Sequel.quote_identifiers = nil
    Sequel::Database.new({}).quote_identifiers?.should == true
    x = Class.new(Sequel::Database){def quote_identifiers_default; false end}
    x.new({}).quote_identifiers?.should == false
    y = Class.new(Sequel::Database){def quote_identifiers_default; true end}
    y.new({}).quote_identifiers?.should == true
  end
  
  specify "should respect the identifier_input_method_default method" do
    class Sequel::Database
      @@identifier_input_method = nil
    end
    x = Class.new(Sequel::Database){def identifier_input_method_default; :downcase end}
    x.new({}).identifier_input_method.should == :downcase
    y = Class.new(Sequel::Database){def identifier_input_method_default; :camelize end}
    y.new({}).identifier_input_method.should == :camelize
  end
  
  specify "should respect the identifier_output_method_default method if Sequel.identifier_output_method is not called" do
    class Sequel::Database
      @@identifier_output_method = nil
    end
    x = Class.new(Sequel::Database){def identifier_output_method_default; :upcase end}
    x.new({}).identifier_output_method.should == :upcase
    y = Class.new(Sequel::Database){def identifier_output_method_default; :underscore end}
    y.new({}).identifier_output_method.should == :underscore
  end

  specify "should just use a :uri option for jdbc with the full connection string" do
    Sequel::Database.should_receive(:adapter_class).once.with(:jdbc).and_return(Sequel::Database)
    db = Sequel.connect('jdbc:test://host/db_name')
    db.should be_a_kind_of(Sequel::Database)
    db.opts[:uri].should == 'jdbc:test://host/db_name'
  end

  specify "should just use a :uri option for do with the full connection string" do
    Sequel::Database.should_receive(:adapter_class).once.with(:do).and_return(Sequel::Database)
    db = Sequel.connect('do:test://host/db_name')
    db.should be_a_kind_of(Sequel::Database)
    db.opts[:uri].should == 'do:test://host/db_name'
  end
end

context "Database#disconnect" do
  specify "should call pool.disconnect" do
    d = Sequel::Database.new
    p = d.pool
    p.should_receive(:disconnect).once.and_return(2)
    d.disconnect.should == 2
  end
end

context "Sequel.extension" do
  specify "should attempt to load the given extension" do
    proc{Sequel.extension :blah}.should raise_error(LoadError)
  end
end

context "Database#connect" do
  specify "should raise NotImplementedError" do
    proc {Sequel::Database.new.connect}.should raise_error(NotImplementedError)
  end
end

context "Database#uri" do
  before do
    @c = Class.new(Sequel::Database) do
      set_adapter_scheme :mau
    end
    
    @db = Sequel.connect('mau://user:pass@localhost:9876/maumau')
  end
  
  specify "should return the connection URI for the database" do
    @db.uri.should == 'mau://user:pass@localhost:9876/maumau'
  end
  
  specify "should be aliased as #url" do
    @db.url.should == 'mau://user:pass@localhost:9876/maumau'
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
  before do
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
  
  specify "should provide a filtered #from dataset if a block is given" do
    d = @db.from(:mau) {:x.sql_number > 100}
    d.should be_a_kind_of(Sequel::Dataset)
    d.sql.should == 'SELECT * FROM mau WHERE (x > 100)'
  end
  
  specify "should provide a #select dataset" do
    d = @db.select(:a, :b, :c).from(:mau)
    d.should be_a_kind_of(Sequel::Dataset)
    d.sql.should == 'SELECT a, b, c FROM mau'
  end
  
  specify "should allow #select to take a block" do
    d = @db.select(:a, :b){c}.from(:mau)
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
  before do
    @c = Class.new(Sequel::Database) do
      define_method(:execute) {|sql, opts| sql}
    end
    @db = @c.new({})
  end
  
  specify "should pass the supplied sql to #execute" do
    (@db << "DELETE FROM items").should == "DELETE FROM items"
  end
  
  specify "should not remove comments and whitespace from strings" do
    s = "INSERT INTO items VALUES ('---abc')"
    (@db << s).should == s
  end
end

context "Database#synchronize" do
  before do
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
  before do
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
  attr_reader :sqls
  
  def execute(sql, opts={})
    @sqls ||= []
    @sqls << sql
  end
  
  def transaction; yield; end

  def dataset
    DummyDataset.new(self)
  end
end

context "Database#create_table" do
  before do
    @db = DummyDatabase.new
  end
  
  specify "should construct proper SQL" do
    @db.create_table :test do
      primary_key :id, :integer, :null => false
      column :name, :text
      index :name, :unique => true
    end
    @db.sqls.should == [
      'CREATE TABLE test (id integer NOT NULL PRIMARY KEY AUTOINCREMENT, name text)',
      'CREATE UNIQUE INDEX test_name_index ON test (name)'
    ]
  end
  
  specify "should create a temporary table" do
    @db.create_table :test_tmp, :temp => true do
      primary_key :id, :integer, :null => false
      column :name, :text
      index :name, :unique => true
    end
    
    @db.sqls.should == [
      'CREATE TEMPORARY TABLE test_tmp (id integer NOT NULL PRIMARY KEY AUTOINCREMENT, name text)',
      'CREATE UNIQUE INDEX test_tmp_name_index ON test_tmp (name)'
    ]
  end
end

context "Database#alter_table" do
  before do
    @db = DummyDatabase.new
  end
  
  specify "should construct proper SQL" do
    @db.alter_table :xyz do
      add_column :aaa, :text, :null => false, :unique => true
      drop_column :bbb
      rename_column :ccc, :ddd
      set_column_type :eee, :integer
      set_column_default :hhh, 'abcd'
      
      add_index :fff, :unique => true
      drop_index :ggg
    end
    
    @db.sqls.should == [
      'ALTER TABLE xyz ADD COLUMN aaa text UNIQUE NOT NULL',
      'ALTER TABLE xyz DROP COLUMN bbb',
      'ALTER TABLE xyz RENAME COLUMN ccc TO ddd',
      'ALTER TABLE xyz ALTER COLUMN eee TYPE integer',
      "ALTER TABLE xyz ALTER COLUMN hhh SET DEFAULT 'abcd'",
      
      'CREATE UNIQUE INDEX xyz_fff_index ON xyz (fff)',
      'DROP INDEX xyz_ggg_index'
    ]
  end
end

context "Database#add_column" do
  before do
    @db = DummyDatabase.new
  end
  
  specify "should construct proper SQL" do
    @db.add_column :test, :name, :text, :unique => true
    @db.sqls.should == [
      'ALTER TABLE test ADD COLUMN name text UNIQUE'
    ]
  end
end

context "Database#drop_column" do
  before do
    @db = DummyDatabase.new
  end
  
  specify "should construct proper SQL" do
    @db.drop_column :test, :name
    @db.sqls.should == [
      'ALTER TABLE test DROP COLUMN name'
    ]
  end
end

context "Database#rename_column" do
  before do
    @db = DummyDatabase.new
  end
  
  specify "should construct proper SQL" do
    @db.rename_column :test, :abc, :def
    @db.sqls.should == [
      'ALTER TABLE test RENAME COLUMN abc TO def'
    ]
  end
end

context "Database#set_column_type" do
  before do
    @db = DummyDatabase.new
  end
  
  specify "should construct proper SQL" do
    @db.set_column_type :test, :name, :integer
    @db.sqls.should == [
      'ALTER TABLE test ALTER COLUMN name TYPE integer'
    ]
  end
end

context "Database#set_column_default" do
  before do
    @db = DummyDatabase.new
  end
  
  specify "should construct proper SQL" do
    @db.set_column_default :test, :name, 'zyx'
    @db.sqls.should == [
      "ALTER TABLE test ALTER COLUMN name SET DEFAULT 'zyx'"
    ]
  end
end

context "Database#add_index" do
  before do
    @db = DummyDatabase.new
  end
  
  specify "should construct proper SQL" do
    @db.add_index :test, :name, :unique => true
    @db.sqls.should == [
      'CREATE UNIQUE INDEX test_name_index ON test (name)'
    ]
  end
  
  specify "should accept multiple columns" do
    @db.add_index :test, [:one, :two]
    @db.sqls.should == [
      'CREATE INDEX test_one_two_index ON test (one, two)'
    ]
  end
end

context "Database#drop_index" do
  before do
    @db = DummyDatabase.new
  end
  
  specify "should construct proper SQL" do
    @db.drop_index :test, :name
    @db.sqls.should == [
      'DROP INDEX test_name_index'
    ]
  end
  
end

class Dummy2Database < Sequel::Database
  attr_reader :sql
  def execute(sql); @sql = sql; end
  def transaction; yield; end
end

context "Database#drop_table" do
  before do
    @db = DummyDatabase.new
  end
  
  specify "should construct proper SQL" do
    @db.drop_table :test
    @db.sqls.should == ['DROP TABLE test']
  end
  
  specify "should accept multiple table names" do
    @db.drop_table :a, :bb, :ccc
    @db.sqls.should == [
      'DROP TABLE a',
      'DROP TABLE bb',
      'DROP TABLE ccc'
    ]
  end
end

context "Database#rename_table" do
  before do
    @db = DummyDatabase.new
  end
  
  specify "should construct proper SQL" do
    @db.rename_table :abc, :xyz
    @db.sqls.should == ['ALTER TABLE abc RENAME TO xyz']
  end
end

context "Database#table_exists?" do
  specify "should try to select the first record from the table's dataset" do
    db2 = DummyDatabase.new
    db2.table_exists?(:a).should be_false
    db2.table_exists?(:b).should be_true
  end
end

class Dummy3Database < Sequel::Database
  attr_reader :sql, :transactions
  def execute(sql, opts={}); @sql ||= []; @sql << sql; end

  class DummyConnection
    def initialize(db); @db = db; end
    def execute(sql); @db.execute(sql); end
  end
end

context "Database#transaction" do
  before do
    @db = Dummy3Database.new
    @db.pool.connection_proc = proc {Dummy3Database::DummyConnection.new(@db)}
  end
  
  specify "should wrap the supplied block with BEGIN + COMMIT statements" do
    @db.transaction {@db.execute 'DROP TABLE test;'}
    @db.sql.should == ['BEGIN', 'DROP TABLE test;', 'COMMIT']
  end
  
  specify "should handle returning inside of the block by committing" do
    def @db.ret_commit
      transaction do
        execute 'DROP TABLE test;'
        return
        execute 'DROP TABLE test2;';
      end
    end
    @db.ret_commit
    @db.sql.should == ['BEGIN', 'DROP TABLE test;', 'COMMIT']
  end
  
  specify "should issue ROLLBACK if an exception is raised, and re-raise" do
    @db.transaction {@db.execute 'DROP TABLE test'; raise RuntimeError} rescue nil
    @db.sql.should == ['BEGIN', 'DROP TABLE test', 'ROLLBACK']
    
    proc {@db.transaction {raise RuntimeError}}.should raise_error(RuntimeError)
  end
  
  specify "should issue ROLLBACK if Sequel::Rollback is called in the transaction" do
    @db.transaction do
      @db.drop_table(:a)
      raise Sequel::Rollback
      @db.drop_table(:b)
    end
    
    @db.sql.should == ['BEGIN', 'DROP TABLE a', 'ROLLBACK']
  end
  
  specify "should raise database errors when commiting a transaction as Sequel::DatabaseError" do
    @db.meta_def(:commit_transaction){raise ArgumentError}
    lambda{@db.transaction{}}.should raise_error(ArgumentError)

    @db.meta_def(:database_error_classes){[ArgumentError]}
    lambda{@db.transaction{}}.should raise_error(Sequel::DatabaseError)
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

context "Database#transaction with savepoints" do
  before do
    @db = Dummy3Database.new
    @db.meta_def(:supports_savepoints?){true}
    @db.pool.connection_proc = proc {Dummy3Database::DummyConnection.new(@db)}
  end
  
  specify "should wrap the supplied block with BEGIN + COMMIT statements" do
    @db.transaction {@db.execute 'DROP TABLE test;'}
    @db.sql.should == ['BEGIN', 'DROP TABLE test;', 'COMMIT']
  end
  
  specify "should use savepoints if given the :savepoint option" do
    @db.transaction{@db.transaction(:savepoint=>true){@db.execute 'DROP TABLE test;'}}
    @db.sql.should == ['BEGIN', 'SAVEPOINT autopoint_1', 'DROP TABLE test;', 'RELEASE SAVEPOINT autopoint_1', 'COMMIT']
  end
  
  specify "should not use a savepoints if no transaction is in progress" do
    @db.transaction(:savepoint=>true){@db.execute 'DROP TABLE test;'}
    @db.sql.should == ['BEGIN', 'DROP TABLE test;', 'COMMIT']
  end
  
  specify "should reuse the current transaction if no :savepoint option is given" do
    @db.transaction{@db.transaction{@db.execute 'DROP TABLE test;'}}
    @db.sql.should == ['BEGIN', 'DROP TABLE test;', 'COMMIT']
  end
  
  specify "should handle returning inside of the block by committing" do
    def @db.ret_commit
      transaction do
        execute 'DROP TABLE test;'
        return
        execute 'DROP TABLE test2;';
      end
    end
    @db.ret_commit
    @db.sql.should == ['BEGIN', 'DROP TABLE test;', 'COMMIT']
  end
  
  specify "should handle returning inside of a savepoint by committing" do
    def @db.ret_commit
      transaction do
        transaction(:savepoint=>true) do
          execute 'DROP TABLE test;'
          return
          execute 'DROP TABLE test2;';
        end
      end
    end
    @db.ret_commit
    @db.sql.should == ['BEGIN', 'SAVEPOINT autopoint_1', 'DROP TABLE test;', 'RELEASE SAVEPOINT autopoint_1', 'COMMIT']
  end
  
  specify "should issue ROLLBACK if an exception is raised, and re-raise" do
    @db.transaction {@db.execute 'DROP TABLE test'; raise RuntimeError} rescue nil
    @db.sql.should == ['BEGIN', 'DROP TABLE test', 'ROLLBACK']
    
    proc {@db.transaction {raise RuntimeError}}.should raise_error(RuntimeError)
  end
  
  specify "should issue ROLLBACK SAVEPOINT if an exception is raised inside a savepoint, and re-raise" do
    @db.transaction{@db.transaction(:savepoint=>true){@db.execute 'DROP TABLE test'; raise RuntimeError}} rescue nil
    @db.sql.should == ['BEGIN', 'SAVEPOINT autopoint_1', 'DROP TABLE test', 'ROLLBACK TO SAVEPOINT autopoint_1', 'ROLLBACK']
    
    proc {@db.transaction {raise RuntimeError}}.should raise_error(RuntimeError)
  end
  
  specify "should issue ROLLBACK if Sequel::Rollback is called in the transaction" do
    @db.transaction do
      @db.drop_table(:a)
      raise Sequel::Rollback
      @db.drop_table(:b)
    end
    
    @db.sql.should == ['BEGIN', 'DROP TABLE a', 'ROLLBACK']
  end
  
  specify "should issue ROLLBACK SAVEPOINT if Sequel::Rollback is called in a savepoint" do
    @db.transaction do
      @db.transaction(:savepoint=>true) do
        @db.drop_table(:a)
        raise Sequel::Rollback
      end
      @db.drop_table(:b)
    end
    
    @db.sql.should == ['BEGIN', 'SAVEPOINT autopoint_1', 'DROP TABLE a', 'ROLLBACK TO SAVEPOINT autopoint_1', 'DROP TABLE b', 'COMMIT']
  end
  
  specify "should raise database errors when commiting a transaction as Sequel::DatabaseError" do
    @db.meta_def(:commit_transaction){raise ArgumentError}
    lambda{@db.transaction{}}.should raise_error(ArgumentError)
    lambda{@db.transaction{@db.transaction(:savepoint=>true){}}}.should raise_error(ArgumentError)

    @db.meta_def(:database_error_classes){[ArgumentError]}
    lambda{@db.transaction{}}.should raise_error(Sequel::DatabaseError)
    lambda{@db.transaction{@db.transaction(:savepoint=>true){}}}.should raise_error(Sequel::DatabaseError)
  end
end

context "A Database adapter with a scheme" do
  before do
    class CCC < Sequel::Database
      if defined?(DISCONNECTS)
        DISCONNECTS.clear
      else
        DISCONNECTS = []
      end
      set_adapter_scheme :ccc
      def disconnect
        DISCONNECTS << self
      end
    end
  end

  specify "should be registered in the ADAPTER_MAP" do
    Sequel::ADAPTER_MAP[:ccc].should == CCC
  end
  
  specify "should give the database_type as the adapter scheme by default" do
    CCC.new.database_type.should == :ccc
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

  specify "should be accessible through Sequel.connect via a block" do
    x = nil
    y = nil
    z = nil

    p = proc do |c|
      c.should be_a_kind_of(CCC)
      c.opts[:host].should == 'localhost'
      c.opts[:database].should == 'db'
      z = y
      y = x
      x = c
    end
    Sequel::Database.connect('ccc://localhost/db', &p).should == nil
    CCC::DISCONNECTS.should == [x]

    Sequel.connect('ccc://localhost/db', &p).should == nil
    CCC::DISCONNECTS.should == [y, x]

    Sequel.send(:def_adapter_method, :ccc)
    Sequel.ccc('db', :host=>'localhost', &p).should == nil
    CCC::DISCONNECTS.should == [z, y, x]
  end

  specify "should be accessible through Sequel.<adapter>" do
    Sequel.send(:def_adapter_method, :ccc)

    # invalid parameters
    proc {Sequel.ccc('abc', 'def')}.should raise_error(Sequel::Error)
    
    c = Sequel.ccc('mydb')
    c.should be_a_kind_of(CCC)
    c.opts.should == {:adapter=>:ccc, :database => 'mydb'}
    
    c = Sequel.ccc('mydb', :host => 'localhost')
    c.should be_a_kind_of(CCC)
    c.opts.should == {:adapter=>:ccc, :database => 'mydb', :host => 'localhost'}
    
    c = Sequel.ccc
    c.should be_a_kind_of(CCC)
    c.opts.should == {:adapter=>:ccc}
    
    c = Sequel.ccc(:database => 'mydb', :host => 'localhost')
    c.should be_a_kind_of(CCC)
    c.opts.should == {:adapter=>:ccc, :database => 'mydb', :host => 'localhost'}
  end
  
  specify "should be accessible through Sequel.connect with options" do
    c = Sequel.connect(:adapter => :ccc, :database => 'mydb')
    c.should be_a_kind_of(CCC)
    c.opts.should == {:adapter => :ccc, :database => 'mydb'}
  end

  specify "should be accessible through Sequel.connect with URL parameters" do
    c = Sequel.connect 'ccc://localhost/db?host=/tmp&user=test'
    c.should be_a_kind_of(CCC)
    c.opts[:host].should == '/tmp'
    c.opts[:database].should == 'db'
    c.opts[:user].should == 'test'
  end

end

context "Sequel::Database.connect" do
  specify "should raise an Error if not given a String or Hash" do
    proc{Sequel::Database.connect(nil)}.should raise_error(Sequel::Error)
    proc{Sequel::Database.connect(Object.new)}.should raise_error(Sequel::Error)
  end
end

context "An unknown database scheme" do
  specify "should raise an error in Sequel::Database.connect" do
    proc {Sequel::Database.connect('ddd://localhost/db')}.should raise_error(Sequel::AdapterNotFound)
  end

  specify "should raise an error in Sequel.connect" do
    proc {Sequel.connect('ddd://localhost/db')}.should raise_error(Sequel::AdapterNotFound)
  end
end

context "A broken adapter (lib is there but the class is not)" do
  before do
    @fn = File.join(File.dirname(__FILE__), '../../lib/sequel/adapters/blah.rb')
    File.open(@fn,'a'){}
  end
  
  after do
    File.delete(@fn)
  end
  
  specify "should raise an error" do
    proc {Sequel.connect('blah://blow')}.should raise_error(Sequel::AdapterNotFound)
  end
end

context "A single threaded database" do
  after do
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
  before do
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
  before do
    Sequel::Database.single_threaded = false
  end
  
  after do
    Sequel::Database.single_threaded = false
  end
  
  specify "should have single_threaded? respond to true if in single threaded mode" do
    db = Sequel::Database.new(:single_threaded => true)
    db.should be_single_threaded
    
    db = Sequel::Database.new(:max_options => 1)
    db.should_not be_single_threaded
    
    db = Sequel::Database.new
    db.should_not be_single_threaded
    
    Sequel::Database.single_threaded = true
    
    db = Sequel::Database.new
    db.should be_single_threaded
    
    db = Sequel::Database.new(:max_options => 4)
    db.should be_single_threaded
  end
  
  specify "should be able to set loggers via the logger= and loggers= methods" do
    db = Sequel::Database.new
    s = "I'm a logger"
    db.logger = s
    db.loggers.should == [s]
    db.logger = nil
    db.loggers.should == []

    db.loggers = [s]
    db.loggers.should == [s]
    db.loggers = []
    db.loggers.should == []

    t = "I'm also a logger"
    db.loggers = [s, t]
    db.loggers.should == [s,t]
  end
end

context "Database#fetch" do
  before do
    @db = Sequel::Database.new
    c = Class.new(Sequel::Dataset) do
      def fetch_rows(sql); yield({:sql => sql}); end
    end
    @db.meta_def(:dataset) {c.new(self)}
  end
  
  specify "should create a dataset and invoke its fetch_rows method with the given sql" do
    sql = nil
    @db.fetch('select * from xyz') {|r| sql = r[:sql]}
    sql.should == 'select * from xyz'
  end
  
  specify "should format the given sql with any additional arguments" do
    sql = nil
    @db.fetch('select * from xyz where x = ? and y = ?', 15, 'abc') {|r| sql = r[:sql]}
    sql.should == "select * from xyz where x = 15 and y = 'abc'"
    
    # and Aman Gupta's example
    @db.fetch('select name from table where name = ? or id in ?',
    'aman', [3,4,7]) {|r| sql = r[:sql]}
    sql.should == "select name from table where name = 'aman' or id in (3, 4, 7)"
  end
  
  specify "should return the dataset if no block is given" do
    @db.fetch('select * from xyz').should be_a_kind_of(Sequel::Dataset)
    
    @db.fetch('select a from b').map {|r| r[:sql]}.should == ['select a from b']

    @db.fetch('select c from d').inject([]) {|m, r| m << r; m}.should == \
      [{:sql => 'select c from d'}]
  end
  
  specify "should return a dataset that always uses the given sql for SELECTs" do
    ds = @db.fetch('select * from xyz')
    ds.select_sql.should == 'select * from xyz'
    ds.sql.should == 'select * from xyz'
    
    ds.filter!(:price.sql_number < 100)
    ds.select_sql.should == 'select * from xyz'
    ds.sql.should == 'select * from xyz'
  end
end


context "Database#[]" do
  before do
    @db = Sequel::Database.new
  end
  
  specify "should return a dataset when symbols are given" do
    ds = @db[:items]
    ds.class.should == Sequel::Dataset
    ds.opts[:from].should == [:items]
  end
  
  specify "should return an enumerator when a string is given" do
    c = Class.new(Sequel::Dataset) do
      def fetch_rows(sql); yield({:sql => sql}); end
    end
    @db.meta_def(:dataset) {c.new(self)}

    sql = nil
    @db['select * from xyz where x = ? and y = ?', 15, 'abc'].each {|r| sql = r[:sql]}
    sql.should == "select * from xyz where x = 15 and y = 'abc'"
  end
end

context "Database#create_view" do
  before do
    @db = DummyDatabase.new
  end
  
  specify "should construct proper SQL with raw SQL" do
    @db.create_view :test, "SELECT * FROM xyz"
    @db.sqls.should == ['CREATE VIEW test AS SELECT * FROM xyz']
    @db.sqls.clear
    @db.create_view :test.identifier, "SELECT * FROM xyz"
    @db.sqls.should == ['CREATE VIEW test AS SELECT * FROM xyz']
  end
  
  specify "should construct proper SQL with dataset" do
    @db.create_view :test, @db[:items].select(:a, :b).order(:c)
    @db.sqls.should == ['CREATE VIEW test AS SELECT a, b FROM items ORDER BY c']
    @db.sqls.clear
    @db.create_view :test.qualify(:sch), @db[:items].select(:a, :b).order(:c)
    @db.sqls.should == ['CREATE VIEW sch.test AS SELECT a, b FROM items ORDER BY c']
  end
end

context "Database#create_or_replace_view" do
  before do
    @db = DummyDatabase.new
  end
  
  specify "should construct proper SQL with raw SQL" do
    @db.create_or_replace_view :test, "SELECT * FROM xyz"
    @db.sqls.should == ['CREATE OR REPLACE VIEW test AS SELECT * FROM xyz']
    @db.sqls.clear
    @db.create_or_replace_view :sch__test, "SELECT * FROM xyz"
    @db.sqls.should == ['CREATE OR REPLACE VIEW sch.test AS SELECT * FROM xyz']
  end

  specify "should construct proper SQL with dataset" do
    @db.create_or_replace_view :test, @db[:items].select(:a, :b).order(:c)
    @db.sqls.should == ['CREATE OR REPLACE VIEW test AS SELECT a, b FROM items ORDER BY c']
    @db.sqls.clear
    @db.create_or_replace_view :test.identifier, @db[:items].select(:a, :b).order(:c)
    @db.sqls.should == ['CREATE OR REPLACE VIEW test AS SELECT a, b FROM items ORDER BY c']
  end
end

context "Database#drop_view" do
  before do
    @db = DummyDatabase.new
  end
  
  specify "should construct proper SQL" do
    @db.drop_view :test
    @db.drop_view :test.identifier
    @db.drop_view :sch__test
    @db.drop_view :test.qualify(:sch)
    @db.sqls.should == ['DROP VIEW test', 'DROP VIEW test', 'DROP VIEW sch.test', 'DROP VIEW sch.test']
  end
end

context "Database#alter_table_sql" do
  before do
    @db = DummyDatabase.new
  end
  
  specify "should raise error for an invalid op" do
    proc {@db.send(:alter_table_sql, :mau, :op => :blah)}.should raise_error(Sequel::Error)
  end
end

context "Database#inspect" do
  before do
    @db = DummyDatabase.new
    
    @db.meta_def(:uri) {'blah://blahblah/blah'}
  end
  
  specify "should include the class name and the connection url" do
    @db.inspect.should == '#<DummyDatabase: "blah://blahblah/blah">'
  end
end

context "Database#get" do
  before do
    @c = Class.new(DummyDatabase) do
      def dataset
        ds = super
        def ds.get(*args, &block)
          @db.execute select(*args, &block).sql
          args
        end
        ds
      end
    end
    
    @db = @c.new
  end
  
  specify "should use Dataset#get to get a single value" do
    @db.get(1).should == [1]
    @db.sqls.last.should == 'SELECT 1'
    
    @db.get(:version.sql_function)
    @db.sqls.last.should == 'SELECT version()'
  end

  specify "should accept a block" do
    @db.get{1}
    @db.sqls.last.should == 'SELECT 1'
    
    @db.get{version(1)}
    @db.sqls.last.should == 'SELECT version(1)'
  end
end

context "Database#call" do
  specify "should call the prepared statement with the given name" do
    db = MockDatabase.new
    db[:items].prepare(:select, :select_all)
    db.call(:select_all).should == [{:id => 1, :x => 1}]
    db[:items].filter(:n=>:$n).prepare(:select, :select_n)
    db.call(:select_n, :n=>1).should == [{:id => 1, :x => 1}]
    db.sqls.should == ['SELECT * FROM items', 'SELECT * FROM items WHERE (n = 1)']
  end
end

context "Database#server_opts" do
  specify "should return the general opts if no :servers option is used" do
    opts = {:host=>1, :database=>2}
    MockDatabase.new(opts).send(:server_opts, :server1).should == {:host=>1, :database=>2}
  end
  
  specify "should return the general opts if entry for the server is present in the :servers option" do
    opts = {:host=>1, :database=>2, :servers=>{}}
    MockDatabase.new(opts).send(:server_opts, :server1).should == {:host=>1, :database=>2}
  end
  
  specify "should return the general opts merged with the specific opts if given as a hash" do
    opts = {:host=>1, :database=>2, :servers=>{:server1=>{:host=>3}}}
    MockDatabase.new(opts).send(:server_opts, :server1).should == {:host=>3, :database=>2}
  end
  
  specify "should return the sgeneral opts merged with the specific opts if given as a proc" do
    opts = {:host=>1, :database=>2, :servers=>{:server1=>proc{|db| {:host=>4}}}}
    MockDatabase.new(opts).send(:server_opts, :server1).should == {:host=>4, :database=>2}
  end
  
  specify "should raise an error if the specific opts is not a proc or hash" do
    opts = {:host=>1, :database=>2, :servers=>{:server1=>2}}
    proc{MockDatabase.new(opts).send(:server_opts, :server1)}.should raise_error(Sequel::Error)
  end
end

context "Database#raise_error" do
  specify "should reraise if the exception class is not in opts[:classes]" do
    e = Class.new(StandardError)
    proc{MockDatabase.new.send(:raise_error, e.new(''), :classes=>[])}.should raise_error(e)
  end

  specify "should convert the exception to a DatabaseError if the exception class is not in opts[:classes]" do
    proc{MockDatabase.new.send(:raise_error, Interrupt.new(''), :classes=>[Interrupt])}.should raise_error(Sequel::DatabaseError)
  end

  specify "should convert the exception to a DatabaseError if opts[:classes] if not present" do
    proc{MockDatabase.new.send(:raise_error, Interrupt.new(''))}.should raise_error(Sequel::DatabaseError)
  end
  
  specify "should convert the exception to a DatabaseDisconnectError if opts[:disconnect] is true" do
    proc{MockDatabase.new.send(:raise_error, Interrupt.new(''), :disconnect=>true)}.should raise_error(Sequel::DatabaseDisconnectError)
  end
end

context "Database#typecast_value" do
  before do
    @db = Sequel::Database.new
  end
  specify "should raise an InvalidValue when given an invalid value" do
    proc{@db.typecast_value(:integer, "13a")}.should raise_error(Sequel::InvalidValue)
    proc{@db.typecast_value(:float, "4.e2")}.should raise_error(Sequel::InvalidValue)
    proc{@db.typecast_value(:decimal, :invalid_value)}.should raise_error(Sequel::InvalidValue)
    proc{@db.typecast_value(:date, Object.new)}.should raise_error(Sequel::InvalidValue)
    proc{@db.typecast_value(:date, 'a')}.should raise_error(Sequel::InvalidValue)
    proc{@db.typecast_value(:time, Date.new)}.should raise_error(Sequel::InvalidValue)
    proc{@db.typecast_value(:datetime, 4)}.should raise_error(Sequel::InvalidValue)
  end
end

context "Database#blank_object?" do
  specify "should return whether the object is considered blank" do
    db = Sequel::Database.new
    c = lambda{|meth, value| Class.new{define_method(meth){value}}.new}

    db.send(:blank_object?, "").should == true
    db.send(:blank_object?, "  ").should == true
    db.send(:blank_object?, nil).should == true
    db.send(:blank_object?, false).should == true
    db.send(:blank_object?, []).should == true
    db.send(:blank_object?, {}).should == true
    db.send(:blank_object?, c[:empty?, true]).should == true
    db.send(:blank_object?, c[:blank?, true]).should == true

    db.send(:blank_object?, " a ").should == false
    db.send(:blank_object?, 1).should == false
    db.send(:blank_object?, 1.0).should == false
    db.send(:blank_object?, true).should == false
    db.send(:blank_object?, [1]).should == false
    db.send(:blank_object?, {1.0=>2.0}).should == false
    db.send(:blank_object?, c[:empty?, false]).should == false
    db.send(:blank_object?, c[:blank?, false]).should == false 
  end
end

context "Database#schema_autoincrementing_primary_key?" do
  specify "should whether the parsed schema row indicates a primary key" do
    m = Sequel::Database.new.method(:schema_autoincrementing_primary_key?)
    m.call(:primary_key=>true).should == true
    m.call(:primary_key=>false).should == false
  end
end

context "Database#supports_savepoints?" do
  specify "should be false by default" do
    Sequel::Database.new.supports_savepoints?.should == false
  end
end

context "Database#input_identifier_meth" do
  specify "should be the input_identifer method of a default dataset for this database" do
    db = Sequel::Database.new
    db.send(:input_identifier_meth).call(:a).should == 'a'
    db.identifier_input_method = :upcase
    db.send(:input_identifier_meth).call(:a).should == 'A'
  end
end

context "Database#output_identifier_meth" do
  specify "should be the output_identifer method of a default dataset for this database" do
    db = Sequel::Database.new
    db.send(:output_identifier_meth).call('A').should == :A
    db.identifier_output_method = :downcase
    db.send(:output_identifier_meth).call('A').should == :a
  end
end

context "Database#metadata_dataset" do
  specify "should be a dataset with the default settings for identifier_input_method and identifier_output_method" do
    ds = Sequel::Database.new.send(:metadata_dataset)
    ds.literal(:a).should == 'A'
    ds.send(:output_identifier, 'A').should == :a
  end
end
