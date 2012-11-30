require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')

describe "A new Database" do
  before do
    @db = Sequel::Database.new(1 => 2, :logger => 3)
  end
  after do
    Sequel.quote_identifiers = false
    Sequel.identifier_input_method = nil
    Sequel.identifier_output_method = nil
  end
  
  specify "should receive options" do
    @db.opts[1].should == 2
    @db.opts[:logger].should == 3  
  end
  
  specify "should set the logger from opts[:logger] and opts[:loggers]" do
    @db.loggers.should == [3]
    Sequel::Database.new(1 => 2, :loggers => 3).loggers.should == [3]
    Sequel::Database.new(1 => 2, :loggers => [3]).loggers.should == [3]
    Sequel::Database.new(1 => 2, :logger => 4, :loggers => 3).loggers.should == [4,3]
    Sequel::Database.new(1 => 2, :logger => [4], :loggers => [3]).loggers.should == [4,3]
  end

  specify "should handle the default string column size" do
    @db.default_string_column_size.should == 255
    db = Sequel::Database.new(:default_string_column_size=>50)
    db.default_string_column_size.should == 50
    db.default_string_column_size = 2
    db.default_string_column_size.should == 2
  end
  
  specify "should set the sql_log_level from opts[:sql_log_level]" do
    Sequel::Database.new(1 => 2, :sql_log_level=>:debug).sql_log_level.should == :debug
    Sequel::Database.new(1 => 2, :sql_log_level=>'debug').sql_log_level.should == :debug
  end
  
  specify "should create a connection pool" do
    @db.pool.should be_a_kind_of(Sequel::ConnectionPool)
    @db.pool.max_size.should == 4
    
    Sequel::Database.new(:max_connections => 10).pool.max_size.should == 10
  end
  
  specify "should pass the supplied block to the connection pool" do
    cc = nil
    d = Sequel::Database.new
    d.meta_def(:connect){|c| 1234}
    d.synchronize {|c| cc = c}
    cc.should == 1234
  end

  specify "should respect the :single_threaded option" do
    db = Sequel::Database.new(:single_threaded=>true){123}
    db.pool.should be_a_kind_of(Sequel::SingleConnectionPool)
    db = Sequel::Database.new(:single_threaded=>'t'){123}
    db.pool.should be_a_kind_of(Sequel::SingleConnectionPool)
    db = Sequel::Database.new(:single_threaded=>'1'){123}
    db.pool.should be_a_kind_of(Sequel::SingleConnectionPool)
    db = Sequel::Database.new(:single_threaded=>false){123}
    db.pool.should be_a_kind_of(Sequel::ConnectionPool)
    db = Sequel::Database.new(:single_threaded=>'f'){123}
    db.pool.should be_a_kind_of(Sequel::ConnectionPool)
    db = Sequel::Database.new(:single_threaded=>'0'){123}
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
    db.identifier_input_method.should be_nil
    db.identifier_input_method = :downcase
    db.identifier_input_method.should == :downcase
    db = Sequel::Database.new(:identifier_input_method=>:upcase)
    db.identifier_input_method.should == :upcase
    db.identifier_input_method = nil
    db.identifier_input_method.should be_nil
    Sequel.identifier_input_method = :downcase
    Sequel::Database.identifier_input_method.should == :downcase
    db = Sequel::Database.new(:identifier_input_method=>nil)
    db.identifier_input_method.should be_nil
    db.identifier_input_method = :upcase
    db.identifier_input_method.should == :upcase
    db = Sequel::Database.new(:identifier_input_method=>:upcase)
    db.identifier_input_method.should == :upcase
    db.identifier_input_method = nil
    db.identifier_input_method.should be_nil
  end
  
  specify "should respect the :identifier_output_method option" do
    Sequel.identifier_output_method = nil
    Sequel::Database.identifier_output_method.should == ""
    db = Sequel::Database.new(:identifier_output_method=>nil)
    db.identifier_output_method.should be_nil
    db.identifier_output_method = :downcase
    db.identifier_output_method.should == :downcase
    db = Sequel::Database.new(:identifier_output_method=>:upcase)
    db.identifier_output_method.should == :upcase
    db.identifier_output_method = nil
    db.identifier_output_method.should be_nil
    Sequel.identifier_output_method = :downcase
    Sequel::Database.identifier_output_method.should == :downcase
    db = Sequel::Database.new(:identifier_output_method=>nil)
    db.identifier_output_method.should be_nil
    db.identifier_output_method = :upcase
    db.identifier_output_method.should == :upcase
    db = Sequel::Database.new(:identifier_output_method=>:upcase)
    db.identifier_output_method.should == :upcase
    db.identifier_output_method = nil
    db.identifier_output_method.should be_nil
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

  specify "should populate :adapter option when using connection string" do
    Sequel.connect('mock:/').opts[:adapter].should == "mock"
  end
end

describe "Database#disconnect" do
  specify "should call pool.disconnect" do
    d = Sequel::Database.new
    p = d.pool
    p.should_receive(:disconnect).once.with({}).and_return(2)
    d.disconnect.should == 2
  end
end

describe "Sequel.extension" do
  specify "should attempt to load the given extension" do
    proc{Sequel.extension :blah}.should raise_error(LoadError)
  end
end

describe "Database#connect" do
  specify "should raise Sequel::NotImplemented" do
    proc {Sequel::Database.new.connect(:default)}.should raise_error(Sequel::NotImplemented)
  end
end

describe "Database#log_info" do
  before do
    @o = Object.new
    def @o.logs; @logs || []; end
    def @o.to_ary; [self]; end
    def @o.method_missing(*args); (@logs ||= []) << args; end
    @db = Sequel::Database.new(:logger=>@o)
  end

  specify "should log message at info level to all loggers" do
    @db.log_info('blah')
    @o.logs.should == [[:info, 'blah']]
  end

  specify "should log message with args at info level to all loggers" do
    @db.log_info('blah', [1, 2])
    @o.logs.should == [[:info, 'blah; [1, 2]']]
  end
end

describe "Database#log_yield" do
  before do
    @o = Object.new
    def @o.logs; @logs || []; end
    def @o.warn(*args); (@logs ||= []) << [:warn] + args; end
    def @o.method_missing(*args); (@logs ||= []) << args; end
    def @o.to_ary; [self]; end
    @db = Sequel::Database.new(:logger=>@o)
  end

  specify "should yield to the passed block" do
    a = nil
    @db.log_yield('blah'){a = 1}
    a.should == 1
  end

  specify "should raise an exception if a block is not passed" do
    proc{@db.log_yield('blah')}.should raise_error
  end

  specify "should log message with duration at info level to all loggers" do
    @db.log_yield('blah'){}
    @o.logs.length.should == 1
    @o.logs.first.length.should == 2
    @o.logs.first.first.should == :info
    @o.logs.first.last.should =~ /\A\(\d\.\d{6}s\) blah\z/ 
  end

  specify "should respect sql_log_level setting" do
    @db.sql_log_level = :debug
    @db.log_yield('blah'){}
    @o.logs.length.should == 1
    @o.logs.first.length.should == 2
    @o.logs.first.first.should == :debug
    @o.logs.first.last.should =~ /\A\(\d\.\d{6}s\) blah\z/ 
  end

  specify "should log message with duration at warn level if duration greater than log_warn_duration" do
    @db.log_warn_duration = 0
    @db.log_yield('blah'){}
    @o.logs.length.should == 1
    @o.logs.first.length.should == 2
    @o.logs.first.first.should == :warn
    @o.logs.first.last.should =~ /\A\(\d\.\d{6}s\) blah\z/ 
  end

  specify "should log message with duration at info level if duration less than log_warn_duration" do
    @db.log_warn_duration = 1000
    @db.log_yield('blah'){}
    @o.logs.length.should == 1
    @o.logs.first.length.should == 2
    @o.logs.first.first.should == :info
    @o.logs.first.last.should =~ /\A\(\d\.\d{6}s\) blah\z/ 
  end

  specify "should log message at error level if block raises an error" do
    @db.log_warn_duration = 0
    proc{@db.log_yield('blah'){raise Sequel::Error, 'adsf'}}.should raise_error
    @o.logs.length.should == 1
    @o.logs.first.length.should == 2
    @o.logs.first.first.should == :error
    @o.logs.first.last.should =~ /\ASequel::Error: adsf: blah\z/ 
  end

  specify "should include args with message if args passed" do
    @db.log_yield('blah', [1, 2]){}
    @o.logs.length.should == 1
    @o.logs.first.length.should == 2
    @o.logs.first.first.should == :info
    @o.logs.first.last.should =~ /\A\(\d\.\d{6}s\) blah; \[1, 2\]\z/ 
  end
end

describe "Database#uri" do
  before do
    @c = Class.new(Sequel::Database) do
      set_adapter_scheme :mau
    end
    
    @db = Sequel.connect('mau://user:pass@localhost:9876/maumau')
  end
  
  specify "should return the connection URI for the database" do
    @db.uri.should == 'mau://user:pass@localhost:9876/maumau'
  end
  
  specify "should return nil if a connection uri was not used" do
    Sequel.mock.uri.should be_nil
  end
  
  specify "should be aliased as #url" do
    @db.url.should == 'mau://user:pass@localhost:9876/maumau'
  end
end

describe "Database.adapter_scheme and #adapter_scheme" do
  specify "should return the database schema" do
    Sequel::Database.adapter_scheme.should be_nil

    @c = Class.new(Sequel::Database) do
      set_adapter_scheme :mau
    end
    
    @c.adapter_scheme.should == :mau
    @c.new({}).adapter_scheme.should == :mau
  end
end

describe "Database#dataset" do
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
    d = @db.from(:mau){x.sql_number > 100}
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

describe "Database#dataset_class" do
  before do
    @db = Sequel::Database.new
    @dsc = Class.new(Sequel::Dataset)
  end
  
  specify "should have setter set the class to use to create datasets" do
    @db.dataset_class = @dsc
    ds = @db.dataset
    ds.should be_a_kind_of(@dsc)
    ds.opts.should == {}
    ds.db.should be(@db)
  end

  specify "should have getter return the class to use to create datasets" do
    @db.dataset_class.should == Sequel::Dataset
    @db.dataset_class = @dsc
    @db.dataset_class.should == @dsc
  end
end
  
describe "Database#extend_datasets" do
  before do
    @db = Sequel::Database.new
    @m = Module.new{def foo() [3] end}
    @m2 = Module.new{def foo() [4] + super end}
    @db.extend_datasets(@m)
  end
  
  specify "should clear a cached dataset" do
    @db = Sequel::Database.new
    @db.literal(1).should == '1'
    @db.extend_datasets{def literal(v) '2' end}
    @db.literal(1).should == '2'
  end

  specify "should change the dataset class to a subclass the first time it is called" do
    @db.dataset_class.superclass.should == Sequel::Dataset
  end

  specify "should not create a subclass of the dataset class if called more than once" do
    @db.extend_datasets(@m2)
    @db.dataset_class.superclass.should == Sequel::Dataset
  end

  specify "should make the dataset class include the module" do
    @db.dataset_class.ancestors.should include(@m)
    @db.dataset_class.ancestors.should_not include(@m2)
    @db.extend_datasets(@m2)
    @db.dataset_class.ancestors.should include(@m)
    @db.dataset_class.ancestors.should include(@m2)
  end

  specify "should have datasets respond to the module's methods" do
    @db.dataset.foo.should == [3]
    @db.extend_datasets(@m2)
    @db.dataset.foo.should == [4, 3]
  end

  specify "should take a block and create a module from it to use" do
    @db.dataset.foo.should == [3]
    @db.extend_datasets{def foo() [5] + super end}
    @db.dataset.foo.should == [5, 3]
  end

  specify "should raise an error if both a module and a block are provided" do
    proc{@db.extend_datasets(@m2){def foo() [5] + super end}}.should raise_error(Sequel::Error)
  end

  specify "should be able to override methods defined in the original Dataset class" do
    @db.extend_datasets(Module.new{def select(*a, &block) super.order(*a, &block) end})
    @db[:t].select(:a, :b).sql.should == 'SELECT a, b FROM t ORDER BY a, b'
  end

  specify "should reapply settings if dataset_class is chagned" do
    c = Class.new(Sequel::Dataset)
    @db.dataset_class = c
    @db.dataset_class.superclass.should == c
    @db.dataset_class.ancestors.should include(@m)
    @db.dataset.foo.should == [3]
  end
end
  
describe "Database#execute" do
  specify "should raise Sequel::NotImplemented" do
    proc {Sequel::Database.new.execute('blah blah')}.should raise_error(Sequel::NotImplemented)
  end
end

describe "Database#tables" do
  specify "should raise Sequel::NotImplemented" do
    proc {Sequel::Database.new.tables}.should raise_error(Sequel::NotImplemented)
  end
end

describe "Database#views" do
  specify "should raise Sequel::NotImplemented" do
    proc {Sequel::Database.new.views}.should raise_error(Sequel::NotImplemented)
  end
end

describe "Database#indexes" do
  specify "should raise Sequel::NotImplemented" do
    proc {Sequel::Database.new.indexes(:table)}.should raise_error(Sequel::NotImplemented)
  end
end

describe "Database#foreign_key_list" do
  specify "should raise Sequel::NotImplemented" do
    proc {Sequel::Database.new.foreign_key_list(:table)}.should raise_error(Sequel::NotImplemented)
  end
end

describe "Database#run" do
  before do
    @db = Sequel.mock(:servers=>{:s1=>{}})
  end
  
  specify "should execute the code on the database" do
    @db.run("DELETE FROM items")
    @db.sqls.should == ["DELETE FROM items"]
  end
  
  specify "should return nil" do
    @db.run("DELETE FROM items").should be_nil
  end
  
  specify "should accept options passed to execute_ddl" do
    @db.run("DELETE FROM items", :server=>:s1)
    @db.sqls.should == ["DELETE FROM items -- s1"]
  end
end

describe "Database#<<" do
  before do
    @db = Sequel.mock
  end

  specify "should execute the code on the database" do
    @db << "DELETE FROM items"
    @db.sqls.should == ["DELETE FROM items"]
  end
  
  specify "should be chainable" do
    @db << "DELETE FROM items" << "DELETE FROM items2"
    @db.sqls.should == ["DELETE FROM items", "DELETE FROM items2"]
  end
end

describe "Database#synchronize" do
  before do
    @db = Sequel::Database.new(:max_connections => 1)
    @db.meta_def(:connect){|c| 12345}
  end
  
  specify "should wrap the supplied block in pool.hold" do
    q, q1, q2, q3 = Queue.new, Queue.new, Queue.new, Queue.new
    c1, c2 = nil
    t1 = Thread.new{@db.synchronize{|c| c1 = c; q.push nil; q1.pop}; q.push nil}
    q.pop
    c1.should == 12345
    t2 = Thread.new{@db.synchronize{|c| c2 = c; q2.push nil}}
    @db.pool.available_connections.should be_empty
    c2.should be_nil
    q1.push nil
    q.pop
    q2.pop
    c2.should == 12345
    t1.join
    t2.join
  end
end

describe "Database#test_connection" do
  before do
    @db = Sequel::Database.new
    pr = proc{@test = rand(100)}
    @db.meta_def(:connect){|c| pr.call}
  end
  
  specify "should attempt to get a connection" do
    @db.test_connection
    @test.should_not be_nil
  end
  
  specify "should return true if successful" do
    @db.test_connection.should be_true
  end

  specify "should raise an error if the attempting to connect raises an error" do
    proc{Sequel::Database.new{raise Sequel::Error, 'blah'}.test_connection}.should raise_error(Sequel::Error)
  end
end

describe "Database#table_exists?" do
  specify "should try to select the first record from the table's dataset" do
    db = Sequel.mock(:fetch=>[Sequel::Error, [], [{:a=>1}]])
    db.table_exists?(:a).should be_false
    db.sqls.should == ["SELECT NULL FROM a LIMIT 1"]
    db.table_exists?(:b).should be_true
    db.table_exists?(:c).should be_true
  end
end

shared_examples_for "Database#transaction" do  
  specify "should wrap the supplied block with BEGIN + COMMIT statements" do
    @db.transaction{@db.execute 'DROP TABLE test;'}
    @db.sqls.should == ['BEGIN', 'DROP TABLE test;', 'COMMIT']
  end
  
  specify "should support transaction isolation levels" do
    @db.meta_def(:supports_transaction_isolation_levels?){true}
    [:uncommitted, :committed, :repeatable, :serializable].each do |l|
      @db.transaction(:isolation=>l){@db.run "DROP TABLE #{l}"}
    end
    @db.sqls.should == ['BEGIN', 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED', 'DROP TABLE uncommitted', 'COMMIT',
                       'BEGIN', 'SET TRANSACTION ISOLATION LEVEL READ COMMITTED', 'DROP TABLE committed', 'COMMIT',
                       'BEGIN', 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ', 'DROP TABLE repeatable', 'COMMIT',
                       'BEGIN', 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE', 'DROP TABLE serializable', 'COMMIT']
  end

  specify "should allow specifying a default transaction isolation level" do
    @db.meta_def(:supports_transaction_isolation_levels?){true}
    [:uncommitted, :committed, :repeatable, :serializable].each do |l|
      @db.transaction_isolation_level = l
      @db.transaction{@db.run "DROP TABLE #{l}"}
    end
    @db.sqls.should == ['BEGIN', 'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED', 'DROP TABLE uncommitted', 'COMMIT',
                       'BEGIN', 'SET TRANSACTION ISOLATION LEVEL READ COMMITTED', 'DROP TABLE committed', 'COMMIT',
                       'BEGIN', 'SET TRANSACTION ISOLATION LEVEL REPEATABLE READ', 'DROP TABLE repeatable', 'COMMIT',
                       'BEGIN', 'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE', 'DROP TABLE serializable', 'COMMIT']
  end
  
  specify "should support :disconnect=>:retry option for automatically retrying on disconnect" do
    a = []
    @db.transaction(:disconnect=>:retry){a << 1; raise Sequel::DatabaseDisconnectError if a.length < 2}
    @db.sqls.should == ['BEGIN', 'ROLLBACK', 'BEGIN', 'COMMIT']
    a.should == [1, 1]
  end
  
  specify "should raise an error if attempting to use :disconnect=>:retry inside another transaction" do
    proc{@db.transaction{@db.transaction(:disconnect=>:retry){}}}.should raise_error(Sequel::Error)
    @db.sqls.should == ['BEGIN', 'ROLLBACK']
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
    @db.sqls.should == ['BEGIN', 'DROP TABLE test;', 'COMMIT']
  end
  
  specify "should issue ROLLBACK if an exception is raised, and re-raise" do
    @db.transaction {@db.execute 'DROP TABLE test'; raise RuntimeError} rescue nil
    @db.sqls.should == ['BEGIN', 'DROP TABLE test', 'ROLLBACK']
    
    proc {@db.transaction {raise RuntimeError}}.should raise_error(RuntimeError)
  end
  
  specify "should handle errors when sending BEGIN" do
    ec = Class.new(StandardError)
    @db.meta_def(:database_error_classes){[ec]}
    @db.meta_def(:log_connection_execute){|c, sql| sql =~ /BEGIN/ ? raise(ec, 'bad') : super(c, sql)}
    begin
      @db.transaction{@db.execute 'DROP TABLE test;'}
    rescue Sequel::DatabaseError => e
    end
    e.should_not be_nil
    e.wrapped_exception.should be_a_kind_of(ec)
    @db.sqls.should == ['ROLLBACK']
  end
  
  specify "should handle errors when sending COMMIT" do
    ec = Class.new(StandardError)
    @db.meta_def(:database_error_classes){[ec]}
    @db.meta_def(:log_connection_execute){|c, sql| sql =~ /COMMIT/ ? raise(ec, 'bad') : super(c, sql)}
    begin
      @db.transaction{@db.execute 'DROP TABLE test;'}
    rescue Sequel::DatabaseError => e
    end
    e.should_not be_nil
    e.wrapped_exception.should be_a_kind_of(ec)
    @db.sqls.should == ['BEGIN', 'DROP TABLE test;']
  end
  
  specify "should handle errors when sending ROLLBACK" do
    ec = Class.new(StandardError)
    @db.meta_def(:database_error_classes){[ec]}
    @db.meta_def(:log_connection_execute){|c, sql| sql =~ /ROLLBACK/ ? raise(ec, 'bad') : super(c, sql)}
    begin
      @db.transaction{raise ArgumentError, 'asdf'}
    rescue Sequel::DatabaseError => e
    end
    e.should_not be_nil
    e.wrapped_exception.should be_a_kind_of(ec)
    @db.sqls.should == ['BEGIN']
  end
  
  specify "should issue ROLLBACK if Sequel::Rollback is called in the transaction" do
    @db.transaction do
      @db.drop_table(:a)
      raise Sequel::Rollback
      @db.drop_table(:b)
    end
    
    @db.sqls.should == ['BEGIN', 'DROP TABLE a', 'ROLLBACK']
  end
  
  specify "should have in_transaction? return true if inside a transaction" do
    c = nil
    @db.transaction{c = @db.in_transaction?}
    c.should be_true
  end
  
  specify "should have in_transaction? handle sharding correctly" do
    c = []
    @db.transaction(:server=>:test){c << @db.in_transaction?}
    @db.transaction(:server=>:test){c << @db.in_transaction?(:server=>:test)}
    c.should == [false, true]
  end
  
  specify "should have in_transaction? return false if not in a transaction" do
    @db.in_transaction?.should be_false
  end
  
  specify "should return nil if Sequel::Rollback is called in the transaction" do
    @db.transaction{raise Sequel::Rollback}.should be_nil
  end
  
  specify "should reraise Sequel::Rollback errors when using :rollback=>:reraise option is given" do
    proc {@db.transaction(:rollback=>:reraise){raise Sequel::Rollback}}.should raise_error(Sequel::Rollback)
    @db.sqls.should == ['BEGIN', 'ROLLBACK']
    proc {@db.transaction(:rollback=>:reraise){raise ArgumentError}}.should raise_error(ArgumentError)
    @db.sqls.should == ['BEGIN', 'ROLLBACK']
    @db.transaction(:rollback=>:reraise){1}.should == 1
    @db.sqls.should == ['BEGIN', 'COMMIT']
  end
  
  specify "should always rollback if :rollback=>:always option is given" do
    proc {@db.transaction(:rollback=>:always){raise ArgumentError}}.should raise_error(ArgumentError)
    @db.sqls.should == ['BEGIN', 'ROLLBACK']
    @db.transaction(:rollback=>:always){raise Sequel::Rollback}.should be_nil
    @db.sqls.should == ['BEGIN', 'ROLLBACK']
    @db.transaction(:rollback=>:always){1}.should be_nil
    @db.sqls.should == ['BEGIN', 'ROLLBACK']
    catch (:foo) do
      @db.transaction(:rollback=>:always){throw :foo}
    end
    @db.sqls.should == ['BEGIN', 'ROLLBACK']
  end
  
  specify "should raise database errors when commiting a transaction as Sequel::DatabaseError" do
    @db.meta_def(:commit_transaction){raise ArgumentError}
    lambda{@db.transaction{}}.should raise_error(ArgumentError)

    @db.meta_def(:database_error_classes){[ArgumentError]}
    lambda{@db.transaction{}}.should raise_error(Sequel::DatabaseError)
  end
  
  specify "should be re-entrant" do
    q, q1 = Queue.new, Queue.new
    cc = nil
    t = Thread.new do
      @db.transaction {@db.transaction {@db.transaction {|c|
        cc = c
        q.pop
        q1.push nil
        q.pop
      }}}
    end
    q.push nil
    q1.pop
    cc.should be_a_kind_of(Sequel::Mock::Connection)
    tr = @db.instance_variable_get(:@transactions)
    tr.keys.should == [cc]
    q.push nil
    t.join
    tr.should be_empty
  end

  specify "should correctly handle nested transacation use with separate shards" do
    @db.transaction do |c1|
      @db.transaction(:server=>:test) do |c2|
        c1.should_not == c2
        @db.execute 'DROP TABLE test;'
      end
    end
    @db.sqls.should == ['BEGIN', 'BEGIN -- test', 'DROP TABLE test;', 'COMMIT -- test', 'COMMIT']
  end
  
  if (!defined?(RUBY_ENGINE) or RUBY_ENGINE == 'ruby' or RUBY_ENGINE == 'rbx') and RUBY_VERSION < '1.9'
    specify "should handle Thread#kill for transactions inside threads" do
      q = Queue.new
      q1 = Queue.new
      t = Thread.new do
        @db.transaction do
          @db.execute 'DROP TABLE test'
          q1.push nil
          q.pop
          @db.execute 'DROP TABLE test2'
        end
      end
      q1.pop
      t.kill
      t.join
      @db.sqls.should == ['BEGIN', 'DROP TABLE test', 'ROLLBACK']
    end
  end

  specify "should raise an Error if after_commit or after_rollback is called without a block" do
    proc{@db.after_commit}.should raise_error(Sequel::Error)
    proc{@db.after_rollback}.should raise_error(Sequel::Error)
  end

  specify "should have after_commit and after_rollback respect :server option" do
    @db.transaction(:server=>:test){@db.after_commit(:server=>:test){@db.execute('foo', :server=>:test)}}
    @db.sqls.should == ['BEGIN -- test', 'COMMIT -- test', 'foo -- test']
    @db.transaction(:server=>:test){@db.after_rollback(:server=>:test){@db.execute('foo', :server=>:test)}; raise Sequel::Rollback}
    @db.sqls.should == ['BEGIN -- test', 'ROLLBACK -- test', 'foo -- test']
  end

  specify "should execute after_commit outside transactions" do
    @db.after_commit{@db.execute('foo')}
    @db.sqls.should == ['foo']
  end

  specify "should ignore after_rollback outside transactions" do
    @db.after_rollback{@db.execute('foo')}
    @db.sqls.should == []
  end

  specify "should support after_commit inside transactions" do
    @db.transaction{@db.after_commit{@db.execute('foo')}}
    @db.sqls.should == ['BEGIN', 'COMMIT', 'foo']
  end

  specify "should support after_rollback inside transactions" do
    @db.transaction{@db.after_rollback{@db.execute('foo')}}
    @db.sqls.should == ['BEGIN', 'COMMIT']
  end

  specify "should not call after_commit if the transaction rolls back" do
    @db.transaction{@db.after_commit{@db.execute('foo')}; raise Sequel::Rollback}
    @db.sqls.should == ['BEGIN', 'ROLLBACK']
  end

  specify "should call after_rollback if the transaction rolls back" do
    @db.transaction{@db.after_rollback{@db.execute('foo')}; raise Sequel::Rollback}
    @db.sqls.should == ['BEGIN', 'ROLLBACK', 'foo']
  end

  specify "should call multiple after_commit blocks in order if called inside transactions" do
    @db.transaction{@db.after_commit{@db.execute('foo')}; @db.after_commit{@db.execute('bar')}}
    @db.sqls.should == ['BEGIN', 'COMMIT', 'foo', 'bar']
  end

  specify "should call multiple after_rollback blocks in order if called inside transactions" do
    @db.transaction{@db.after_rollback{@db.execute('foo')}; @db.after_rollback{@db.execute('bar')}; raise Sequel::Rollback}
    @db.sqls.should == ['BEGIN', 'ROLLBACK', 'foo', 'bar']
  end

  specify "should support after_commit inside nested transactions" do
    @db.transaction{@db.transaction{@db.after_commit{@db.execute('foo')}}}
    @db.sqls.should == ['BEGIN', 'COMMIT', 'foo']
  end

  specify "should support after_rollback inside nested transactions" do
    @db.transaction{@db.transaction{@db.after_rollback{@db.execute('foo')}}; raise Sequel::Rollback}
    @db.sqls.should == ['BEGIN', 'ROLLBACK', 'foo']
  end

  specify "should raise an error if you attempt to use after_commit inside a prepared transaction" do
    @db.meta_def(:supports_prepared_transactions?){true}
    proc{@db.transaction(:prepare=>'XYZ'){@db.after_commit{@db.execute('foo')}}}.should raise_error(Sequel::Error)
    @db.sqls.should == ['BEGIN', 'ROLLBACK']
  end

  specify "should raise an error if you attempt to use after_rollback inside a prepared transaction" do
    @db.meta_def(:supports_prepared_transactions?){true}
    proc{@db.transaction(:prepare=>'XYZ'){@db.after_rollback{@db.execute('foo')}}}.should raise_error(Sequel::Error)
    @db.sqls.should == ['BEGIN', 'ROLLBACK']
  end
end

describe "Database#transaction with savepoint support" do
  before do
    @db = Sequel.mock(:servers=>{:test=>{}})
  end

  it_should_behave_like "Database#transaction"

  specify "should support after_commit inside savepoints" do
    @db.meta_def(:supports_savepoints?){true}
    @db.transaction do
      @db.after_commit{@db.execute('foo')}
      @db.transaction(:savepoint=>true){@db.after_commit{@db.execute('bar')}}
      @db.after_commit{@db.execute('baz')}
    end
    @db.sqls.should == ['BEGIN', 'SAVEPOINT autopoint_1', 'RELEASE SAVEPOINT autopoint_1', 'COMMIT', 'foo', 'bar', 'baz']
  end

  specify "should support after_rollback inside savepoints" do
    @db.meta_def(:supports_savepoints?){true}
    @db.transaction do
      @db.after_rollback{@db.execute('foo')}
      @db.transaction(:savepoint=>true){@db.after_rollback{@db.execute('bar')}}
      @db.after_rollback{@db.execute('baz')}
      raise Sequel::Rollback
    end
    @db.sqls.should == ['BEGIN', 'SAVEPOINT autopoint_1', 'RELEASE SAVEPOINT autopoint_1', 'ROLLBACK', 'foo', 'bar', 'baz']
  end

  specify "should raise an error if you attempt to use after_commit inside a savepoint in a prepared transaction" do
    @db.meta_def(:supports_savepoints?){true}
    @db.meta_def(:supports_prepared_transactions?){true}
    proc{@db.transaction(:prepare=>'XYZ'){@db.transaction(:savepoint=>true){@db.after_commit{@db.execute('foo')}}}}.should raise_error(Sequel::Error)
    @db.sqls.should == ['BEGIN', 'SAVEPOINT autopoint_1','ROLLBACK TO SAVEPOINT autopoint_1', 'ROLLBACK']
  end

  specify "should raise an error if you attempt to use after_rollback inside a savepoint in a prepared transaction" do
    @db.meta_def(:supports_savepoints?){true}
    @db.meta_def(:supports_prepared_transactions?){true}
    proc{@db.transaction(:prepare=>'XYZ'){@db.transaction(:savepoint=>true){@db.after_rollback{@db.execute('foo')}}}}.should raise_error(Sequel::Error)
    @db.sqls.should == ['BEGIN', 'SAVEPOINT autopoint_1','ROLLBACK TO SAVEPOINT autopoint_1', 'ROLLBACK']
  end
end
  
describe "Database#transaction without savepoint support" do
  before do
    @db = Sequel.mock(:servers=>{:test=>{}})
    @db.meta_def(:supports_savepoints?){false}
  end

  it_should_behave_like "Database#transaction"
end
  
describe "Sequel.transaction" do
  before do
    @sqls = []
    @db1 = Sequel.mock(:append=>'1', :sqls=>@sqls)
    @db2 = Sequel.mock(:append=>'2', :sqls=>@sqls)
    @db3 = Sequel.mock(:append=>'3', :sqls=>@sqls)
  end
  
  specify "should run the block inside transacitons on all three databases" do
    Sequel.transaction([@db1, @db2, @db3]){1}.should == 1
    @sqls.should == ['BEGIN -- 1', 'BEGIN -- 2', 'BEGIN -- 3', 'COMMIT -- 3', 'COMMIT -- 2', 'COMMIT -- 1']
  end
  
  specify "should pass options to all the blocks" do
    Sequel.transaction([@db1, @db2, @db3], :rollback=>:always){1}.should be_nil
    @sqls.should == ['BEGIN -- 1', 'BEGIN -- 2', 'BEGIN -- 3', 'ROLLBACK -- 3', 'ROLLBACK -- 2', 'ROLLBACK -- 1']
  end
  
  specify "should handle Sequel::Rollback exceptions raised by the block to rollback on all databases" do
    Sequel.transaction([@db1, @db2, @db3]){raise Sequel::Rollback}.should be_nil
    @sqls.should == ['BEGIN -- 1', 'BEGIN -- 2', 'BEGIN -- 3', 'ROLLBACK -- 3', 'ROLLBACK -- 2', 'ROLLBACK -- 1']
  end
  
  specify "should handle nested transactions" do
    Sequel.transaction([@db1, @db2, @db3]){Sequel.transaction([@db1, @db2, @db3]){1}}.should == 1
    @sqls.should == ['BEGIN -- 1', 'BEGIN -- 2', 'BEGIN -- 3', 'COMMIT -- 3', 'COMMIT -- 2', 'COMMIT -- 1']
  end
  
  specify "should handle savepoints" do
    Sequel.transaction([@db1, @db2, @db3]){Sequel.transaction([@db1, @db2, @db3], :savepoint=>true){1}}.should == 1
    @sqls.should == ['BEGIN -- 1', 'BEGIN -- 2', 'BEGIN -- 3',
      'SAVEPOINT autopoint_1 -- 1', 'SAVEPOINT autopoint_1 -- 2', 'SAVEPOINT autopoint_1 -- 3',
      'RELEASE SAVEPOINT autopoint_1 -- 3', 'RELEASE SAVEPOINT autopoint_1 -- 2', 'RELEASE SAVEPOINT autopoint_1 -- 1',
      'COMMIT -- 3', 'COMMIT -- 2', 'COMMIT -- 1']
  end
end
  
describe "Database#transaction with savepoints" do
  before do
    @db = Sequel.mock
  end
  
  specify "should wrap the supplied block with BEGIN + COMMIT statements" do
    @db.transaction {@db.execute 'DROP TABLE test;'}
    @db.sqls.should == ['BEGIN', 'DROP TABLE test;', 'COMMIT']
  end
  
  specify "should use savepoints if given the :savepoint option" do
    @db.transaction{@db.transaction(:savepoint=>true){@db.execute 'DROP TABLE test;'}}
    @db.sqls.should == ['BEGIN', 'SAVEPOINT autopoint_1', 'DROP TABLE test;', 'RELEASE SAVEPOINT autopoint_1', 'COMMIT']
  end
  
  specify "should not use a savepoints if no transaction is in progress" do
    @db.transaction(:savepoint=>true){@db.execute 'DROP TABLE test;'}
    @db.sqls.should == ['BEGIN', 'DROP TABLE test;', 'COMMIT']
  end
  
  specify "should reuse the current transaction if no :savepoint option is given" do
    @db.transaction{@db.transaction{@db.execute 'DROP TABLE test;'}}
    @db.sqls.should == ['BEGIN', 'DROP TABLE test;', 'COMMIT']
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
    @db.sqls.should == ['BEGIN', 'DROP TABLE test;', 'COMMIT']
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
    @db.sqls.should == ['BEGIN', 'SAVEPOINT autopoint_1', 'DROP TABLE test;', 'RELEASE SAVEPOINT autopoint_1', 'COMMIT']
  end
  
  specify "should issue ROLLBACK if an exception is raised, and re-raise" do
    @db.transaction {@db.execute 'DROP TABLE test'; raise RuntimeError} rescue nil
    @db.sqls.should == ['BEGIN', 'DROP TABLE test', 'ROLLBACK']
    
    proc {@db.transaction {raise RuntimeError}}.should raise_error(RuntimeError)
  end
  
  specify "should issue ROLLBACK SAVEPOINT if an exception is raised inside a savepoint, and re-raise" do
    @db.transaction{@db.transaction(:savepoint=>true){@db.execute 'DROP TABLE test'; raise RuntimeError}} rescue nil
    @db.sqls.should == ['BEGIN', 'SAVEPOINT autopoint_1', 'DROP TABLE test', 'ROLLBACK TO SAVEPOINT autopoint_1', 'ROLLBACK']
    
    proc {@db.transaction {raise RuntimeError}}.should raise_error(RuntimeError)
  end
  
  specify "should issue ROLLBACK if Sequel::Rollback is raised in the transaction" do
    @db.transaction do
      @db.drop_table(:a)
      raise Sequel::Rollback
      @db.drop_table(:b)
    end
    
    @db.sqls.should == ['BEGIN', 'DROP TABLE a', 'ROLLBACK']
  end
  
  specify "should issue ROLLBACK SAVEPOINT if Sequel::Rollback is raised in a savepoint" do
    @db.transaction do
      @db.transaction(:savepoint=>true) do
        @db.drop_table(:a)
        raise Sequel::Rollback
      end
      @db.drop_table(:b)
    end
    
    @db.sqls.should == ['BEGIN', 'SAVEPOINT autopoint_1', 'DROP TABLE a', 'ROLLBACK TO SAVEPOINT autopoint_1', 'DROP TABLE b', 'COMMIT']
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

describe "A Database adapter with a scheme" do
  before do
    @ccc = Class.new(Sequel::Database)
    @ccc.send(:set_adapter_scheme, :ccc)
  end

  specify "should be registered in the ADAPTER_MAP" do
    Sequel::ADAPTER_MAP[:ccc].should == @ccc
  end
  
  specify "should give the database_type as the adapter scheme by default" do
    @ccc.new.database_type.should == :ccc
  end
  
  specify "should be instantiated when its scheme is specified" do
    c = Sequel::Database.connect('ccc://localhost/db')
    c.should be_a_kind_of(@ccc)
    c.opts[:host].should == 'localhost'
    c.opts[:database].should == 'db'
  end
  
  specify "should be accessible through Sequel.connect" do
    c = Sequel.connect 'ccc://localhost/db'
    c.should be_a_kind_of(@ccc)
    c.opts[:host].should == 'localhost'
    c.opts[:database].should == 'db'
  end

  specify "should be accessible through Sequel.connect via a block" do
    x = nil
    y = nil
    z = nil
    returnValue = 'anything'

    p = proc do |c|
      c.should be_a_kind_of(@ccc)
      c.opts[:host].should == 'localhost'
      c.opts[:database].should == 'db'
      z = y
      y = x
      x = c
      returnValue
    end

    @ccc.class_eval do
      self::DISCONNECTS = []
      def disconnect
        self.class::DISCONNECTS << self
      end
    end
    Sequel::Database.connect('ccc://localhost/db', &p).should == returnValue
    @ccc::DISCONNECTS.should == [x]

    Sequel.connect('ccc://localhost/db', &p).should == returnValue
    @ccc::DISCONNECTS.should == [y, x]

    Sequel.send(:def_adapter_method, :ccc)
    Sequel.ccc('db', :host=>'localhost', &p).should == returnValue
    @ccc::DISCONNECTS.should == [z, y, x]
  end

  specify "should be accessible through Sequel.<adapter>" do
    Sequel.send(:def_adapter_method, :ccc)

    # invalid parameters
    proc {Sequel.ccc('abc', 'def')}.should raise_error(Sequel::Error)
    
    c = Sequel.ccc('mydb')
    c.should be_a_kind_of(@ccc)
    c.opts.values_at(:adapter, :database, :adapter_class).should == [:ccc, 'mydb', @ccc]
    
    c = Sequel.ccc('mydb', :host => 'localhost')
    c.should be_a_kind_of(@ccc)
    c.opts.values_at(:adapter, :database, :host, :adapter_class).should == [:ccc, 'mydb', 'localhost', @ccc]
    
    c = Sequel.ccc
    c.should be_a_kind_of(@ccc)
    c.opts.values_at(:adapter, :adapter_class).should == [:ccc, @ccc]
    
    c = Sequel.ccc(:database => 'mydb', :host => 'localhost')
    c.should be_a_kind_of(@ccc)
    c.opts.values_at(:adapter, :database, :host, :adapter_class).should == [:ccc, 'mydb', 'localhost', @ccc]
  end
  
  specify "should be accessible through Sequel.connect with options" do
    c = Sequel.connect(:adapter => :ccc, :database => 'mydb')
    c.should be_a_kind_of(@ccc)
    c.opts[:adapter].should == :ccc
  end

  specify "should be accessible through Sequel.connect with URL parameters" do
    c = Sequel.connect 'ccc:///db?host=/tmp&user=test'
    c.should be_a_kind_of(@ccc)
    c.opts[:host].should == '/tmp'
    c.opts[:database].should == 'db'
    c.opts[:user].should == 'test'
  end
  
  specify "should have URL parameters take precedence over fixed URL parts" do
    c = Sequel.connect 'ccc://localhost/db?host=a&database=b'
    c.should be_a_kind_of(@ccc)
    c.opts[:host].should == 'a'
    c.opts[:database].should == 'b'
  end
  
  specify "should have hash options take predence over URL parameters or parts" do
    c = Sequel.connect 'ccc://localhost/db?host=/tmp', :host=>'a', :database=>'b', :user=>'c'
    c.should be_a_kind_of(@ccc)
    c.opts[:host].should == 'a'
    c.opts[:database].should == 'b'
    c.opts[:user].should == 'c'
  end

  specify "should unescape values of URL parameters and parts" do
    c = Sequel.connect 'ccc:///d%5bb%5d?host=domain%5cinstance'
    c.should be_a_kind_of(@ccc)
    c.opts[:database].should == 'd[b]'
    c.opts[:host].should == 'domain\\instance'
  end

  specify "should test the connection if test parameter is truthy" do
    proc{Sequel.connect 'ccc:///d%5bb%5d?test=t'}.should raise_error(Sequel::DatabaseConnectionError)
    proc{Sequel.connect 'ccc:///d%5bb%5d?test=1'}.should raise_error(Sequel::DatabaseConnectionError)
    proc{Sequel.connect 'ccc:///d%5bb%5d', :test=>true}.should raise_error(Sequel::DatabaseConnectionError)
    proc{Sequel.connect 'ccc:///d%5bb%5d', :test=>'t'}.should raise_error(Sequel::DatabaseConnectionError)
  end

  specify "should not test the connection if test parameter is not truthy" do
    proc{Sequel.connect 'ccc:///d%5bb%5d?test=f'}.should_not raise_error
    proc{Sequel.connect 'ccc:///d%5bb%5d?test=0'}.should_not raise_error
    proc{Sequel.connect 'ccc:///d%5bb%5d', :test=>false}.should_not raise_error
    proc{Sequel.connect 'ccc:///d%5bb%5d', :test=>'f'}.should_not raise_error
  end
end

describe "Sequel::Database.connect" do
  specify "should raise an Error if not given a String or Hash" do
    proc{Sequel::Database.connect(nil)}.should raise_error(Sequel::Error)
    proc{Sequel::Database.connect(Object.new)}.should raise_error(Sequel::Error)
  end
end

describe "An unknown database scheme" do
  specify "should raise an error in Sequel::Database.connect" do
    proc {Sequel::Database.connect('ddd://localhost/db')}.should raise_error(Sequel::AdapterNotFound)
  end

  specify "should raise an error in Sequel.connect" do
    proc {Sequel.connect('ddd://localhost/db')}.should raise_error(Sequel::AdapterNotFound)
  end
end

describe "A broken adapter (lib is there but the class is not)" do
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

describe "A single threaded database" do
  after do
    Sequel::Database.single_threaded = false
  end
  
  specify "should use a SingleConnectionPool instead of a ConnectionPool" do
    db = Sequel::Database.new(:single_threaded => true){123}
    db.pool.should be_a_kind_of(Sequel::SingleConnectionPool)
  end
  
  specify "should be constructable using :single_threaded => true option" do
    db = Sequel::Database.new(:single_threaded => true){123}
    db.pool.should be_a_kind_of(Sequel::SingleConnectionPool)
  end
  
  specify "should be constructable using Database.single_threaded = true" do
    Sequel::Database.single_threaded = true
    db = Sequel::Database.new{123}
    db.pool.should be_a_kind_of(Sequel::SingleConnectionPool)
  end

  specify "should be constructable using Sequel.single_threaded = true" do
    Sequel.single_threaded = true
    db = Sequel::Database.new{123}
    db.pool.should be_a_kind_of(Sequel::SingleConnectionPool)
  end
end

describe "A single threaded database" do
  before do
    conn = 1234567
    @db = Sequel::Database.new(:single_threaded => true)
    @db.meta_def(:connect) do |c|
      conn += 1
    end
  end
  
  specify "should invoke connection_proc only once" do
    @db.pool.hold {|c| c.should == 1234568}
    @db.pool.hold {|c| c.should == 1234568}
  end
  
  specify "should disconnect correctly" do
    def @db.disconnect_connection(c); @dc = c end
    def @db.dc; @dc end
    x = nil
    @db.pool.hold{|c| x = c}
    @db.pool.hold{|c| c.should == x}
    proc{@db.disconnect}.should_not raise_error
    @db.dc.should == x
  end
  
  specify "should convert an Exception on connection into a DatabaseConnectionError" do
    db = Sequel::Database.new(:single_threaded => true, :servers=>{}){raise Exception}
    proc {db.pool.hold {|c|}}.should raise_error(Sequel::DatabaseConnectionError)
  end
  
  specify "should raise a DatabaseConnectionError if the connection proc returns nil" do
    db = Sequel::Database.new(:single_threaded => true, :servers=>{}){nil}
    proc {db.pool.hold {|c|}}.should raise_error(Sequel::DatabaseConnectionError)
  end
end

describe "A database" do
  after do
    Sequel::Database.single_threaded = false
  end
  
  specify "should have single_threaded? respond to true if in single threaded mode" do
    db = Sequel::Database.new(:single_threaded => true){1234}
    db.should be_single_threaded
    
    db = Sequel::Database.new(:max_options => 1)
    db.should_not be_single_threaded
    
    db = Sequel::Database.new
    db.should_not be_single_threaded
    
    Sequel::Database.single_threaded = true
    
    db = Sequel::Database.new{123}
    db.should be_single_threaded
    
    db = Sequel::Database.new(:max_options => 4){123}
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

describe "Database#fetch" do
  before do
    @db = Sequel.mock(:fetch=>proc{|sql| {:sql => sql}})
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
    
    @db.fetch('select name from table where name = ? or id in ?', 'aman', [3,4,7]) {|r| sql = r[:sql]}
    sql.should == "select name from table where name = 'aman' or id in (3, 4, 7)"
  end
  
  specify "should format the given sql with named arguments" do
    sql = nil
    @db.fetch('select * from xyz where x = :x and y = :y', :x=>15, :y=>'abc') {|r| sql = r[:sql]}
    sql.should == "select * from xyz where x = 15 and y = 'abc'"
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
    
    ds.filter!{price.sql_number < 100}
    ds.select_sql.should == 'select * from xyz'
    ds.sql.should == 'select * from xyz'
  end
end


describe "Database#[]" do
  before do
    @db = Sequel.mock
  end
  
  specify "should return a dataset when symbols are given" do
    ds = @db[:items]
    ds.should be_a_kind_of(Sequel::Dataset)
    ds.opts[:from].should == [:items]
  end
  
  specify "should return a dataset when a string is given" do
    @db.fetch = proc{|sql| {:sql=>sql}}
    sql = nil
    @db['select * from xyz where x = ? and y = ?', 15, 'abc'].each {|r| sql = r[:sql]}
    sql.should == "select * from xyz where x = 15 and y = 'abc'"
  end
end

describe "Database#inspect" do
  specify "should include the class name and the connection url" do
    Sequel.connect('mock://foo/bar').inspect.should == '#<Sequel::Mock::Database: "mock://foo/bar">'
  end

  specify "should include the class name and the connection options if an options hash was given" do
    Sequel.connect(:adapter=>:mock).inspect.should =~ /#<Sequel::Mock::Database: \{:adapter=>:mock\}>/
  end

  specify "should include the class name, uri, and connection options if uri and options hash was given" do
    Sequel.connect('mock://foo', :database=>'bar').inspect.should =~ /#<Sequel::Mock::Database: "mock:\/\/foo" \{:database=>"bar"\}>/
  end
end

describe "Database#get" do
  before do
    @db = Sequel.mock(:fetch=>{:a=>1})
  end
  
  specify "should use Dataset#get to get a single value" do
    @db.get(1).should == 1
    @db.sqls.should == ['SELECT 1 LIMIT 1']
    
    @db.get(Sequel.function(:version))
    @db.sqls.should == ['SELECT version() LIMIT 1']
  end

  specify "should accept a block" do
    @db.get{1}
    @db.sqls.should == ['SELECT 1 LIMIT 1']
    
    @db.get{version(1)}
    @db.sqls.should == ['SELECT version(1) LIMIT 1']
  end
end

describe "Database#call" do
  specify "should call the prepared statement with the given name" do
    db = Sequel.mock(:fetch=>{:id => 1, :x => 1})
    db[:items].prepare(:select, :select_all)
    db.call(:select_all).should == [{:id => 1, :x => 1}]
    db[:items].filter(:n=>:$n).prepare(:select, :select_n)
    db.call(:select_n, :n=>1).should == [{:id => 1, :x => 1}]
    db.sqls.should == ['SELECT * FROM items', 'SELECT * FROM items WHERE (n = 1)']
  end
end

describe "Database#server_opts" do
  specify "should return the general opts if no :servers option is used" do
    opts = {:host=>1, :database=>2}
    Sequel::Database.new(opts).send(:server_opts, :server1)[:host].should == 1
  end
  
  specify "should return the general opts if entry for the server is present in the :servers option" do
    opts = {:host=>1, :database=>2, :servers=>{}}
    Sequel::Database.new(opts).send(:server_opts, :server1)[:host].should == 1
  end
  
  specify "should return the general opts merged with the specific opts if given as a hash" do
    opts = {:host=>1, :database=>2, :servers=>{:server1=>{:host=>3}}}
    Sequel::Database.new(opts).send(:server_opts, :server1)[:host].should == 3
  end
  
  specify "should return the sgeneral opts merged with the specific opts if given as a proc" do
    opts = {:host=>1, :database=>2, :servers=>{:server1=>proc{|db| {:host=>4}}}}
    Sequel::Database.new(opts).send(:server_opts, :server1)[:host].should == 4
  end
  
  specify "should raise an error if the specific opts is not a proc or hash" do
    opts = {:host=>1, :database=>2, :servers=>{:server1=>2}}
    proc{Sequel::Database.new(opts).send(:server_opts, :server1)}.should raise_error(Sequel::Error)
  end

  specify "should return the general opts merged with given opts if given opts is a Hash" do
    opts = {:host=>1, :database=>2}
    Sequel::Database.new(opts).send(:server_opts, :host=>2)[:host].should == 2
  end
end

describe "Database#add_servers" do
  before do
    @db = Sequel.mock(:host=>1, :database=>2, :servers=>{:server1=>{:host=>3}})
  end

  specify "should add new servers to the connection pool" do
    @db.synchronize{|c| c.opts[:host].should == 1}
    @db.synchronize(:server1){|c| c.opts[:host].should == 3}
    @db.synchronize(:server2){|c| c.opts[:host].should == 1}

    @db.add_servers(:server2=>{:host=>6})
    @db.synchronize{|c| c.opts[:host].should == 1}
    @db.synchronize(:server1){|c| c.opts[:host].should == 3}
    @db.synchronize(:server2){|c| c.opts[:host].should == 6}

    @db.disconnect
    @db.synchronize{|c| c.opts[:host].should == 1}
    @db.synchronize(:server1){|c| c.opts[:host].should == 3}
    @db.synchronize(:server2){|c| c.opts[:host].should == 6}
  end

  specify "should replace options for future connections to existing servers" do
    @db.synchronize{|c| c.opts[:host].should == 1}
    @db.synchronize(:server1){|c| c.opts[:host].should == 3}
    @db.synchronize(:server2){|c| c.opts[:host].should == 1}

    @db.add_servers(:default=>proc{{:host=>4}}, :server1=>{:host=>8})
    @db.synchronize{|c| c.opts[:host].should == 1}
    @db.synchronize(:server1){|c| c.opts[:host].should == 3}
    @db.synchronize(:server2){|c| c.opts[:host].should == 1}

    @db.disconnect
    @db.synchronize{|c| c.opts[:host].should == 4}
    @db.synchronize(:server1){|c| c.opts[:host].should == 8}
    @db.synchronize(:server2){|c| c.opts[:host].should == 4}
  end
end

describe "Database#remove_servers" do
  before do
    @db = Sequel.mock(:host=>1, :database=>2, :servers=>{:server1=>{:host=>3}, :server2=>{:host=>4}})
  end

  specify "should remove servers from the connection pool" do
    @db.synchronize{|c| c.opts[:host].should == 1}
    @db.synchronize(:server1){|c| c.opts[:host].should == 3}
    @db.synchronize(:server2){|c| c.opts[:host].should == 4}

    @db.remove_servers(:server1, :server2)
    @db.synchronize{|c| c.opts[:host].should == 1}
    @db.synchronize(:server1){|c| c.opts[:host].should == 1}
    @db.synchronize(:server2){|c| c.opts[:host].should == 1}
  end
  
  specify "should accept arrays of symbols" do
    @db.remove_servers([:server1, :server2])
    @db.synchronize{|c| c.opts[:host].should == 1}
    @db.synchronize(:server1){|c| c.opts[:host].should == 1}
    @db.synchronize(:server2){|c| c.opts[:host].should == 1}
  end

  specify "should allow removal while connections are still open" do
    @db.synchronize do |c1|
      c1.opts[:host].should == 1
      @db.synchronize(:server1) do |c2|
        c2.opts[:host].should == 3
        @db.synchronize(:server2) do |c3|
          c3.opts[:host].should == 4
          @db.remove_servers(:server1, :server2)
            @db.synchronize(:server1) do |c4|
              c4.should_not == c2
              c4.should == c1
              c4.opts[:host].should == 1
              @db.synchronize(:server2) do |c5|
                c5.should_not == c3
                c5.should == c1
                c5.opts[:host].should == 1
              end
            end
          c3.opts[:host].should == 4
         end
        c2.opts[:host].should == 3
      end
      c1.opts[:host].should == 1
    end
  end
end

describe "Database#each_server with do/jdbc adapter connection string without :adapter option" do
  specify "should yield a separate database object for each server" do
    klass = Class.new(Sequel::Database)
    klass.should_receive(:adapter_class).once.with(:jdbc).and_return(Sequel::Mock::Database)
    @db = klass.connect('jdbc:blah:', :host=>1, :database=>2, :servers=>{:server1=>{:host=>3}})

    hosts = []
    @db.each_server do |db|
      db.should be_a_kind_of(Sequel::Database)
      db.should_not == @db
      db.opts[:adapter_class].should == Sequel::Mock::Database
      db.opts[:database].should == 2
      hosts << db.opts[:host]
    end
    hosts.sort.should == [1, 3]
  end
end

describe "Database#each_server" do
  before do
    @db = Sequel.mock(:host=>1, :database=>2, :servers=>{:server1=>{:host=>3}, :server2=>{:host=>4}})
  end

  specify "should yield a separate database object for each server" do
    hosts = []
    @db.each_server do |db|
      db.should be_a_kind_of(Sequel::Database)
      db.should_not == @db
      db.opts[:adapter].should == :mock
      db.opts[:database].should == 2
      hosts << db.opts[:host]
    end
    hosts.sort.should == [1, 3, 4]
  end

  specify "should disconnect and remove entry from Sequel::DATABASES after use" do
    dbs = []
    dcs = []
    @db.each_server do |db|
      dbs << db
      Sequel::DATABASES.should include(db)
      db.meta_def(:disconnect){dcs << db}
    end
    dbs.each do |db|
      Sequel::DATABASES.should_not include(db)
    end
    dbs.should == dcs
  end
end
  
describe "Database#raise_error" do
  before do
    @db = Sequel.mock
  end

  specify "should reraise if the exception class is not in opts[:classes]" do
    e = Class.new(StandardError)
    proc{@db.send(:raise_error, e.new(''), :classes=>[])}.should raise_error(e)
  end

  specify "should convert the exception to a DatabaseError if the exception class is in opts[:classes]" do
    proc{@db.send(:raise_error, Interrupt.new(''), :classes=>[Interrupt])}.should raise_error(Sequel::DatabaseError)
  end

  specify "should convert the exception to a DatabaseError if opts[:classes] if not present" do
    proc{@db.send(:raise_error, Interrupt.new(''))}.should raise_error(Sequel::DatabaseError)
  end
  
  specify "should convert the exception to a DatabaseDisconnectError if opts[:disconnect] is true" do
    proc{@db.send(:raise_error, Interrupt.new(''), :disconnect=>true)}.should raise_error(Sequel::DatabaseDisconnectError)
  end
end

describe "Database#typecast_value" do
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

  specify "should handle integers with leading 0 as base 10" do
    @db.typecast_value(:integer, "013").should == 13
    @db.typecast_value(:integer, "08").should == 8
    @db.typecast_value(:integer, "000013").should == 13
    @db.typecast_value(:integer, "000008").should == 8
  end

  specify "should handle integers with leading 0x as base 16" do
    @db.typecast_value(:integer, "0x013").should == 19
    @db.typecast_value(:integer, "0x80").should == 128
  end

  specify "should typecast blobs as as Sequel::SQL::Blob" do
    v = @db.typecast_value(:blob, "0x013")
    v.should be_a_kind_of(Sequel::SQL::Blob)
    v.should == Sequel::SQL::Blob.new("0x013")
    @db.typecast_value(:blob, v).object_id.should == v.object_id
  end

  specify "should typecast boolean values to true, false, or nil" do
    @db.typecast_value(:boolean, false).should be_false
    @db.typecast_value(:boolean, 0).should be_false
    @db.typecast_value(:boolean, "0").should be_false
    @db.typecast_value(:boolean, 'f').should be_false
    @db.typecast_value(:boolean, 'false').should be_false
    @db.typecast_value(:boolean, true).should be_true
    @db.typecast_value(:boolean, 1).should be_true
    @db.typecast_value(:boolean, '1').should be_true
    @db.typecast_value(:boolean, 't').should be_true
    @db.typecast_value(:boolean, 'true').should be_true
    @db.typecast_value(:boolean, '').should be_nil
  end

  specify "should typecast date values to Date" do
    @db.typecast_value(:date, Date.today).should == Date.today
    @db.typecast_value(:date, DateTime.now).should == Date.today
    @db.typecast_value(:date, Time.now).should == Date.today
    @db.typecast_value(:date, Date.today.to_s).should == Date.today
    @db.typecast_value(:date, :year=>Date.today.year, :month=>Date.today.month, :day=>Date.today.day).should == Date.today
  end

  specify "should have Sequel.application_to_database_timestamp convert to Sequel.database_timezone" do
    begin
      t = Time.utc(2011, 1, 2, 3, 4, 5) # UTC Time
      t2 = Time.mktime(2011, 1, 2, 3, 4, 5) # Local Time
      t3 = Time.utc(2011, 1, 2, 3, 4, 5) - (t - t2) # Local Time in UTC Time
      t4 = Time.mktime(2011, 1, 2, 3, 4, 5) + (t - t2) # UTC Time in Local Time
      Sequel.application_timezone = :utc
      Sequel.database_timezone = :local
      Sequel.application_to_database_timestamp(t).should == t4
      Sequel.application_timezone = :local
      Sequel.database_timezone = :utc
      Sequel.application_to_database_timestamp(t2).should == t3
    ensure
      Sequel.default_timezone = nil
    end
  end

  specify "should have Database#to_application_timestamp convert values using the database's timezone" do
    begin
      t = Time.utc(2011, 1, 2, 3, 4, 5) # UTC Time
      t2 = Time.mktime(2011, 1, 2, 3, 4, 5) # Local Time
      t3 = Time.utc(2011, 1, 2, 3, 4, 5) - (t - t2) # Local Time in UTC Time
      t4 = Time.mktime(2011, 1, 2, 3, 4, 5) + (t - t2) # UTC Time in Local Time
      Sequel.default_timezone = :utc
      @db.to_application_timestamp('2011-01-02 03:04:05').should == t
      Sequel.database_timezone = :local
      @db.to_application_timestamp('2011-01-02 03:04:05').should == t3
      Sequel.default_timezone = :local
      @db.to_application_timestamp('2011-01-02 03:04:05').should == t2
      Sequel.database_timezone = :utc
      @db.to_application_timestamp('2011-01-02 03:04:05').should == t4

      Sequel.default_timezone = :utc
      @db.timezone = :local
      @db.to_application_timestamp('2011-01-02 03:04:05').should == t3
      Sequel.default_timezone = :local
      @db.timezone = :utc
      @db.to_application_timestamp('2011-01-02 03:04:05').should == t4
    ensure
      Sequel.default_timezone = nil
    end
  end

  specify "should typecast datetime values to Sequel.datetime_class with correct timezone handling" do
    t = Time.utc(2011, 1, 2, 3, 4, 5, 500000) # UTC Time
    t2 = Time.mktime(2011, 1, 2, 3, 4, 5, 500000) # Local Time
    t3 = Time.utc(2011, 1, 2, 3, 4, 5, 500000) - (t - t2) # Local Time in UTC Time
    t4 = Time.mktime(2011, 1, 2, 3, 4, 5, 500000) + (t - t2) # UTC Time in Local Time
    secs = defined?(Rational) ? Rational(11, 2) : 5.5
    r1 = defined?(Rational) ? Rational(t2.utc_offset, 86400) : t2.utc_offset/86400.0
    r2 = defined?(Rational) ? Rational((t - t2).to_i, 86400) : (t - t2).to_i/86400.0
    dt = DateTime.civil(2011, 1, 2, 3, 4, secs)
    dt2 = DateTime.civil(2011, 1, 2, 3, 4, secs, r1)
    dt3 = DateTime.civil(2011, 1, 2, 3, 4, secs) - r2
    dt4 = DateTime.civil(2011, 1, 2, 3, 4, secs, r1) + r2

    t.should == t4
    t2.should == t3
    dt.should == dt4
    dt2.should == dt3

    check = proc do |i, o| 
      v = @db.typecast_value(:datetime, i)
      v.should == o
      if o.is_a?(Time)
        v.utc_offset.should == o.utc_offset
      else
        v.offset.should == o.offset
      end
    end
    @db.extend_datasets(Module.new{def supports_timestamp_timezones?; true; end})
    begin
      @db.typecast_value(:datetime, dt).should == t
      @db.typecast_value(:datetime, dt2).should == t2
      @db.typecast_value(:datetime, t).should == t
      @db.typecast_value(:datetime, t2).should == t2
      @db.typecast_value(:datetime, @db.literal(dt)[1...-1]).should == t
      @db.typecast_value(:datetime, dt.strftime('%F %T.%N')).should == t2
      @db.typecast_value(:datetime, Date.civil(2011, 1, 2)).should == Time.mktime(2011, 1, 2, 0, 0, 0)
      @db.typecast_value(:datetime, :year=>dt.year, :month=>dt.month, :day=>dt.day, :hour=>dt.hour, :minute=>dt.min, :second=>dt.sec, :nanos=>500000000).should == t2

      Sequel.datetime_class = DateTime
      @db.typecast_value(:datetime, dt).should == dt
      @db.typecast_value(:datetime, dt2).should == dt2
      @db.typecast_value(:datetime, t).should == dt
      @db.typecast_value(:datetime, t2).should == dt2
      @db.typecast_value(:datetime, @db.literal(dt)[1...-1]).should == dt
      @db.typecast_value(:datetime, dt.strftime('%F %T.%N')).should == dt
      @db.typecast_value(:datetime, Date.civil(2011, 1, 2)).should == DateTime.civil(2011, 1, 2, 0, 0, 0)
      @db.typecast_value(:datetime, :year=>dt.year, :month=>dt.month, :day=>dt.day, :hour=>dt.hour, :minute=>dt.min, :second=>dt.sec, :nanos=>500000000).should == dt

      Sequel.application_timezone = :utc
      Sequel.typecast_timezone = :local
      Sequel.datetime_class = Time
      check[dt, t]
      check[dt2, t3]
      check[t, t]
      check[t2, t3]
      check[@db.literal(dt)[1...-1], t]
      check[dt.strftime('%F %T.%N'), t3]
      check[Date.civil(2011, 1, 2), Time.utc(2011, 1, 2, 0, 0, 0)]
      check[{:year=>dt.year, :month=>dt.month, :day=>dt.day, :hour=>dt.hour, :minute=>dt.min, :second=>dt.sec, :nanos=>500000000}, t3]

      Sequel.datetime_class = DateTime
      check[dt, dt]
      check[dt2, dt3]
      check[t, dt]
      check[t2, dt3]
      check[@db.literal(dt)[1...-1], dt]
      check[dt.strftime('%F %T.%N'), dt3]
      check[Date.civil(2011, 1, 2), DateTime.civil(2011, 1, 2, 0, 0, 0)]
      check[{:year=>dt.year, :month=>dt.month, :day=>dt.day, :hour=>dt.hour, :minute=>dt.min, :second=>dt.sec, :nanos=>500000000}, dt3]

      Sequel.typecast_timezone = :utc
      Sequel.datetime_class = Time
      check[dt, t]
      check[dt2, t3]
      check[t, t]
      check[t2, t3]
      check[@db.literal(dt)[1...-1], t]
      check[dt.strftime('%F %T.%N'), t]
      check[Date.civil(2011, 1, 2), Time.utc(2011, 1, 2, 0, 0, 0)]
      check[{:year=>dt.year, :month=>dt.month, :day=>dt.day, :hour=>dt.hour, :minute=>dt.min, :second=>dt.sec, :nanos=>500000000}, t]

      Sequel.datetime_class = DateTime
      check[dt, dt]
      check[dt2, dt3]
      check[t, dt]
      check[t2, dt3]
      check[@db.literal(dt)[1...-1], dt]
      check[dt.strftime('%F %T.%N'), dt]
      check[Date.civil(2011, 1, 2), DateTime.civil(2011, 1, 2, 0, 0, 0)]
      check[{:year=>dt.year, :month=>dt.month, :day=>dt.day, :hour=>dt.hour, :minute=>dt.min, :second=>dt.sec, :nanos=>500000000}, dt]

      Sequel.application_timezone = :local
      Sequel.datetime_class = Time
      check[dt, t4]
      check[dt2, t2]
      check[t, t4]
      check[t2, t2]
      check[@db.literal(dt)[1...-1], t4]
      check[dt.strftime('%F %T.%N'), t4]
      check[Date.civil(2011, 1, 2), Time.local(2011, 1, 2, 0, 0, 0)]
      check[{:year=>dt.year, :month=>dt.month, :day=>dt.day, :hour=>dt.hour, :minute=>dt.min, :second=>dt.sec, :nanos=>500000000}, t4]

      Sequel.datetime_class = DateTime
      check[dt, dt4]
      check[dt2, dt2]
      check[t, dt4]
      check[t2, dt2]
      check[@db.literal(dt)[1...-1], dt4]
      check[dt.strftime('%F %T.%N'), dt4]
      check[Date.civil(2011, 1, 2), DateTime.civil(2011, 1, 2, 0, 0, 0, r1)]
      check[{:year=>dt.year, :month=>dt.month, :day=>dt.day, :hour=>dt.hour, :minute=>dt.min, :second=>dt.sec, :nanos=>500000000}, dt4]

      Sequel.typecast_timezone = :local
      Sequel.datetime_class = Time
      check[dt, t4]
      check[dt2, t2]
      check[t, t4]
      check[t2, t2]
      check[@db.literal(dt)[1...-1], t4]
      check[dt.strftime('%F %T.%N'), t2]
      check[Date.civil(2011, 1, 2), Time.local(2011, 1, 2, 0, 0, 0)]
      check[{:year=>dt.year, :month=>dt.month, :day=>dt.day, :hour=>dt.hour, :minute=>dt.min, :second=>dt.sec, :nanos=>500000000}, t2]

      Sequel.datetime_class = DateTime
      check[dt, dt4]
      check[dt2, dt2]
      check[t, dt4]
      check[t2, dt2]
      check[@db.literal(dt)[1...-1], dt4]
      check[dt.strftime('%F %T.%N'), dt2]
      check[Date.civil(2011, 1, 2), DateTime.civil(2011, 1, 2, 0, 0, 0, r1)]
      check[{:year=>dt.year, :month=>dt.month, :day=>dt.day, :hour=>dt.hour, :minute=>dt.min, :second=>dt.sec, :nanos=>500000000}, dt2]

    ensure
      Sequel.default_timezone = nil
      Sequel.datetime_class = Time
    end
  end

  specify "should handle arrays when typecasting timestamps" do
    begin
      @db.typecast_value(:datetime, [2011, 10, 11, 12, 13, 14]).should == Time.local(2011, 10, 11, 12, 13, 14)
      @db.typecast_value(:datetime, [2011, 10, 11, 12, 13, 14, 500000000]).should == Time.local(2011, 10, 11, 12, 13, 14, 500000)

      Sequel.datetime_class = DateTime
      @db.typecast_value(:datetime, [2011, 10, 11, 12, 13, 14]).should == DateTime.civil(2011, 10, 11, 12, 13, 14)
      @db.typecast_value(:datetime, [2011, 10, 11, 12, 13, 14, 500000000]).should == DateTime.civil(2011, 10, 11, 12, 13, (defined?(Rational) ? Rational(29, 2) : 14.5))
      @db.typecast_value(:datetime, [2011, 10, 11, 12, 13, 14, 500000000, (defined?(Rational) ? Rational(1, 2) : 0.5)]).should == DateTime.civil(2011, 10, 11, 12, 13, (defined?(Rational) ? Rational(29, 2) : 14.5), (defined?(Rational) ? Rational(1, 2) : 0.5))
    ensure
      Sequel.datetime_class = Time
    end
  end

  specify "should handle hashes when typecasting timestamps" do
    begin
      @db.typecast_value(:datetime, :year=>2011, :month=>10, :day=>11, :hour=>12, :minute=>13, :second=>14).should == Time.local(2011, 10, 11, 12, 13, 14)
      @db.typecast_value(:datetime, :year=>2011, :month=>10, :day=>11, :hour=>12, :minute=>13, :second=>14, :nanos=>500000000).should == Time.local(2011, 10, 11, 12, 13, 14, 500000)
      @db.typecast_value(:datetime, 'year'=>2011, 'month'=>10, 'day'=>11, 'hour'=>12, 'minute'=>13, 'second'=>14).should == Time.local(2011, 10, 11, 12, 13, 14)
      @db.typecast_value(:datetime, 'year'=>2011, 'month'=>10, 'day'=>11, 'hour'=>12, 'minute'=>13, 'second'=>14, 'nanos'=>500000000).should == Time.local(2011, 10, 11, 12, 13, 14, 500000)

      Sequel.datetime_class = DateTime
      @db.typecast_value(:datetime, :year=>2011, :month=>10, :day=>11, :hour=>12, :minute=>13, :second=>14).should == DateTime.civil(2011, 10, 11, 12, 13, 14)
      @db.typecast_value(:datetime, :year=>2011, :month=>10, :day=>11, :hour=>12, :minute=>13, :second=>14, :nanos=>500000000).should == DateTime.civil(2011, 10, 11, 12, 13, (defined?(Rational) ? Rational(29, 2) : 14.5))
      @db.typecast_value(:datetime, 'year'=>2011, 'month'=>10, 'day'=>11, 'hour'=>12, 'minute'=>13, 'second'=>14).should == DateTime.civil(2011, 10, 11, 12, 13, 14)
      @db.typecast_value(:datetime, 'year'=>2011, 'month'=>10, 'day'=>11, 'hour'=>12, 'minute'=>13, 'second'=>14, 'nanos'=>500000000).should == DateTime.civil(2011, 10, 11, 12, 13, (defined?(Rational) ? Rational(29, 2) : 14.5))
      @db.typecast_value(:datetime, :year=>2011, :month=>10, :day=>11, :hour=>12, :minute=>13, :second=>14, :offset=>(defined?(Rational) ? Rational(1, 2) : 0.5)).should == DateTime.civil(2011, 10, 11, 12, 13, 14, (defined?(Rational) ? Rational(1, 2) : 0.5))
      @db.typecast_value(:datetime, :year=>2011, :month=>10, :day=>11, :hour=>12, :minute=>13, :second=>14, :nanos=>500000000, :offset=>(defined?(Rational) ? Rational(1, 2) : 0.5)).should == DateTime.civil(2011, 10, 11, 12, 13, (defined?(Rational) ? Rational(29, 2) : 14.5), (defined?(Rational) ? Rational(1, 2) : 0.5))
      @db.typecast_value(:datetime, 'year'=>2011, 'month'=>10, 'day'=>11, 'hour'=>12, 'minute'=>13, 'second'=>14, 'offset'=>(defined?(Rational) ? Rational(1, 2) : 0.5)).should == DateTime.civil(2011, 10, 11, 12, 13, 14, (defined?(Rational) ? Rational(1, 2) : 0.5))
      @db.typecast_value(:datetime, 'year'=>2011, 'month'=>10, 'day'=>11, 'hour'=>12, 'minute'=>13, 'second'=>14, 'nanos'=>500000000, 'offset'=>(defined?(Rational) ? Rational(1, 2) : 0.5)).should == DateTime.civil(2011, 10, 11, 12, 13, (defined?(Rational) ? Rational(29, 2) : 14.5), (defined?(Rational) ? Rational(1, 2) : 0.5))
    ensure
      Sequel.datetime_class = Time
    end
  end

  specify "should typecast decimal values to BigDecimal" do
    [1.0, 1, '1.0', BigDecimal('1.0')].each do |i|
      v = @db.typecast_value(:decimal, i)
      v.should be_a_kind_of(BigDecimal)
      v.should == BigDecimal.new('1.0')
    end
  end

  specify "should typecast float values to Float" do
    [1.0, 1, '1.0', BigDecimal('1.0')].each do |i|
      v = @db.typecast_value(:float, i)
      v.should be_a_kind_of(Float)
      v.should == 1.0
    end
  end

  specify "should typecast string values to String" do
    [1.0, '1.0', Sequel.blob('1.0')].each do |i|
      v = @db.typecast_value(:string, i)
      v.should be_an_instance_of(String)
      v.should == "1.0"
    end
  end

  specify "should typecast time values to SQLTime" do
    t = Time.now
    st = Sequel::SQLTime.local(t.year, t.month, t.day, 1, 2, 3)
    [st, Time.utc(t.year, t.month, t.day, 1, 2, 3), Time.local(t.year, t.month, t.day, 1, 2, 3), '01:02:03', {:hour=>1, :minute=>2, :second=>3}].each do |i|
      v = @db.typecast_value(:time, i)
      v.should be_an_instance_of(Sequel::SQLTime)
      v.should == st
    end
  end

  specify "should correctly handle time value conversion to SQLTime with fractional seconds" do
    t = Time.now
    st = Sequel::SQLTime.local(t.year, t.month, t.day, 1, 2, 3, 500000)
    t = Time.local(t.year, t.month, t.day, 1, 2, 3, 500000)
    @db.typecast_value(:time, t).should == st
  end

  specify "should have an underlying exception class available at wrapped_exception" do
    begin
      @db.typecast_value(:date, 'a')
      true.should == false
    rescue Sequel::InvalidValue => e
      e.wrapped_exception.should be_a_kind_of(ArgumentError)
    end
  end

  specify "should include underlying exception class in #inspect" do
    begin
      @db.typecast_value(:date, 'a')
      true.should == false
    rescue Sequel::InvalidValue => e
      e.inspect.should =~ /\A#<Sequel::InvalidValue: ArgumentError: .*>\z/
    end
  end
end

describe "Database#blank_object?" do
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

describe "Database#schema_autoincrementing_primary_key?" do
  specify "should indicate whether the parsed schema row indicates a primary key" do
    m = Sequel::Database.new.method(:schema_autoincrementing_primary_key?)
    m.call(:primary_key=>true, :db_type=>'integer').should == true
    m.call(:primary_key=>true, :db_type=>'varchar(255)').should == false
    m.call(:primary_key=>false, :db_type=>'integer').should == false
  end
end

describe "Database#supports_transactional_ddl?" do
  specify "should be false by default" do
    Sequel::Database.new.supports_transactional_ddl?.should == false
  end
end

describe "Database#global_index_namespace?" do
  specify "should be true by default" do
    Sequel::Database.new.global_index_namespace?.should == true
  end
end

describe "Database#supports_savepoints?" do
  specify "should be false by default" do
    Sequel::Database.new.supports_savepoints?.should == false
  end
end

describe "Database#supports_savepoints_in_prepared_transactions?" do
  specify "should be false by default" do
    Sequel::Database.new.supports_savepoints_in_prepared_transactions?.should == false
  end

  specify "should be true if both savepoints and prepared transactions are supported" do
    db = Sequel::Database.new
    db.meta_def(:supports_savepoints?){true}
    db.meta_def(:supports_prepared_transactions?){true}
    db.supports_savepoints_in_prepared_transactions?.should == true
  end
end

describe "Database#supports_prepared_transactions?" do
  specify "should be false by default" do
    Sequel::Database.new.supports_prepared_transactions?.should == false
  end
end

describe "Database#supports_transaction_isolation_levels?" do
  specify "should be false by default" do
    Sequel::Database.new.supports_transaction_isolation_levels?.should == false
  end
end

describe "Database#input_identifier_meth" do
  specify "should be the input_identifer method of a default dataset for this database" do
    db = Sequel::Database.new
    db.send(:input_identifier_meth).call(:a).should == 'a'
    db.identifier_input_method = :upcase
    db.send(:input_identifier_meth).call(:a).should == 'A'
  end
end

describe "Database#output_identifier_meth" do
  specify "should be the output_identifer method of a default dataset for this database" do
    db = Sequel::Database.new
    db.send(:output_identifier_meth).call('A').should == :A
    db.identifier_output_method = :downcase
    db.send(:output_identifier_meth).call('A').should == :a
  end
end

describe "Database#metadata_dataset" do
  specify "should be a dataset with the default settings for identifier_input_method and identifier_output_method" do
    ds = Sequel::Database.new.send(:metadata_dataset)
    ds.literal(:a).should == 'A'
    ds.send(:output_identifier, 'A').should == :a
  end
end

describe "Database#column_schema_to_ruby_default" do
  specify "should handle converting many default formats" do
    db = Sequel::Database.new
    p = lambda{|d,t| db.send(:column_schema_to_ruby_default, d, t)}
    p[nil, :integer].should be_nil
    p[1, :integer].should == 1
    p['1', :integer].should == 1
    p['-1', :integer].should == -1
    p[1.0, :float].should == 1.0
    p['1.0', :float].should == 1.0
    p['-1.0', :float].should == -1.0
    p['1.0', :decimal].should == BigDecimal.new('1.0')
    p['-1.0', :decimal].should == BigDecimal.new('-1.0')
    p[true, :boolean].should == true
    p[false, :boolean].should == false
    p['1', :boolean].should == true
    p['0', :boolean].should == false
    p['true', :boolean].should == true
    p['false', :boolean].should == false
    p["'t'", :boolean].should == true
    p["'f'", :boolean].should == false
    p["'a'", :string].should == 'a'
    p["'a'", :blob].should == Sequel.blob('a')
    p["'a'", :blob].should be_a_kind_of(Sequel::SQL::Blob)
    p["''", :string].should == ''
    p["'\\a''b'", :string].should == "\\a'b"
    p["'NULL'", :string].should == "NULL"
    p[Date.today, :date].should == Date.today
    p["'2009-10-29'", :date].should == Date.new(2009,10,29)
    p["CURRENT_TIMESTAMP", :date].should == Sequel::CURRENT_DATE
    p["CURRENT_DATE", :date].should == Sequel::CURRENT_DATE
    p["now()", :date].should == Sequel::CURRENT_DATE
    p["getdate()", :date].should == Sequel::CURRENT_DATE
    p["CURRENT_TIMESTAMP", :datetime].should == Sequel::CURRENT_TIMESTAMP
    p["CURRENT_DATE", :datetime].should == Sequel::CURRENT_TIMESTAMP
    p["now()", :datetime].should == Sequel::CURRENT_TIMESTAMP
    p["getdate()", :datetime].should == Sequel::CURRENT_TIMESTAMP
    p["'2009-10-29T10:20:30-07:00'", :datetime].should == DateTime.parse('2009-10-29T10:20:30-07:00')
    p["'2009-10-29 10:20:30'", :datetime].should == DateTime.parse('2009-10-29 10:20:30')
    p["'10:20:30'", :time].should == Time.parse('10:20:30')
    p["NaN", :float].should be_nil

    db = Sequel.mock(:host=>'postgres')
    p["''::text", :string].should == ""
    p["'\\a''b'::character varying", :string].should == "\\a'b"
    p["'a'::bpchar", :string].should == "a"
    p["(-1)", :integer].should == -1
    p["(-1.0)", :float].should == -1.0
    p['(-1.0)', :decimal].should == BigDecimal.new('-1.0')
    p["'a'::bytea", :blob].should == Sequel.blob('a')
    p["'a'::bytea", :blob].should be_a_kind_of(Sequel::SQL::Blob)
    p["'2009-10-29'::date", :date].should == Date.new(2009,10,29)
    p["'2009-10-29 10:20:30.241343'::timestamp without time zone", :datetime].should == DateTime.parse('2009-10-29 10:20:30.241343')
    p["'10:20:30'::time without time zone", :time].should == Time.parse('10:20:30')

    db = Sequel.mock(:host=>'mysql')
    p["\\a'b", :string].should == "\\a'b"
    p["a", :string].should == "a"
    p["NULL", :string].should == "NULL"
    p["-1", :float].should == -1.0
    p['-1', :decimal].should == BigDecimal.new('-1.0')
    p["2009-10-29", :date].should == Date.new(2009,10,29)
    p["2009-10-29 10:20:30", :datetime].should == DateTime.parse('2009-10-29 10:20:30')
    p["10:20:30", :time].should == Time.parse('10:20:30')
    p["a", :enum].should == "a"
    p["a,b", :set].should == "a,b"
    
    db = Sequel.mock(:host=>'mssql')
    p["(N'a')", :string].should == "a"
    p["((-12))", :integer].should == -12
    p["((12.1))", :float].should == 12.1
    p["((-12.1))", :decimal].should == BigDecimal.new('-12.1')
  end
end

describe "Database extensions" do
  before(:all) do
    class << Sequel
      alias _extension extension
      def extension(*)
      end
    end
  end
  after(:all) do
    class << Sequel
      alias extension _extension
    end
  end
  before do
    @db = Sequel.mock
  end

  specify "should be able to register an extension with a module Database#extension extend the module" do
    Sequel::Database.register_extension(:foo, Module.new{def a; 1; end})
    @db.extension(:foo).a.should == 1
  end

  specify "should be able to register an extension with a block and Database#extension call the block" do
    @db.quote_identifiers = false
    Sequel::Database.register_extension(:foo){|db| db.quote_identifiers = true}
    @db.extension(:foo).quote_identifiers?.should be_true
  end

  specify "should be able to register an extension with a callable and Database#extension call the callable" do
    @db.quote_identifiers = false
    Sequel::Database.register_extension(:foo, proc{|db| db.quote_identifiers = true})
    @db.extension(:foo).quote_identifiers?.should be_true
  end

  specify "should be able to load multiple extensions in the same call" do
    @db.quote_identifiers = false
    @db.identifier_input_method = :downcase
    Sequel::Database.register_extension(:foo, proc{|db| db.quote_identifiers = true})
    Sequel::Database.register_extension(:bar, proc{|db| db.identifier_input_method = nil})
    @db.extension(:foo, :bar)
    @db.quote_identifiers?.should be_true
    @db.identifier_input_method.should be_nil
  end

  specify "should return the receiver" do
    Sequel::Database.register_extension(:foo, Module.new{def a; 1; end})
    @db.extension(:foo).should equal(@db)
  end

  specify "should raise an Error if registering with both a module and a block" do
    proc{Sequel::Database.register_extension(:foo, Module.new){}}.should raise_error(Sequel::Error)
  end

  specify "should raise an Error if attempting to load an incompatible extension" do
    proc{@db.extension(:foo2)}.should raise_error(Sequel::Error)
  end
end
