SEQUEL_ADAPTER_TEST = :fdbsql unless defined? SEQUEL_ADAPTER_TEST and SEQUEL_ADAPTER_TEST == :fdbsql

unless defined? SEQUEL_PATH
  require 'sequel'
  SEQUEL_PATH = Gem.loaded_specs['sequel'].full_gem_path
  require File.join("#{SEQUEL_PATH}",'spec','adapters','spec_helper.rb')
end

describe 'Fdbsql' do
  before(:all) do
    @db = DB
  end

  describe 'automatic NotCommitted retry' do
    before do
      @db.drop_table?(:some_table)
      @db.create_table(:some_table) {text :name; primary_key :id}
      @db2 = Sequel.connect(@db.url)
    end
    after do
      @db2.disconnect
      @db.drop_table?(:some_table)
    end
    specify 'within a transaction' do
      proc do
        @db.transaction do
          @db[:some_table].insert(name: 'a')
          @db2.drop_table(:some_table)
        end
      end.should raise_error(Sequel::Fdbsql::NotCommittedError)
    end
  end

  describe 'connection.in_transaction' do
    before(:all) do
      raise 'too many servers' if @db.servers.count > 1
      @db.drop_table?(:some_table)
      @db.create_table(:some_table) {text :name; primary_key :id}
      @conn = @db.pool.hold(@db.servers.first) {|conn| conn}
    end
    after(:all) do
      @db.drop_table?(:some_table)
    end

    specify 'is unset by default' do
      @conn.in_transaction.should be_false
    end
    specify 'is set in a transaction' do
      @db.transaction do
        @conn.in_transaction.should be_true
      end
    end
    specify 'is unset after a commit' do
      @db.transaction do
        @db[:some_table].insert(name: 'a')
        @conn.in_transaction.should be_true
      end
      @conn.in_transaction.should be_false
    end
    specify 'is unset after a rollback' do
      @db.transaction do
        raise Sequel::Rollback.new
      end
      @conn.in_transaction.should be_false
    end

  end

  describe 'schema_parsing' do
    after do
      @db.drop_table?(:test)
    end

    specify 'without primary key' do
      @db.create_table(:test) do
        text :name
        int :value
      end
      schema = DB.schema(:test, reload: true)
      schema.count.should eq 2
      schema[0][0].should eq :name
      schema[1][0].should eq :value
      schema.each {|col| col[1][:primary_key].should be_false}
    end

    specify 'with one primary key' do
      @db.create_table(:test) do
        text :name
        primary_key :id
      end
      schema = DB.schema(:test, reload: true)
      schema.count.should eq 2
      id_col = schema[0]
      name_col = schema[1]
      name_col[0].should eq :name
      id_col[0].should eq :id
      name_col[1][:primary_key].should be_false
      id_col[1][:primary_key].should be_true
    end

    specify 'with multiple primary keys' do
      @db.create_table(:test) do
        Integer :id
        Integer :id2
        primary_key [:id, :id2]
      end
      schema = DB.schema(:test, reload: true)
      schema.count.should eq 2
      id_col = schema[0]
      id2_col = schema[1]
      id_col[0].should eq :id
      id2_col[0].should eq :id2
      id_col[1][:primary_key].should be_true
      id2_col[1][:primary_key].should be_true
    end

    specify 'with other constraints' do
      @db.create_table(:test) do
        primary_key :id
        Integer :unique, unique: true
      end
      schema = DB.schema(:test, reload: true)
      schema.count.should eq 2
      id_col = schema[0]
      unique_col = schema[1]
      id_col[0].should eq :id
      unique_col[0].should eq :unique
      id_col[1][:primary_key].should be_true
      unique_col[1][:primary_key].should be_false
    end
    after do
      @db.drop_table?(:other_table)
    end
    specify 'with other tables' do
      @db.create_table(:test) do
        Integer :id
        text :name
      end
      @db.create_table(:other_table) do
        primary_key :id
        varchar :name, unique: true
      end
      schema = DB.schema(:test, reload: true)
      schema.count.should eq 2
      schema.each {|col| col[1][:primary_key].should be_false}
    end

    describe 'with explicit schema' do
      before do
        @db.create_table(:test) do
          primary_key :id
        end
        @schema = @db['SELECT CURRENT_SCHEMA'].first.values.first
        @second_schema = @schema + "--2"
        @db.create_table(Sequel.qualify(@second_schema,:test)) do
          primary_key :id2
          Integer :id
        end
      end
      after do
        @db.drop_table?(Sequel.qualify(@second_schema,:test))
        @db.drop_table?(:test)
      end

      specify 'gets info for correct table' do
        schema = DB.schema(:test, reload: true, schema: @second_schema)
        schema.count.should eq 2
        id2_col = schema[0]
        id_col = schema[1]
        id_col[0].should eq :id
        id2_col[0].should eq :id2
        id_col[1][:primary_key].should be_false
        id2_col[1][:primary_key].should be_true
      end
    end
  end

  describe 'primary_key' do
    after do
      @db.drop_table?(:test)
      @db.drop_table?(:other_table)
    end

    specify 'without primary key' do
      @db.create_table(:test) do
        text :name
        int :value
      end
      DB.primary_key(:test).should eq nil
    end

    specify 'with one primary key' do
      @db.create_table(:test) do
        text :name
        primary_key :id
      end
      DB.primary_key(:test).should eq :id
    end

    specify 'with multiple primary keys' do
      @db.create_table(:test) do
        Integer :id
        Integer :id2
        primary_key [:id, :id2]
      end
      primary_key = DB.primary_key(:test)
      primary_key.should match_array([:id, :id2])
    end

    specify 'with other constraints' do
      @db.create_table(:test) do
        primary_key :id
        Integer :unique, unique: true
      end
      DB.primary_key(:test).should eq :id
    end

    specify 'with other tables' do
      @db.create_table(:test) do
        Integer :id
        text :name
      end
      @db.create_table(:other_table) do
        primary_key :id
        varchar :name, unique: true
      end
      DB.primary_key(:other_table).should eq :id
    end

    specify 'responds to alter table' do
      @db.create_table(:test) do
        Integer :id
        text :name
      end
      @db.alter_table(:test) do
        add_primary_key :quid
      end
      DB.primary_key(:test).should eq :quid
    end

    describe 'with explicit schema' do
      before do
        @db.create_table(:test) do
          primary_key :id
        end
        @schema = @db['SELECT CURRENT_SCHEMA'].first.values.first
        @second_schema = @schema + "--2"
        @db.create_table(Sequel.qualify(@second_schema,:test)) do
          primary_key :id2
        end
      end
      after do
        @db.drop_table?(Sequel.qualify(@second_schema,:test))
        @db.drop_table?(:test)
      end

      specify 'gets correct primary key' do
        DB.primary_key(:test, schema: @second_schema).should eq :id2
      end
    end
  end

  describe '#tables' do
    before do
      @schema = @db['SELECT CURRENT_SCHEMA'].first.values.first
      @second_schema = @schema + "--2"
      @db.create_table(:test) do
        primary_key :id
      end
      @db.create_table(Sequel.qualify(@second_schema,:test2)) do
        primary_key :id
      end
    end
    after do
      @db.drop_table?(Sequel.qualify(@second_schema,:test2))
      @db.drop_table?(:test)
    end
    specify 'on explicit schema' do
      tables = @db.tables(schema: @second_schema)
      tables.should include(:test2)
      tables.should_not include(:test)
    end
    specify 'qualified' do
      tables = @db.tables(qualify: true)
      tables.should include(Sequel::SQL::QualifiedIdentifier.new(@schema.to_sym, :test))
      tables.should_not include(:test)
    end
  end

  describe '#views' do
    def drop_things
      @db.drop_view(Sequel.qualify(@second_schema,:test_view2), if_exists: true)
      @db.drop_table?(Sequel.qualify(@second_schema,:test_table))
      @db.drop_view(:test_view, if_exists: true)
      @db.drop_table?(:test_table)
    end
    before do
      @schema = @db['SELECT CURRENT_SCHEMA'].single_value
      @second_schema = @schema + "--2"
      drop_things
      @db.create_table(:test_table){Integer :a}
      @db.create_view :test_view, @db[:test_table]
      @db.create_table(Sequel.qualify(@second_schema,:test_table)) do
        Integer :b
      end
      @db.create_view(Sequel.qualify(@second_schema, :test_view2),
                      @db[Sequel.qualify(@second_schema, :test_table)])
    end
    after do
      drop_things
    end
    specify 'on explicit schema' do
      views = @db.views(schema: @second_schema)
      views.should include(:test_view2)
      views.should_not include(:test_view)
    end
    specify 'qualified' do
      views = @db.views(qualify: true)
      views.should include(Sequel::SQL::QualifiedIdentifier.new(@schema.to_sym, :test_view))
      views.should_not include(:test)
    end
  end

  describe 'prepared statements' do
    def create_table
      @db.create_table!(:test) {Integer :a; Text :b}
      @db[:test].insert(1, 'blueberries')
      @db[:test].insert(2, 'trucks')
      @db[:test].insert(3, 'foxes')
    end
    def drop_table
      @db.drop_table?(:test)
    end
    before do
      create_table
    end
    after do
      drop_table
    end

    it 're-prepares on stale statement' do
      @db[:test].filter(:a=>:$n).prepare(:all, :select_a).call(:n=>2).to_a.should == [{:a => 2, :b => 'trucks'}]
      drop_table
      create_table
      @db[:test].filter(:a=>:$n).prepare(:all, :select_a).call(:n=>2).to_a.should == [{:a => 2, :b => 'trucks'}]
    end

    it 'can call already prepared' do
      @db[:test].filter(:a=>:$n).prepare(:all, :select_a).call(:n=>2).to_a.should == [{:a => 2, :b => 'trucks'}]
      drop_table
      create_table
      @db.call(:select_a, :n=>2).to_a.should == [{:a => 2, :b => 'trucks'}]
    end
  end

  describe 'connecting' do
    specify '#fdbsql' do
      db2 = Sequel.fdbsql(DB.uri)
    end

    describe 'opts' do
      before do
        @fake_conn = double('connection class')
        stub_const('PG::Connection', @fake_conn)
      end

      def fake_conn(args)
        fake_conn_instance = double("fake connection")
        fake_conn_instance.stub(:set_notice_receiver)
        fake_conn_instance.stub(:query).with('SELECT VERSION()', nil).
          and_return(double(cmd_tuples: 1, first: {'_SQL_COL_1' => 'FoundationDB 1.9.6'}))
        @fake_conn.should_receive(:new).with(args).once.and_return(fake_conn_instance)
        fake_conn_instance.should_receive(:close).once
      end

      [['database', {:database => 'mydb'}, {:dbname => 'mydb'}],
       ['host', {:host => 'somewhere.com'}, {:host => 'somewhere.com'}],
       ['host', {:host => nil}, {:host => 'localhost'}],
       ['password', {:password => 'mypw'}, {:password => 'mypw'}],
       ['user', {:user => 'uxt'}, {:user => 'uxt'}],
       ['username', {:username => 'uxt'}, {:user => 'uxt'}],
       ['hostaddr', {:hostaddr => '192.168.1.35'}, {:hostaddr => '192.168.1.35'}],
       ['port', {:port => 3487}, {:port => 3487}],
       ['default port', {}, {:port => 15432}],
       ['connect_timeout', {:connect_timeout => 4890}, {:connect_timeout => 4890}],
       ['default connect_timeout', {}, {:connect_timeout => 20}],
       ['sslmode', {:sslmode => 'require'}, {:sslmode => 'require'}], # (disable|allow|prefer|require)
      ].each do |opts|
        specify opts[0] do
          fake_conn(include(opts[2]))
          default = {:adapter => 'fdbsql', :database => 'the database', :host => 'localhost'}
          Sequel.connect(default.merge(opts[1])) do |db|
            db.run('SELECT VERSION()')
          end
        end
      end
    end
  end

  describe 'Database schema modifiers' do
    # this test was copied from sequel's integration/schema_test because that one drops a serial primary key which is not
    # currently supported in fdbsql
    specify "should be able to specify constraint names for column constraints" do
      @db.create_table!(:items2){Integer :id, :primary_key=>true, :primary_key_constraint_name=>:foo_pk}
      @db.create_table!(:items){foreign_key :id, :items2, :unique=>true, :foreign_key_constraint_name => :foo_fk, :unique_constraint_name => :foo_uk, :null=>false}
      @db.alter_table(:items){drop_constraint :foo_fk, :type=>:foreign_key; drop_constraint :foo_uk, :type=>:unique}
      @db.alter_table(:items2){drop_constraint :foo_pk, :type=>:primary_key}
    end
  end
end
