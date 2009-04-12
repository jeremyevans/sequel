require File.join(File.dirname(__FILE__), 'spec_helper.rb')

unless defined?(FIREBIRD_DB)
  FIREBIRD_URL = 'firebird://sysdba:masterkey@localhost/reality_spec' unless defined? FIREBIRD_URL
  FIREBIRD_DB = Sequel.connect(ENV['SEQUEL_FB_SPEC_DB']||FIREBIRD_URL)
end

FIREBIRD_DB.create_table! :test do
  varchar :name,  :size => 50
  integer :val,   :index => true
end

FIREBIRD_DB.create_table! :test2 do
  integer :val
  timestamp :time_stamp
end

FIREBIRD_DB.create_table! :test3 do
  integer :val
  timestamp :time_stamp
end

FIREBIRD_DB.create_table! :test5 do
  primary_key :xid
  integer :val
end

FIREBIRD_DB.create_table! :test6 do
  primary_key :xid
  blob :val
  String :val2
  varchar :val3, :size=>200
  text :val4
end

context "A Firebird database" do
  before do
    @db = FIREBIRD_DB
  end

  specify "should provide disconnect functionality" do
    @db.tables
    @db.pool.size.should == 1
    @db.disconnect
    @db.pool.size.should == 0
  end

  specify "should raise Sequel::Error on error" do
    proc{@db << "SELECT 1 + 'a'"}.should raise_error(Sequel::Error)
  end
end

context "A Firebird dataset" do
  before do
    @d = FIREBIRD_DB[:test]
    @d.delete # remove all records
  end

  specify "should return the correct record count" do
    @d.count.should == 0
    @d << {:name => 'abc', :val => 123}
    @d << {:name => 'abc', :val  => 456}
    @d << {:name => 'def', :val => 789}
    @d.count.should == 3
  end

  specify "should return the correct records" do
    @d.to_a.should == []
    @d << {:name => 'abc', :val => 123}
    @d << {:name => 'abc', :val => 456}
    @d << {:name => 'def', :val => 789}

    @d.order(:val).to_a.should == [
      {:name => 'abc', :val => 123},
      {:name => 'abc', :val => 456},
      {:name => 'def', :val => 789}
    ]
  end

  specify "should update records correctly" do
    @d << {:name => 'abc', :val => 123}
    @d << {:name => 'abc', :val => 456}
    @d << {:name => 'def', :val => 789}
    @d.filter(:name => 'abc').update(:val => 530)

    # the third record should stay the same
    # floating-point precision bullshit
    @d[:name => 'def'][:val].should == 789
    @d.filter(:val => 530).count.should == 2
  end

  specify "should delete records correctly" do
    @d << {:name => 'abc', :val => 123}
    @d << {:name => 'abc', :val => 456}
    @d << {:name => 'def', :val => 789}
    @d.filter(:name => 'abc').delete

    @d.count.should == 1
    @d.first[:name].should == 'def'
  end

  specify "should be able to literalize booleans" do
    proc {@d.literal(true)}.should_not raise_error
    proc {@d.literal(false)}.should_not raise_error
  end

  specify "should quote columns and tables using double quotes if quoting identifiers" do
    @d.quote_identifiers = true
    @d.select(:name).sql.should == \
      'SELECT "NAME" FROM "TEST"'

    @d.select('COUNT(*)'.lit).sql.should == \
      'SELECT COUNT(*) FROM "TEST"'

    @d.select(:max[:val]).sql.should == \
      'SELECT max("VAL") FROM "TEST"'

    @d.select(:now[]).sql.should == \
    'SELECT now() FROM "TEST"'

    @d.select(:max[:items__val]).sql.should == \
      'SELECT max("ITEMS"."VAL") FROM "TEST"'

    @d.order(:name.desc).sql.should == \
      'SELECT * FROM "TEST" ORDER BY "NAME" DESC'

    @d.select('TEST.NAME AS item_:name'.lit).sql.should == \
      'SELECT TEST.NAME AS item_:name FROM "TEST"'

    @d.select('"NAME"'.lit).sql.should == \
      'SELECT "NAME" FROM "TEST"'

    @d.select('max(TEST."NAME") AS "max_:name"'.lit).sql.should == \
      'SELECT max(TEST."NAME") AS "max_:name" FROM "TEST"'

    @d.select(:test[:ABC, 'hello']).sql.should == \
      "SELECT test(\"ABC\", 'hello') FROM \"TEST\""

    @d.select(:test[:ABC__DEF, 'hello']).sql.should == \
      "SELECT test(\"ABC\".\"DEF\", 'hello') FROM \"TEST\""

    @d.select(:test[:ABC__DEF, 'hello'].as(:X2)).sql.should == \
      "SELECT test(\"ABC\".\"DEF\", 'hello') AS \"X2\" FROM \"TEST\""

    @d.insert_sql(:val => 333).should =~ \
      /\AINSERT INTO "TEST" \("VAL"\) VALUES \(333\)( RETURNING NULL)?\z/

    @d.insert_sql(:X => :Y).should =~ \
      /\AINSERT INTO "TEST" \("X"\) VALUES \("Y"\)( RETURNING NULL)?\z/
  end

  specify "should quote fields correctly when reversing the order if quoting identifiers" do
    @d.quote_identifiers = true
    @d.reverse_order(:name).sql.should == \
      'SELECT * FROM "TEST" ORDER BY "NAME" DESC'

    @d.reverse_order(:name.desc).sql.should == \
      'SELECT * FROM "TEST" ORDER BY "NAME" ASC'

    @d.reverse_order(:name, :test.desc).sql.should == \
      'SELECT * FROM "TEST" ORDER BY "NAME" DESC, "TEST" ASC'

    @d.reverse_order(:name.desc, :test).sql.should == \
      'SELECT * FROM "TEST" ORDER BY "NAME" ASC, "TEST" DESC'
  end

  specify "should support transactions" do
    FIREBIRD_DB.transaction do
      @d << {:name => 'abc', :val => 1}
    end

    @d.count.should == 1
  end

  specify "should have #transaction yield the connection" do
    FIREBIRD_DB.transaction do |conn|
      conn.should_not == nil
    end
  end

  specify "should correctly rollback transactions" do
    proc do
      FIREBIRD_DB.transaction do
        @d << {:name => 'abc', :val => 1}
        raise RuntimeError, 'asdf'
      end
    end.should raise_error(RuntimeError)

    @d.count.should == 0
  end

  specify "should handle returning inside of the block by committing" do
    def FIREBIRD_DB.ret_commit
      transaction do
        self[:test] << {:name => 'abc'}
        return
        self[:test] << {:name => 'd'}
      end
    end
    @d.count.should == 0
    FIREBIRD_DB.ret_commit
    @d.count.should == 1
    FIREBIRD_DB.ret_commit
    @d.count.should == 2
    proc do
      FIREBIRD_DB.transaction do
        raise RuntimeError, 'asdf'
      end
    end.should raise_error(RuntimeError)

    @d.count.should == 2
  end

  specify "should quote and upcase reserved keywords" do
    @d = FIREBIRD_DB[:testing]
    @d.quote_identifiers = true
    @d.select(:select).sql.should == \
      'SELECT "SELECT" FROM "TESTING"'
  end
end

context "A Firebird dataset with a timestamp field" do
  before do
    @d = FIREBIRD_DB[:test3]
    @d.delete
  end

  specify "should store milliseconds in time fields" do
    t = Time.now
    @d << {:val=>1, :time_stamp=>t}
    @d.literal(@d[:val =>'1'][:time_stamp]).should == @d.literal(t)
    @d[:val=>'1'][:time_stamp].usec.should == t.usec - t.usec % 100
  end
end

context "A Firebird database" do
  before do
    @db = FIREBIRD_DB
  end

  specify "should allow us to name the sequences" do
    g = Sequel::Schema::Generator.new(FIREBIRD_DB) do
      primary_key :id, :sequence_name => "seq_test"
    end
    FIREBIRD_DB.send(:create_table_sql_list, :posts, *g.create_info).should == [[
      "CREATE TABLE POSTS (ID integer PRIMARY KEY )",
      "CREATE SEQUENCE SEQ_TEST",
      "          CREATE TRIGGER BI_POSTS_ID for POSTS\n          ACTIVE BEFORE INSERT position 0\n          as               begin\n                if ((new.ID is null) or (new.ID = 0)) then\n                begin\n                  new.ID = next value for seq_test;\n                end\n              end\n\n"
    ], "DROP SEQUENCE SEQ_TEST" ]
  end

  specify "should allow us to set the starting position for the sequences" do
    g = Sequel::Schema::Generator.new(FIREBIRD_DB) do
      primary_key :id, :sequence_start_position => 999
    end
    FIREBIRD_DB.send(:create_table_sql_list, :posts, *g.create_info).should == [[
      "CREATE TABLE POSTS (ID integer PRIMARY KEY )",
      "CREATE SEQUENCE SEQ_POSTS_ID",
      "ALTER SEQUENCE SEQ_POSTS_ID RESTART WITH 999",
      "          CREATE TRIGGER BI_POSTS_ID for POSTS\n          ACTIVE BEFORE INSERT position 0\n          as               begin\n                if ((new.ID is null) or (new.ID = 0)) then\n                begin\n                  new.ID = next value for seq_posts_id;\n                end\n              end\n\n"
    ], "DROP SEQUENCE SEQ_POSTS_ID" ]
  end

  specify "should allow us to name and set the starting position for the sequences" do
    g = Sequel::Schema::Generator.new(FIREBIRD_DB) do
      primary_key :id, :sequence_name => "seq_test", :sequence_start_position => 999
    end
    FIREBIRD_DB.send(:create_table_sql_list, :posts, *g.create_info).should == [[
      "CREATE TABLE POSTS (ID integer PRIMARY KEY )",
      "CREATE SEQUENCE SEQ_TEST",
      "ALTER SEQUENCE SEQ_TEST RESTART WITH 999",
      "          CREATE TRIGGER BI_POSTS_ID for POSTS\n          ACTIVE BEFORE INSERT position 0\n          as               begin\n                if ((new.ID is null) or (new.ID = 0)) then\n                begin\n                  new.ID = next value for seq_test;\n                end\n              end\n\n"
    ], "DROP SEQUENCE SEQ_TEST" ]
  end

  specify "should allow us to name the triggers" do
    g = Sequel::Schema::Generator.new(FIREBIRD_DB) do
      primary_key :id, :trigger_name => "trig_test"
    end
    FIREBIRD_DB.send(:create_table_sql_list, :posts, *g.create_info).should == [[
      "CREATE TABLE POSTS (ID integer PRIMARY KEY )",
      "CREATE SEQUENCE SEQ_POSTS_ID",
      "          CREATE TRIGGER TRIG_TEST for POSTS\n          ACTIVE BEFORE INSERT position 0\n          as               begin\n                if ((new.ID is null) or (new.ID = 0)) then\n                begin\n                  new.ID = next value for seq_posts_id;\n                end\n              end\n\n"
    ], "DROP SEQUENCE SEQ_POSTS_ID" ]
  end

  specify "should allow us to not create the sequence" do
    g = Sequel::Schema::Generator.new(FIREBIRD_DB) do
      primary_key :id, :create_sequence => false
    end
    FIREBIRD_DB.send(:create_table_sql_list, :posts, *g.create_info).should == [[
      "CREATE TABLE POSTS (ID integer PRIMARY KEY )",
      "          CREATE TRIGGER BI_POSTS_ID for POSTS\n          ACTIVE BEFORE INSERT position 0\n          as               begin\n                if ((new.ID is null) or (new.ID = 0)) then\n                begin\n                  new.ID = next value for seq_posts_id;\n                end\n              end\n\n"
    ], nil]
  end

  specify "should allow us to not create the trigger" do
    g = Sequel::Schema::Generator.new(FIREBIRD_DB) do
      primary_key :id, :create_trigger => false
    end
    FIREBIRD_DB.send(:create_table_sql_list, :posts, *g.create_info).should == [[
      "CREATE TABLE POSTS (ID integer PRIMARY KEY )",
      "CREATE SEQUENCE SEQ_POSTS_ID",
    ], "DROP SEQUENCE SEQ_POSTS_ID"]
  end

  specify "should allow us to not create either the sequence nor the trigger" do
    g = Sequel::Schema::Generator.new(FIREBIRD_DB) do
      primary_key :id, :create_sequence => false, :create_trigger => false
    end
    FIREBIRD_DB.send(:create_table_sql_list, :posts, *g.create_info).should == [[
      "CREATE TABLE POSTS (ID integer PRIMARY KEY )"
    ], nil]
  end

  specify "should support column operations" do
    @db.create_table!(:test2){varchar :name, :size => 50; integer :val}
    @db[:test2] << {}
    @db[:test2].columns.should == [:name, :val]

    @db.add_column :test2, :xyz, :varchar, :size => 50
    @db[:test2].columns.should == [:name, :val, :xyz]

    @db[:test2].columns.should == [:name, :val, :xyz]
    @db.drop_column :test2, :xyz

    @db[:test2].columns.should == [:name, :val]

    @db[:test2].delete
    @db.add_column :test2, :xyz, :varchar, :default => '000', :size => 50#, :create_domain => 'xyz_varchar'
    @db[:test2] << {:name => 'mmm', :val => 111, :xyz => 'qqqq'}

    @db[:test2].columns.should == [:name, :val, :xyz]
    @db.rename_column :test2, :xyz, :zyx
    @db[:test2].columns.should == [:name, :val, :zyx]
    @db[:test2].first[:zyx].should == 'qqqq'

    @db.add_column :test2, :xyz, :decimal, :elements => [12, 2]
    @db[:test2].delete
    @db[:test2] << {:name => 'mmm', :val => 111, :xyz => 56.4}
    @db.set_column_type :test2, :xyz, :varchar, :size => 50

    @db[:test2].first[:xyz].should == "56.40"
  end

  specify "should allow us to retrieve the primary key for a table" do
    @db.create_table!(:test2){primary_key :id}
    @db.primary_key(:test2).should == ["id"]
  end
end

context "Postgres::Dataset#insert" do
  before do
    @ds = FIREBIRD_DB[:test5]
    @ds.delete
  end

  specify "should using call insert_returning_sql" do
#    @ds.should_receive(:single_value).once.with(:sql=>'INSERT INTO TEST5 (VAL) VALUES (10) RETURNING XID', :server=> :default)
    @ds.should_receive(:single_value).once
    @ds.insert(:val=>10)
  end

  specify "should have insert_returning_sql use the RETURNING keyword" do
    @ds.insert_returning_sql(:XID, :val=>10).should == "INSERT INTO TEST5 (VAL) VALUES (10) RETURNING XID"
    @ds.insert_returning_sql('*'.lit, :val=>10).should == "INSERT INTO TEST5 (VAL) VALUES (10) RETURNING *"
    @ds.insert_returning_sql('NULL'.lit, :val=>10).should == "INSERT INTO TEST5 (VAL) VALUES (10) RETURNING NULL"
  end

  specify "should correctly return the inserted record's primary key value" do
    value1 = 10
    id1 = @ds.insert(:val=>value1)
    @ds.first(:XID=>id1)[:val].should == value1
    value2 = 20
    id2 = @ds.insert(:val=>value2)
    @ds.first(:XID=>id2)[:val].should == value2
  end

  specify "should return nil if the table has no primary key" do
    ds = FIREBIRD_DB[:test]
    ds.delete
    ds.insert(:name=>'a').should == nil
  end
end

context "Postgres::Dataset#insert" do
  before do
    @ds = FIREBIRD_DB[:test6]
    @ds.delete
  end

  specify "should insert and retrieve a blob successfully" do
    value1 = "\1\2\2\2\2222\2\2\2"
    value2 = "abcd"
    value3 = "efgh"
    value4 = "ijkl"
    id1 = @ds.insert(:val=>value1, :val2=>value2, :val3=>value3, :val4=>value4)
    @ds.first(:XID=>id1)[:val].should == value1
    @ds.first(:XID=>id1)[:val2].should == value2
    @ds.first(:XID=>id1)[:val3].should == value3
    @ds.first(:XID=>id1)[:val4].should == value4
  end
end
