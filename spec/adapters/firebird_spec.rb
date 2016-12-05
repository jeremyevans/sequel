SEQUEL_ADAPTER_TEST = :firebird

require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

def DB.sqls
  (@sqls ||= [])
end
logger = Object.new
def logger.method_missing(m, msg)
  DB.sqls.push(msg)
end
DB.loggers = [logger]

DB.create_table! :test do
  varchar :name,  :size => 50
  integer :val,   :index => true
end

DB.create_table! :test2 do
  integer :val
  timestamp :time_stamp
end

DB.create_table! :test3 do
  integer :val
  timestamp :time_stamp
end

DB.create_table! :test5 do
  primary_key :xid
  integer :val
end

DB.create_table! :test6 do
  primary_key :xid
  blob :val
  String :val2
  varchar :val3, :size=>200
  String :val4, :text=>true
end

describe "A Firebird database" do
  before do
    @db = DB
  end

  it "should provide disconnect functionality" do
    @db.tables
    @db.pool.size.must_equal 1
    @db.disconnect
    @db.pool.size.must_equal 0
  end

  it "should raise Sequel::Error on error" do
    proc{@db << "SELECT 1 + 'a'"}.must_raise(Sequel::Error)
  end
end

describe "A Firebird dataset" do
  before do
    @d = DB[:test].with_quote_identifiers(true)
    @d.delete
  end

  it "should return the correct record count" do
    @d.count.must_equal 0
    @d << {:name => 'abc', :val => 123}
    @d << {:name => 'abc', :val  => 456}
    @d << {:name => 'def', :val => 789}
    @d.count.must_equal 3
  end

  it "should return the correct records" do
    @d.to_a.must_equal []
    @d << {:name => 'abc', :val => 123}
    @d << {:name => 'abc', :val => 456}
    @d << {:name => 'def', :val => 789}

    @d.order(:val).to_a.must_equal [
      {:name => 'abc', :val => 123},
      {:name => 'abc', :val => 456},
      {:name => 'def', :val => 789}
    ]
  end

  it "should update records correctly" do
    @d << {:name => 'abc', :val => 123}
    @d << {:name => 'abc', :val => 456}
    @d << {:name => 'def', :val => 789}
    @d.filter(:name => 'abc').update(:val => 530)

    @d[:name => 'def'][:val].must_equal 789
    @d.filter(:val => 530).count.must_equal 2
  end

  it "should delete records correctly" do
    @d << {:name => 'abc', :val => 123}
    @d << {:name => 'abc', :val => 456}
    @d << {:name => 'def', :val => 789}
    @d.filter(:name => 'abc').delete

    @d.count.must_equal 1
    @d.first[:name].must_equal 'def'
  end

  it "should be able to literalize booleans" do
    @d.literal(true)
    @d.literal(false)
  end

  it "should quote columns and tables using double quotes if quoting identifiers" do
    @d.select(:name).sql.must_equal \
      'SELECT "NAME" FROM "TEST"'

    @d.select('COUNT(*)'.lit).sql.must_equal \
      'SELECT COUNT(*) FROM "TEST"'

    @d.select(:max[:val]).sql.must_equal \
      'SELECT max("VAL") FROM "TEST"'

    @d.select(:now[]).sql.must_equal \
    'SELECT now() FROM "TEST"'

    @d.select(:max[Sequel[:items][:val]]).sql.must_equal \
      'SELECT max("ITEMS"."VAL") FROM "TEST"'

    @d.order(:name.desc).sql.must_equal \
      'SELECT * FROM "TEST" ORDER BY "NAME" DESC'

    @d.select('TEST.NAME AS item_:name'.lit).sql.must_equal \
      'SELECT TEST.NAME AS item_:name FROM "TEST"'

    @d.select('"NAME"'.lit).sql.must_equal \
      'SELECT "NAME" FROM "TEST"'

    @d.select('max(TEST."NAME") AS "max_:name"'.lit).sql.must_equal \
      'SELECT max(TEST."NAME") AS "max_:name" FROM "TEST"'

    @d.select(:test[:ABC, 'hello']).sql.must_equal \
      "SELECT test(\"ABC\", 'hello') FROM \"TEST\""

    @d.select(:test[Sequel[:ABC][:DEF], 'hello']).sql.must_equal \
      "SELECT test(\"ABC\".\"DEF\", 'hello') FROM \"TEST\""

    @d.select(:test[Sequel[:ABC][:DEF], 'hello'].as(:X2)).sql.must_equal \
      "SELECT test(\"ABC\".\"DEF\", 'hello') AS \"X2\" FROM \"TEST\""

    @d.insert_sql(:val => 333).must_match \
      /\AINSERT INTO "TEST" \("VAL"\) VALUES \(333\)( RETURNING NULL)?\z/

    @d.insert_sql(:X => :Y).must_match \
      /\AINSERT INTO "TEST" \("X"\) VALUES \("Y"\)( RETURNING NULL)?\z/
  end

  it "should quote fields correctly when reversing the order if quoting identifiers" do
    @d.reverse_order(:name).sql.must_equal \
      'SELECT * FROM "TEST" ORDER BY "NAME" DESC'

    @d.reverse_order(:name.desc).sql.must_equal \
      'SELECT * FROM "TEST" ORDER BY "NAME" ASC'

    @d.reverse_order(:name, :test.desc).sql.must_equal \
      'SELECT * FROM "TEST" ORDER BY "NAME" DESC, "TEST" ASC'

    @d.reverse_order(:name.desc, :test).sql.must_equal \
      'SELECT * FROM "TEST" ORDER BY "NAME" ASC, "TEST" DESC'
  end

  it "should support transactions" do
    DB.transaction do
      @d << {:name => 'abc', :val => 1}
    end

    @d.count.must_equal 1
  end

  it "should have #transaction yield the connection" do
    DB.transaction do |conn|
      conn.wont_equal nil
    end
  end

  it "should correctly rollback transactions" do
    proc do
      DB.transaction do
        @d << {:name => 'abc', :val => 1}
        raise RuntimeError, 'asdf'
      end
    end.must_raise(RuntimeError)

    @d.count.must_equal 0
  end

  it "should handle returning inside of the block by committing" do
    def DB.ret_commit
      transaction do
        self[:test] << {:name => 'abc'}
        return
        self[:test] << {:name => 'd'}
      end
    end
    @d.count.must_equal 0
    DB.ret_commit
    @d.count.must_equal 1
    DB.ret_commit
    @d.count.must_equal 2
    proc do
      DB.transaction do
        raise RuntimeError, 'asdf'
      end
    end.must_raise(RuntimeError)

    @d.count.must_equal 2
  end

  it "should quote and upcase reserved keywords" do
    @d = DB[:testing]
    @d.select(:select).sql.must_equal \
      'SELECT "SELECT" FROM "TESTING"'
  end
end

describe "A Firebird dataset with a timestamp field" do
  before do
    @d = DB[:test3]
    @d.delete
  end

  it "should store milliseconds in time fields" do
    t = Time.now
    @d << {:val=>1, :time_stamp=>t}
    @d.literal(@d[:val =>'1'][:time_stamp]).must_equal @d.literal(t)
    @d[:val=>'1'][:time_stamp].usec.must_equal t.usec - t.usec % 100
  end
end

describe "A Firebird database" do
  before do
    @db = DB
    @db.drop_table?(:posts)
    @db.sqls.clear
  end

  it "should allow us to name the sequences" do
    @db.create_table(:posts){primary_key :id, :sequence_name => "seq_test"}
    check_sqls do
      @db.sqls.must_equal [
        "DROP SEQUENCE SEQ_TEST",
        "CREATE TABLE POSTS (ID integer PRIMARY KEY )",
        "CREATE SEQUENCE SEQ_TEST",
        "          CREATE TRIGGER BI_POSTS_ID for POSTS\n          ACTIVE BEFORE INSERT position 0\n          as               begin\n                if ((new.ID is null) or (new.ID = 0)) then\n                begin\n                  new.ID = next value for seq_test;\n                end\n              end\n\n"
      ]
    end
  end

  it "should allow us to set the starting position for the sequences" do
    @db.create_table(:posts){primary_key :id, :sequence_start_position => 999}
    check_sqls do
      @db.sqls.must_equal [
        "DROP SEQUENCE SEQ_POSTS_ID",
        "CREATE TABLE POSTS (ID integer PRIMARY KEY )",
        "CREATE SEQUENCE SEQ_POSTS_ID",
        "ALTER SEQUENCE SEQ_POSTS_ID RESTART WITH 999",
        "          CREATE TRIGGER BI_POSTS_ID for POSTS\n          ACTIVE BEFORE INSERT position 0\n          as               begin\n                if ((new.ID is null) or (new.ID = 0)) then\n                begin\n                  new.ID = next value for seq_posts_id;\n                end\n              end\n\n"
      ]
    end
  end

  it "should allow us to name and set the starting position for the sequences" do
    @db.create_table(:posts){primary_key :id, :sequence_name => "seq_test", :sequence_start_position => 999}
    check_sqls do
      @db.sqls.must_equal [
        "DROP SEQUENCE SEQ_TEST",
        "CREATE TABLE POSTS (ID integer PRIMARY KEY )",
        "CREATE SEQUENCE SEQ_TEST",
        "ALTER SEQUENCE SEQ_TEST RESTART WITH 999",
        "          CREATE TRIGGER BI_POSTS_ID for POSTS\n          ACTIVE BEFORE INSERT position 0\n          as               begin\n                if ((new.ID is null) or (new.ID = 0)) then\n                begin\n                  new.ID = next value for seq_test;\n                end\n              end\n\n"
      ]
    end
  end

  it "should allow us to name the triggers" do
    @db.create_table(:posts){primary_key :id, :trigger_name => "trig_test"}
    check_sqls do
      @db.sqls.must_equal [
        "DROP SEQUENCE SEQ_POSTS_ID",
        "CREATE TABLE POSTS (ID integer PRIMARY KEY )",
        "CREATE SEQUENCE SEQ_POSTS_ID",
        "          CREATE TRIGGER TRIG_TEST for POSTS\n          ACTIVE BEFORE INSERT position 0\n          as               begin\n                if ((new.ID is null) or (new.ID = 0)) then\n                begin\n                  new.ID = next value for seq_posts_id;\n                end\n              end\n\n"
      ]
    end
  end

  it "should allow us to not create the sequence" do
    @db.create_table(:posts){primary_key :id, :create_sequence => false}
    check_sqls do
      @db.sqls.must_equal [
        "CREATE TABLE POSTS (ID integer PRIMARY KEY )",
        "          CREATE TRIGGER BI_POSTS_ID for POSTS\n          ACTIVE BEFORE INSERT position 0\n          as               begin\n                if ((new.ID is null) or (new.ID = 0)) then\n                begin\n                  new.ID = next value for seq_posts_id;\n                end\n              end\n\n"
      ]
    end
  end

  it "should allow us to not create the trigger" do
    @db.create_table(:posts){primary_key :id, :create_trigger => false}
    check_sqls do
      @db.sqls.must_equal [
        "DROP SEQUENCE SEQ_POSTS_ID",
        "CREATE TABLE POSTS (ID integer PRIMARY KEY )",
        "CREATE SEQUENCE SEQ_POSTS_ID",
      ]
    end
  end

  it "should allow us to not create either the sequence nor the trigger" do
    @db.create_table(:posts){primary_key :id, :create_sequence => false, :create_trigger => false}
    check_sqls do
      @db.sqls.must_equal [
        "CREATE TABLE POSTS (ID integer PRIMARY KEY )"
      ]
    end
  end

  it "should support column operations" do
    @db.create_table!(:test2){varchar :name, :size => 50; integer :val}
    @db[:test2] << {}
    @db[:test2].columns.must_equal [:name, :val]

    @db.add_column :test2, :xyz, :varchar, :size => 50
    @db[:test2].columns.must_equal [:name, :val, :xyz]

    @db[:test2].columns.must_equal [:name, :val, :xyz]
    @db.drop_column :test2, :xyz

    @db[:test2].columns.must_equal [:name, :val]

    @db[:test2].delete
    @db.add_column :test2, :xyz, :varchar, :default => '000', :size => 50#, :create_domain => 'xyz_varchar'
    @db[:test2] << {:name => 'mmm', :val => 111, :xyz => 'qqqq'}

    @db[:test2].columns.must_equal [:name, :val, :xyz]
    @db.rename_column :test2, :xyz, :zyx
    @db[:test2].columns.must_equal [:name, :val, :zyx]
    @db[:test2].first[:zyx].must_equal 'qqqq'

    @db.add_column :test2, :xyz, :decimal, :elements => [12, 2]
    @db[:test2].delete
    @db[:test2] << {:name => 'mmm', :val => 111, :xyz => 56.4}
    @db.set_column_type :test2, :xyz, :varchar, :size => 50

    @db[:test2].first[:xyz].must_equal "56.40"
  end

  it "should allow us to retrieve the primary key for a table" do
    @db.create_table!(:test2){primary_key :id}
    @db.primary_key(:test2).must_equal ["id"]
  end
end

describe "Postgres::Dataset#insert" do
  before do
    @ds = DB[:test5]
    @ds.delete
  end

  it "should have insert_returning_sql use the RETURNING keyword" do
    @ds.insert_returning_sql(:XID, :val=>10).must_equal "INSERT INTO TEST5 (VAL) VALUES (10) RETURNING XID"
    @ds.insert_returning_sql('*'.lit, :val=>10).must_equal "INSERT INTO TEST5 (VAL) VALUES (10) RETURNING *"
    @ds.insert_returning_sql('NULL'.lit, :val=>10).must_equal "INSERT INTO TEST5 (VAL) VALUES (10) RETURNING NULL"
  end

  it "should correctly return the inserted record's primary key value" do
    value1 = 10
    id1 = @ds.insert(:val=>value1)
    @ds.first(:XID=>id1)[:val].must_equal value1
    value2 = 20
    id2 = @ds.insert(:val=>value2)
    @ds.first(:XID=>id2)[:val].must_equal value2
  end

  it "should return nil if the table has no primary key" do
    ds = DB[:test]
    ds.delete
    ds.insert(:name=>'a').must_be_nil
  end
end

describe "Postgres::Dataset#insert" do
  before do
    @ds = DB[:test6]
    @ds.delete
  end

  it "should insert and retrieve a blob successfully" do
    value1 = "\1\2\2\2\2222\2\2\2"
    value2 = "abcd"
    value3 = "efgh"
    value4 = "ijkl"
    id1 = @ds.insert(:val=>value1, :val2=>value2, :val3=>value3, :val4=>value4)
    @ds.first(:XID=>id1)[:val].must_equal value1
    @ds.first(:XID=>id1)[:val2].must_equal value2
    @ds.first(:XID=>id1)[:val3].must_equal value3
    @ds.first(:XID=>id1)[:val4].must_equal value4
  end
end
