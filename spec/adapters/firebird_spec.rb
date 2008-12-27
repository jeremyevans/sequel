require File.join(File.dirname(__FILE__), 'spec_helper.rb')

unless defined?(FIREBIRD_DB)
  FIREBIRD_URL = 'firebird://sysdba:masterkey@localhost/reality_spec' unless defined? FIREBIRD_URL
  FIREBIRD_DB = Sequel.connect(ENV['SEQUEL_FB_SPEC_DB']||FIREBIRD_URL)
end

FIREBIRD_DB.create_table! :TEST do
  varchar :NAME,  :size => 50
  integer :VAL,   :index => true
end
#FIREBIRD_DB.create_table! :TEST2 do
#  text :NAME
#  integer :VAL
#end
FIREBIRD_DB.create_table! :TEST3 do
  integer :VAL
  timestamp :TIME_STAMP
end
#FIREBIRD_DB.create_table! :TEST4 do
#  varchar :NAME, :size => 20
#  bytea :VAL
#end
FIREBIRD_DB.create_table! :TEST5 do
  primary_key :XID
  integer :VAL
end

context "A Firebird database" do
  setup do
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
  setup do
    @d = FIREBIRD_DB[:TEST]
    @d.delete # remove all records
  end

  specify "should return the correct record count" do
    @d.count.should == 0
    @d << {:NAME => 'abc', :VAL => 123}
    @d << {:NAME => 'abc', :VAL  => 456}
    @d << {:NAME => 'def', :VAL => 789}
    @d.count.should == 3
  end

  specify "should return the correct records" do
    @d.to_a.should == []
    @d << {:NAME => 'abc', :VAL => 123}
    @d << {:NAME => 'abc', :VAL => 456}
    @d << {:NAME => 'def', :VAL => 789}

    @d.order(:VAL).to_a.should == [
      {:NAME => 'abc', :VAL => 123},
      {:NAME => 'abc', :VAL => 456},
      {:NAME => 'def', :VAL => 789}
    ]
  end

  specify "should update records correctly" do
    @d << {:NAME => 'abc', :VAL => 123}
    @d << {:NAME => 'abc', :VAL => 456}
    @d << {:NAME => 'def', :VAL => 789}
    @d.filter(:NAME => 'abc').update(:VAL => 530)

    # the third record should stay the same
    # floating-point precision bullshit
    @d[:NAME => 'def'][:VAL].should == 789
    @d.filter(:VAL => 530).count.should == 2
  end

  specify "should delete records correctly" do
    @d << {:NAME => 'abc', :VAL => 123}
    @d << {:NAME => 'abc', :VAL => 456}
    @d << {:NAME => 'def', :VAL => 789}
    @d.filter(:NAME => 'abc').delete

    @d.count.should == 1
    @d.first[:NAME].should == 'def'
  end

  specify "should be able to literalize booleans" do
    proc {@d.literal(true)}.should_not raise_error
    proc {@d.literal(false)}.should_not raise_error
  end

  specify "should quote columns and tables using double quotes if quoting identifiers" do
    @d.quote_identifiers = true
    @d.select(:NAME).sql.should == \
      'SELECT "NAME" FROM "TEST"'

    @d.select('COUNT(*)'.lit).sql.should == \
      'SELECT COUNT(*) FROM "TEST"'

    @d.select(:max[:VAL]).sql.should == \
      'SELECT max("VAL") FROM "TEST"'

    @d.select(:NOW[]).sql.should == \
    'SELECT NOW() FROM "TEST"'

    @d.select(:max[:ITEMS__VAL]).sql.should == \
      'SELECT max("ITEMS"."VAL") FROM "TEST"'

    @d.order(:NAME.desc).sql.should == \
      'SELECT * FROM "TEST" ORDER BY "NAME" DESC'

    @d.select('TEST.NAME AS item_:NAME'.lit).sql.should == \
      'SELECT TEST.NAME AS item_:NAME FROM "TEST"'

    @d.select('"NAME"'.lit).sql.should == \
      'SELECT "NAME" FROM "TEST"'

    @d.select('max(TEST."NAME") AS "max_:NAME"'.lit).sql.should == \
      'SELECT max(TEST."NAME") AS "max_:NAME" FROM "TEST"'

    @d.select(:TEST[:ABC, 'hello']).sql.should == \
      "SELECT TEST(\"ABC\", 'hello') FROM \"TEST\""

    @d.select(:TEST[:ABC__DEF, 'hello']).sql.should == \
      "SELECT TEST(\"ABC\".\"DEF\", 'hello') FROM \"TEST\""

    @d.select(:TEST[:ABC__DEF, 'hello'].as(:X2)).sql.should == \
      "SELECT TEST(\"ABC\".\"DEF\", 'hello') AS \"X2\" FROM \"TEST\""

    @d.insert_sql(:VAL => 333).should =~ \
      /\AINSERT INTO "TEST" \("VAL"\) VALUES \(333\)( RETURNING NULL)?\z/

    @d.insert_sql(:X => :Y).should =~ \
      /\AINSERT INTO "TEST" \("X"\) VALUES \("Y"\)( RETURNING NULL)?\z/
  end

  specify "should quote fields correctly when reversing the order if quoting identifiers" do
    @d.quote_identifiers = true
    @d.reverse_order(:NAME).sql.should == \
      'SELECT * FROM "TEST" ORDER BY "NAME" DESC'

    @d.reverse_order(:NAME.desc).sql.should == \
      'SELECT * FROM "TEST" ORDER BY "NAME" ASC'

    @d.reverse_order(:NAME, :TEST.desc).sql.should == \
      'SELECT * FROM "TEST" ORDER BY "NAME" DESC, "TEST" ASC'

    @d.reverse_order(:NAME.desc, :TEST).sql.should == \
      'SELECT * FROM "TEST" ORDER BY "NAME" ASC, "TEST" DESC'
  end

  specify "should support transactions" do
    FIREBIRD_DB.transaction do
      @d << {:NAME => 'abc', :VAL => 1}
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
        @d << {:NAME => 'abc', :VAL => 1}
        raise RuntimeError, 'asdf'
      end
    end.should raise_error(RuntimeError)

    @d.count.should == 0
  end

  specify "should handle returning inside of the block by committing" do
    def FIREBIRD_DB.ret_commit
      transaction do
        self[:test] << {:NAME => 'abc'}
        return
        self[:test] << {:NAME => 'd'}
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
end

context "A Firebird dataset with a timestamp field" do
  setup do
    @d = FIREBIRD_DB[:test3]
    @d.delete
  end

  specify "should store milliseconds in time fields" do
    t = Time.now
    @d << {:VAL=>1, :TIME_STAMP=>t}
    @d.literal(@d[:VAL =>'1'][:TIME_STAMP]).should == @d.literal(t)
    @d[:VAL=>'1'][:TIME_STAMP].usec.should == t.usec - t.usec % 100
  end
end

context "A Firebird database" do
  setup do
    @db = FIREBIRD_DB
  end

  specify "should support column operations" do
    @db.create_table!(:test2){varchar :NAME, :size => 50; integer :VAL}
    @db[:test2] << {}
    @db[:test2].columns.should == [:NAME, :VAL]

#    @db.create_domain!(:xyz_varchar){varchar :default => '000', :size => 50}
    @db.add_column :test2, :XYZ, :varchar, :size => 50
    @db[:test2].columns.should == [:NAME, :VAL, :XYZ]
#    @db[:test2] << {:NAME => 'mmm', :VAL => 111}
#    @db[:test2].first[:XYZ].should == '000'

    @db[:test2].columns.should == [:NAME, :VAL, :XYZ]
    @db.drop_column :test2, :XYZ

    @db[:test2].columns.should == [:NAME, :VAL]

    @db[:test2].delete
    @db.add_column :test2, :XYZ, :varchar, :default => '000', :size => 50#, :create_domain => 'xyz_varchar'
    @db[:test2] << {:NAME => 'mmm', :VAL => 111, :XYZ => 'qqqq'}

    @db[:test2].columns.should == [:NAME, :VAL, :XYZ]
    @db.rename_column :test2, :XYZ, :ZYX
    @db[:test2].columns.should == [:NAME, :VAL, :ZYX]
    @db[:test2].first[:ZYX].should == 'qqqq'

    @db.add_column :test2, :XYZ, :decimal, :elements => [12, 2]
    @db[:test2].delete
    @db[:test2] << {:NAME => 'mmm', :VAL => 111, :XYZ => 56.4}
    @db.set_column_type :test2, :XYZ, :varchar, :size => 50

    @db[:test2].first[:XYZ].should == "56.40"
  end
end

context "Postgres::Dataset#insert" do
  setup do
    @ds = FIREBIRD_DB[:test5]
    @ds.delete
  end

  specify "should using call insert_returning_sql" do
    @ds.should_receive(:single_value).once.with(:sql=>'INSERT INTO TEST5 (VAL) VALUES (10) RETURNING XID')
    @ds.insert(:VAL=>10)
  end

  specify "should have insert_returning_sql use the RETURNING keyword" do
    @ds.insert_returning_sql(:XID, :VAL=>10).should == "INSERT INTO TEST5 (VAL) VALUES (10) RETURNING XID"
    @ds.insert_returning_sql('*'.lit, :VAL=>10).should == "INSERT INTO TEST5 (VAL) VALUES (10) RETURNING *"
    @ds.insert_returning_sql('NULL'.lit, :VAL=>10).should == "INSERT INTO TEST5 (VAL) VALUES (10) RETURNING NULL"
  end

  specify "should correctly return the inserted record's primary key value" do
    value1 = 10
    id1 = @ds.insert(:VAL=>value1)
    @ds.first(:XID=>id1)[:VAL].should == value1
    value2 = 20
    id2 = @ds.insert(:VAL=>value2)
    @ds.first(:XID=>id2)[:VAL].should == value2
  end

  specify "should return nil if the table has no primary key" do
    ds = FIREBIRD_DB[:test]
    ds.delete
    ds.insert(:NAME=>'a').should == nil
  end
end

