SEQUEL_ADAPTER_TEST = :db2

require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

if DB.table_exists?(:test)
  DB.drop_table(:test)
end

describe Sequel::Database do
  before do
    @db = DB
    @db.create_table(:test){String :a}
    @ds = @db[:test]
  end

  after do
    @db.drop_table(:test)
  end

  specify "should provide disconnect functionality after preparing a connection" do
    @ds.prepare(:first, :a).call
    @db.disconnect
    @db.pool.size.should == 0
  end

  specify "should return version correctly" do
    @db.db2_version.should match(/DB2 v/i)
  end
end

describe "Simple Dataset operations" do
  before(:all) do
    Sequel::DB2.use_clob_as_blob = false
    DB.create_table!(:items) do
      Integer :id, :primary_key => true
      Integer :number
      column  :bin_string, 'varchar(20) for bit data'
      column  :bin_blob, 'blob'
    end
    @ds = DB[:items]
  end
  after(:each) do
    @ds.delete
  end
  after(:all) do
    Sequel::DB2.use_clob_as_blob = true
    DB.drop_table(:items)
  end

  specify "should insert with a primary key specified" do
    @ds.insert(:id => 1,   :number => 10)
    @ds.insert(:id => 100, :number => 20)
    @ds.select_hash(:id, :number).should == {1 => 10, 100 => 20}
  end

  specify "should insert into binary columns" do
    @ds.insert(:id => 1, :bin_string => Sequel.blob("\1"), :bin_blob => Sequel.blob("\2"))
    @ds.select(:bin_string, :bin_blob).first.should == {:bin_string => "\1", :bin_blob => "\2"}
  end
end

describe Sequel::Database do
  before do
    @db = DB
  end
  after do
    @db.drop_table(:items)
  end
  specify "should parse primary keys from the schema properly" do
    @db.create_table!(:items){Integer :number}
    @db.schema(:items).collect{|k,v| k if v[:primary_key]}.compact.should == []
    @db.create_table!(:items){primary_key :number}
    @db.schema(:items).collect{|k,v| k if v[:primary_key]}.compact.should == [:number]
    @db.create_table!(:items){Integer :number1, :null => false; Integer :number2, :null => false; primary_key [:number1, :number2]}
    @db.schema(:items).collect{|k,v| k if v[:primary_key]}.compact.should == [:number1, :number2]
  end
end

describe "Sequel::IBMDB.convert_smallint_to_bool" do
  before do
    @db = DB
    @db.create_table!(:booltest){column :b, 'smallint'; column :i, 'integer'}
    @ds = @db[:booltest]
  end
  after do
    Sequel::IBMDB.convert_smallint_to_bool = true
    @db.drop_table(:booltest)
  end

  specify "should consider smallint datatypes as boolean if set, but not larger smallints" do
    @db.schema(:booltest, :reload=>true).first.last[:type].should == :boolean
    @db.schema(:booltest, :reload=>true).first.last[:db_type].should match /smallint/i
    Sequel::IBMDB.convert_smallint_to_bool = false
    @db.schema(:booltest, :reload=>true).first.last[:type].should == :integer
    @db.schema(:booltest, :reload=>true).first.last[:db_type].should match /smallint/i
  end

  specify "should return smallints as bools and integers as integers when set" do
    Sequel::IBMDB.convert_smallint_to_bool = true
    @ds.delete
    @ds << {:b=>true, :i=>10}
    @ds.all.should == [{:b=>true, :i=>10}]
    @ds.delete
    @ds << {:b=>false, :i=>0}
    @ds.all.should == [{:b=>false, :i=>0}]
    @ds.delete
    @ds << {:b=>true, :i=>1}
    @ds.all.should == [{:b=>true, :i=>1}]
  end

  specify "should return all smallints as integers when unset" do
    Sequel::IBMDB.convert_smallint_to_bool = false
    @ds.delete
    @ds << {:b=>true, :i=>10}
    @ds.all.should == [{:b=>1, :i=>10}]
    @ds.delete
    @ds << {:b=>false, :i=>0}
    @ds.all.should == [{:b=>0, :i=>0}]

    @ds.delete
    @ds << {:b=>1, :i=>10}
    @ds.all.should == [{:b=>1, :i=>10}]
    @ds.delete
    @ds << {:b=>0, :i=>0}
    @ds.all.should == [{:b=>0, :i=>0}]
  end
end if DB.adapter_scheme == :ibmdb

describe "Simple Dataset operations in transactions" do
  before do
    DB.create_table!(:items_insert_in_transaction) do
      Integer :id, :primary_key => true
      integer :number
    end
    @ds = DB[:items_insert_in_transaction]
  end
  after do
    DB.drop_table(:items_insert_in_transaction)
  end

  specify "should insert correctly with a primary key specified inside a transaction" do
    DB.transaction do
      @ds.insert(:id=>100, :number=>20)
      @ds.count.should == 1
      @ds.order(:id).all.should == [{:id=>100, :number=>20}]
    end
  end
end
