#!/usr/bin/env ruby
# coding: utf-8
#Author: Roy L Zuo (roylzuo at gmail dot com)
#Description: 

require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

IBMDB_DB = Sequel.connect(IBMDB_URL)

if IBMDB_DB.table_exists?(:test)
  IBMDB_DB.drop_table :test
end
INTEGRATION_DB = IBMDB_DB unless defined?(INTEGRATION_DB)

describe "Sequel::IBMDB.convert_smallint_to_bool" do
  before do
    @db = IBMDB_DB
    @db.create_table(:booltest){column :b, 'smallint'; column :i, 'integer'}
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
end
