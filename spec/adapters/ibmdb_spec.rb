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

describe "A IBMDB database" do 
  specify "should provide disconnect functionality" do
    IBMDB_DB.tables
    IBMDB_DB.pool.size.should == 1
    IBMDB_DB.disconnect
    IBMDB_DB.pool.size.should == 0
  end

  specify "should be able to create new tables" do
    IBMDB_DB.create_table :test do
      Integer   :id, :null => false
      String    :blah
      Float     :point
      Date      :d, :null => false
      primary_key [:id, :d]
    end

    IBMDB_DB.tables.include?(:test).should be_true
  end

  specify "should be able to delete a table" do
    IBMDB_DB.drop_table :test
    IBMDB_DB.tables.include?(:test).should be_false
  end
end

describe "A IBMDB dataset" do

  before do 
    IBMDB_DB.create_table :test do
      primary_key   :id
      String    :blah
      Float     :point
      Date      :d
      Integer   :ii
    end
    @d = IBMDB_DB[:test]
  end

  after do
    IBMDB_DB.drop_table :test
  end

  specify "should return number of records inserted" do
    @d.count.should == 0
    @d << {:blah => 'abc', :point => 5.5, :d => Date.today}
    @d << {:blah => 'abd', :point => 5.5, :d => Date.today - 1}
    @d.count.should == 2
  end

  specify "should update a record correctly" do
    @d << {:blah => 'xxx', :ii => 50}
    @d << {:blah => 'xxx', :ii => 51}
    @d << {:blah => 'xxy', :ii => 52}
    @d.filter(:blah => 'xxx').update(:ii => 555)
    @d.filter(:ii => 555).count.should == 2
  end

  specify "should delete records correctly" do
    @d.delete
    @d << {:blah => 'abc', :ii => 123}
    @d << {:blah => 'abc', :ii => 456}
    @d << {:blah => 'def', :ii => 789}
    @d.count.should == 3
    @d.filter(:blah => 'abc').delete
    
    @d.count.should == 1
    @d.first[:blah].should == 'def'
  end

  specify "should return number of rows affected in transaction" do 
    @d << {:blah => 'def', :ii => 789}
    @d << {:blah => 'deg', :ii => 789}
    @d.filter(:blah => 'deg').delete.should == 1
  end
  
  specify "should support transactions" do
    IBMDB_DB.transaction do
      @d << {:blah => 'abc', :ii => 1}
    end

    @d.count.should == 1
  end

  specify "should be able to tell names of columns even when empty" do 
    @d.columns.should include(:ii)
  end

  #specify "should be able to prepare statements" do
    #@d << {:blah => 'def', :ii => 789}
    #@d << {:blah => 'deg', :ii => 789}
    #ds = @d.filter :blah => :$b
    #ps = ds.preapre(:select, :select_by_b)
    #ps.call(:b => deg)[:blah].should == 'deg'
  #end
  
end
