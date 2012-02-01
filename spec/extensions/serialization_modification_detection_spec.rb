require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")
require 'yaml'

describe "serialization_modification_detection plugin" do
  before do
    @c = Class.new(Sequel::Model(:items))
    @c.class_eval do
      columns :id, :h
      plugin :serialization, :yaml, :h
      plugin :serialization_modification_detection
    end
    @o1 = @c.new(:h=>{})
    @o2 = @c.load(:id=>1, :h=>"--- {}\n\n")
    @o3 = @c.new
    @o4 = @c.load(:id=>1, :h=>nil)
    MODEL_DB.reset
  end
  
  it "should not detect columns that haven't been changed" do
    @o1.changed_columns.should == []
    @o1.h.should == {}
    @o1.h[1] = 2
    @o1.h.clear
    @o1.changed_columns.should == []

    @o2.changed_columns.should == []
    @o2.h.should == {}
    @o2.h[1] = 2
    @o2.h.clear
    @o2.changed_columns.should == []
  end
  
  it "should detect columns that have been changed" do
    @o1.changed_columns.should == []
    @o1.h.should == {}
    @o1.h[1] = 2
    @o1.changed_columns.should == [:h]

    @o2.changed_columns.should == []
    @o2.h.should == {}
    @o2.h[1] = 2
    @o2.changed_columns.should == [:h]

    @o3.changed_columns.should == []
    @o3.h.should == nil
    @o3.h = {}
    @o3.changed_columns.should == [:h]

    @o4.changed_columns.should == []
    @o4.h.should == nil
    @o4.h = {}
    @o4.changed_columns.should == [:h]
  end
  
  it "should report correct changed_columns after saving" do
    @o1.h[1] = 2
    @o1.save
    @o1.changed_columns.should == []

    @o2.h[1] = 2
    @o2.save_changes
    @o2.changed_columns.should == []

    @o3.h = {1=>2}
    @o3.save
    @o3.changed_columns.should == []

    @o4.h = {1=>2}
    @o4.save
    @o4.changed_columns.should == []
  end
end
