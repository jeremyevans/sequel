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
    @o1 = @c.new
    @o2 = @c.load(:id=>1, :h=>"--- {}\n\n")
    MODEL_DB.reset
  end
  
  it "should not detect columns that haven't been changed" do
    @o2.changed_columns.should == []
    @o2.h.should == {}
    @o2.h[1] = 2
    @o2.h.clear
    @o2.changed_columns.should == []
  end
  
  it "should detect columns that have been changed" do
    @o2.changed_columns.should == []
    @o2.h.should == {}
    @o2.h[1] = 2
    @o2.changed_columns.should == [:h]
  end
  
  it "should report correct changed_columns after saving" do
    @o2.h[1] = 2
    @o2.save_changes
    @o2.changed_columns.should == []
  end
end
