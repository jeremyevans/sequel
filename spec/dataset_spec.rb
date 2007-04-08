require File.join(File.dirname(__FILE__), '../lib/sequel')

context "Dataset" do
  setup do
    @dataset = Sequel::Dataset.new(nil)
  end
  
  specify "should provide dup_merge for chainability." do
    d1 = @dataset.dup_merge(:from => :test)
    d1.class.should == @dataset.class
    d1.should_not == @dataset
    d1.opts[:from].should == :test
    @dataset.opts[:from].should_be_nil
    
    d2 = d1.dup_merge(:order => :name)
    d2.class.should == @dataset.class
    d2.should_not == d1
    d2.should_not == @dataset
    d2.opts[:from].should == :test
    d2.opts[:order].should == :name
    d1.opts[:order].should_be_nil
    
    # dup_merge should preserve @record_class
    a_class = Class.new
    d3 = Sequel::Dataset.new(nil, nil, a_class)
    d4 = @dataset.dup_merge({})
    d3.record_class.should == a_class
    d4.record_class.should_be_nil
    d5 = d3.dup_merge(:from => :test)
    d5.record_class.should == a_class
  end
end