require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "accessed_columns plugin" do
  before do
    @db = Sequel.mock(:fetch=>{:name=>'a', :b=>'c'}, :numrows=>1)
    @c = Class.new(Sequel::Model(@db[:test]))
    @c.columns :name, :b
    @c.plugin :accessed_columns
    @o = @c.new
  end

  it "should record columns accessed" do
    @o.accessed_columns.should == []
    @o.name
    @o.accessed_columns.should == [:name]
    @o.name
    @o.accessed_columns.should == [:name]
    @o.b
    @o.accessed_columns.sort_by{|s| s.to_s}.should == [:b, :name]
  end

  it "should clear accessed columns when refreshing" do
    @o.name
    @o.refresh
    @o.accessed_columns.should == []
  end

  it "should clear accessed columns when saving" do
    @o.name
    @o.save
    @o.accessed_columns.should == []
  end

  it "should work when duping and cloning instances" do
    @o.name
    o = @o.dup
    @o.accessed_columns.should == [:name]
    @o.b
    @o.accessed_columns.sort_by{|s| s.to_s}.should == [:b, :name]
    o.accessed_columns.should == [:name]
    o2 = o.clone
    o2.refresh
    o.accessed_columns.should == [:name]
    o2.accessed_columns.should == []
  end

  it "should not raise exceptions when object is frozen" do
    @o.freeze
    proc{@o.name}.should_not raise_error
  end
end
