require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "column_conflicts plugin" do
  before do
    @db = Sequel.mock
    @c = Class.new(Sequel::Model(@db[:test]))
    @c.columns :model, :use_transactions, :foo
    @c.plugin :column_conflicts
    @o = @c.load(:model=>1, :use_transactions=>2, :foo=>4)
  end

  it "should have mass assignment work correctly" do
    @o.set_fields({:use_transactions=>3}, [:use_transactions])
    @o.get_column_value(:use_transactions).should == 3
  end

  it "should handle both symbols and strings" do
    @o.get_column_value(:model).should == 1
    @o.get_column_value("model").should == 1
    @o.set_column_value(:use_transactions=, 3)
    @o.get_column_value(:use_transactions).should == 3
    @o.set_column_value(:use_transactions=, 4)
    @o.get_column_value(:use_transactions).should == 4
  end

  it "should allow manual setting of conflicted columns" do
    @c.send(:define_method, :foo){raise}
    @c.get_column_conflict!(:foo)
    @o.get_column_value(:foo).should == 4

    @c.send(:define_method, :model=){raise}
    @c.set_column_conflict!(:model)
    @o.set_column_value(:model=, 2).should == 2
    @o.get_column_value(:model).should == 2
  end

  it "should work correctly in subclasses" do
    @o = Class.new(@c).load(:model=>1, :use_transactions=>2)
    @o.get_column_value(:model).should == 1
    @o.get_column_value("model").should == 1
    @o.set_column_value(:use_transactions=, 3)
    @o.get_column_value(:use_transactions).should == 3
    @o.set_column_value(:use_transactions=, 4)
    @o.get_column_value(:use_transactions).should == 4
  end

  it "should work correctly for dataset changes" do
    ds = @db[:test]
    def ds.columns; [:object_id] end
    @c.dataset = ds
    o = @c.load(:object_id=>3)
    o.get_column_value(:object_id).should == 3
    o.object_id.should_not == 3
  end
end
