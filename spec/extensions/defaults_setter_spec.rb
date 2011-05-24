require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::DefaultsSetter" do
  before do
    @db = db = Sequel::Database.new
    @c = c = Class.new(Sequel::Model(db[:foo]))
    @c.instance_variable_set(:@db_schema, {:a=>{}})
    @c.plugin :defaults_setter
    @c.columns :a
    @pr = proc{|x| db.meta_def(:schema){|*| [[:a, {:ruby_default => x}]]}; c.dataset = c.dataset; c}
  end

  it "should set default value upon initialization" do
    @pr.call(2).new.values.should == {:a=>2}
  end

  it "should not mark the column as modified" do
    @pr.call(2).new.changed_columns.should == []
  end

  it "should not set a default of nil" do
    @pr.call(nil).new.values.should == {}
  end

  it "should not override a given value" do
    @pr.call(2)
    @c.new('a'=>3).values.should == {:a=>3}
    @c.new('a'=>nil).values.should == {:a=>nil}
  end

  it "should work correctly when subclassing" do
    Class.new(@pr.call(2)).new.values.should == {:a=>2}
  end

  it "should contain the default values in default_values" do
    @pr.call(2).default_values.should == {:a=>2}
    @pr.call(nil).default_values.should == {}
  end

  it "should allow modifications of default values" do
    @pr.call(2)
    @c.default_values[:a] = 3
    @c.new.values.should == {:a => 3}
  end

  it "should allow proc default values" do
    @pr.call(2)
    @c.default_values[:a] = proc{3}
    @c.new.values.should == {:a => 3}
  end

  it "should have procs that set default values set them to nil" do
    @pr.call(2)
    @c.default_values[:a] = proc{nil}
    @c.new.values.should == {:a => nil}
  end

  it "should work correctly on a model without a dataset" do
    @pr.call(2)
    c = Class.new(Sequel::Model(@db[:bar]))
    c.plugin :defaults_setter
    c.default_values.should == {:a=>2}
  end
end
