require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model, "Subclasses plugin" do
  before do
    @c = Class.new(Sequel::Model)
    @c.plugin :subclasses
  end

  specify "#subclasses should record direct subclasses of the given model" do
    @c.subclasses.should == []

    sc1 = Class.new(@c)
    @c.subclasses.should == [sc1]
    sc1.subclasses.should == []

    sc2 = Class.new(@c)
    @c.subclasses.should == [sc1, sc2]
    sc1.subclasses.should == []
    sc2.subclasses.should == []

    ssc1 = Class.new(sc1)
    @c.subclasses.should == [sc1, sc2]
    sc1.subclasses.should == [ssc1]
    sc2.subclasses.should == []
  end

  specify "#descendents should record all descendent subclasses of the given model" do
    @c.descendents.should == []

    sc1 = Class.new(@c)
    @c.descendents.should == [sc1]
    sc1.descendents.should == []

    sc2 = Class.new(@c)
    @c.descendents.should == [sc1, sc2]
    sc1.descendents.should == []
    sc2.descendents.should == []

    ssc1 = Class.new(sc1)
    @c.descendents.should == [sc1, ssc1, sc2]
    sc1.descendents.should == [ssc1]
    sc2.descendents.should == []
    ssc1.descendents.should == []

    sssc1 = Class.new(ssc1)
    @c.descendents.should == [sc1, ssc1, sssc1, sc2]
    sc1.descendents.should == [ssc1, sssc1]
    sc2.descendents.should == []
    ssc1.descendents.should == [sssc1]
    sssc1.descendents.should == []
  end

  specify "plugin block should be called with each subclass created" do
    c = Class.new(Sequel::Model)
    a = []
    c.plugin(:subclasses){|sc| a << sc}
    sc1 = Class.new(c)
    a.should == [sc1]
    sc2 = Class.new(c)
    a.should == [sc1, sc2]
    sc3 = Class.new(sc1)
    a.should == [sc1, sc2, sc3]
    sc4 = Class.new(sc3)
    a.should == [sc1, sc2, sc3, sc4]
  end
end
