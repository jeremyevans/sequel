require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model, "Subclasses plugin" do
  before do
    @c = Class.new(Sequel::Model)
    @c.plugin :subclasses
  end

  it "#subclasses should record direct subclasses of the given model" do
    @c.subclasses.must_equal []

    sc1 = Class.new(@c)
    @c.subclasses.must_equal [sc1]
    sc1.subclasses.must_equal []

    sc2 = Class.new(@c)
    @c.subclasses.must_equal [sc1, sc2]
    sc1.subclasses.must_equal []
    sc2.subclasses.must_equal []

    ssc1 = Class.new(sc1)
    @c.subclasses.must_equal [sc1, sc2]
    sc1.subclasses.must_equal [ssc1]
    sc2.subclasses.must_equal []
  end

  it "#descendents should record all descendent subclasses of the given model" do
    @c.descendents.must_equal []

    sc1 = Class.new(@c)
    @c.descendents.must_equal [sc1]
    sc1.descendents.must_equal []

    sc2 = Class.new(@c)
    @c.descendents.must_equal [sc1, sc2]
    sc1.descendents.must_equal []
    sc2.descendents.must_equal []

    ssc1 = Class.new(sc1)
    @c.descendents.must_equal [sc1, ssc1, sc2]
    sc1.descendents.must_equal [ssc1]
    sc2.descendents.must_equal []
    ssc1.descendents.must_equal []

    sssc1 = Class.new(ssc1)
    @c.descendents.must_equal [sc1, ssc1, sssc1, sc2]
    sc1.descendents.must_equal [ssc1, sssc1]
    sc2.descendents.must_equal []
    ssc1.descendents.must_equal [sssc1]
    sssc1.descendents.must_equal []
  end

  it "plugin block should be called with each subclass created" do
    c = Class.new(Sequel::Model)
    a = []
    c.plugin(:subclasses){|sc| a << sc}
    sc1 = Class.new(c)
    a.must_equal [sc1]
    sc2 = Class.new(c)
    a.must_equal [sc1, sc2]
    sc3 = Class.new(sc1)
    a.must_equal [sc1, sc2, sc3]
    sc4 = Class.new(sc3)
    a.must_equal [sc1, sc2, sc3, sc4]
  end
end
