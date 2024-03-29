require_relative "spec_helper"

describe Sequel::Model, "Subclasses plugin" do
  before do
    @c = Class.new(Sequel::Model)
    @c.plugin :subclasses
  end

  it "#subclasses should record direct subclasses of the given model" do
    @c.subclasses.must_equal []

    sc1 = Class.new(@c)
    def sc1.name; 'A'; end
    @c.subclasses.must_equal [sc1]
    sc1.subclasses.must_equal []

    sc2 = Class.new(@c)
    def sc2.name; 'B'; end
    @c.subclasses.sort_by(&:name).must_equal [sc1, sc2]
    sc1.subclasses.must_equal []
    sc2.subclasses.must_equal []

    ssc1 = Class.new(sc1)
    @c.subclasses.sort_by(&:name).must_equal [sc1, sc2]
    sc1.subclasses.must_equal [ssc1]
    sc2.subclasses.must_equal []
  end

  it "#descendants should record all descendent subclasses of the given model" do
    @c.descendants.must_equal []

    sc1 = Class.new(@c)
    def sc1.name; 'A'; end
    @c.descendants.must_equal [sc1]
    sc1.descendants.must_equal []

    sc2 = Class.new(@c)
    def sc2.name; 'B'; end
    @c.descendants.sort_by(&:name).must_equal [sc1, sc2]
    sc1.descendants.must_equal []
    sc2.descendants.must_equal []

    ssc1 = Class.new(sc1)
    def ssc1.name; 'C'; end
    @c.descendants.sort_by(&:name).must_equal [sc1, sc2, ssc1]
    sc1.descendants.must_equal [ssc1]
    sc2.descendants.must_equal []
    ssc1.descendants.must_equal []

    sssc1 = Class.new(ssc1)
    def sssc1.name; 'D'; end
    @c.descendants.sort_by(&:name).must_equal [sc1, sc2, ssc1, sssc1]
    sc1.descendants.must_equal [ssc1, sssc1]
    sc2.descendants.must_equal []
    ssc1.descendants.must_equal [sssc1]
    sssc1.descendants.must_equal []
  end

  it "#descendents should be an alias of descendants" do
    @c.descendents.must_equal []

    sc1 = Class.new(@c)
    @c.descendents.must_equal [sc1]
    sc1.descendents.must_equal []
  end

  it "#freeze_descendants should finalize the associations for all descendants" do
    sc1 = Class.new(@c)
    sc1.set_dataset :bars
    sc1.set_primary_key :foo
    sc2 = Class.new(@c)
    sc2.set_dataset :bazs
    sc2.many_to_one :bar, :class=>sc1
    @c.freeze_descendants
    sc1.frozen?.must_equal true
    sc2.frozen?.must_equal true
    sc2.association_reflection(:bar)[:primary_key].must_equal :foo
  end

  it "#freeze_descendents should be an alias of freeze_descendants" do
    sc1 = Class.new(@c)
    sc1.set_dataset :bars
    sc1.set_primary_key :foo
    sc2 = Class.new(@c)
    sc2.set_dataset :bazs
    sc2.many_to_one :bar, :class=>sc1
    @c.freeze_descendents
    sc1.frozen?.must_equal true
    sc2.frozen?.must_equal true
    sc2.association_reflection(:bar)[:primary_key].must_equal :foo
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
