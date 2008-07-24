require File.join(File.dirname(__FILE__), "spec_helper")

describe Sequel::Model::Associations::AssociationReflection, "#associated_class" do
  before do
    @c = Class.new(Sequel::Model)
    class ::ParParent < Sequel::Model; end
  end

  it "should use the :class value if present" do
    @c.many_to_one :c, :class=>ParParent
    @c.association_reflection(:c).should include(:class)
    @c.association_reflection(:c).associated_class.should == ParParent
  end
  it "should figure out the class if the :class value is not present" do
    @c.many_to_one :c, :class=>'ParParent'
    @c.association_reflection(:c).should_not include(:class)
    @c.association_reflection(:c).associated_class.should == ParParent
  end
end

describe Sequel::Model::Associations::AssociationReflection, "#associated_primary_key" do
  before do
    @c = Class.new(Sequel::Model)
    class ::ParParent < Sequel::Model; end
  end

  it "should use the :right_primary_key value if present" do
    @c.many_to_one :c, :class=>ParParent, :associated_primary_key=>:blah__blah
    @c.association_reflection(:c).should include(:associated_primary_key)
    @c.association_reflection(:c).associated_primary_key.should == :blah__blah
  end
  it "should use the associated table's primary key if :associated_primary_key is not present" do
    @c.many_to_one :c, :class=>'ParParent'
    @c.association_reflection(:c).should_not include(:associated_primary_key)
    @c.association_reflection(:c).associated_primary_key.should == :id
  end
end

describe Sequel::Model::Associations::AssociationReflection, "#reciprocal" do
  it "should use the :reciprocal value if present" do
    @c = Class.new(Sequel::Model)
    @d = Class.new(Sequel::Model)
    @c.many_to_one :c, :class=>@d, :reciprocal=>:xx
    @c.association_reflection(:c).should include(:reciprocal)
    @c.association_reflection(:c).reciprocal.should == :xx
  end

  it "should figure out the reciprocal if the :reciprocal value is not present" do
    class ::ParParent < Sequel::Model; end
    class ::ParParentTwo < Sequel::Model; end
    class ::ParParentThree < Sequel::Model; end
    ParParent.many_to_one :par_parent_two
    ParParentTwo.one_to_many :par_parents
    ParParent.many_to_many :par_parent_threes
    ParParentThree.many_to_many :par_parents

    ParParent.association_reflection(:par_parent_two).should_not include(:reciprocal)
    ParParent.association_reflection(:par_parent_two).reciprocal.should == :par_parents
    ParParentTwo.association_reflection(:par_parents).should_not include(:reciprocal)
    ParParentTwo.association_reflection(:par_parents).reciprocal.should == :par_parent_two
    ParParent.association_reflection(:par_parent_threes).should_not include(:reciprocal)
    ParParent.association_reflection(:par_parent_threes).reciprocal.should == :par_parents
    ParParentThree.association_reflection(:par_parents).should_not include(:reciprocal)
    ParParentThree.association_reflection(:par_parents).reciprocal.should == :par_parent_threes
  end
end

describe Sequel::Model::Associations::AssociationReflection, "#select" do
  before do
    @c = Class.new(Sequel::Model)
    class ::ParParent < Sequel::Model; end
  end

  it "should use the :select value if present" do
    @c.many_to_one :c, :class=>ParParent, :select=>[:par_parents__id]
    @c.association_reflection(:c).should include(:select)
    @c.association_reflection(:c).select.should == [:par_parents__id]
  end
  it "should be the associated_table.* if :select is not present for a many_to_many associaiton" do
    @c.many_to_many :cs, :class=>'ParParent'
    @c.association_reflection(:cs).should_not include(:select)
    @c.association_reflection(:cs).select.should == :par_parents.*
  end
  it "should be if :select is not present for a many_to_one and one_to_many associaiton" do
    @c.one_to_many :cs, :class=>'ParParent'
    @c.association_reflection(:cs).should_not include(:select)
    @c.association_reflection(:cs).select.should == nil
    @c.many_to_one :c, :class=>'ParParent'
    @c.association_reflection(:c).should_not include(:select)
    @c.association_reflection(:c).select.should == nil
  end
end

