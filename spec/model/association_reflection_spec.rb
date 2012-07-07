require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model::Associations::AssociationReflection, "#associated_class" do
  before do
    @c = Class.new(Sequel::Model(:foo))
    class ::ParParent < Sequel::Model; end
  end
  after do
    Object.send(:remove_const, :ParParent)
  end

  it "should use the :class value if present" do
    @c.many_to_one :c, :class=>ParParent
    @c.association_reflection(:c).keys.should include(:class)
    @c.association_reflection(:c).associated_class.should == ParParent
  end

  it "should figure out the class if the :class value is not present" do
    @c.many_to_one :c, :class=>'ParParent'
    @c.association_reflection(:c).keys.should_not include(:class)
    @c.association_reflection(:c).associated_class.should == ParParent
  end
end

describe Sequel::Model::Associations::AssociationReflection, "#primary_key" do
  before do
    @c = Class.new(Sequel::Model(:foo))
    class ::ParParent < Sequel::Model; end
  end
  after do
    Object.send(:remove_const, :ParParent)
  end

  it "should use the :primary_key value if present" do
    @c.many_to_one :c, :class=>ParParent, :primary_key=>:blah__blah
    @c.association_reflection(:c).keys.should include(:primary_key)
    @c.association_reflection(:c).primary_key.should == :blah__blah
  end

  it "should use the associated table's primary key if :primary_key is not present" do
    @c.many_to_one :c, :class=>'ParParent'
    @c.association_reflection(:c).keys.should_not include(:primary_key)
    @c.association_reflection(:c).primary_key.should == :id
  end
end

describe Sequel::Model::Associations::AssociationReflection, "#reciprocal" do
  before do
    class ::ParParent < Sequel::Model; end
    class ::ParParentTwo < Sequel::Model; end
    class ::ParParentThree < Sequel::Model; end
  end
  after do
    Object.send(:remove_const, :ParParent)
    Object.send(:remove_const, :ParParentTwo)
    Object.send(:remove_const, :ParParentThree)
  end

  it "should use the :reciprocal value if present" do
    @c = Class.new(Sequel::Model(:foo))
    @d = Class.new(Sequel::Model(:foo))
    @c.many_to_one :c, :class=>@d, :reciprocal=>:xx
    @c.association_reflection(:c).keys.should include(:reciprocal)
    @c.association_reflection(:c).reciprocal.should == :xx
  end

  it "should require the associated class is the current class to be a reciprocal" do
    ParParent.many_to_one :par_parent_two, :key=>:blah
    ParParent.many_to_one :par_parent_three, :key=>:blah
    ParParentTwo.one_to_many :par_parents, :key=>:blah
    ParParentThree.one_to_many :par_parents, :key=>:blah

    ParParentTwo.association_reflection(:par_parents).reciprocal.should == :par_parent_two
    ParParentThree.association_reflection(:par_parents).reciprocal.should == :par_parent_three

    ParParent.many_to_many :par_parent_twos, :left_key=>:l, :right_key=>:r, :join_table=>:jt
    ParParent.many_to_many :par_parent_threes, :left_key=>:l, :right_key=>:r, :join_table=>:jt
    ParParentTwo.many_to_many :par_parents, :right_key=>:l, :left_key=>:r, :join_table=>:jt
    ParParentThree.many_to_many :par_parents, :right_key=>:l, :left_key=>:r, :join_table=>:jt

    ParParentTwo.association_reflection(:par_parents).reciprocal.should == :par_parent_twos
    ParParentThree.association_reflection(:par_parents).reciprocal.should == :par_parent_threes
  end

  it "should handle composite keys" do
    ParParent.many_to_one :par_parent_two, :key=>[:a, :b], :primary_key=>[:c, :b]
    ParParent.many_to_one :par_parent_three, :key=>[:d, :e], :primary_key=>[:c, :b]
    ParParentTwo.one_to_many :par_parents, :primary_key=>[:c, :b], :key=>[:a, :b]
    ParParentThree.one_to_many :par_parents, :primary_key=>[:c, :b], :key=>[:d, :e]

    ParParentTwo.association_reflection(:par_parents).reciprocal.should == :par_parent_two
    ParParentThree.association_reflection(:par_parents).reciprocal.should == :par_parent_three

    ParParent.many_to_many :par_parent_twos, :left_key=>[:l1, :l2], :right_key=>[:r1, :r2], :left_primary_key=>[:pl1, :pl2], :right_primary_key=>[:pr1, :pr2], :join_table=>:jt
    ParParent.many_to_many :par_parent_threes, :right_key=>[:l1, :l2], :left_key=>[:r1, :r2], :left_primary_key=>[:pl1, :pl2], :right_primary_key=>[:pr1, :pr2], :join_table=>:jt
    ParParentTwo.many_to_many :par_parents, :right_key=>[:l1, :l2], :left_key=>[:r1, :r2], :right_primary_key=>[:pl1, :pl2], :left_primary_key=>[:pr1, :pr2], :join_table=>:jt
    ParParentThree.many_to_many :par_parents, :left_key=>[:l1, :l2], :right_key=>[:r1, :r2], :right_primary_key=>[:pl1, :pl2], :left_primary_key=>[:pr1, :pr2], :join_table=>:jt

    ParParentTwo.association_reflection(:par_parents).reciprocal.should == :par_parent_twos
    ParParentThree.association_reflection(:par_parents).reciprocal.should == :par_parent_threes
  end

  it "should figure out the reciprocal if the :reciprocal value is not present" do
    ParParent.many_to_one :par_parent_two
    ParParentTwo.one_to_many :par_parents
    ParParent.many_to_many :par_parent_threes
    ParParentThree.many_to_many :par_parents

    ParParent.association_reflection(:par_parent_two).keys.should_not include(:reciprocal)
    ParParent.association_reflection(:par_parent_two).reciprocal.should == :par_parents
    ParParentTwo.association_reflection(:par_parents).keys.should_not include(:reciprocal)
    ParParentTwo.association_reflection(:par_parents).reciprocal.should == :par_parent_two
    ParParent.association_reflection(:par_parent_threes).keys.should_not include(:reciprocal)
    ParParent.association_reflection(:par_parent_threes).reciprocal.should == :par_parents
    ParParentThree.association_reflection(:par_parents).keys.should_not include(:reciprocal)
    ParParentThree.association_reflection(:par_parents).reciprocal.should == :par_parent_threes
  end
end

describe Sequel::Model::Associations::AssociationReflection, "#select" do
  before do
    @c = Class.new(Sequel::Model(:foo))
    class ::ParParent < Sequel::Model; end
  end
  after do
    Object.send(:remove_const, :ParParent)
  end

  it "should use the :select value if present" do
    @c.many_to_one :c, :class=>ParParent, :select=>[:par_parents__id]
    @c.association_reflection(:c).keys.should include(:select)
    @c.association_reflection(:c).select.should == [:par_parents__id]
  end
  it "should be the associated_table.* if :select is not present for a many_to_many associaiton" do
    @c.many_to_many :cs, :class=>'ParParent'
    @c.association_reflection(:cs).keys.should_not include(:select)
    @c.association_reflection(:cs).select.should == :par_parents.*
  end
  it "should be if :select is not present for a many_to_one and one_to_many associaiton" do
    @c.one_to_many :cs, :class=>'ParParent'
    @c.association_reflection(:cs).keys.should_not include(:select)
    @c.association_reflection(:cs).select.should == nil
    @c.many_to_one :c, :class=>'ParParent'
    @c.association_reflection(:c).keys.should_not include(:select)
    @c.association_reflection(:c).select.should == nil
  end
end

describe Sequel::Model::Associations::AssociationReflection, "#can_have_associated_objects?" do
  it "should be true for any given object (for backward compatibility)" do
    Sequel::Model::Associations::AssociationReflection.new.can_have_associated_objects?(Object.new).should == true
  end
end

describe Sequel::Model::Associations::AssociationReflection, "#associated_object_keys" do
  before do
    @c = Class.new(Sequel::Model(:foo))
    class ::ParParent < Sequel::Model; end
  end
  after do
    Object.send(:remove_const, :ParParent)
  end

  it "should use the primary keys for a many_to_one association" do
    @c.many_to_one :c, :class=>ParParent
    @c.association_reflection(:c).associated_object_keys.should == [:id]
    @c.many_to_one :c, :class=>ParParent, :primary_key=>:d_id
    @c.association_reflection(:c).associated_object_keys.should == [:d_id]
    @c.many_to_one :c, :class=>ParParent, :key=>[:c_id1, :c_id2], :primary_key=>[:id1, :id2]
    @c.association_reflection(:c).associated_object_keys.should == [:id1, :id2]
  end
  it "should use the keys for a one_to_many association" do
    ParParent.one_to_many :cs, :class=>ParParent
    ParParent.association_reflection(:cs).associated_object_keys.should == [:par_parent_id]
    @c.one_to_many :cs, :class=>ParParent, :key=>:d_id
    @c.association_reflection(:cs).associated_object_keys.should == [:d_id]
    @c.one_to_many :cs, :class=>ParParent, :key=>[:c_id1, :c_id2], :primary_key=>[:id1, :id2]
    @c.association_reflection(:cs).associated_object_keys.should == [:c_id1, :c_id2]
  end
  it "should use the right primary keys for a many_to_many association" do
    @c.many_to_many :cs, :class=>ParParent
    @c.association_reflection(:cs).associated_object_keys.should == [:id]
    @c.many_to_many :cs, :class=>ParParent, :right_primary_key=>:d_id
    @c.association_reflection(:cs).associated_object_keys.should == [:d_id]
    @c.many_to_many :cs, :class=>ParParent, :right_key=>[:c_id1, :c_id2], :right_primary_key=>[:id1, :id2]
    @c.association_reflection(:cs).associated_object_keys.should == [:id1, :id2]
  end
end

describe Sequel::Model::Associations::AssociationReflection do
  before do
    @c = Class.new(Sequel::Model(:foo))
    @c.meta_def(:name){"C"}
  end

  it "#eager_loading_predicate_key should be an alias of predicate_key for backwards compatibility" do
    @c.one_to_many :cs, :class=>@c
    @c.dataset.literal(@c.association_reflection(:cs).eager_loading_predicate_key).should == 'foo.c_id'
  end

  it "one_to_many #qualified_primary_key should be a qualified version of the primary key" do
    @c.one_to_many :cs, :class=>@c
    @c.dataset.literal(@c.association_reflection(:cs).qualified_primary_key).should == 'foo.id'
  end

  it "many_to_many #associated_key_column should be the left key" do
    @c.many_to_many :cs, :class=>@c
    @c.association_reflection(:cs).associated_key_column.should == :c_id
  end

  it "many_to_many #qualified_right_key should be a qualified version of the primary key" do
    @c.many_to_many :cs, :class=>@c, :right_key=>:c2_id
    @c.dataset.literal(@c.association_reflection(:cs).qualified_right_key).should == 'cs_cs.c2_id'
  end

  it "many_to_many #qualified_right_primary_key should be a qualified version of the primary key" do
    @c.many_to_many :cs, :class=>@c
    @c.dataset.literal(@c.association_reflection(:cs).qualified_right_primary_key).should == 'foo.id'
  end
end

describe Sequel::Model::Associations::AssociationReflection, "#remove_before_destroy?" do
  before do
    @c = Class.new(Sequel::Model(:foo))
  end

  it "should be true for many_to_one and many_to_many associations" do
    @c.many_to_one :c, :class=>@c
    @c.association_reflection(:c).remove_before_destroy?.should be_true
    @c.many_to_many :cs, :class=>@c
    @c.association_reflection(:cs).remove_before_destroy?.should be_true
  end

  it "should be false for one_to_one and one_to_many associations" do
    @c.one_to_one :c, :class=>@c
    @c.association_reflection(:c).remove_before_destroy?.should be_false
    @c.one_to_many :cs, :class=>@c
    @c.association_reflection(:cs).remove_before_destroy?.should be_false
  end
end

describe Sequel::Model::Associations::AssociationReflection, "#eager_limit_strategy" do
  before do
    @c = Class.new(Sequel::Model(:a))
  end
  after do
    Sequel::Model.default_eager_limit_strategy = nil
  end

  it "should be nil by default for *_one associations" do
    @c.many_to_one :c, :class=>@c
    @c.association_reflection(:c).eager_limit_strategy.should be_nil
    @c.one_to_one :c, :class=>@c
    @c.association_reflection(:c).eager_limit_strategy.should be_nil
  end

  it "should be :ruby by default for *_many associations" do
    @c.one_to_many :cs, :class=>@c, :limit=>1
    @c.association_reflection(:cs).eager_limit_strategy.should == :ruby
    @c.many_to_many :cs, :class=>@c, :limit=>1
    @c.association_reflection(:cs).eager_limit_strategy.should == :ruby
  end

  it "should be nil for many_to_one associations" do
    @c.many_to_one :c, :class=>@c, :eager_limit_strategy=>true
    @c.association_reflection(:c).eager_limit_strategy.should be_nil
    @c.many_to_one :c, :class=>@c, :eager_limit_strategy=>:distinct_on
    @c.association_reflection(:c).eager_limit_strategy.should be_nil
  end

  it "should be a symbol for other associations if given a symbol" do
    @c.one_to_one :c, :class=>@c, :eager_limit_strategy=>:distinct_on
    @c.association_reflection(:c).eager_limit_strategy.should == :distinct_on
    @c.one_to_many :cs, :class=>@c, :eager_limit_strategy=>:window_function, :limit=>1
    @c.association_reflection(:cs).eager_limit_strategy.should == :window_function
  end

  it "should use :distinct_on for one_to_one associations if picking and the association dataset supports ordered distinct on" do
    @c.dataset.meta_def(:supports_ordered_distinct_on?){true}
    @c.one_to_one :c, :class=>@c, :eager_limit_strategy=>true
    @c.association_reflection(:c).eager_limit_strategy.should == :distinct_on
  end

  it "should use :window_function for associations if picking and the association dataset supports window functions" do
    @c.dataset.meta_def(:supports_window_functions?){true}
    @c.one_to_one :c, :class=>@c, :eager_limit_strategy=>true
    @c.association_reflection(:c).eager_limit_strategy.should == :window_function
    @c.one_to_many :cs, :class=>@c, :eager_limit_strategy=>true, :limit=>1
    @c.association_reflection(:cs).eager_limit_strategy.should == :window_function
    @c.many_to_many :cs, :class=>@c, :eager_limit_strategy=>true, :limit=>1
    @c.association_reflection(:cs).eager_limit_strategy.should == :window_function
  end

  it "should use :ruby for *_many associations if picking and the association dataset doesn't window functions" do
    @c.one_to_many :cs, :class=>@c, :eager_limit_strategy=>true, :limit=>1
    @c.association_reflection(:cs).eager_limit_strategy.should == :ruby
    @c.many_to_many :cs, :class=>@c, :eager_limit_strategy=>true, :limit=>1
    @c.association_reflection(:cs).eager_limit_strategy.should == :ruby
  end

  it "should respect Model.default_eager_limit_strategy to *_many associations" do
    Sequel::Model.default_eager_limit_strategy = :window_function
    Sequel::Model.default_eager_limit_strategy.should == :window_function
    c = Class.new(Sequel::Model)
    c.dataset = :a
    c.default_eager_limit_strategy.should == :window_function
    c.one_to_many :cs, :class=>c, :limit=>1
    c.association_reflection(:cs).eager_limit_strategy.should == :window_function
    c.many_to_many :cs, :class=>c, :limit=>1
    c.association_reflection(:cs).eager_limit_strategy.should == :window_function

    Sequel::Model.default_eager_limit_strategy = true
    c = Class.new(Sequel::Model)
    c.dataset = :a
    c.one_to_many :cs, :class=>c, :limit=>1
    c.association_reflection(:cs).eager_limit_strategy.should == :ruby
    c.dataset.meta_def(:supports_window_functions?){true}
    c.many_to_many :cs, :class=>c, :limit=>1
    c.association_reflection(:cs).eager_limit_strategy.should == :window_function

    c.default_eager_limit_strategy = :correlated_subquery
    c.many_to_many :cs, :class=>c, :limit=>1
    c.association_reflection(:cs).eager_limit_strategy.should == :correlated_subquery
  end

  it "should ignore Model.default_eager_limit_strategy for one_to_one associations" do
    @c.default_eager_limit_strategy = :correlated_subquery
    @c.one_to_one :c, :class=>@c
    @c.association_reflection(:c).eager_limit_strategy.should be_nil
  end
end

describe Sequel::Model, " association reflection methods" do
  before do
    @c1 = Class.new(Sequel::Model(:nodes)) do
      def self.name; 'Node'; end
      def self.to_s; 'Node'; end
    end
    MODEL_DB.reset
  end

  it "#all_association_reflections should include all association reflection hashes" do
    @c1.all_association_reflections.should == []

    @c1.associate :many_to_one, :parent, :class => @c1
    @c1.all_association_reflections.collect{|v| v[:name]}.should == [:parent]
    @c1.all_association_reflections.collect{|v| v[:type]}.should == [:many_to_one]
    @c1.all_association_reflections.collect{|v| v[:class]}.should == [@c1]

    @c1.associate :one_to_many, :children, :class => @c1
    @c1.all_association_reflections.sort_by{|x|x[:name].to_s}
    @c1.all_association_reflections.sort_by{|x|x[:name].to_s}.collect{|v| v[:name]}.should == [:children, :parent]
    @c1.all_association_reflections.sort_by{|x|x[:name].to_s}.collect{|v| v[:type]}.should == [:one_to_many, :many_to_one]
    @c1.all_association_reflections.sort_by{|x|x[:name].to_s}.collect{|v| v[:class]}.should == [@c1, @c1]
  end

  it "#association_reflection should return nil for nonexistent association" do
    @c1.association_reflection(:blah).should == nil
  end

  it "#association_reflection should return association reflection hash if association exists" do
    @c1.associate :many_to_one, :parent, :class => @c1
    @c1.association_reflection(:parent).should be_a_kind_of(Sequel::Model::Associations::AssociationReflection)
    @c1.association_reflection(:parent)[:name].should == :parent
    @c1.association_reflection(:parent)[:type].should == :many_to_one
    @c1.association_reflection(:parent)[:class].should == @c1

    @c1.associate :one_to_many, :children, :class => @c1
    @c1.association_reflection(:children).should be_a_kind_of(Sequel::Model::Associations::AssociationReflection)
    @c1.association_reflection(:children)[:name].should == :children
    @c1.association_reflection(:children)[:type].should == :one_to_many
    @c1.association_reflection(:children)[:class].should == @c1
  end

  it "#associations should include all association names" do
    @c1.associations.should == []
    @c1.associate :many_to_one, :parent, :class => @c1
    @c1.associations.should == [:parent]
    @c1.associate :one_to_many, :children, :class => @c1
    @c1.associations.sort_by{|x|x.to_s}.should == [:children, :parent]
  end

  it "association reflections should be copied upon subclasing" do
    @c1.associate :many_to_one, :parent, :class => @c1
    c = Class.new(@c1)
    @c1.associations.should == [:parent]
    c.associations.should == [:parent]
    c.associate :many_to_one, :parent2, :class => @c1
    @c1.associations.should == [:parent]
    c.associations.sort_by{|x| x.to_s}.should == [:parent, :parent2]
    c.instance_methods.map{|x| x.to_s}.should include('parent')
  end
end
