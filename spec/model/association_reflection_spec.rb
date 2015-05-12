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
    @c.association_reflection(:c).keys.must_include(:class)
    @c.association_reflection(:c).associated_class.must_equal ParParent
  end

  it "should figure out the class if the :class value is not present" do
    @c.many_to_one :c, :class=>'ParParent'
    @c.association_reflection(:c).keys.wont_include(:class)
    @c.association_reflection(:c).associated_class.must_equal ParParent
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
    @c.association_reflection(:c).keys.must_include(:primary_key)
    @c.association_reflection(:c).primary_key.must_equal :blah__blah
  end

  it "should use the associated table's primary key if :primary_key is not present" do
    @c.many_to_one :c, :class=>'ParParent'
    @c.association_reflection(:c).keys.wont_include(:primary_key)
    @c.association_reflection(:c).primary_key.must_equal :id
  end
end

describe Sequel::Model::Associations::AssociationReflection, "#reciprocal_type" do
  it "should include a specific type if only one matches" do
    c = Class.new(Sequel::Model(:a))
    c.one_to_many :cs, :class=>c, :key=>:c_id
    c.many_to_one :c, :class=>c, :key=>:c_id
    c.association_reflection(:c).send(:reciprocal_type).must_equal :one_to_many
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
    @c.association_reflection(:c).keys.must_include(:reciprocal)
    @c.association_reflection(:c).reciprocal.must_equal :xx
  end

  it "should not raise an error if some reciprocal associations have invalid associated classes" do
    @c = Class.new(Sequel::Model(:foo))
    @c.one_to_many :sadfakloasdfioas
    @c.many_to_one :c, :class=>@c
    @c.association_reflection(:c).reciprocal
  end

  it "should require the associated class is the current class to be a reciprocal" do
    ParParent.many_to_one :par_parent_two, :key=>:blah
    ParParent.many_to_one :par_parent_three, :key=>:blah
    ParParentTwo.one_to_many :par_parents, :key=>:blah
    ParParentThree.one_to_many :par_parents, :key=>:blah

    ParParentTwo.association_reflection(:par_parents).reciprocal.must_equal :par_parent_two
    ParParentThree.association_reflection(:par_parents).reciprocal.must_equal :par_parent_three

    ParParent.many_to_many :par_parent_twos, :left_key=>:l, :right_key=>:r, :join_table=>:jt
    ParParent.many_to_many :par_parent_threes, :left_key=>:l, :right_key=>:r, :join_table=>:jt
    ParParentTwo.many_to_many :par_parents, :right_key=>:l, :left_key=>:r, :join_table=>:jt
    ParParentThree.many_to_many :par_parents, :right_key=>:l, :left_key=>:r, :join_table=>:jt

    ParParentTwo.association_reflection(:par_parents).reciprocal.must_equal :par_parent_twos
    ParParentThree.association_reflection(:par_parents).reciprocal.must_equal :par_parent_threes
  end
  
  it "should handle composite keys" do
    ParParent.many_to_one :par_parent_two, :key=>[:a, :b], :primary_key=>[:c, :b]
    ParParent.many_to_one :par_parent_three, :key=>[:d, :e], :primary_key=>[:c, :b]
    ParParentTwo.one_to_many :par_parents, :primary_key=>[:c, :b], :key=>[:a, :b]
    ParParentThree.one_to_many :par_parents, :primary_key=>[:c, :b], :key=>[:d, :e]

    ParParentTwo.association_reflection(:par_parents).reciprocal.must_equal :par_parent_two
    ParParentThree.association_reflection(:par_parents).reciprocal.must_equal :par_parent_three

    ParParent.many_to_many :par_parent_twos, :left_key=>[:l1, :l2], :right_key=>[:r1, :r2], :left_primary_key=>[:pl1, :pl2], :right_primary_key=>[:pr1, :pr2], :join_table=>:jt
    ParParent.many_to_many :par_parent_threes, :right_key=>[:l1, :l2], :left_key=>[:r1, :r2], :left_primary_key=>[:pl1, :pl2], :right_primary_key=>[:pr1, :pr2], :join_table=>:jt
    ParParentTwo.many_to_many :par_parents, :right_key=>[:l1, :l2], :left_key=>[:r1, :r2], :right_primary_key=>[:pl1, :pl2], :left_primary_key=>[:pr1, :pr2], :join_table=>:jt
    ParParentThree.many_to_many :par_parents, :left_key=>[:l1, :l2], :right_key=>[:r1, :r2], :right_primary_key=>[:pl1, :pl2], :left_primary_key=>[:pr1, :pr2], :join_table=>:jt

    ParParentTwo.association_reflection(:par_parents).reciprocal.must_equal :par_parent_twos
    ParParentThree.association_reflection(:par_parents).reciprocal.must_equal :par_parent_threes
  end

  it "should figure out the reciprocal if the :reciprocal value is not present" do
    ParParent.many_to_one :par_parent_two
    ParParentTwo.one_to_many :par_parents
    ParParent.many_to_many :par_parent_threes
    ParParentThree.many_to_many :par_parents

    ParParent.association_reflection(:par_parent_two).keys.wont_include(:reciprocal)
    ParParent.association_reflection(:par_parent_two).reciprocal.must_equal :par_parents
    ParParentTwo.association_reflection(:par_parents).keys.wont_include(:reciprocal)
    ParParentTwo.association_reflection(:par_parents).reciprocal.must_equal :par_parent_two
    ParParent.association_reflection(:par_parent_threes).keys.wont_include(:reciprocal)
    ParParent.association_reflection(:par_parent_threes).reciprocal.must_equal :par_parents
    ParParentThree.association_reflection(:par_parents).keys.wont_include(:reciprocal)
    ParParentThree.association_reflection(:par_parents).reciprocal.must_equal :par_parent_threes
  end

  it "should handle ambiguous reciprocals where only one doesn't have conditions/blocks" do
    ParParent.many_to_one :par_parent_two, :class=>ParParentTwo, :key=>:par_parent_two_id
    ParParent.many_to_one :par_parent_two2, :clone=>:par_parent_two, :conditions=>{:id=>:id}
    ParParentTwo.one_to_many :par_parents
    ParParent.many_to_many :par_parent_threes, :class=>ParParentThree, :right_key=>:par_parent_three_id
    ParParent.many_to_many :par_parent_threes2, :clone=>:par_parent_threes do |ds|
      ds
    end
    ParParentThree.many_to_many :par_parents

    ParParentTwo.association_reflection(:par_parents).reciprocal.must_equal :par_parent_two
    ParParentThree.association_reflection(:par_parents).reciprocal.must_equal :par_parent_threes
  end

  it "should handle ambiguous reciprocals where only one has matching primary keys" do
    ParParent.many_to_one :par_parent_two, :class=>ParParentTwo, :key=>:par_parent_two_id
    ParParent.many_to_one :par_parent_two2, :clone=>:par_parent_two, :primary_key=>:foo
    ParParentTwo.one_to_many :par_parents, :class=>ParParent, :key=>:par_parent_two_id
    ParParentTwo.one_to_many :par_parents2, :clone=>:par_parents, :primary_key=>:foo
    ParParent.many_to_many :par_parent_threes, :class=>ParParentThree, :right_key=>:par_parent_three_id
    ParParent.many_to_many :par_parent_threes2, :clone=>:par_parent_threes, :right_primary_key=>:foo
    ParParent.many_to_many :par_parent_threes3, :clone=>:par_parent_threes, :left_primary_key=>:foo
    ParParentThree.many_to_many :par_parents

    ParParent.association_reflection(:par_parent_two).reciprocal.must_equal :par_parents
    ParParent.association_reflection(:par_parent_two2).reciprocal.must_equal :par_parents2
    ParParentTwo.association_reflection(:par_parents).reciprocal.must_equal :par_parent_two
    ParParentTwo.association_reflection(:par_parents2).reciprocal.must_equal :par_parent_two2
    ParParentThree.association_reflection(:par_parents).reciprocal.must_equal :par_parent_threes
  end

  it "should handle reciprocals where current association has conditions/block" do
    ParParent.many_to_one :par_parent_two, :conditions=>{:id=>:id}
    ParParentTwo.one_to_many :par_parents
    ParParent.many_to_many :par_parent_threes do |ds|
      ds
    end
    ParParentThree.many_to_many :par_parents

    ParParent.association_reflection(:par_parent_two).reciprocal.must_equal :par_parents
    ParParent.association_reflection(:par_parent_threes).reciprocal.must_equal :par_parents
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
    @c.association_reflection(:c).keys.must_include(:select)
    @c.association_reflection(:c).select.must_equal [:par_parents__id]
  end
  it "should be the associated_table.* if :select is not present for a many_to_many associaiton" do
    @c.many_to_many :cs, :class=>'ParParent'
    @c.association_reflection(:cs).keys.wont_include(:select)
    @c.association_reflection(:cs).select.must_equal Sequel::SQL::ColumnAll.new(:par_parents)
  end
  it "should be blank if :select is not present for a many_to_one and one_to_many associaiton" do
    @c.one_to_many :cs, :class=>'ParParent'
    @c.association_reflection(:cs).keys.wont_include(:select)
    @c.association_reflection(:cs).select.must_equal nil
    @c.many_to_one :c, :class=>'ParParent'
    @c.association_reflection(:c).keys.wont_include(:select)
    @c.association_reflection(:c).select.must_equal nil
  end
end

describe Sequel::Model::Associations::AssociationReflection, "#can_have_associated_objects?" do
  it "should be true for any given object (for backward compatibility)" do
    Sequel::Model::Associations::AssociationReflection.new.can_have_associated_objects?(Object.new).must_equal true
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
    @c.association_reflection(:c).associated_object_keys.must_equal [:id]
    @c.many_to_one :c, :class=>ParParent, :primary_key=>:d_id
    @c.association_reflection(:c).associated_object_keys.must_equal [:d_id]
    @c.many_to_one :c, :class=>ParParent, :key=>[:c_id1, :c_id2], :primary_key=>[:id1, :id2]
    @c.association_reflection(:c).associated_object_keys.must_equal [:id1, :id2]
  end
  it "should use the keys for a one_to_many association" do
    ParParent.one_to_many :cs, :class=>ParParent
    ParParent.association_reflection(:cs).associated_object_keys.must_equal [:par_parent_id]
    @c.one_to_many :cs, :class=>ParParent, :key=>:d_id
    @c.association_reflection(:cs).associated_object_keys.must_equal [:d_id]
    @c.one_to_many :cs, :class=>ParParent, :key=>[:c_id1, :c_id2], :primary_key=>[:id1, :id2]
    @c.association_reflection(:cs).associated_object_keys.must_equal [:c_id1, :c_id2]
  end
  it "should use the right primary keys for a many_to_many association" do
    @c.many_to_many :cs, :class=>ParParent
    @c.association_reflection(:cs).associated_object_keys.must_equal [:id]
    @c.many_to_many :cs, :class=>ParParent, :right_primary_key=>:d_id
    @c.association_reflection(:cs).associated_object_keys.must_equal [:d_id]
    @c.many_to_many :cs, :class=>ParParent, :right_key=>[:c_id1, :c_id2], :right_primary_key=>[:id1, :id2]
    @c.association_reflection(:cs).associated_object_keys.must_equal [:id1, :id2]
  end
end

describe Sequel::Model::Associations::AssociationReflection do
  before do
    @c = Class.new(Sequel::Model(:foo))
    def @c.name() "C" end
  end

  it "#eager_loading_predicate_key should be an alias of predicate_key for backwards compatibility" do
    @c.one_to_many :cs, :class=>@c
    @c.dataset.literal(@c.association_reflection(:cs).eager_loading_predicate_key).must_equal 'foo.c_id'
  end

  it "one_to_many #qualified_primary_key should be a qualified version of the primary key" do
    @c.one_to_many :cs, :class=>@c
    @c.dataset.literal(@c.association_reflection(:cs).qualified_primary_key).must_equal 'foo.id'
  end

  it "many_to_many #associated_key_column should be the left key" do
    @c.many_to_many :cs, :class=>@c
    @c.association_reflection(:cs).associated_key_column.must_equal :c_id
  end

  it "many_to_many #qualified_right_key should be a qualified version of the primary key" do
    @c.many_to_many :cs, :class=>@c, :right_key=>:c2_id
    @c.dataset.literal(@c.association_reflection(:cs).qualified_right_key).must_equal 'cs_cs.c2_id'
  end

  it "many_to_many #qualified_right_primary_key should be a qualified version of the primary key" do
    @c.many_to_many :cs, :class=>@c
    @c.dataset.literal(@c.association_reflection(:cs).qualified_right_primary_key).must_equal 'foo.id'
  end
end

describe Sequel::Model::Associations::AssociationReflection, "#remove_before_destroy?" do
  before do
    @c = Class.new(Sequel::Model(:foo))
  end

  it "should be true for many_to_one and many_to_many associations" do
    @c.many_to_one :c, :class=>@c
    @c.association_reflection(:c).remove_before_destroy?.must_equal true
    @c.many_to_many :cs, :class=>@c
    @c.association_reflection(:cs).remove_before_destroy?.must_equal true
  end

  it "should be false for one_to_one and one_to_many associations" do
    @c.one_to_one :c, :class=>@c
    @c.association_reflection(:c).remove_before_destroy?.must_equal false
    @c.one_to_many :cs, :class=>@c
    @c.association_reflection(:cs).remove_before_destroy?.must_equal false
  end
end

describe Sequel::Model::Associations::AssociationReflection, "#filter_by_associations_limit_strategy" do
  before do
    @db = Sequel.mock
    @c = Class.new(Sequel::Model(@db[:a]))
  end
  after do
    Sequel::Model.default_eager_limit_strategy = true
  end

  it "should be nil by default for *_one associations" do
    @c.many_to_one :c, :class=>@c
    @c.association_reflection(:c).send(:filter_by_associations_limit_strategy).must_equal nil
    @c.one_to_one :c, :class=>@c
    @c.association_reflection(:c).send(:filter_by_associations_limit_strategy).must_equal nil
    @c.one_through_one :c, :class=>@c
    @c.association_reflection(:c).send(:filter_by_associations_limit_strategy).must_equal nil
  end

  it "should be :correlated_subquery by default for one_to_many and one_to_one with :order associations" do
    @c.one_to_one :c, :class=>@c, :order=>:a
    @c.association_reflection(:c).send(:filter_by_associations_limit_strategy).must_equal :correlated_subquery
    @c.one_to_many :cs, :class=>@c, :limit=>1
    @c.association_reflection(:cs).send(:filter_by_associations_limit_strategy).must_equal :correlated_subquery
  end

  it "should be :ruby by default for many_to_many and one_through_one with :order associations" do
    @c.one_through_one :c, :class=>@c, :order=>:a
    @c.association_reflection(:c).send(:filter_by_associations_limit_strategy).must_equal :ruby
    @c.many_to_many :cs, :class=>@c, :limit=>1
    @c.association_reflection(:cs).send(:filter_by_associations_limit_strategy).must_equal :ruby
  end

  it "should be nil for many_to_one associations even if :eager_limit_strategy or :filter_limit_strategy is used" do
    @c.many_to_one :c, :class=>@c, :eager_limit_strategy=>true
    @c.association_reflection(:c).send(:filter_by_associations_limit_strategy).must_equal nil
    @c.many_to_one :c, :class=>@c, :eager_limit_strategy=>:distinct_on
    @c.association_reflection(:c).send(:filter_by_associations_limit_strategy).must_equal nil
    @c.many_to_one :c, :class=>@c, :filter_limit_strategy=>true
    @c.association_reflection(:c).send(:filter_by_associations_limit_strategy).must_equal nil
  end

  it "should be a symbol for other associations if given a symbol" do
    @c.one_to_one :c, :class=>@c, :eager_limit_strategy=>:distinct_on
    @c.association_reflection(:c).send(:filter_by_associations_limit_strategy).must_equal :distinct_on
    @c.one_to_many :cs, :class=>@c, :eager_limit_strategy=>:window_function, :limit=>1
    @c.association_reflection(:cs).send(:filter_by_associations_limit_strategy).must_equal :window_function
  end

  it "should use :distinct_on for one_to_one associations if picking and the association dataset supports ordered distinct on" do
    def (@c.dataset).supports_ordered_distinct_on?() true end
    @c.one_to_one :c, :class=>@c, :eager_limit_strategy=>true
    @c.association_reflection(:c).send(:filter_by_associations_limit_strategy).must_equal :distinct_on
  end

  it "should use :window_function for associations if picking and the association dataset supports window functions" do
    def (@c.dataset).supports_window_functions?() true end
    @c.one_to_one :c, :class=>@c, :eager_limit_strategy=>true
    @c.association_reflection(:c).send(:filter_by_associations_limit_strategy).must_equal :window_function
    @c.one_to_many :cs, :class=>@c, :eager_limit_strategy=>true, :limit=>1
    @c.association_reflection(:cs).send(:filter_by_associations_limit_strategy).must_equal :window_function
    @c.many_to_many :cs, :class=>@c, :eager_limit_strategy=>true, :limit=>1
    @c.association_reflection(:cs).send(:filter_by_associations_limit_strategy).must_equal :window_function
  end

  it "should use :ruby for one_to_many associations if the database doesn't support limits in subqueries" do
    def (@c.dataset).supports_limits_in_correlated_subqueries?; false; end
    @c.one_to_many :cs, :class=>@c, :eager_limit_strategy=>true, :limit=>1
    @c.association_reflection(:cs).send(:filter_by_associations_limit_strategy).must_equal :ruby
  end

  it "should use :ruby for one_to_many associations if offset doesn't work in correlated subqueries and an offset is used" do
    def (@c.dataset).supports_offsets_in_correlated_subqueries?; false; end
    @c.one_to_many :cs, :class=>@c, :eager_limit_strategy=>true, :limit=>1
    @c.association_reflection(:cs).send(:filter_by_associations_limit_strategy).must_equal :correlated_subquery
    @c.one_to_many :cs, :class=>@c, :eager_limit_strategy=>true, :limit=>[1, 1]
    @c.association_reflection(:cs).send(:filter_by_associations_limit_strategy).must_equal :ruby
  end

  it "should use :ruby for one_to_many associations if composite primary key is used and database does not multiple columns in IN" do
    def (@c.dataset).supports_multiple_column_in?; false; end
    @c.set_primary_key [:id, :id2]
    @c.one_to_many :cs, :class=>@c, :eager_limit_strategy=>true, :limit=>1, :key=>[:id, :id2]
    @c.association_reflection(:cs).send(:filter_by_associations_limit_strategy).must_equal :ruby
  end

  it "should use :ruby for many_to_many associations if picking and the association dataset doesn't window functions" do
    @c.many_to_many :cs, :class=>@c, :eager_limit_strategy=>true, :limit=>1
    @c.association_reflection(:cs).send(:filter_by_associations_limit_strategy).must_equal :ruby
  end

  it "should respect Model.default_eager_limit_strategy to *_many associations" do
    Sequel::Model.default_eager_limit_strategy = :window_function
    Sequel::Model.default_eager_limit_strategy.must_equal :window_function
    c = Class.new(Sequel::Model)
    c.dataset = :a
    c.default_eager_limit_strategy.must_equal :window_function
    c.one_to_many :cs, :class=>c, :limit=>1
    c.association_reflection(:cs).send(:filter_by_associations_limit_strategy).must_equal :window_function
    c.many_to_many :cs, :class=>c, :limit=>1
    c.association_reflection(:cs).send(:filter_by_associations_limit_strategy).must_equal :window_function

    Sequel::Model.default_eager_limit_strategy = true
    c = Class.new(Sequel::Model)
    c.dataset = :a
    c.one_to_many :cs, :class=>c, :limit=>1
    c.association_reflection(:cs).send(:filter_by_associations_limit_strategy).must_equal :correlated_subquery
    def (c.dataset).supports_window_functions?() true end
    c.many_to_many :cs, :class=>c, :limit=>1
    c.association_reflection(:cs).send(:filter_by_associations_limit_strategy).must_equal :window_function
  end

  it "should ignore Model.default_eager_limit_strategy for one_to_one associations" do
    @c.default_eager_limit_strategy = :window_function
    @c.one_to_one :c, :class=>@c
    @c.association_reflection(:c).send(:filter_by_associations_limit_strategy).must_equal nil
  end
end

describe Sequel::Model::Associations::AssociationReflection, "#apply_eager_dataset_changes" do
  it "should apply the eager block as well as the association options to the dataset" do
    @c = Class.new(Sequel::Model(:foo))
    @c.one_to_many :cs, :class=>@c, :select=>:a, :order=>:b do |ds| ds.where(:c) end
    @c.association_reflection(:cs).apply_eager_dataset_changes(@c.dataset).sql.must_equal 'SELECT a FROM foo WHERE c ORDER BY b'
  end
end

describe Sequel::Model, " association reflection methods" do
  before do
    @c1 = Class.new(Sequel::Model(:nodes)) do
      def self.name; 'Node'; end
      def self.to_s; 'Node'; end
    end
    DB.reset
  end
  
  it "#all_association_reflections should include all association reflection hashes" do
    @c1.all_association_reflections.must_equal []

    @c1.associate :many_to_one, :parent, :class => @c1
    @c1.all_association_reflections.collect{|v| v[:name]}.must_equal [:parent]
    @c1.all_association_reflections.collect{|v| v[:type]}.must_equal [:many_to_one]
    @c1.all_association_reflections.collect{|v| v[:class]}.must_equal [@c1]

    @c1.associate :one_to_many, :children, :class => @c1
    @c1.all_association_reflections.sort_by{|x|x[:name].to_s}
    @c1.all_association_reflections.sort_by{|x|x[:name].to_s}.collect{|v| v[:name]}.must_equal [:children, :parent]
    @c1.all_association_reflections.sort_by{|x|x[:name].to_s}.collect{|v| v[:type]}.must_equal [:one_to_many, :many_to_one]
    @c1.all_association_reflections.sort_by{|x|x[:name].to_s}.collect{|v| v[:class]}.must_equal [@c1, @c1]
  end

  it "#association_reflection should return nil for nonexistent association" do
    @c1.association_reflection(:blah).must_equal nil
  end

  it "#association_reflection should return association reflection hash if association exists" do
    @c1.associate :many_to_one, :parent, :class => @c1
    @c1.association_reflection(:parent).must_be_kind_of(Sequel::Model::Associations::AssociationReflection)
    @c1.association_reflection(:parent)[:name].must_equal :parent
    @c1.association_reflection(:parent)[:type].must_equal :many_to_one
    @c1.association_reflection(:parent)[:class].must_equal @c1

    @c1.associate :one_to_many, :children, :class => @c1
    @c1.association_reflection(:children).must_be_kind_of(Sequel::Model::Associations::AssociationReflection)
    @c1.association_reflection(:children)[:name].must_equal :children
    @c1.association_reflection(:children)[:type].must_equal :one_to_many
    @c1.association_reflection(:children)[:class].must_equal @c1
  end

  it "#associations should include all association names" do
    @c1.associations.must_equal []
    @c1.associate :many_to_one, :parent, :class => @c1
    @c1.associations.must_equal [:parent]
    @c1.associate :one_to_many, :children, :class => @c1
    @c1.associations.sort_by{|x|x.to_s}.must_equal [:children, :parent]
  end

  it "association reflections should be copied upon subclasing" do
    @c1.associate :many_to_one, :parent, :class => @c1
    c = Class.new(@c1)
    @c1.associations.must_equal [:parent]
    c.associations.must_equal [:parent]
    c.associate :many_to_one, :parent2, :class => @c1
    @c1.associations.must_equal [:parent]
    c.associations.sort_by{|x| x.to_s}.must_equal [:parent, :parent2]
    c.instance_methods.map{|x| x.to_s}.must_include('parent')
  end
end

describe Sequel::Model::Associations::AssociationReflection, "with caching disabled" do
  before do
    @db = Sequel.mock
    @c = Class.new(Sequel::Model)
    @c.dataset = @db[:foo]
    @c.cache_associations = false
  end

  it "should not cache metadata" do
    begin
      class ::ParParent < Sequel::Model; end
      c = ParParent
      @c.many_to_one :c, :class=>:ParParent
      @c.association_reflection(:c).associated_class.must_equal c
      Object.send(:remove_const, :ParParent)
      class ::ParParent < Sequel::Model; end
      c = ParParent
      @c.association_reflection(:c).associated_class.must_equal c
    ensure
      Object.send(:remove_const, :ParParent)
    end
  end

  it "should not used cached schema" do
    def @db.supports_schema_parsing?; true end
    def @db.schema(table, opts={})
      [[opts[:reload] ? :reload : :id, {}]]
    end
    @c.dataset = @db[:items]
    @c.columns.must_equal [:reload]

    @c.cache_associations = true
    @c.dataset = @db[:items]
    @c.columns.must_equal [:id]
  end
end

