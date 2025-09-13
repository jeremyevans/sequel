require_relative "spec_helper"

describe "deprecated_associations plugin" do
  before do
    @db = Sequel.mock(:autoid=>1, :fetch=>[{:id=>1, :c_id=>2}], :numrows=>1)
    @c = Class.new(Sequel::Model(@db[:c]))
    def @c.name; :C end
    @c.columns :id, :c_id
    @c.plugin :deprecated_associations
    @warnings = warnings = []
    @c.define_singleton_method(:warn){|a,*| warnings << a}
    @o = @c.load(:id=>1, :c_id=>2)
  end

  it "does not show warnings for non-deprecated singular associations" do
    @c.many_to_one :c, :class => @c
    @c.association_reflection(:c)
    @o.c
    @o.c_dataset
    @o.c = nil
    @warnings.must_equal []
  end

  it "does not show warnings for non-deprecated plural associations" do
    @c.one_to_many :cs, :key => :c_id, :class => @c
    @c.association_reflection(:cs)
    @o.cs
    @o.cs_dataset
    o = @c.create
    @o.add_c(o)
    @o.remove_c(o)
    @o.remove_all_cs
    @warnings.must_equal []
  end

  it "shows warnings for deprecated singular associations" do
    @c.many_to_one :c, :class => @c, :deprecated => true
    2.times do
      @c.association_reflection(:c)
      @c.eager(:c)
      @c.eager_graph(:c)
      @c.where(:c=>@o).sql
      @o.c
      @o.c_dataset
      @o.c = nil
    end
    @warnings.must_equal [
      "Access of association reflection for deprecated association: class:C association:c",
      "Calling deprecated association method: class:C association:c method:c",
      "Calling deprecated association method: class:C association:c method:c_dataset",
      "Calling deprecated association method: class:C association:c method:c="
    ]
  end

  it "shows warnings for deprecated plural associations" do
    @c.one_to_many :cs, :key => :c_id, :class => @c, :deprecated => true
    o = @c.create
    2.times do
      @c.association_reflection(:cs)
      @c.eager(:cs)
      @c.eager_graph(:cs)
      @c.where(:cs=>@o).sql
      @o.cs
      @o.cs_dataset
      @o.add_c(o)
      @o.remove_c(o)
      @o.remove_all_cs
    end
    @warnings.must_equal [
      "Access of association reflection for deprecated association: class:C association:cs",
      "Calling deprecated association method: class:C association:cs method:cs",
      "Calling deprecated association method: class:C association:cs method:cs_dataset",
      "Calling deprecated association method: class:C association:cs method:add_c",
      "Calling deprecated association method: class:C association:cs method:remove_c",
      "Calling deprecated association method: class:C association:cs method:remove_all_cs"
    ]
  end

  it "does not deduplicate warnings when not caching associations" do
    @c.cache_associations = false
    @c.many_to_one :c, :class => @c, :deprecated => true
    2.times do
      @c.association_reflection(:c)
      @c.eager(:c)
      @c.eager_graph(:c)
      @c.where(:c=>@o).sql
      @o.c
      @o.c_dataset
      @o.c = nil
    end
    @warnings.must_equal [
      "Access of association reflection for deprecated association: class:C association:c",
      "Access of association reflection for deprecated association: class:C association:c",
      "Access of association reflection for deprecated association: class:C association:c",
      "Access of association reflection for deprecated association: class:C association:c",
      "Calling deprecated association method: class:C association:c method:c",
      "Calling deprecated association method: class:C association:c method:c_dataset",
      "Calling deprecated association method: class:C association:c method:c="
    ] * 2
  end

  it "does not deduplicate warnings when using deduplicate: false plugin option" do
    @c.plugin :deprecated_associations, :deduplicate => false
    @c.one_to_many :cs, :key => :c_id, :class => @c, :deprecated => true
    o = @c.create
    2.times do
      @c.association_reflection(:cs)
      @c.eager(:cs)
      @c.eager_graph(:cs)
      @c.where(:cs=>@o).sql
      @o.cs
      @o.cs_dataset
      @o.add_c(o)
      @o.remove_c(o)
      @o.remove_all_cs
    end
    @warnings.must_equal [
      "Access of association reflection for deprecated association: class:C association:cs",
      "Access of association reflection for deprecated association: class:C association:cs",
      "Access of association reflection for deprecated association: class:C association:cs",
      "Access of association reflection for deprecated association: class:C association:cs",
      "Calling deprecated association method: class:C association:cs method:cs",
      "Calling deprecated association method: class:C association:cs method:cs_dataset",
      "Calling deprecated association method: class:C association:cs method:add_c",
      "Calling deprecated association method: class:C association:cs method:remove_c",
      "Calling deprecated association method: class:C association:cs method:cs_dataset",
      "Calling deprecated association method: class:C association:cs method:remove_all_cs"
    ] * 2
  end

  it "includes backtrace in warning when using :backtrace plugin option" do
    @c.plugin :deprecated_associations, :backtrace => true
    @c.many_to_one :c, :class => @c, :deprecated => true
    warnings = nil
    @c.singleton_class.send(:remove_method, :warn)
    @c.define_singleton_method(:warn){|*a| warnings = a}
    line = __LINE__ + 1
    @c.association_reflection(:c)
    warnings[0].must_equal "Access of association reflection for deprecated association: class:C association:c"
    warnings[1][0].must_include "#{__FILE__}:#{line}"
  end

  it "raises for deprecated association when using :raise plugin option" do
    @c.plugin :deprecated_associations, :raise => true
    @c.many_to_one :c, :class => @c, :deprecated => true
    proc{@c.association_reflection(:c)}.must_raise Sequel::Plugins::DeprecatedAssociations::Access
  end
end
