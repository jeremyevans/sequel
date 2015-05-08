require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::TacticalEagerLoading" do
  before do
    class ::TacticalEagerLoadingModel < Sequel::Model
      plugin :tactical_eager_loading
      columns :id, :parent_id
      many_to_one :parent, :class=>self
      one_to_many :children, :class=>self, :key=>:parent_id
      dataset._fetch = proc do |sql|
        if sql !~ /WHERE/
          [{:id=>1, :parent_id=>101}, {:id=>2, :parent_id=>102}, {:id=>101, :parent_id=>nil}, {:id=>102, :parent_id=>nil}]
        elsif sql =~ /WHERE.*\bid = (\d+)/
          [{:id=>$1.to_i, :parent_id=>nil}]
        elsif sql =~ /WHERE.*\bid IN \(([\d, ]*)\)/
          $1.split(', ').map{|x| {:id=>x.to_i, :parent_id=>nil}}
        elsif sql =~ /WHERE.*\bparent_id IN \(([\d, ]*)\)/
          $1.split(', ').map{|x| {:id=>x.to_i - 100, :parent_id=>x.to_i} if x.to_i > 100}.compact
        end
      end
    end
    @c = ::TacticalEagerLoadingModel
    @ds = TacticalEagerLoadingModel.dataset
    DB.reset
  end
  after do
    Object.send(:remove_const, :TacticalEagerLoadingModel)
  end

  it "Dataset#all should set the retrieved_by and retrieved_with attributes" do
    ts = @c.all
    ts.map{|x| [x.retrieved_by, x.retrieved_with]}.must_equal [[@ds,ts], [@ds,ts], [@ds,ts], [@ds,ts]]
  end

  it "Dataset#all shouldn't raise an error if a Sequel::Model instance is not returned" do
    @c.naked.all
  end

  it "association getter methods should eagerly load the association if the association isn't cached" do
    DB.sqls.length.must_equal 0
    ts = @c.all
    DB.sqls.length.must_equal 1
    ts.map{|x| x.parent}.must_equal [ts[2], ts[3], nil, nil]
    DB.sqls.length.must_equal 1
    ts.map{|x| x.children}.must_equal [[], [], [ts[0]], [ts[1]]]
    DB.sqls.length.must_equal 1
  end

  it "association getter methods should not eagerly load the association if the association is cached" do
    DB.sqls.length.must_equal 0
    ts = @c.all
    DB.sqls.length.must_equal 1
    ts.map{|x| x.parent}.must_equal [ts[2], ts[3], nil, nil]
    def @ds.eager_load(*) raise end
    ts.map{|x| x.parent}.must_equal [ts[2], ts[3], nil, nil]
  end

  it "should handle case where an association is valid on an instance, but not on all instances" do
    c = Class.new(@c)
    c.many_to_one :parent2, :class=>@c, :key=>:parent_id
    @c.dataset.row_proc = proc{|r| (r[:parent_id] == 101 ? c : @c).call(r)}
    @c.all{|x| x.parent2 if x.is_a?(c)}
  end

  it "association getter methods should not eagerly load the association if an instance is frozen" do
    ts = @c.all
    ts.first.freeze
    DB.sqls.length.must_equal 1
    ts.map{|x| x.parent}.must_equal [ts[2], ts[3], nil, nil]
    DB.sqls.length.must_equal 2
    ts.map{|x| x.children}.must_equal [[], [], [ts[0]], [ts[1]]]
    DB.sqls.length.must_equal 2
    ts.map{|x| x.parent}.must_equal [ts[2], ts[3], nil, nil]
    DB.sqls.length.must_equal 1
    ts.map{|x| x.children}.must_equal [[], [], [ts[0]], [ts[1]]]
    DB.sqls.length.must_equal 1
  end

  it "#marshallable should make marshalling not fail" do
    Marshal.dump(@c.all.map{|x| x.marshallable!})
  end
end
