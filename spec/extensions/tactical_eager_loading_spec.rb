require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::TacticalEagerLoading" do
  before do
    class ::TaticalEagerLoadingModel < Sequel::Model
      plugin :tactical_eager_loading
      columns :id, :parent_id
      many_to_one :parent, :class=>self
      one_to_many :children, :class=>self, :key=>:parent_id
      dataset._fetch = proc do |sql|
        if sql !~ /WHERE/
          [{:id=>1, :parent_id=>101}, {:id=>2, :parent_id=>102}, {:id=>101, :parent_id=>nil}, {:id=>102, :parent_id=>nil}]
        elsif sql =~ /WHERE.*\bid IN \(([\d, ]*)\)/
          $1.split(', ').map{|x| {:id=>x.to_i, :parent_id=>nil}}
        elsif sql =~ /WHERE.*\bparent_id IN \(([\d, ]*)\)/
          $1.split(', ').map{|x| {:id=>x.to_i - 100, :parent_id=>x.to_i} if x.to_i > 100}.compact
        end
      end
    end
    @c = ::TaticalEagerLoadingModel
    @ds = TaticalEagerLoadingModel.dataset
    MODEL_DB.reset
  end
  after do
    Object.send(:remove_const, :TaticalEagerLoadingModel)
  end

  it "Dataset#all should set the retrieved_by and retrieved_with attributes" do
    ts = @c.all
    ts.map{|x| [x.retrieved_by, x.retrieved_with]}.should == [[@ds,ts], [@ds,ts], [@ds,ts], [@ds,ts]]
  end

  it "Dataset#all shouldn't raise an error if a Sequel::Model instance is not returned" do
    proc{@c.naked.all}.should_not raise_error
  end

  it "association getter methods should eagerly load the association if the association isn't cached" do
    MODEL_DB.sqls.length.should == 0
    ts = @c.all
    MODEL_DB.sqls.length.should == 1
    ts.map{|x| x.parent}.should == [ts[2], ts[3], nil, nil]
    MODEL_DB.sqls.length.should == 1
    ts.map{|x| x.children}.should == [[], [], [ts[0]], [ts[1]]]
    MODEL_DB.sqls.length.should == 1
  end

  it "association getter methods should not eagerly load the association if the association is cached" do
    MODEL_DB.sqls.length.should == 0
    ts = @c.all
    MODEL_DB.sqls.length.should == 1
    ts.map{|x| x.parent}.should == [ts[2], ts[3], nil, nil]
    @ds.should_not_receive(:eager_load)
    ts.map{|x| x.parent}.should == [ts[2], ts[3], nil, nil]
  end

  it "should handle case where an association is valid on an instance, but not on all instances" do
    c = Class.new(@c)
    c.many_to_one :parent2, :class=>@c, :key=>:parent_id
    @c.dataset.row_proc = proc{|r| (r[:parent_id] == 101 ? c : @c).call(r)}
    @c.all{|x| x.parent2 if x.is_a?(c)}
  end

end
