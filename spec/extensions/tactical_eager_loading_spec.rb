require File.join(File.dirname(__FILE__), "spec_helper")

describe "Sequel::Plugins::TacticalEagerLoading" do
  before do
    class ::TaticalEagerLoadingModel < Sequel::Model
      plugin :tactical_eager_loading
      columns :id, :parent_id
      many_to_one :parent, :class=>self
      one_to_many :children, :class=>self, :key=>:parent_id
      ds = dataset
      def ds.fetch_rows(sql)
        execute(sql)
        where = @opts[:where]
        if !where
          yield(:id=>1, :parent_id=>101)
          yield(:id=>2, :parent_id=>102)
          yield(:id=>101, :parent_id=>nil)
          yield(:id=>102, :parent_id=>nil)
        elsif where.args.first.column == :id
          Array(where.args.last).each do |x|
            yield(:id=>x, :parent_id=>nil)
          end
        elsif where.args.first.column == :parent_id
          Array(where.args.last).each do |x|
            yield(:id=>x-100, :parent_id=>x) if x > 100
          end
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

  it "Dataset#all should set the retrieved_by and reteived_with attributes" do
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
    MODEL_DB.sqls.length.should == 2
    ts.map{|x| x.children}.should == [[], [], [ts[0]], [ts[1]]]
    MODEL_DB.sqls.length.should == 3
  end

  it "association getter methods should not eagerly load the association if the association is cached" do
    MODEL_DB.sqls.length.should == 0
    ts = @c.all
    MODEL_DB.sqls.length.should == 1
    ts.map{|x| x.parent}.should == [ts[2], ts[3], nil, nil]
    @ds.should_not_receive(:eager_load)
    ts.map{|x| x.parent}.should == [ts[2], ts[3], nil, nil]
  end
end
