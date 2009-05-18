require File.join(File.dirname(__FILE__), "spec_helper")

describe "Sequel::Plugins::TacticalEagerLoading" do
  before do
    class ::TaticalEagerLoadingModel < Sequel::Model
      plugin :identity_map
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
          yield(:id=>2, :parent_id=>201)
          yield(:id=>101, :parent_id=>nil)
          yield(:id=>201, :parent_id=>nil)
        else
          Array(where.args.last).each do |x|
            yield(:id=>x, :parent_id=>100+x) if x
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

  it "Dataset#all should set the retreived_by and reteived_with attributes if there is an active identity map" do
    ts = TaticalEagerLoadingModel.all
    ts.map{|x| [x.retreived_by, x.retreived_with]}.should == [[nil,nil], [nil,nil], [nil,nil], [nil,nil]]
  end

  it "Dataset#all should not set the retreived_by and reteived_with attributes if there is no active identity map" do
    TaticalEagerLoadingModel.with_identity_map do
      ts = TaticalEagerLoadingModel.all
      ts.map{|x| [x.retreived_by, x.retreived_with]}.should == [[@ds,ts], [@ds,ts], [@ds,ts], [@ds,ts]]
    end
  end

  it "association getter methods should eagerly load the association if there is an active identity map and the association isn't cached" do
    TaticalEagerLoadingModel.with_identity_map do
      MODEL_DB.sqls.length.should == 0
      ts = TaticalEagerLoadingModel.all
      MODEL_DB.sqls.length.should == 1
      ts.map{|x| x.parent}.should == [ts[2], ts[3], nil, nil]
      MODEL_DB.sqls.length.should == 2
      ts.map{|x| x.children}.should == [[], [], [ts[0]], [ts[1]]]
      MODEL_DB.sqls.length.should == 3
    end
  end

  it "association getter methods should not eagerly load the association if there is no active identity map" do
    MODEL_DB.sqls.length.should == 0
    ts = TaticalEagerLoadingModel.all
    MODEL_DB.sqls.length.should == 1
    ts.map{|x| x.parent}
    MODEL_DB.sqls.length.should == 3
    ts.map{|x| x.children}
    MODEL_DB.sqls.length.should == 7
  end

  it "association getter methods should not eagerly load the association if the association is cached" do
    TaticalEagerLoadingModel.with_identity_map do
      MODEL_DB.sqls.length.should == 0
      ts = TaticalEagerLoadingModel.all
      MODEL_DB.sqls.length.should == 1
      ts.map{|x| x.parent}.should == [ts[2], ts[3], nil, nil]
      @ds.should_not_receive(:eager_load)
      ts.map{|x| x.parent}.should == [ts[2], ts[3], nil, nil]
    end
  end
end
