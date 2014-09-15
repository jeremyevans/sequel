require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::SplitValues" do
  it "should skip the refresh after saving a new object" do
    c = Class.new(Sequel::Model(:a))
    c.columns :id, :x
    c.plugin :split_values
    c.dataset._fetch = {:id=>1, :x=>2, :y=>3}
    o = c.first
    c.db.reset

    o.should == c.load(:id=>1, :x=>2)
    o[:id].should == 1
    o[:x].should == 2
    o[:y].should == 3
    {c.load(:id=>1, :x=>2)=>4}[o].should == 4
    o.values.should == {:id=>1, :x=>2}

    o.save
    c.db.sqls.should == ["UPDATE a SET x = 2 WHERE (id = 1)"]
  end
end
