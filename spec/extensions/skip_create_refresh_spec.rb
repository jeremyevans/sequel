require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::SkipCreateRefresh" do
  it "should skip the refresh after saving a new object" do
    c = Class.new(Sequel::Model(:a))
    c.columns :id, :x
    c.db.reset
    c.dataset.meta_def(:insert){|*a| super(*a); 2}
    c.create(:x=>1)
    c.db.sqls.should == ['INSERT INTO a (x) VALUES (1)', 'SELECT * FROM a WHERE (id = 2) LIMIT 1']

    c.plugin :skip_create_refresh
    c.db.reset
    c.create(:x=>3).values.should == {:id=>2, :x=>3}
    c.db.sqls.should == ['INSERT INTO a (x) VALUES (3)']
  end
end
