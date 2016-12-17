require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::SplitValues" do
  it "stores non-columns in a separate hash" do
    c = Class.new(Sequel::Model(:a))
    c.columns :id, :x
    c.plugin :split_values
    c.dataset = c.dataset.with_fetch(:id=>1, :x=>2, :y=>3)
    o = c.first
    c.db.reset

    o.must_equal c.load(:id=>1, :x=>2)
    o[:id].must_equal 1
    o[:x].must_equal 2
    o[:y].must_equal 3
    {c.load(:id=>1, :x=>2)=>4}[o].must_equal 4
    o.values.must_equal(:id=>1, :x=>2)

    o.save
    c.db.sqls.must_equal ["UPDATE a SET x = 2 WHERE (id = 1)"]
  end
end
