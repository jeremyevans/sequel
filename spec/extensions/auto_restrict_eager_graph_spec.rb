require_relative "spec_helper"

describe "auto_restrict_eager_graph plugin" do
  before do
    @db = Sequel.mock
    @c = Class.new(Sequel::Model(@db[:items]))
    @c.plugin :auto_restrict_eager_graph
  end 

  it "should restrict eager_graph for associations with blocks without :graph_* options" do
    @c.many_to_one :cs, :class=>@c do |ds| ds.where(:x) end 
    proc{@c.eager_graph(:cs)}.must_raise Sequel::Error
  end

  it "should not restrict eager_graph for associations without blocks" do
    @c.many_to_one :cs, :class=>@c
    @c.eager_graph(:cs).sql.must_equal "SELECT * FROM items LEFT OUTER JOIN items AS cs ON (cs.id = items.cs_id)"
  end

  it "should not restrict eager_graph for associations with :graph_* options" do
    @c.many_to_one :cs, :class=>@c, :graph_conditions=>{:x=>true} do |ds| ds.where(:x) end 
    @c.eager_graph(:cs).sql.must_equal "SELECT * FROM items LEFT OUTER JOIN items AS cs ON ((cs.id = items.cs_id) AND (cs.x IS TRUE))"
  end

  it "should not restrict eager_graph for associations with :allow_eager_graph option" do
    @c.many_to_one :cs, :class=>@c, :allow_eager_graph=>true do |ds| ds.where(:x) end 
    @c.eager_graph(:cs).sql.must_equal "SELECT * FROM items LEFT OUTER JOIN items AS cs ON (cs.id = items.cs_id)"
  end
end
