require_relative "spec_helper"

describe "association_lazy_eager_option plugin" do
  before do
    @c = Class.new(Sequel::Model)
    @db = Sequel.mock
    @c.set_dataset @db[:ts]
    @c.plugin :association_lazy_eager_option
    @c.columns :id, :t_id
    @c.many_to_one :t, :class=>@c, :key=>:t_id
    @c.one_to_many :ts, :class=>@c, :key=>:t_id
    @c.many_to_many :mtm_ts, :class=>@c, :join_table=>:ts, :left_primary_key=>:t_id, :left_key=>:id, :right_key=>:t_id
    @c.one_to_one :otoo_t, :class=>@c, :key=>:t_id
    @c.one_through_one :oto_t, :class=>@c, :join_table=>:ts, :left_primary_key=>:t_id, :left_key=>:id, :right_key=>:t_id
    @o1 = @c.load(:id=>1, :t_id=>2)
    @o2 = @c.load(:id=>2, :t_id=>3)
    @o3 = @c.load(:id=>3, :t_id=>4)
    @db.sqls
  end

  it "should support the :eager association method option when doing lazy association loads" do
    @db.fetch = [[{:id=>2, :t_id=>3}], [{:id=>3, :t_id=>4}]]
    o = @o1.t(:eager=>:t)
    @db.sqls.must_equal ["SELECT * FROM ts WHERE (ts.id = 2) LIMIT 1", "SELECT * FROM ts WHERE (ts.id IN (3))"]
    o.must_equal @o2
    o.associations[:t].must_equal @o3

    @db.fetch = [[{:id=>2, :t_id=>3}], [{:id=>3, :t_id=>4}]]
    o = @o3.ts(:eager=>[:t])
    @db.sqls.must_equal ["SELECT * FROM ts WHERE (ts.t_id = 3)", "SELECT * FROM ts WHERE (ts.id IN (3))"]
    o.must_equal [@o2]
    o.first.associations[:t].must_equal @o3

    @db.fetch = [[{:id=>2, :t_id=>3}], [{:id=>3, :t_id=>4}]]
    o = @o1.mtm_ts(:eager=>{:t=>{}})
    @db.sqls.must_equal ["SELECT ts.* FROM ts INNER JOIN ts ON (ts.t_id = ts.id) WHERE (ts.id = 2)", "SELECT * FROM ts WHERE (ts.id IN (3))"]
    o.must_equal [@o2]
    o.first.associations[:t].must_equal @o3

    @db.fetch = [[{:id=>2, :t_id=>3}], [{:id=>1, :t_id=>2}]]
    o = @o1.otoo_t(:eager=>:ts)
    @db.sqls.must_equal ["SELECT * FROM ts WHERE (ts.t_id = 1) LIMIT 1", "SELECT * FROM ts WHERE (ts.t_id IN (2))"]
    o.must_equal @o2
    o.associations[:ts].must_equal [@o1]

    @db.fetch = [[{:id=>2, :t_id=>3}], [{:id=>3, :t_id=>4}]]
    o = @o1.oto_t(:eager=>:t)
    @db.sqls.must_equal ["SELECT ts.* FROM ts INNER JOIN ts ON (ts.t_id = ts.id) WHERE (ts.id = 2) LIMIT 1", "SELECT * FROM ts WHERE (ts.id IN (3))"]
    o.must_equal @o2
    o.associations[:t].must_equal @o3
  end

  it "should ignore the :eager and :eager_graph options and return cached result when association is already loaded" do
    @db.fetch = [{:id=>2, :t_id=>3}]
    @o1.t.must_equal @o2
    @o1.ts.must_equal [@o2]
    @o1.mtm_ts.must_equal [@o2]
    @o1.otoo_t.must_equal @o2
    @o1.oto_t.must_equal @o2
    @db.sqls

    @o1.t(:eager=>:t).must_equal @o2
    @o1.ts(:eager=>:t).must_equal [@o2]
    @o1.mtm_ts(:eager=>:t).must_equal [@o2]
    @o1.otoo_t(:eager=>:t).must_equal @o2
    @o1.oto_t(:eager=>:t).must_equal @o2
    @db.sqls.must_equal []
  end

  it "should work normally if the :eager option is not passed to the association method" do
    @db.fetch = {:id=>2, :t_id=>3}
    o = @o1.mtm_ts{|ds| ds}
    @db.sqls.must_equal ["SELECT ts.* FROM ts INNER JOIN ts ON (ts.t_id = ts.id) WHERE (ts.id = 2)"]
    o.must_equal [@o2]
    o.first.associations.must_be_empty
  end
end
