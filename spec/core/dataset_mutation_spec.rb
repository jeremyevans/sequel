require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

# SEQUEL5: Remove
unless Sequel.mock.dataset.frozen?

describe "Dataset" do
  before do
    @d = Sequel.mock.dataset.from(:x)
  end

  it "should support self-changing select!" do
    @d.select!(:y)
    @d.sql.must_equal "SELECT y FROM x"
  end
  
  it "should support self-changing from!" do
    @d.from!(:y)
    @d.sql.must_equal "SELECT * FROM y"
  end

  it "should support self-changing order!" do
    @d.order!(:y)
    @d.sql.must_equal "SELECT * FROM x ORDER BY y"
  end
  
  it "should support self-changing filter!" do
    @d.filter!(:y => 1)
    @d.sql.must_equal "SELECT * FROM x WHERE (y = 1)"
  end

  it "should support self-changing filter! with block" do
    @d.filter!{y < 2}
    @d.sql.must_equal "SELECT * FROM x WHERE (y < 2)"
  end
  
  it "should raise for ! methods that don't return a dataset" do
    proc {@d.opts!}.must_raise(NoMethodError)
  end
  
  it "should raise for missing methods" do
    proc {@d.xuyz}.must_raise(NoMethodError)
    proc {@d.xyz!}.must_raise(NoMethodError)
    proc {@d.xyz?}.must_raise(NoMethodError)
  end
  
  it "should support chaining of bang methods" do
      @d.order!(:y).filter!(:y => 1).sql.must_equal "SELECT * FROM x WHERE (y = 1) ORDER BY y"
  end
end

describe "Frozen Datasets" do
  before do
    @ds = Sequel.mock[:test].freeze
  end

  it "should have dups not be frozen" do
    @ds.dup.wont_be :frozen?
  end

  it "should raise an error when calling mutation methods" do
    proc{@ds.select!(:a)}.must_raise RuntimeError
    proc{@ds.row_proc = proc{}}.must_raise RuntimeError
    proc{@ds.extension! :query}.must_raise RuntimeError
    proc{@ds.naked!}.must_raise RuntimeError
    proc{@ds.from_self!}.must_raise RuntimeError
  end
end

describe "Dataset mutation methods" do
  def m(&block)
    ds = Sequel.mock[:t]
    def ds.supports_cte?(*) true end
    ds.instance_exec(&block)
    ds.sql
  end

  it "should modify the dataset in place" do
    dsc = Sequel.mock[:u]
    dsc.send(:columns=, [:v])

    m{and!(:a=>1).or!(:b=>2)}.must_equal "SELECT * FROM t WHERE ((a = 1) OR (b = 2))"
    m{select!(:f).graph!(dsc, :b=>:c).set_graph_aliases!(:e=>[:m, :n]).add_graph_aliases!(:d=>[:g, :c])}.must_equal "SELECT m.n AS e, g.c AS d FROM t LEFT OUTER JOIN u ON (u.b = t.c)"
    m{cross_join!(:a)}.must_equal "SELECT * FROM t CROSS JOIN a"
    m{distinct!}.must_equal "SELECT DISTINCT * FROM t"
    m{except!(dsc)}.must_equal "SELECT * FROM (SELECT * FROM t EXCEPT SELECT * FROM u) AS t1"
    m{exclude!(:a=>1)}.must_equal "SELECT * FROM t WHERE (a != 1)"
    m{exclude_having!(:a=>1)}.must_equal "SELECT * FROM t HAVING (a != 1)"
    m{exclude_where!(:a=>1)}.must_equal "SELECT * FROM t WHERE (a != 1)"
    m{filter!(:a=>1)}.must_equal "SELECT * FROM t WHERE (a = 1)"
    m{for_update!}.must_equal "SELECT * FROM t FOR UPDATE"
    m{from!(:p)}.must_equal "SELECT * FROM p"
    m{full_join!(:a, [:b])}.must_equal "SELECT * FROM t FULL JOIN a USING (b)"
    m{full_outer_join!(:a, [:b])}.must_equal "SELECT * FROM t FULL OUTER JOIN a USING (b)"
    m{grep!(:a, 'b')}.must_equal "SELECT * FROM t WHERE ((a LIKE 'b' ESCAPE '\\'))"
    m{group!(:a)}.must_equal "SELECT * FROM t GROUP BY a"
    m{group_and_count!(:a)}.must_equal "SELECT a, count(*) AS count FROM t GROUP BY a"
    m{group_by!(:a)}.must_equal "SELECT * FROM t GROUP BY a"
    m{having!(:a)}.must_equal "SELECT * FROM t HAVING a"
    m{inner_join!(:a, [:b])}.must_equal "SELECT * FROM t INNER JOIN a USING (b)"
    m{intersect!(dsc)}.must_equal "SELECT * FROM (SELECT * FROM t INTERSECT SELECT * FROM u) AS t1"
    m{where!(:a).invert!}.must_equal "SELECT * FROM t WHERE NOT a"
    m{join!(:a, [:b])}.must_equal "SELECT * FROM t INNER JOIN a USING (b)"
    m{join_table!(:inner, :a, [:b])}.must_equal "SELECT * FROM t INNER JOIN a USING (b)"
    m{left_join!(:a, [:b])}.must_equal "SELECT * FROM t LEFT JOIN a USING (b)"
    m{left_outer_join!(:a, [:b])}.must_equal "SELECT * FROM t LEFT OUTER JOIN a USING (b)"
    m{limit!(1)}.must_equal "SELECT * FROM t LIMIT 1"
    m{lock_style!(:update)}.must_equal "SELECT * FROM t FOR UPDATE"
    m{natural_full_join!(:a)}.must_equal "SELECT * FROM t NATURAL FULL JOIN a"
    m{natural_join!(:a)}.must_equal "SELECT * FROM t NATURAL JOIN a"
    m{natural_left_join!(:a)}.must_equal "SELECT * FROM t NATURAL LEFT JOIN a"
    m{natural_right_join!(:a)}.must_equal "SELECT * FROM t NATURAL RIGHT JOIN a"
    m{offset!(1)}.must_equal "SELECT * FROM t OFFSET 1"
    m{order!(:a).reverse_order!}.must_equal "SELECT * FROM t ORDER BY a DESC"
    m{order_by!(:a).order_more!(:b).order_append!(:c).order_prepend!(:d).reverse!}.must_equal "SELECT * FROM t ORDER BY d DESC, a DESC, b DESC, c DESC"
    m{qualify!}.must_equal "SELECT t.* FROM t"
    m{right_join!(:a, [:b])}.must_equal "SELECT * FROM t RIGHT JOIN a USING (b)"
    m{right_outer_join!(:a, [:b])}.must_equal "SELECT * FROM t RIGHT OUTER JOIN a USING (b)"
    m{select!(:a)}.must_equal "SELECT a FROM t"
    m{select_all!(:t).select_more!(:b).select_append!(:c)}.must_equal "SELECT t.*, b, c FROM t"
    m{select_group!(:a)}.must_equal "SELECT a FROM t GROUP BY a"
    m{where!(:a).unfiltered!}.must_equal "SELECT * FROM t"
    m{group!(:a).ungrouped!}.must_equal "SELECT * FROM t"
    m{limit!(1).unlimited!}.must_equal "SELECT * FROM t"
    m{order!(:a).unordered!}.must_equal "SELECT * FROM t"
    m{union!(dsc)}.must_equal "SELECT * FROM (SELECT * FROM t UNION SELECT * FROM u) AS t1"
    m{with!(:a, dsc)}.must_equal "WITH a AS (SELECT * FROM u) SELECT * FROM t"
    m{with_recursive!(:a, dsc, dsc)}.must_equal "WITH a AS (SELECT * FROM u UNION ALL SELECT * FROM u) SELECT * FROM t"
    m{with_sql!('SELECT foo')}.must_equal "SELECT foo"

    dsc.server!(:a)
    dsc.opts[:server].must_equal :a
    dsc.graph!(dsc, {:b=>:c}, :table_alias=>:foo).ungraphed!.opts[:graph].must_be_nil
  end

  it "should clear the cache" do
    ds = Sequel.mock[:a]
    ds.columns
    ds.send(:cache_set, :columns, [:a])
    ds.select!(:foo, :bar).send(:cache_get, :columns).must_be_nil
  end
end

describe "Dataset#clone" do
  before do
    @dataset = Sequel.mock.dataset.from(:items)
  end

  it "should copy the dataset opts" do
    clone = @dataset.clone
    clone.opts.must_equal @dataset.opts
    @dataset.filter!(:a => 'b')
    clone.opts[:filter].must_be_nil
  end
end
  
describe "Dataset extensions" do
  before(:all) do
    class << Sequel
      alias _extension extension
      remove_method :extension
      def extension(*)
      end
    end
  end
  after(:all) do
    class << Sequel
      remove_method :extension
      alias extension _extension
      remove_method :_extension
    end
  end
  before do
    @ds = Sequel.mock.dataset
  end

  it "should have #extension! modify the receiver" do
    Sequel::Dataset.register_extension(:foo, Module.new{def a; 1; end})
    @ds.extension!(:foo)
    @ds.a.must_equal 1
  end

  it "should have #extension! return the receiver" do
    Sequel::Dataset.register_extension(:foo, Module.new{def a; 1; end})
    @ds.extension!(:foo).must_be_same_as(@ds)
  end
end

describe "Dataset#naked!" do
  it "should remove any existing row_proc" do
    d = Sequel.mock.dataset.with_row_proc(Proc.new{|r| r})
    d.naked!.row_proc.must_be_nil
    d.row_proc.must_be_nil
  end
end

describe "Dataset#row_proc=" do
  it "should set the row_proc" do
    d = Sequel.mock.dataset.with_row_proc(Proc.new{|r| r})
    d.row_proc.wont_be_nil
    d.row_proc = nil
    d.row_proc.must_be_nil
  end
end

describe "Dataset#quote_identifiers=" do
  it "should change quote identifiers setting" do
    d = Sequel.mock.dataset.with_quote_identifiers(true)
    d.literal(:a).must_equal '"a"'
    d.quote_identifiers = false
    d.literal(:a).must_equal 'a'
  end
end

describe "Dataset#from_self!" do
  it "should work" do
    Sequel.mock.dataset.from(:test).select(:name).limit(1).from_self!.sql.must_equal 'SELECT * FROM (SELECT name FROM test LIMIT 1) AS t1'
  end
end

describe "Sequel Mock Adapter" do
  it "should be able to set the rows returned by each on a per dataset basis using _fetch" do
    rs = []
    db = Sequel.mock(:fetch=>{:a=>1})
    ds = db[:t]
    ds.each{|r| rs << r}
    rs.must_equal [{:a=>1}]
    ds._fetch = {:b=>2}
    ds.each{|r| rs << r}
    rs.must_equal [{:a=>1}, {:b=>2}]
  end

  it "should be able to set the number of rows modified by update and delete on a per dataset basis" do
    db = Sequel.mock(:numrows=>2)
    ds = db[:t]
    ds.update(:a=>1).must_equal 2
    ds.delete.must_equal 2
    ds.numrows = 3
    ds.update(:a=>1).must_equal 3
    ds.delete.must_equal 3
  end

  it "should be able to set the autogenerated primary key returned by insert on a per dataset basis" do
    db = Sequel.mock(:autoid=>1)
    ds = db[:t]
    ds.insert(:a=>1).must_equal 1
    ds.autoid = 5
    ds.insert(:a=>1).must_equal 5
    ds.insert(:a=>1).must_equal 6
    db[:t].insert(:a=>1).must_equal 2
  end
end

end
