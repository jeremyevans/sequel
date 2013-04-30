require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::StaticCache" do
  before do
    @db = Sequel.mock
    @db.fetch = [{:id=>1}, {:id=>2}]
    @c = Class.new(Sequel::Model(@db[:t]))
    @c.columns :id
    @c.plugin :static_cache
    @c1 = @c.cache[1]
    @c2 = @c.cache[2]
    @db.sqls
  end

  it "should use a ruby hash as a cache of all model instances" do
    @c.cache.should == {1=>@c.load(:id=>1), 2=>@c.load(:id=>2)}
  end

  it "should work correctly with composite keys" do
    @db.fetch = [{:id=>1, :id2=>1}, {:id=>2, :id2=>1}]
    @c = Class.new(Sequel::Model(@db[:t]))
    @c.columns :id, :id2
    @c.set_primary_key([:id, :id2])
    @c.plugin :static_cache
    @db.sqls
    @c1 = @c.cache[[1, 2]]
    @c2 = @c.cache[[2, 1]]
    @c[[1, 2]].should equal(@c1)
    @c[[2, 1]].should equal(@c2)
    @db.sqls.should == []
  end

  it "should make .[] method with primary key use the cache" do
    @c[1].should equal(@c1)
    @c[2].should equal(@c2)
    @c[3].should be_nil
    @c[[1, 2]].should be_nil
    @c[nil].should be_nil
    @c[].should be_nil
    @db.sqls.should == []
  end

  it "should have .[] with a hash not use the cache" do
    @db.fetch = {:id=>2}
    @c[:id=>2].should == @c2
    @db.sqls.should == ['SELECT * FROM t WHERE (id = 2) LIMIT 1']
  end

  it "should support cache_get_pk" do
    @c.cache_get_pk(1).should equal(@c1)
    @c.cache_get_pk(2).should equal(@c2)
    @c.cache_get_pk(3).should be_nil
    @db.sqls.should == []
  end

  it "should have each just iterate over the hash's values without sending a query" do
    a = []
    @c.each{|o| a << o}
    a = a.sort_by{|o| o.id}
    a.first.should equal(@c1)
    a.last.should equal(@c2)
    @db.sqls.should == []
  end

  it "should have map just iterate over the hash's values without sending a query if no argument is given" do
    @c.map{|v| v.id}.sort.should == [1, 2]
    @db.sqls.should == []
  end

  it "should have count with no argument or block not issue a query" do
    @c.count.should == 2
    @db.sqls.should == []
  end

  it "should have count with argument or block not issue a query" do
    @db.fetch = [[{:count=>1}], [{:count=>2}]]
    @c.count(:a).should == 1
    @c.count{b}.should == 2
    @db.sqls.should == ["SELECT COUNT(a) AS count FROM t LIMIT 1", "SELECT COUNT(b) AS count FROM t LIMIT 1"]
  end

  it "should have map not send a query if given an argument" do
    @c.map(:id).sort.should == [1, 2]
    @db.sqls.should == []
    @c.map([:id,:id]).sort.should == [[1,1], [2,2]]
    @db.sqls.should == []
  end

  it "should have map without a block or argument not raise an exception or issue a query" do
    @c.map.to_a.should == @c.all
    @db.sqls.should == []
  end

  it "should have map without a block not return a frozen object" do
    @c.map.frozen?.should be_false
  end

  it "should have map with a block and argument raise" do
    proc{@c.map(:id){}}.should raise_error(Sequel::Error)
  end

  it "should have other enumerable methods work without sending a query" do
    a = @c.sort_by{|o| o.id}
    a.first.should equal(@c1)
    a.last.should equal(@c2)
    @db.sqls.should == []
  end

  it "should have all just return the cached values" do
    a = @c.all.sort_by{|o| o.id}
    a.first.should equal(@c1)
    a.last.should equal(@c2)
    @db.sqls.should == []
  end

  it "should have all not return a frozen object" do
    @c.all.frozen?.should be_false
  end

  it "should have all return things in dataset order" do
    @c.all.should == [@c1, @c2]
  end

  it "should have to_hash without arguments return the cached objects without a query" do
    a = @c.to_hash
    a.should == {1=>@c1, 2=>@c2}
    a[1].should equal(@c1)
    a[2].should equal(@c2)
    @db.sqls.should == []
  end

  it "should have to_hash with arguments return the cached objects without a query" do
    a = @c.to_hash(:id)
    a.should == {1=>@c1, 2=>@c2}
    a[1].should equal(@c1)
    a[2].should equal(@c2)

    a = @c.to_hash([:id])
    a.should == {[1]=>@c1, [2]=>@c2}
    a[[1]].should equal(@c1)
    a[[2]].should equal(@c2)

    @c.to_hash(:id, :id).should == {1=>1, 2=>2}
    @c.to_hash([:id], :id).should == {[1]=>1, [2]=>2}
    @c.to_hash(:id, [:id]).should == {1=>[1], 2=>[2]}
    @c.to_hash([:id], [:id]).should == {[1]=>[1], [2]=>[2]}

    @db.sqls.should == []
  end

  it "should have to_hash not return a frozen object" do
    @c.to_hash.frozen?.should be_false
  end

  it "should have to_hash_groups without arguments return the cached objects without a query" do
    a = @c.to_hash_groups(:id)
    a.should == {1=>[@c1], 2=>[@c2]}
    a[1].first.should equal(@c1)
    a[2].first.should equal(@c2)

    a = @c.to_hash_groups([:id])
    a.should == {[1]=>[@c1], [2]=>[@c2]}
    a[[1]].first.should equal(@c1)
    a[[2]].first.should equal(@c2)

    @c.to_hash_groups(:id, :id).should == {1=>[1], 2=>[2]}
    @c.to_hash_groups([:id], :id).should == {[1]=>[1], [2]=>[2]}
    @c.to_hash_groups(:id, [:id]).should == {1=>[[1]], 2=>[[2]]}
    @c.to_hash_groups([:id], [:id]).should == {[1]=>[[1]], [2]=>[[2]]}

    @db.sqls.should == []
  end

  it "all of the static cache values (model instances) should be frozen" do
    @c.all.all?{|o| o.frozen?}.should be_true
  end

  it "subclasses should work correctly" do
    c = Class.new(@c)
    c.all.should == [c.load(:id=>1), c.load(:id=>2)]
    c.to_hash.should == {1=>c.load(:id=>1), 2=>c.load(:id=>2)}
    @db.sqls.should == ['SELECT * FROM t']
  end

  it "set_dataset should work correctly" do
    ds = @c.dataset.from(:t2)
    ds.instance_variable_set(:@columns, [:id])
    ds._fetch = {:id=>3}
    @c.dataset = ds
    @c.all.should == [@c.load(:id=>3)]
    @c.to_hash.should == {3=>@c.load(:id=>3)}
    @c.to_hash[3].should equal(@c.all.first)
    @db.sqls.should == ['SELECT * FROM t2']
  end
end
