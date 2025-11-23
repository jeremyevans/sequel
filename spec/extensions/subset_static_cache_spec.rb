require_relative "spec_helper"

describe "subset_static_cache plugin" do
  before do
    @db = Sequel.mock
    @db.fetch = [{:id=>1}, {:id=>2}]
    @db.numrows = 1
    @c = Class.new(Sequel::Model(@db[:t])) do
      columns :id, :name

      dataset_module do
        where :foo, :bar
      end

      plugin :subset_static_cache
    end
    @db.sqls.must_equal ["SELECT * FROM t LIMIT 0"]

    @c.cache_subset :foo
    @db.sqls.must_equal ["SELECT * FROM t WHERE bar"]

    @ds = @c.foo
    @c1 = @c.load(:id=>1)
    @c2 = @c.load(:id=>2)
  end

  it "should give temporary name to name model-specific module" do
    c = Sequel::Model(:items)
    c.class_eval do
      dataset_module{where :foo, :bar}
      plugin :subset_static_cache
      cache_subset :foo
    end
    c.singleton_class.ancestors[1].name.must_equal "Sequel::_Model(:items)::@subset_static_cache_module"
  end if RUBY_VERSION >= '3.3'

  it "should give temporary name to name model-specific module" do
    c = Sequel::Model(:items)
    c.class_eval do
      dataset_module{where :foo, :bar}
      plugin :subset_static_cache
      cache_subset :foo
    end
    c.singleton_class.ancestors[1].name.must_equal "Sequel::_Model(:items)::@subset_static_cache_module"
  end if RUBY_VERSION >= '3.3'

  it "should have .with_pk use the cache without a query" do
    @ds.with_pk(1)
    @ds.with_pk(1).must_equal @c1
    @ds.with_pk(2).must_equal @c2
    @ds.with_pk(3).must_be_nil
    @ds.with_pk([1,2]).must_be_nil
    @ds.with_pk(nil).must_be_nil
    @db.sqls.must_equal []
  end

  it "should have .with_pk work on cloned datasets using a query" do
    @ds.where(:baz).with_pk(1).must_equal @c1
    @db.sqls.must_equal ["SELECT * FROM t WHERE (bar AND baz AND (t.id = 1)) LIMIT 1"]
  end

  it "should have .first without arguments return first cached row without a query" do
    @ds.first.must_equal @c1
    @db.sqls.must_equal []
  end

  it "should have .first with single integer argument just returns instances without a query" do
    @ds.first(0).must_equal []
    @ds.first(1).must_equal [@c1]
    @ds.first(2).must_equal [@c1, @c2]
    @ds.first(3).must_equal [@c1, @c2]
    @db.sqls.must_equal []
  end

  it "should have .first with other arguments use a query" do
    @db.fetch = lambda do |s|
      case s
      when /id = '?(\d+)'?/
        id = $1.to_i
        id <= 2 ? { id: id } : nil
      when /id >= '?(\d+)'?/
        id = $1.to_i
        id <= 2 ? (id..2).map { |i| { id: i } } : []
      end
    end

    @ds.first(id: 2).must_equal @c2
    @ds.first(id: '2').must_equal @c2
    @ds.first(id: 3).must_be_nil
    @ds.first { id >= 2 }.must_equal @c2
    @ds.first(2) { id >= 1 }.must_equal [@c1, @c2]
    @ds.first(Sequel.lit('id = ?', 2)).must_equal @c2
    @db.sqls.must_equal [
      "SELECT * FROM t WHERE (bar AND (id = 2)) LIMIT 1",
      "SELECT * FROM t WHERE (bar AND (id = '2')) LIMIT 1",
      "SELECT * FROM t WHERE (bar AND (id = 3)) LIMIT 1",
      "SELECT * FROM t WHERE (bar AND (id >= 2)) LIMIT 1",
      "SELECT * FROM t WHERE (bar AND (id >= 1)) LIMIT 2",
      "SELECT * FROM t WHERE (bar AND (id = 2)) LIMIT 1"
    ]
  end

  it "should have .first work on cloned datasets using a query" do
    @ds.where(:baz).first.must_equal @c1
    @db.sqls.must_equal ["SELECT * FROM t WHERE (bar AND baz) LIMIT 1"]
  end

  it "should have .each yield frozen instances without a query" do
    a = []
    @ds.each{|o| a << o}
    a.must_equal [@c1, @c2]
    a.first.must_be :frozen?
    a.last.must_be :frozen?
    @db.sqls.must_equal []
  end

  it "should have .each work on cloned datasets using a query" do
    a = []
    @ds.where(:baz).each{|o| a << o}
    a.must_equal [@c1, @c2]
    @db.sqls.must_equal ["SELECT * FROM t WHERE (bar AND baz)"]
  end

  it "should have .map with block iterate map over instances without a query" do
    @ds.map(&:id).sort.must_equal [1, 2]
    @db.sqls.must_equal []
  end

  it "should have .map with symbol argument return array without a query" do
    @ds.map(:id).sort.must_equal [1, 2]
    @db.sqls.must_equal []
  end

  it "should have .map with array argument return array without a query" do
    @ds.map([:id]).sort.must_equal [[1], [2]]
    @db.sqls.must_equal []
  end

  it "should have .map without a block not return a frozen object" do
    @ds.map(:a).frozen?.must_equal false
  end

  it "should have .map without a block or arguments return an Enumerator" do
    @ds.map.class.must_equal Enumerator
  end

  it "should have .map with a block and argument raise" do
    proc{@ds.map(:id){}}.must_raise(Sequel::Error)
  end

  it "should have .map work on cloned datasets using a query" do
    @ds.where(:baz).map(:id).must_equal [1, 2]
    @db.sqls.must_equal ["SELECT * FROM t WHERE (bar AND baz)"]
  end

  it "should have .as_set with symbol argument return set without a query" do
    @ds.as_set(:id).must_equal Set[1, 2]
    @db.sqls.must_equal []
  end

  it "should have .as_set with array argument return set without a query" do
    @ds.as_set([:id]).must_equal Set[[1], [2]]
    @db.sqls.must_equal []
  end

  it "should have .as_set work on cloned datasets using a query" do
    @ds.where(:baz).as_set(:id).must_equal Set[1, 2]
    @db.sqls.must_equal ["SELECT * FROM t WHERE (bar AND baz)"]
  end

  it "should have .count with no argument or block return result without a query" do
    @ds.count.must_equal 2
    @db.sqls.must_equal []
  end

  it "should have .count with argument or block use a query" do
    @db.fetch = [[{:count=>1}], [{:count=>2}]]
    @ds.count(:a).must_equal 1
    @ds.count{b}.must_equal 2
    @db.sqls.must_equal ["SELECT count(a) AS count FROM t WHERE bar LIMIT 1", "SELECT count(b) AS count FROM t WHERE bar LIMIT 1"]
  end

  it "should have .count work on cloned datasets using a query" do
    @ds.where(:baz).count.must_equal 1
    @db.sqls.must_equal ["SELECT count(*) AS count FROM t WHERE (bar AND baz) LIMIT 1"]
  end

  it "should have other enumerable methods work without sending a query" do
    a = @ds.sort_by{|o| o.id}
    a.first.must_equal @c1
    a.last.must_equal @c2
    @db.sqls.must_equal []
  end

  it "should have .all work on cloned datasets using a query" do
    @ds.where(:baz).sort_by(&:id).must_equal [@c1, @c2]
    @db.sqls.must_equal ["SELECT * FROM t WHERE (bar AND baz)"]
  end

  it "should have .all return all objects without a query" do
    @ds.all.must_equal [@c1, @c2]
    @db.sqls.must_equal []
  end

  it "should have .all not return a frozen object" do
    @ds.all.frozen?.must_equal false
  end

  it "should have .all yield instances to block without a query" do
    a = []
    b = @ds.all { |o| a << o }
    a.must_equal [@c1, @c2]
    a.must_equal b
    @db.sqls.must_equal []
  end

  it "should have .all work on cloned datasets using a query" do
    @ds.where(:baz).all.must_equal [@c1, @c2]
    @db.sqls.must_equal ["SELECT * FROM t WHERE (bar AND baz)"]
  end

  it "should have .as_hash/.to_hash without arguments return results without a query" do
    a = @ds.to_hash
    a.must_equal(1=>@c1, 2=>@c2)

    a = @ds.as_hash
    a.must_equal(1=>@c1, 2=>@c2)
    @db.sqls.must_equal []
  end

  it "should have .as_hash handle :hash option without a query" do
    h = {}
    a = @ds.as_hash(nil, nil, :hash=>h)
    a.must_be_same_as h
    a.must_equal(1=>@c1, 2=>@c2)

    h = {}
    a = @ds.as_hash(:id, nil, :hash=>h)
    a.must_be_same_as h
    a.must_equal(1=>@c1, 2=>@c2)

    @db.sqls.must_equal []
  end

  it "should have .as_hash with arguments return results without a query" do
    a = @ds.as_hash(:id)
    a.must_equal(1=>@c1, 2=>@c2)

    a = @ds.as_hash([:id])
    a.must_equal([1]=>@c1, [2]=>@c2)

    @ds.as_hash(:id, :id).must_equal(1=>1, 2=>2)
    @ds.as_hash([:id], :id).must_equal([1]=>1, [2]=>2)
    @ds.as_hash(:id, [:id]).must_equal(1=>[1], 2=>[2])
    @ds.as_hash([:id], [:id]).must_equal([1]=>[1], [2]=>[2])

    @db.sqls.must_equal []
  end

  it "should have .as_hash not return a frozen object" do
    @ds.as_hash.frozen?.must_equal false
  end

  it "should have .as_hash work on cloned datasets using a query" do
    a = @ds.where(:baz).to_hash
    a.must_equal(1=>@c1, 2=>@c2)
    @db.sqls.must_equal ["SELECT * FROM t WHERE (bar AND baz)"]
  end

  it "should have .to_hash_groups without value_column argument return the cached objects without a query" do
    a = @ds.to_hash_groups(:id)
    a.must_equal(1=>[@c1], 2=>[@c2])
    a = @ds.to_hash_groups([:id])
    a.must_equal([1]=>[@c1], [2]=>[@c2])
    @db.sqls.must_equal []
  end

  it "should have .to_hash_groups handle :hash option" do
    h = {}
    a = @ds.to_hash_groups(:id, nil, :hash=>h)
    a.must_be_same_as h
    a.must_equal(1=>[@c1], 2=>[@c2])
    @db.sqls.must_equal []
  end

  it "should have .to_hash_groups without arguments return the cached objects without a query" do
    @ds.to_hash_groups(:id, :id).must_equal(1=>[1], 2=>[2])
    @ds.to_hash_groups([:id], :id).must_equal([1]=>[1], [2]=>[2])
    @ds.to_hash_groups(:id, [:id]).must_equal(1=>[[1]], 2=>[[2]])
    @ds.to_hash_groups([:id], [:id]).must_equal([1]=>[[1]], [2]=>[[2]])
    @db.sqls.must_equal []
  end

  it "should have .to_hash_groups work on cloned datasets using a query" do
    a = @ds.where(:baz).to_hash_groups(:id)
    a.must_equal(1=>[@c1], 2=>[@c2])
    @db.sqls.must_equal ["SELECT * FROM t WHERE (bar AND baz)"]
  end

  it "subclasses should not use the cache" do
    c = Class.new(@c)
    c.foo.all.must_equal [c.load(:id=>1), c.load(:id=>2)]
    @db.sqls.must_equal ['SELECT * FROM t WHERE bar']
    c.foo.as_hash.must_equal(1=>c.load(:id=>1), 2=>c.load(:id=>2))
    @db.sqls.must_equal ['SELECT * FROM t WHERE bar']
  end

  it "methods should be overridable and allow calling super" do
    @c.define_singleton_method(:foo){super()}
    @c.foo.all.must_equal [@c1, @c2]
    @db.sqls.must_equal []
  end

  it "methods after set_dataset should not use the cache" do
    ds = @c.dataset.from(:t2).columns(:id).with_fetch(:id=>3)
    @c.dataset = ds
    @c.foo.all.must_equal [@c.load(:id=>3)]
    @db.sqls.must_equal ['SELECT * FROM t2 WHERE bar']
    @c.foo.as_hash.must_equal(3=>@c.load(:id=>3))
    @db.sqls.must_equal ['SELECT * FROM t2 WHERE bar']
    @c.foo.as_hash[3].must_equal @c.load(:id=>3)
    @db.sqls.must_equal ['SELECT * FROM t2 WHERE bar']
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
    @c[[1, 2]].must_be_same_as(@c1)
    @c[[2, 1]].must_be_same_as(@c2)
    @db.sqls.must_equal []

    @c = Class.new(Sequel::Model(@db[:t])) do
      columns :id, :id2
      set_primary_key [:id, :id2]

      dataset_module do
        where :foo, :bar
      end

      plugin :subset_static_cache
    end
    @db.sqls.must_equal ["SELECT * FROM t LIMIT 0"]

    @c.cache_subset :foo
    @db.sqls.must_equal ["SELECT * FROM t WHERE bar"]

    @c.foo.to_hash.must_equal([1, 1]=>@c.load(:id=>1, :id2=>1), [2, 1]=>@c.load(:id=>2, :id2=>1))
    @db.sqls.must_equal []
  end
end
