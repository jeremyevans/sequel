require_relative "spec_helper"

pg_xmin_optimistic_locking_specs = Module.new do
  extend Minitest::Spec::DSL

  it "should include the xmin column in the model's datasets" do
    @c.dataset.sql.must_equal "SELECT *, xmin FROM items"
    @c.instance_dataset.sql.must_equal "SELECT *, xmin FROM items LIMIT 1"
  end

  it "should include the lock column when updating multiple times" do
    @db.fetch = [[{:xmin=>3}], [{:xmin => 4}]]
    @o.save
    @db.sqls.must_equal ["UPDATE items SET name = 'a' WHERE ((id = 1) AND (xmin = 2)) RETURNING xmin"]
    @o.save
    @db.sqls.must_equal ["UPDATE items SET name = 'a' WHERE ((id = 1) AND (xmin = 3)) RETURNING xmin"]
  end

  it "should not include the lock column if not present in the values" do
    @db.fetch = [[{:xmin=>3}], [{:xmin => 4}]]
    @o.values.delete(:xmin)
    @o.save
    @db.sqls.must_equal ["UPDATE items SET name = 'a' WHERE (id = 1) RETURNING xmin"]
    @o.save
    @db.sqls.must_equal ["UPDATE items SET name = 'a' WHERE ((id = 1) AND (xmin = 3)) RETURNING xmin"]
  end

  it "should include the primary key column when updating if it has changed" do
    @db.fetch = [[{:xmin=>3}]]
    @o.id = 4
    @o.save
    @db.sqls.must_equal ["UPDATE items SET id = 4, name = 'a' WHERE ((id = 4) AND (xmin = 2)) RETURNING xmin"]
  end

  it "should automatically update lock column using new value from database" do
    @db.fetch = [[{:xmin=>3}]]
    @o.save
    @o.xmin.must_equal 3
  end

  it "should raise error when updating stale object" do
    @db.fetch = []
    proc{@o.save}.must_raise(Sequel::NoExistingObject)
    @db.sqls.must_equal ["UPDATE items SET name = 'a' WHERE ((id = 1) AND (xmin = 2)) RETURNING xmin"]
    @o.xmin.must_equal 2
  end

  it "should raise error when destroying stale object" do
    @db.numrows = 0
    proc{@o.destroy}.must_raise(Sequel::NoExistingObject)
    @db.sqls.must_equal ["DELETE FROM items WHERE ((id = 1) AND (xmin = 2))"]
  end

  it "should allow refresh after failed save" do
    @db.fetch = []
    proc{@o.save}.must_raise(Sequel::NoExistingObject)
    @db.sqls.must_equal ["UPDATE items SET name = 'a' WHERE ((id = 1) AND (xmin = 2)) RETURNING xmin"]
    @db.fetch = {:id=>1, :name=>'a', :xmin =>3}
    @o.refresh
    @db.sqls.must_equal ["SELECT *, xmin FROM items WHERE (id = 1) LIMIT 1"]
    @o.save
    @db.sqls.must_equal ["UPDATE items SET name = 'a' WHERE ((id = 1) AND (xmin = 3)) RETURNING xmin"]
  end

  it "should work when subclassing" do
    c = Class.new(@c)
    o = c.load(:id=>1, :name=>'a', :xmin=>2)
    @db.fetch = [[{:xmin=>3}]]
    o.save
    @db.sqls.must_equal ["UPDATE items SET name = 'a' WHERE ((id = 1) AND (xmin = 2)) RETURNING xmin"]
  end
end

describe "pg_xmin_optimistic_locking plugin loaded into class with dataset" do
  before do
    @db = Sequel.mock(:host=>'postgres')
    @ds = @db[:items].with_quote_identifiers(false).with_extend do
      def columns!
        cs = [:id, :name]
        cs << :xmin if opts[:select] && opts[:select].include?(:xmin)
        cs
      end
    end
    @c = Class.new(Sequel::Model(@ds))
    @c.columns :id, :name
    @c.plugin :pg_xmin_optimistic_locking
    @o = @c.load(:id=>1, :name=>'a', :xmin=>2)
    @db.sqls
  end

  include pg_xmin_optimistic_locking_specs
end

describe "pg_xmin_optimistic_locking plugin loaded into base class" do
  before do
    @db = Sequel.mock(:host=>'postgres')
    @ds = @db[:items].with_quote_identifiers(false).with_extend do
      def columns!
        cs = [:id, :name]
        cs << :xmin if opts[:select] && opts[:select].include?(:xmin)
        cs
      end
    end
    @bc = Class.new(Sequel::Model)
    @bc.plugin :pg_xmin_optimistic_locking
    @c = @bc::Model(@ds)
    @c.columns :id, :name
    @o = @c.load(:id=>1, :name=>'a', :xmin=>2)
    @db.sqls
  end

  include pg_xmin_optimistic_locking_specs

  it "should handle datasets not selecting from tables" do
    ds = @ds.with_extend{def columns!; raise Sequel::DatabaseError if opts[:select] && opts[:select].include?(:xmin); super end}
    @c = @bc::Model(ds)
    @c.columns :id, :name
    @db.sqls

    @c.dataset.sql.must_equal "SELECT * FROM items"
    @c.instance_dataset.sql.must_equal "SELECT * FROM items LIMIT 1"
    @db.fetch = {:id=>1, :name=>'a'}
    @c.first.must_equal @c.load(:id=>1, :name=>'a')
    @db.sqls.must_equal ["SELECT * FROM items LIMIT 1"]
  end

  it "should handle datasets where returned columns do not include xmin" do
    ds = @ds.with_extend{def columns!; [:id, :name] end}
    @c = @bc::Model(ds)
    @c.columns :id, :name
    @db.sqls

    @c.dataset.sql.must_equal "SELECT * FROM items"
    @c.instance_dataset.sql.must_equal "SELECT * FROM items LIMIT 1"
    @db.fetch = {:id=>1, :name=>'a'}
    @c.first.must_equal @c.load(:id=>1, :name=>'a')
    @db.sqls.must_equal ["SELECT * FROM items LIMIT 1"]
  end

  it "should raise connection errors when loading" do
    ds = @ds.with_extend{def columns!; raise Sequel::DatabaseConnectionError if opts[:select] && opts[:select].include?(:xmin); super end}
    proc{@c = @bc::Model(ds)}.must_raise Sequel::DatabaseConnectionError
  end
end
