require_relative "spec_helper"

describe "paged_operations plugin" do
  before do
    @db = Sequel.mock
    @c = Class.new(Sequel::Model(@db[:albums]))
    @c.plugin :paged_operations
    @ds = @c.dataset
    @db.sqls
    @db.fetch = [[{:id=>1002}], [{:id=>2002}]]
    @db.numrows = [1000, 1000, 2]
  end

  it "#paged_delete should delete using multiple queries" do
    @ds.paged_delete.must_equal 2002
    @db.sqls.must_equal [
      "SELECT id FROM albums ORDER BY id LIMIT 1 OFFSET 1000",
      "DELETE FROM albums WHERE (id < 1002)",
      "SELECT id FROM albums ORDER BY id LIMIT 1 OFFSET 1000",
      "DELETE FROM albums WHERE (id < 2002)",
      "SELECT id FROM albums ORDER BY id LIMIT 1 OFFSET 1000",
      "DELETE FROM albums"
    ]
  end

  it "#paged_update should update using multiple queries" do
    @ds.paged_update(:x=>1).must_equal 2002
    @db.sqls.must_equal [
      "SELECT id FROM albums ORDER BY id LIMIT 1 OFFSET 1000",
      "UPDATE albums SET x = 1 WHERE (id < 1002)",
      "SELECT id FROM albums WHERE (id >= 1002) ORDER BY id LIMIT 1 OFFSET 1000",
      "UPDATE albums SET x = 1 WHERE ((id < 2002) AND (id >= 1002))",
      "SELECT id FROM albums WHERE (id >= 2002) ORDER BY id LIMIT 1 OFFSET 1000",
      "UPDATE albums SET x = 1 WHERE (id >= 2002)"
    ]
  end

  it "#paged_datasets should yield multiple datasets making up dataset" do
    sqls = []
    @ds.paged_datasets{|ds| sqls << ds.sql}
    sqls.must_equal [
      "SELECT * FROM albums WHERE (id < 1002)",
      "SELECT * FROM albums WHERE ((id < 2002) AND (id >= 1002))",
      "SELECT * FROM albums WHERE (id >= 2002)"
    ]
    @db.sqls.must_equal [
      "SELECT id FROM albums ORDER BY id LIMIT 1 OFFSET 1000",
      "SELECT id FROM albums WHERE (id >= 1002) ORDER BY id LIMIT 1 OFFSET 1000",
      "SELECT id FROM albums WHERE (id >= 2002) ORDER BY id LIMIT 1 OFFSET 1000",
    ]
  end

  it "#paged_datasets should support returning enum" do
    enum = @ds.paged_datasets
    enum.must_be_kind_of Enumerator
    enum.map(&:sql).must_equal [
      "SELECT * FROM albums WHERE (id < 1002)",
      "SELECT * FROM albums WHERE ((id < 2002) AND (id >= 1002))",
      "SELECT * FROM albums WHERE (id >= 2002)"
    ]
    @db.sqls.must_equal [
      "SELECT id FROM albums ORDER BY id LIMIT 1 OFFSET 1000",
      "SELECT id FROM albums WHERE (id >= 1002) ORDER BY id LIMIT 1 OFFSET 1000",
      "SELECT id FROM albums WHERE (id >= 2002) ORDER BY id LIMIT 1 OFFSET 1000",
    ]
  end

  it "#paged_delete should handle case where number of rows is less than page size" do
    @db.fetch = []
    @db.numrows = [2]
    @ds.paged_delete.must_equal 2
    @db.sqls.must_equal [
      "SELECT id FROM albums ORDER BY id LIMIT 1 OFFSET 1000",
      "DELETE FROM albums"
    ]
  end

  it "#paged_update should handle case where number of rows is less than page size" do
    @db.fetch = []
    @db.numrows = [2]
    @ds.paged_update(:x=>1).must_equal 2
    @db.sqls.must_equal [
      "SELECT id FROM albums ORDER BY id LIMIT 1 OFFSET 1000",
      "UPDATE albums SET x = 1"
    ]
  end

  it "#paged_datasets should handle case where number of rows is less than page size" do
    @db.fetch = []
    @ds.paged_datasets.map(&:sql).must_equal ['SELECT * FROM albums']
    @db.sqls.must_equal ["SELECT id FROM albums ORDER BY id LIMIT 1 OFFSET 1000"]
  end

  it "#paged_delete should respect existing filters" do
    @ds.where{x > 3}.paged_delete.must_equal 2002
    @db.sqls.must_equal [
      "SELECT id FROM albums WHERE (x > 3) ORDER BY id LIMIT 1 OFFSET 1000",
      "DELETE FROM albums WHERE ((x > 3) AND (id < 1002))",
      "SELECT id FROM albums WHERE (x > 3) ORDER BY id LIMIT 1 OFFSET 1000",
      "DELETE FROM albums WHERE ((x > 3) AND (id < 2002))",
      "SELECT id FROM albums WHERE (x > 3) ORDER BY id LIMIT 1 OFFSET 1000",
      "DELETE FROM albums WHERE (x > 3)"
    ]
  end

  it "#paged_update should respect existing filters" do
    @ds.where{x > 3}.paged_update(:x=>1).must_equal 2002
    @db.sqls.must_equal [
      "SELECT id FROM albums WHERE (x > 3) ORDER BY id LIMIT 1 OFFSET 1000",
      "UPDATE albums SET x = 1 WHERE ((x > 3) AND (id < 1002))",
      "SELECT id FROM albums WHERE ((x > 3) AND (id >= 1002)) ORDER BY id LIMIT 1 OFFSET 1000",
      "UPDATE albums SET x = 1 WHERE ((x > 3) AND (id < 2002) AND (id >= 1002))",
      "SELECT id FROM albums WHERE ((x > 3) AND (id >= 2002)) ORDER BY id LIMIT 1 OFFSET 1000",
      "UPDATE albums SET x = 1 WHERE ((x > 3) AND (id >= 2002))"
    ]
  end

  it "#paged_datasets should respect existing filters" do
    @ds.where{x > 3}.paged_datasets.map(&:sql).must_equal [
      "SELECT * FROM albums WHERE ((x > 3) AND (id < 1002))",
      "SELECT * FROM albums WHERE ((x > 3) AND (id < 2002) AND (id >= 1002))",
      "SELECT * FROM albums WHERE ((x > 3) AND (id >= 2002))"
    ]

    @db.sqls.must_equal [
      "SELECT id FROM albums WHERE (x > 3) ORDER BY id LIMIT 1 OFFSET 1000",
      "SELECT id FROM albums WHERE ((x > 3) AND (id >= 1002)) ORDER BY id LIMIT 1 OFFSET 1000",
      "SELECT id FROM albums WHERE ((x > 3) AND (id >= 2002)) ORDER BY id LIMIT 1 OFFSET 1000",
    ]
  end

  it "#paged_delete should support :rows_per_page option" do
    @db.numrows = [4, 4, 2]
    @ds.paged_delete(:rows_per_page=>4).must_equal 10
    @db.sqls.must_equal [
      "SELECT id FROM albums ORDER BY id LIMIT 1 OFFSET 4",
      "DELETE FROM albums WHERE (id < 1002)",
      "SELECT id FROM albums ORDER BY id LIMIT 1 OFFSET 4",
      "DELETE FROM albums WHERE (id < 2002)",
      "SELECT id FROM albums ORDER BY id LIMIT 1 OFFSET 4",
      "DELETE FROM albums"
    ]
  end

  it "#paged_update should support :rows_per_page option" do
    @db.numrows = [4, 4, 2]
    @ds.paged_update({:x=>1}, :rows_per_page=>4).must_equal 10
    @db.sqls.must_equal [
      "SELECT id FROM albums ORDER BY id LIMIT 1 OFFSET 4",
      "UPDATE albums SET x = 1 WHERE (id < 1002)",
      "SELECT id FROM albums WHERE (id >= 1002) ORDER BY id LIMIT 1 OFFSET 4",
      "UPDATE albums SET x = 1 WHERE ((id < 2002) AND (id >= 1002))",
      "SELECT id FROM albums WHERE (id >= 2002) ORDER BY id LIMIT 1 OFFSET 4",
      "UPDATE albums SET x = 1 WHERE (id >= 2002)"
    ]
  end

  it "#paged_datasets should support :rows_per_page option" do
    @db.numrows = [4, 4, 2]
    @ds.paged_datasets(:rows_per_page=>4).map(&:sql).must_equal [
      "SELECT * FROM albums WHERE (id < 1002)",
      "SELECT * FROM albums WHERE ((id < 2002) AND (id >= 1002))",
      "SELECT * FROM albums WHERE (id >= 2002)"
    ]
    @db.sqls.must_equal [
      "SELECT id FROM albums ORDER BY id LIMIT 1 OFFSET 4",
      "SELECT id FROM albums WHERE (id >= 1002) ORDER BY id LIMIT 1 OFFSET 4",
      "SELECT id FROM albums WHERE (id >= 2002) ORDER BY id LIMIT 1 OFFSET 4",
    ]
  end
  it "should raise error for invalid :rows_per_page option" do
    proc{@ds.paged_datasets(:rows_per_page=>0){}}.must_raise Sequel::Error
    proc{@ds.paged_datasets(:rows_per_page=>-1){}}.must_raise Sequel::Error
    proc{@ds.paged_delete(:rows_per_page=>0)}.must_raise Sequel::Error
    proc{@ds.paged_update({:x=>1}, :rows_per_page=>0)}.must_raise Sequel::Error
  end

  it "should raise error for dataset with limit" do
    proc{@ds.limit(1).paged_delete}.must_raise Sequel::Error
    proc{@ds.limit(1).paged_update(:x=>1)}.must_raise Sequel::Error
    proc{@ds.limit(1).paged_datasets{}}.must_raise Sequel::Error
  end

  it "should raise error for DB2 with emulated offsets" do
    @c.dataset = Sequel.mock('db2')[:t]
    ds = @c.dataset
    proc{ds.offset(1).paged_delete}.must_raise Sequel::Error
    proc{ds.offset(1).paged_update(:x=>1)}.must_raise Sequel::Error
    proc{ds.offset(1).paged_datasets{}}.must_raise Sequel::Error
  end

  it "should raise error paged_delete for Oracle with emulated offsets" do
    db = Sequel.mock('oracle')
    def db.server_version; 11020002; end
    @c.dataset = db[:t]
    ds = @c.dataset
    proc{ds.offset(1).paged_delete}.must_raise Sequel::Error
  end

  it "should raise error for dataset with offset" do
    proc{@ds.offset(1).paged_delete}.must_raise Sequel::Error
    proc{@ds.offset(1).paged_update(:x=>1)}.must_raise Sequel::Error
    proc{@ds.offset(1).paged_datasets{}}.must_raise Sequel::Error
  end

  it "should raise error for model with composite primary key" do
    @c.set_primary_key [:id, :x]
    proc{@c.dataset.paged_delete}.must_raise Sequel::Error
    proc{@c.dataset.paged_update(:x=>1)}.must_raise Sequel::Error
    proc{@c.dataset.paged_datasets{}}.must_raise Sequel::Error
  end

  it "should raise error for model with no primary key" do
    @c.no_primary_key
    proc{@c.dataset.paged_delete}.must_raise Sequel::Error
    proc{@c.dataset.paged_update(:x=>1)}.must_raise Sequel::Error
    proc{@c.dataset.paged_datasets{}}.must_raise Sequel::Error
  end

  it "should offer paged_delete class method" do
    @c.paged_delete.must_equal 2002
    @db.sqls.must_equal [
      "SELECT id FROM albums ORDER BY id LIMIT 1 OFFSET 1000",
      "DELETE FROM albums WHERE (id < 1002)",
      "SELECT id FROM albums ORDER BY id LIMIT 1 OFFSET 1000",
      "DELETE FROM albums WHERE (id < 2002)",
      "SELECT id FROM albums ORDER BY id LIMIT 1 OFFSET 1000",
      "DELETE FROM albums"
    ]
  end

  it "should offer paged_update class method" do
    @c.paged_update(:x=>1).must_equal 2002
    @db.sqls.must_equal [
      "SELECT id FROM albums ORDER BY id LIMIT 1 OFFSET 1000",
      "UPDATE albums SET x = 1 WHERE (id < 1002)",
      "SELECT id FROM albums WHERE (id >= 1002) ORDER BY id LIMIT 1 OFFSET 1000",
      "UPDATE albums SET x = 1 WHERE ((id < 2002) AND (id >= 1002))",
      "SELECT id FROM albums WHERE (id >= 2002) ORDER BY id LIMIT 1 OFFSET 1000",
      "UPDATE albums SET x = 1 WHERE (id >= 2002)"
    ]
  end

  it "should offer paged_datasets class method" do
    @c.paged_datasets.map(&:sql).must_equal [
      "SELECT * FROM albums WHERE (id < 1002)",
      "SELECT * FROM albums WHERE ((id < 2002) AND (id >= 1002))",
      "SELECT * FROM albums WHERE (id >= 2002)"
    ]
    @db.sqls.must_equal [
      "SELECT id FROM albums ORDER BY id LIMIT 1 OFFSET 1000",
      "SELECT id FROM albums WHERE (id >= 1002) ORDER BY id LIMIT 1 OFFSET 1000",
      "SELECT id FROM albums WHERE (id >= 2002) ORDER BY id LIMIT 1 OFFSET 1000",
    ]
  end
end
