require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "no_auto_literal_strings extension" do
  before do
    @ds = Sequel.mock[:t].extension(:no_auto_literal_strings)
  end

  it "should raise exception for plain strings in filter methods" do
    proc{@ds.where("a")}.must_raise Sequel::Error
    proc{@ds.having("a")}.must_raise Sequel::Error
    proc{@ds.filter("a")}.must_raise Sequel::Error
    proc{@ds.exclude_where("a")}.must_raise Sequel::Error
    proc{@ds.exclude_having("a")}.must_raise Sequel::Error
    proc{@ds.and("a")}.must_raise Sequel::Error
    proc{@ds.where(:a).or("a")}.must_raise Sequel::Error
    proc{@ds.first("a")}.must_raise Sequel::Error
    proc{@ds.order(:a).last("a")}.must_raise Sequel::Error
    proc{@ds["a"]}.must_raise Sequel::Error
  end
  
  it "should raise exception for plain strings arrays in filter methods" do
    proc{@ds.where(["a"])}.must_raise Sequel::Error
  end

  it "should handle explicit literal strings in filter methods" do
    @ds.where(Sequel.lit("a")).sql.must_equal 'SELECT * FROM t WHERE (a)'
    @ds.having(Sequel.lit("a")).sql.must_equal 'SELECT * FROM t HAVING (a)'
    @ds.filter(Sequel.lit("a")).sql.must_equal 'SELECT * FROM t WHERE (a)'
    @ds.exclude_where(Sequel.lit("a")).sql.must_equal 'SELECT * FROM t WHERE NOT (a)'
    @ds.exclude_having(Sequel.lit("a")).sql.must_equal 'SELECT * FROM t HAVING NOT (a)'
    @ds.and(Sequel.lit("a")).sql.must_equal 'SELECT * FROM t WHERE (a)'
    @ds.where(:a).or(Sequel.lit("a")).sql.must_equal 'SELECT * FROM t WHERE (a OR (a))'
    @ds.first(Sequel.lit("a"))
    @ds.order(:a).last(Sequel.lit("a"))
    @ds[Sequel.lit("a")]
    @ds.db.sqls.must_equal ["SELECT * FROM t WHERE (a) LIMIT 1",
                            "SELECT * FROM t WHERE (a) ORDER BY a DESC LIMIT 1",
                            "SELECT * FROM t WHERE (a) LIMIT 1"]
  end
  
  it "should handle literal strings in arrays in filter methods" do
    @ds.where([Sequel.lit("a")]).sql.must_equal 'SELECT * FROM t WHERE (a)'
  end

  it "should handle other objects in filter methods" do
    @ds.where(:a).sql.must_equal 'SELECT * FROM t WHERE a'
  end
  
  it "should raise exception for plain strings in update methods" do
    proc{@ds.update("a = a + 1")}.must_raise Sequel::Error
    proc{@ds.update_sql("a = a + 1")}.must_raise Sequel::Error
  end
  
  it "should handle explicit literal strings in update methods" do
    @ds.update_sql(Sequel.lit("a = a + 1")).must_equal "UPDATE t SET a = a + 1"
    @ds.update(Sequel.lit("a = a + 1"))
    @ds.db.sqls.must_equal ["UPDATE t SET a = a + 1"]
  end
  
  it "should handle other objects in update methods" do
    @ds.update_sql(:a=>:a).must_equal "UPDATE t SET a = a"
    @ds.update(:a=>:a)
    @ds.db.sqls.must_equal ["UPDATE t SET a = a"]
  end
end
