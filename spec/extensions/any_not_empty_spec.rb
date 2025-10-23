require_relative "spec_helper"

describe "any_not_empty extension" do
  before do
    @ds = Sequel.mock[:t].extension(:any_not_empty)
  end

  it "should use a limited query if no block is given" do
    @ds.with_fetch(:one=>1).any?.must_equal true
    @ds.db.sqls.must_equal ["SELECT 1 AS one FROM t LIMIT 1"]
    @ds.with_fetch([]).any?.must_equal false
    @ds.db.sqls.must_equal ["SELECT 1 AS one FROM t LIMIT 1"]
  end

  it "should use a limited query if called on a model" do
    @c = Sequel::Model(@ds)
    @ds.db.sqls
    @ds.db.fetch = {:one=>1}
    @c.any?.must_equal true
    @ds.db.sqls.must_equal ["SELECT 1 AS one FROM t LIMIT 1"]
    @ds.db.fetch = []
    @c.any?.must_equal false
    @ds.db.sqls.must_equal ["SELECT 1 AS one FROM t LIMIT 1"]
  end

  it "should use default behavior if block is given" do
    @ds.with_fetch(:one=>1).any?{|x| x[:one] == 1}.must_equal true
    @ds.db.sqls.must_equal ["SELECT * FROM t"]
    @ds.with_fetch(:one=>1).any?{|x| x[:one] != 1}.must_equal false
    @ds.db.sqls.must_equal ["SELECT * FROM t"]
    @ds.with_fetch([]).any?{|x| x[:one] == 1}.must_equal false
    @ds.db.sqls.must_equal ["SELECT * FROM t"]
  end

  it "should use default behavior if argument is given" do
    @ds.with_fetch(:one=>1).any?(Hash).must_equal true
    @ds.db.sqls.must_equal ["SELECT * FROM t"]
    @ds.with_fetch(:one=>1).any?(Array).must_equal false
    @ds.db.sqls.must_equal ["SELECT * FROM t"]
    @ds.with_fetch([]).any?(Hash).must_equal false
    @ds.db.sqls.must_equal ["SELECT * FROM t"]
  end if RUBY_VERSION >= '2.5'
end
