require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "filter_having extension" do
  before do
    @ds = Sequel.mock[:t].extension(:filter_having)
    @dsh = @ds.having(:a)
  end

  it "should make filter operate on HAVING clause if dataset has a HAVING clause" do
    @dsh.filter(:b).sql.must_equal 'SELECT * FROM t HAVING (a AND b)'
  end

  it "should make filter operate on WHERE clause if dataset does not have a HAVING clause" do
    @ds.filter(:b).sql.must_equal 'SELECT * FROM t WHERE b'
  end

  it "should make and operate on HAVING clause if dataset has a HAVING clause" do
    @dsh.and(:b).sql.must_equal 'SELECT * FROM t HAVING (a AND b)'
  end

  it "should make and operate on WHERE clause if dataset does not have a HAVING clause" do
    @ds.where(:a).and(:b).sql.must_equal 'SELECT * FROM t WHERE (a AND b)'
  end

  it "should make or operate on HAVING clause if dataset has a HAVING clause" do
    @dsh.or(:b).sql.must_equal 'SELECT * FROM t HAVING (a OR b)'
  end

  it "should make or operate on WHERE clause if dataset does not have a HAVING clause" do
    @ds.where(:a).or(:b).sql.must_equal 'SELECT * FROM t WHERE (a OR b)'
  end

  it "should make exclude operate on HAVING clause if dataset has a HAVING clause" do
    @dsh.exclude(:b).sql.must_equal 'SELECT * FROM t HAVING (a AND NOT b)'
  end

  it "should make exclude operate on WHERE clause if dataset does not have a HAVING clause" do
    @ds.exclude(:b).sql.must_equal 'SELECT * FROM t WHERE NOT b'
  end
end
