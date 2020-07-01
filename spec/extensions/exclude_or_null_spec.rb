require_relative "spec_helper"

describe "exclude_or_null extension" do
  before do
    @ds = Sequel.mock[:t].extension(:exclude_or_null)
  end

  it "#exclude_or_null should add WHERE condition where a is false or NULL" do
    @ds.exclude_or_null(:a).sql.must_equal "SELECT * FROM t WHERE NOT coalesce(a, 'f')"
  end

  it "#exclude_or_null_having should add HAVING condition where a is false or NULL" do
    @ds.exclude_or_null_having(:a).sql.must_equal "SELECT * FROM t HAVING NOT coalesce(a, 'f')"
  end

  it "should not effect normal exclude" do
    @ds.exclude(:a).sql.must_equal "SELECT * FROM t WHERE NOT a"
  end
end
