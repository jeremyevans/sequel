require_relative "spec_helper"

describe "set_literalizer extension" do
  before do
    @db = Sequel.mock
    deprecated do
      @db.extension :set_literalizer
    end
    @set = Set.new([1, 'c'])
    @empty_set = Set.new([])
    @cond_set = Set.new([[:c, 'c'], [:d, 2]])
  end

  it "should allow literalizing Set instances" do
    @db.literal(@set).must_equal "(1, 'c')"
    @db.literal(@empty_set).must_equal "(NULL)"
    @db.literal(@cond_set).must_equal "((c = 'c') AND (d = 2))"
  end

  it "should automatically handle Set instances in right hand side of condition specifiers the same as array instances" do
    @db[:a].where(:b=>@set).sql.must_equal "SELECT * FROM a WHERE (b IN (1, 'c'))"
    @db[:a].where(:b=>@empty_set).sql.must_equal "SELECT * FROM a WHERE (1 = 0)"
    @db[:a].where([:b, :e]=>@cond_set).sql.must_equal "SELECT * FROM a WHERE ((b, e) IN ((c, 'c'), (d, 2)))"
    @db[:a].where(:f=>@cond_set).sql.must_equal "SELECT * FROM a WHERE (f IN ((c = 'c') AND (d = 2)))"
    @db[:a].exclude(:b=>@set).sql.must_equal "SELECT * FROM a WHERE (b NOT IN (1, 'c'))"
    @db[:a].exclude(:b=>@empty_set).sql.must_equal "SELECT * FROM a WHERE (1 = 1)"
    @db[:a].exclude([:b, :e]=>@cond_set).sql.must_equal "SELECT * FROM a WHERE ((b, e) NOT IN ((c, 'c'), (d, 2)))"
    @db[:a].exclude(:f=>@cond_set).sql.must_equal "SELECT * FROM a WHERE (f NOT IN ((c = 'c') AND (d = 2)))"

    @db[:a].where(:b=>@set).sql.must_equal @db[:a].where(:b=>@set.to_a).sql
    @db[:a].where(:b=>@empty_set).sql.must_equal @db[:a].where(:b=>@empty_set.to_a).sql
    @db[:a].where([:b, :e]=>@cond_set).sql.must_equal @db[:a].where([:b, :e]=>@cond_set.to_a).sql
    @db[:a].where(:f=>@cond_set).sql.must_equal @db[:a].where(:f=>@cond_set.to_a).sql
    @db[:a].exclude(:b=>@set).sql.must_equal @db[:a].exclude(:b=>@set.to_a).sql
    @db[:a].exclude(:b=>@empty_set).sql.must_equal @db[:a].exclude(:b=>@empty_set.to_a).sql
    @db[:a].exclude([:b, :e]=>@cond_set).sql.must_equal @db[:a].exclude([:b, :e]=>@cond_set.to_a).sql
    @db[:a].exclude(:f=>@cond_set).sql.must_equal @db[:a].exclude(:f=>@cond_set.to_a).sql
  end

  it "should not affect literalization of other objects or complex expressions" do
    @db[:a].where(:b=>1).sql.must_equal "SELECT * FROM a WHERE (b = 1)"
    o = Object.new
    def o.sql_literal_append(ds, sql)
      sql << '2'
    end
    @db[:a].where(:b=>o).sql.must_equal "SELECT * FROM a WHERE (b = 2)"
  end
end
