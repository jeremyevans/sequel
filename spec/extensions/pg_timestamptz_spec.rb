require_relative "spec_helper"

describe "pg_timestamptz extension" do
  before do
    @db = Sequel.mock(:host=>'postgres').extension :pg_timestamptz
  end

  it "should use timestamptz as default timestamp type" do
    @db.create_table(:t){Time :t; DateTime :tz; Time :ot, :only_time=>true}
    @db.sqls.must_equal ['CREATE TABLE "t" ("t" timestamptz, "tz" timestamptz, "ot" time)']
  end

  it "should automatically cast Time and DateTime instances using TIMESTAMP WITH TIME ZONE when using auto_cast_date_and_time " do
    t = Time.utc(2000)
    dt = DateTime.new(2000)
    expected = "TIMESTAMP WITH TIME ZONE '2000-01-01 00:00:00.000000+0000'"

    @db.timezone = :utc
    @db.extension :auto_cast_date_and_time
    @db.literal(t).must_equal expected
    @db.literal(dt).must_equal expected

    db = Sequel.mock(:host=>'postgres').extension :auto_cast_date_and_time
    db.extension :pg_timestamptz
    db.literal(t).must_equal expected
    db.literal(dt).must_equal expected
  end

  it "should use timestamptz when casting" do
    @db.get(Sequel.cast('a', Time))
    @db.sqls.must_equal ["SELECT CAST('a' AS timestamptz) AS \"v\" LIMIT 1"]
  end
end
