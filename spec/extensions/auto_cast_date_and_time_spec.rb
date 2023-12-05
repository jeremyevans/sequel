require_relative "spec_helper"

describe "auto_cast_date_and_time extension" do
  before do
    @db = Sequel.mock.extension :auto_cast_date_and_time
  end

  it "should automatically cast Time instances" do
    @db.literal(Time.local(2000)).must_equal "TIMESTAMP '2000-01-01 00:00:00.000000'"
  end

  it "should automatically cast DateTime instances" do
    @db.literal(DateTime.new(2000)).must_equal "TIMESTAMP '2000-01-01 00:00:00.000000'"
  end

  it "should automatically cast SQLTime instances" do
    @db.literal(Sequel::SQLTime.create(10, 20, 30)).must_equal "TIME '10:20:30.000000'"
  end

  it "should automatically cast Date instances" do
    @db.literal(Date.new(2000)).must_equal "DATE '2000-01-01'"
  end
end
