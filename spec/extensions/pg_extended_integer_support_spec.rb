require_relative "spec_helper"

describe "pg_extended_integer_support extension" do
  before do
    @db = Sequel.mock(:host=>'postgres').extension(:pg_extended_integer_support)
  end

  it "should literalize integers out of range using single quotes by default" do
    @db.literal(2**63).must_equal "'9223372036854775808'"
  end

  it "should literalize integers out of range without quotes when configured with :raw strategy" do
    @db.dataset.integer_outside_bigint_range_strategy(:raw).literal(2**63).must_equal "9223372036854775808"
  end

  it "should raise for integers out of range when configured with :raise strategy" do
    ds = @db.dataset.integer_outside_bigint_range_strategy(:raise)
    proc{ds.literal(2**63)}.must_raise Sequel::InvalidValue
  end

  it "should raise for integers out of range when configured with :quote strategy" do
    @db.dataset.integer_outside_bigint_range_strategy(:quote).literal(2**63).must_equal "'9223372036854775808'"
  end

  it "should respect :integer_outside_bigint_range_strategy Database option for strategy" do
    @db.opts[:integer_outside_bigint_range_strategy] = :raw
    @db.literal(2**63).must_equal "9223372036854775808"

    @db.opts[:integer_outside_bigint_range_strategy] = :quote
    @db.literal(2**63).must_equal "'9223372036854775808'"

    @db.opts[:integer_outside_bigint_range_strategy] = :raise
    proc{@db.literal(2**63)}.must_raise Sequel::InvalidValue
  end
end
