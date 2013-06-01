require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

Sequel.extension :pg_array, :pg_range, :pg_range_ops

describe "Sequel::Postgres::RangeOp" do
  before do
    @ds = Sequel.connect('mock://postgres', :quote_identifiers=>false).dataset
    @h = Sequel.pg_range_op(:h)
  end

  it "#pg_range should return self" do
    @h.pg_range.should equal(@h)
  end

  it "Sequel.pg_range_op should return argument if already a RangeOp" do
    Sequel.pg_range_op(@h).should equal(@h)
  end

  it "Sequel.pg_range should return a new RangeOp if not given a range" do
    @ds.literal(Sequel.pg_range(:h).lower).should == "lower(h)"
  end

  it "#pg_range should return a RangeOp for literal strings, and expressions" do
    @ds.literal(Sequel.function(:b, :h).pg_range.lower).should == "lower(b(h))"
    @ds.literal(Sequel.lit('h').pg_range.lower).should == "lower(h)"
  end

  it "PGRange#op should return a RangeOp" do
    @ds.literal(Sequel.pg_range(1..2, :numrange).op.lower).should == "lower('[1,2]'::numrange)"
  end

  it "should define methods for all of the PostgreSQL range operators" do
    @ds.literal(@h.contains(@h)).should == "(h @> h)"
    @ds.literal(@h.contained_by(@h)).should == "(h <@ h)"
    @ds.literal(@h.overlaps(@h)).should == "(h && h)"
    @ds.literal(@h.left_of(@h)).should == "(h << h)"
    @ds.literal(@h.right_of(@h)).should == "(h >> h)"
    @ds.literal(@h.ends_before(@h)).should == "(h &< h)"
    @ds.literal(@h.starts_after(@h)).should == "(h &> h)"
    @ds.literal(@h.adjacent_to(@h)).should == "(h -|- h)"
  end

  qspecify "should define methods for the deprecated PostgreSQL range operators" do
    @ds.literal(@h.starts_before(@h)).should == "(h &< h)"
    @ds.literal(@h.ends_after(@h)).should == "(h &> h)"
  end

  it "should define methods for all of the PostgreSQL range functions" do
    @ds.literal(@h.lower).should == "lower(h)"
    @ds.literal(@h.upper).should == "upper(h)"
    @ds.literal(@h.isempty).should == "isempty(h)"
    @ds.literal(@h.lower_inc).should == "lower_inc(h)"
    @ds.literal(@h.upper_inc).should == "upper_inc(h)"
    @ds.literal(@h.lower_inf).should == "lower_inf(h)"
    @ds.literal(@h.upper_inf).should == "upper_inf(h)"
  end

  it "+ - * operators should be defined and return a RangeOp" do
    @ds.literal((@h + @h).lower).should == "lower((h + h))"
    @ds.literal((@h * @h).lower).should == "lower((h * h))"
    @ds.literal((@h - @h).lower).should == "lower((h - h))"
  end
end
