require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

Sequel.extension :pg_inet, :pg_inet_ops

describe "Sequel::Postgres::InetOp" do
  before do
    @ds = Sequel.connect('mock://postgres', :quote_identifiers=>false).dataset
    @h = Sequel.pg_inet_op(:h)
  end

  it "#pg_inet should return self" do
    @h.pg_inet.must_be_same_as(@h)
  end

  it "Sequel.pg_inet_op should return argument if already an InetOp" do
    Sequel.pg_inet_op(@h).must_be_same_as(@h)
  end

  it "#pg_inet should return a InetOp for literal strings, and expressions" do
    @ds.literal(Sequel.function(:b, :h).pg_inet.abbrev).must_equal "abbrev(b(h))"
    @ds.literal(Sequel.lit('h').pg_inet.abbrev).must_equal "abbrev(h)"
  end

  it "should define methods for all of the PostgreSQL inet operators" do
    @ds.literal(@h.less_than(@h)).must_equal "(h < h)"
    @ds.literal(@h.less_than_or_equal(@h)).must_equal "(h <= h)"
    @ds.literal(@h.equals(@h)).must_equal "(h = h)"
    @ds.literal(@h.greater_than_or_equal(@h)).must_equal "(h >= h)"
    @ds.literal(@h.greater_than(@h)).must_equal "(h > h)"
    @ds.literal(@h.not_equal(@h)).must_equal "(h <> h)"
    @ds.literal(@h.contained_by(@h)).must_equal "(h << h)"
    @ds.literal(@h.contained_by_or_equals(@h)).must_equal "(h <<= h)"
    @ds.literal(@h.contains(@h)).must_equal "(h >> h)"
    @ds.literal(@h.contains_or_equals(@h)).must_equal "(h >>= h)"
    @ds.literal(@h.contains_or_contained_by(@h)).must_equal "(h && h)"
  end

  it "should define methods for all of the PostgreSQL inet functions" do
    @ds.literal(@h.abbrev).must_equal "abbrev(h)"
    @ds.literal(@h.broadcast).must_equal "broadcast(h)"
    @ds.literal(@h.family).must_equal "family(h)"
    @ds.literal(@h.host).must_equal "host(h)"
    @ds.literal(@h.hostmask).must_equal "hostmask(h)"
    @ds.literal(@h.masklen).must_equal "masklen(h)"
    @ds.literal(@h.netmask).must_equal "netmask(h)"
    @ds.literal(@h.network).must_equal "network(h)"
    @ds.literal(@h.text).must_equal "text(h)"
  end

end
