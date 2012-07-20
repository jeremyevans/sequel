require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Postgres::PGRowOp" do
  before do
    @db = Sequel.connect('mock://postgres', :quote_identifiers=>false)
    @a = Sequel.pg_row_op(:a)
  end

  it "#[] should access members of the composite type" do
    @db.literal(@a[:b]).should == "(a).b"
  end

  it "#[] should be chainable" do
    @db.literal(@a[:b][:c]).should == "((a).b).c"
  end

  it "#[] should support array access if not given an identifier" do
    @db.literal(@a[:b][1]).should == "(a).b[1]"
  end

  it "#[] should be chainable with array access" do
    @db.literal(@a[1][:b]).should == "(a[1]).b"
  end

  it "#splat should return a splatted argument" do
    @db.literal(@a.splat).should == "(a.*)"
  end

  it "#splat(type) should return a splatted argument cast to given type" do
    @db.literal(@a.splat(:b)).should == "(a.*)::b"
  end

  it "#splat should not work on an already accessed composite type" do
    proc{@a[:a].splat(:b)}.should raise_error(Sequel::Error)
  end

  it "#pg_row should be callable on literal strings" do
    @db.literal(Sequel.lit('a').pg_row[:b]).should == "(a).b"
  end

  it "#pg_row should be callable on Sequel expressions" do
    @db.literal(Sequel.function(:a).pg_row[:b]).should == "(a()).b"
  end

  it "Sequel.pg_row should work as well if the pg_row extension is loaded" do
    @db.literal(Sequel.pg_row(Sequel.function(:a))[:b]).should == "(a()).b"
  end
end
