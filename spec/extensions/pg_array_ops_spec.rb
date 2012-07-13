require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Postgres::ArrayOp" do
  before do
    @db = Sequel.connect('mock://postgres', :quote_identifiers=>false)
    @a = Sequel.pg_array_op(:a)
  end

  it "should support the standard mathematical operators" do
    @db.literal(@a < @a).should == "(a < a)"
    @db.literal(@a <= @a).should == "(a <= a)"
    @db.literal(@a > @a).should == "(a > a)"
    @db.literal(@a >= @a).should == "(a >= a)"
  end

  it "#[] should support subscript access" do
    @db.literal(@a[1]).should == "a[1]"
    @db.literal(@a[1][2]).should == "a[1][2]"
  end

  it "#any should use the ANY method" do
    @db.literal(1=>@a.any).should == "(1 = ANY(a))"
  end

  it "#all should use the ALL method" do
    @db.literal(1=>@a.all).should == "(1 = ALL(a))"
  end

  it "#contains should use the @> operator" do
    @db.literal(@a.contains(:b)).should == "(a @> b)"
  end

  it "#contained_by should use the <@ operator" do
    @db.literal(@a.contained_by(:b)).should == "(a <@ b)"
  end

  it "#overlaps should use the && operator" do
    @db.literal(@a.overlaps(:b)).should == "(a && b)"
  end

  it "#push/concat should use the || operator in append mode" do
    @db.literal(@a.push(:b)).should == "(a || b)"
    @db.literal(@a.concat(:b)).should == "(a || b)"
  end

  it "#unshift should use the || operator in prepend mode" do
    @db.literal(@a.unshift(:b)).should == "(b || a)"
  end

  it "#dims should use the array_dims function" do
    @db.literal(@a.dims).should == "array_dims(a)"
  end

  it "#length should use the array_length function" do
    @db.literal(@a.length).should == "array_length(a, 1)"
    @db.literal(@a.length(2)).should == "array_length(a, 2)"
  end

  it "#length should use the array_lower function" do
    @db.literal(@a.lower).should == "array_lower(a, 1)"
    @db.literal(@a.lower(2)).should == "array_lower(a, 2)"
  end

  it "#to_string/join should use the array_to_string function" do
    @db.literal(@a.to_string).should == "array_to_string(a, '', NULL)"
    @db.literal(@a.join).should == "array_to_string(a, '', NULL)"
    @db.literal(@a.join(':')).should == "array_to_string(a, ':', NULL)"
    @db.literal(@a.join(':', '*')).should == "array_to_string(a, ':', '*')"
  end

  it "#unnest should use the unnest function" do
    @db.literal(@a.unnest).should == "unnest(a)"
  end

  it "#pg_array should return self" do
    @a.pg_array.should equal(@a)
  end

  it "Sequel.pg_array_op should return arg for ArrayOp" do
    Sequel.pg_array_op(@a).should equal(@a)
  end

  it "should be able to turn expressions into array ops using pg_array" do
    @db.literal(Sequel.qualify(:b, :a).pg_array.push(3)).should == "(b.a || 3)"
    @db.literal(Sequel.function(:a, :b).pg_array.push(3)).should == "(a(b) || 3)"
  end

  it "should be able to turn literal strings into array ops using pg_array" do
    @db.literal(Sequel.lit('a').pg_array.unnest).should == "unnest(a)"
  end

  it "should be able to turn symbols into array ops using Sequel.pg_array_op" do
    @db.literal(Sequel.pg_array_op(:a).unnest).should == "unnest(a)"
  end

  it "should be able to turn symbols into array ops using Sequel.pg_array" do
    @db.literal(Sequel.pg_array(:a).unnest).should == "unnest(a)"
  end

  it "should allow transforming PGArray instances into ArrayOp instances" do
    @db.literal(Sequel.pg_array([1,2]).op.push(3)).should == "(ARRAY[1,2] || 3)"
  end
end
