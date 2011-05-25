require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model, "to_dot extension" do
  def dot(ds)
    Sequel::ToDot.new(ds).instance_variable_get(:@dot)[4...-1]
  end

  before do
    @db = MODEL_DB
    @ds = @db.dataset
  end

  it "should output a string suitable for input to the graphviz dot program" do
    @ds.to_dot.should == (<<END
digraph G {
0 [label="self"];
0 -> 1 [label=""];
1 [label="Dataset"];
}
END
).strip
  end

  it "should handle an empty dataset" do
    dot(@ds).should == []
  end

  it "should handle WITH" do
    a = dot(@ds.with(:a, @ds))
    a[0..3].should == ["1 -> 2 [label=\"with\"];", "2 [label=\"Array\"];", "2 -> 3 [label=\"0\"];", "3 [label=\"Hash\"];"]
    [["3 -> 4 [label=\"dataset\"];", "4 [label=\"Dataset\"];", "3 -> 5 [label=\"name\"];", "5 [label=\":a\"];"],
     ["3 -> 4 [label=\"name\"];", "4 [label=\":a\"];", "3 -> 5 [label=\"dataset\"];", "5 [label=\"Dataset\"];"]].should include(a[4..-1])
  end

  it "should handle DISTINCT" do
    dot(@ds.distinct).should == ["1 -> 2 [label=\"distinct\"];", "2 [label=\"Array\"];"]
  end

  it "should handle FROM" do
    dot(@ds.from(:a)).should == ["1 -> 2 [label=\"from\"];", "2 [label=\"Array\"];", "2 -> 3 [label=\"0\"];", "3 [label=\":a\"];"]
  end

  it "should handle JOIN" do
    dot(@ds.join(:a)).should == ["1 -> 2 [label=\"join\"];", "2 [label=\"Array\"];", "2 -> 3 [label=\"0\"];", "3 [label=\"INNER JOIN\"];", "3 -> 4 [label=\"table\"];", "4 [label=\":a\"];"]
  end

  it "should handle WHERE" do
    dot(@ds.filter(true)).should == ["1 -> 2 [label=\"where\"];", "2 [label=\"ComplexExpression: NOOP\"];", "2 -> 3 [label=\"0\"];", "3 [label=\"true\"];"]
  end

  it "should handle GROUP" do
    dot(@ds.group(:a)).should == ["1 -> 2 [label=\"group\"];", "2 [label=\"Array\"];", "2 -> 3 [label=\"0\"];", "3 [label=\":a\"];"]
  end

  it "should handle HAVING" do
    dot(@ds.having(:a)).should == ["1 -> 2 [label=\"having\"];", "2 [label=\":a\"];"]
  end

  it "should handle UNION" do
    dot(@ds.union(@ds)).should == ["1 -> 2 [label=\"from\"];", "2 [label=\"Array\"];", "2 -> 3 [label=\"0\"];", "3 [label=\"AliasedExpression\"];", "3 -> 4 [label=\"expression\"];", "4 [label=\"Dataset\"];", "4 -> 5 [label=\"compounds\"];", "5 [label=\"Array\"];", "5 -> 6 [label=\"0\"];", "6 [label=\"Array\"];", "6 -> 7 [label=\"0\"];", "7 [label=\":union\"];", "6 -> 8 [label=\"1\"];", "8 [label=\"Dataset\"];", "6 -> 9 [label=\"2\"];", "9 [label=\"nil\"];", "3 -> 10 [label=\"alias\"];", "10 [label=\":t1\"];"]
  end

  it "should handle INTERSECT" do
    dot(@ds.intersect(@ds)).should == ["1 -> 2 [label=\"from\"];", "2 [label=\"Array\"];", "2 -> 3 [label=\"0\"];", "3 [label=\"AliasedExpression\"];", "3 -> 4 [label=\"expression\"];", "4 [label=\"Dataset\"];", "4 -> 5 [label=\"compounds\"];", "5 [label=\"Array\"];", "5 -> 6 [label=\"0\"];", "6 [label=\"Array\"];", "6 -> 7 [label=\"0\"];", "7 [label=\":intersect\"];", "6 -> 8 [label=\"1\"];", "8 [label=\"Dataset\"];", "6 -> 9 [label=\"2\"];", "9 [label=\"nil\"];", "3 -> 10 [label=\"alias\"];", "10 [label=\":t1\"];"]
  end

  it "should handle EXCEPT" do
    dot(@ds.except(@ds)).should == ["1 -> 2 [label=\"from\"];", "2 [label=\"Array\"];", "2 -> 3 [label=\"0\"];", "3 [label=\"AliasedExpression\"];", "3 -> 4 [label=\"expression\"];", "4 [label=\"Dataset\"];", "4 -> 5 [label=\"compounds\"];", "5 [label=\"Array\"];", "5 -> 6 [label=\"0\"];", "6 [label=\"Array\"];", "6 -> 7 [label=\"0\"];", "7 [label=\":except\"];", "6 -> 8 [label=\"1\"];", "8 [label=\"Dataset\"];", "6 -> 9 [label=\"2\"];", "9 [label=\"nil\"];", "3 -> 10 [label=\"alias\"];", "10 [label=\":t1\"];"]
  end

  it "should handle ORDER" do
    dot(@ds.order(:a)).should == ["1 -> 2 [label=\"order\"];", "2 [label=\"Array\"];", "2 -> 3 [label=\"0\"];", "3 [label=\":a\"];"]
  end

  it "should handle LIMIT and OFFSET" do
    dot(@ds.limit(1, 2)).should == ["1 -> 2 [label=\"limit\"];", "2 [label=\"1\"];", "1 -> 3 [label=\"offset\"];", "3 [label=\"2\"];"]
  end

  it "should handle FOR UPDATE" do
    dot(@ds.for_update).should == ["1 -> 2 [label=\"lock\"];", "2 [label=\":update\"];"]
  end

  it "should handle LiteralStrings" do
    dot(@ds.filter('a')).should == ["1 -> 2 [label=\"where\"];", "2 [label=\"\\\"(a)\\\".lit\"];"]
  end

  it "should handle true, false, nil" do
    dot(@ds.select(true, false, nil)).should == ["1 -> 2 [label=\"select\"];", "2 [label=\"Array\"];", "2 -> 3 [label=\"0\"];", "3 [label=\"true\"];", "2 -> 4 [label=\"1\"];", "4 [label=\"false\"];", "2 -> 5 [label=\"2\"];", "5 [label=\"nil\"];"]
  end

  it "should handle SQL::ComplexExpressions" do
    dot(@ds.filter(:a=>:b)).should == ["1 -> 2 [label=\"where\"];", "2 [label=\"ComplexExpression: =\"];", "2 -> 3 [label=\"0\"];", "3 [label=\":a\"];", "2 -> 4 [label=\"1\"];", "4 [label=\":b\"];"]
  end

  it "should handle SQL::Identifiers" do
    dot(@ds.select{a}).should == ["1 -> 2 [label=\"select\"];", "2 [label=\"Array\"];", "2 -> 3 [label=\"0\"];", "3 [label=\"Identifier\"];", "3 -> 4 [label=\"value\"];", "4 [label=\":a\"];"]
  end

  it "should handle SQL::QualifiedIdentifiers" do
    dot(@ds.select{a__b}).should == ["1 -> 2 [label=\"select\"];", "2 [label=\"Array\"];", "2 -> 3 [label=\"0\"];", "3 [label=\"QualifiedIdentifier\"];", "3 -> 4 [label=\"table\"];", "4 [label=\"\\\"a\\\"\"];", "3 -> 5 [label=\"column\"];", "5 [label=\"\\\"b\\\"\"];"]
  end

  it "should handle SQL::OrderedExpressions" do
    dot(@ds.order(:a.desc(:nulls=>:last))).should == ["1 -> 2 [label=\"order\"];", "2 [label=\"Array\"];", "2 -> 3 [label=\"0\"];", "3 [label=\"OrderedExpression: DESC NULLS LAST\"];", "3 -> 4 [label=\"expression\"];", "4 [label=\":a\"];"]
  end

  it "should handle SQL::AliasedExpressions" do
    dot(@ds.from(:a.as(:b))).should == ["1 -> 2 [label=\"from\"];", "2 [label=\"Array\"];", "2 -> 3 [label=\"0\"];", "3 [label=\"AliasedExpression\"];", "3 -> 4 [label=\"expression\"];", "4 [label=\":a\"];", "3 -> 5 [label=\"alias\"];", "5 [label=\":b\"];"]
  end

  it "should handle SQL::CaseExpressions" do
    dot(@ds.select({:a=>:b}.case(:c, :d))).should == ["1 -> 2 [label=\"select\"];", "2 [label=\"Array\"];", "2 -> 3 [label=\"0\"];", "3 [label=\"CaseExpression\"];", "3 -> 4 [label=\"expression\"];", "4 [label=\":d\"];", "3 -> 5 [label=\"conditions\"];", "5 [label=\"Array\"];", "5 -> 6 [label=\"0\"];", "6 [label=\"Array\"];", "6 -> 7 [label=\"0\"];", "7 [label=\":a\"];", "6 -> 8 [label=\"1\"];", "8 [label=\":b\"];", "3 -> 9 [label=\"default\"];", "9 [label=\":c\"];"]
  end

  it "should handle SQL::Cast" do
    dot(@ds.select(:a.cast(Integer))).should ==  ["1 -> 2 [label=\"select\"];", "2 [label=\"Array\"];", "2 -> 3 [label=\"0\"];", "3 [label=\"Cast\"];", "3 -> 4 [label=\"expr\"];", "4 [label=\":a\"];", "3 -> 5 [label=\"type\"];", "5 [label=\"Integer\"];"]
  end

  it "should handle SQL::Function" do
    dot(@ds.select{a(b)}).should == ["1 -> 2 [label=\"select\"];", "2 [label=\"Array\"];", "2 -> 3 [label=\"0\"];", "3 [label=\"Function: a\"];", "3 -> 4 [label=\"0\"];", "4 [label=\"Identifier\"];", "4 -> 5 [label=\"value\"];", "5 [label=\":b\"];"]
  end

  it "should handle SQL::Subscript" do
    dot(@ds.select(:a.sql_subscript(1))).should == ["1 -> 2 [label=\"select\"];", "2 [label=\"Array\"];", "2 -> 3 [label=\"0\"];", "3 [label=\"Subscript\"];", "3 -> 4 [label=\"f\"];", "4 [label=\":a\"];", "3 -> 5 [label=\"sub\"];", "5 [label=\"Array\"];", "5 -> 6 [label=\"0\"];", "6 [label=\"1\"];"]
  end

  it "should handle SQL::WindowFunction" do
    dot(@ds.select{sum(:over, :partition=>(:a)){}}).should == ["1 -> 2 [label=\"select\"];", "2 [label=\"Array\"];", "2 -> 3 [label=\"0\"];", "3 [label=\"WindowFunction\"];", "3 -> 4 [label=\"function\"];", "4 [label=\"Function: sum\"];", "3 -> 5 [label=\"window\"];", "5 [label=\"Window\"];", "5 -> 6 [label=\"opts\"];", "6 [label=\"Hash\"];", "6 -> 7 [label=\"partition\"];", "7 [label=\":a\"];"]
  end

  it "should handle SQL::PlaceholderLiteralString" do
    dot(@ds.where("?", true)).should == ["1 -> 2 [label=\"where\"];", "2 [label=\"PlaceholderLiteralString: \\\"(?)\\\"\"];", "2 -> 3 [label=\"args\"];", "3 [label=\"Array\"];", "3 -> 4 [label=\"0\"];", "4 [label=\"true\"];"]
  end

  it "should handle JOIN ON" do
    dot(@ds.from(:a).join(:d, :b=>:c)).should == ["1 -> 2 [label=\"from\"];", "2 [label=\"Array\"];", "2 -> 3 [label=\"0\"];", "3 [label=\":a\"];", "1 -> 4 [label=\"join\"];", "4 [label=\"Array\"];", "4 -> 5 [label=\"0\"];", "5 [label=\"INNER JOIN ON\"];", "5 -> 6 [label=\"table\"];", "6 [label=\":d\"];", "5 -> 7 [label=\"on\"];", "7 [label=\"Array\"];", "7 -> 8 [label=\"0\"];", "8 [label=\"Array\"];", "8 -> 9 [label=\"0\"];", "9 [label=\"QualifiedIdentifier\"];", "9 -> 10 [label=\"table\"];", "10 [label=\"\\\"d\\\"\"];", "9 -> 11 [label=\"column\"];", "11 [label=\"\\\"b\\\"\"];", "8 -> 12 [label=\"1\"];", "12 [label=\"QualifiedIdentifier\"];", "12 -> 13 [label=\"table\"];", "13 [label=\"\\\"a\\\"\"];", "12 -> 14 [label=\"column\"];", "14 [label=\"\\\"c\\\"\"];"]
  end

  it "should handle JOIN USING" do
    dot(@ds.from(:a).join(:d, [:c], :table_alias=>:c)).should == ["1 -> 2 [label=\"from\"];", "2 [label=\"Array\"];", "2 -> 3 [label=\"0\"];", "3 [label=\":a\"];", "1 -> 4 [label=\"join\"];", "4 [label=\"Array\"];", "4 -> 5 [label=\"0\"];", "5 [label=\"INNER JOIN USING\"];", "5 -> 6 [label=\"table\"];", "6 [label=\":d\"];", "5 -> 7 [label=\"alias\"];", "7 [label=\":c\"];", "5 -> 8 [label=\"using\"];", "8 [label=\"Array\"];", "8 -> 9 [label=\"0\"];", "9 [label=\":c\"];"]
  end

  it "should handle other types" do
    o = Object.new
    def o.inspect
      "blah"
    end
    dot(@ds.select(o)).should == ["1 -> 2 [label=\"select\"];", "2 [label=\"Array\"];", "2 -> 3 [label=\"0\"];", "3 [label=\"Unhandled: blah\"];"]
  end
end
