require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')

describe "Sequel sql_expr extension" do
  before do
    @ds = Sequel.mock.dataset
  end

  specify "Object#sql_expr should wrap the object in a GenericComplexExpression" do
    o = Object.new
    def o.sql_literal(ds) 'foo' end
    s = o.sql_expr
    @ds.literal(s).should == "foo"
    @ds.literal(s+1).should == "(foo + 1)"
    @ds.literal(s & true).should == "(foo AND 't')"
    @ds.literal(s < 1).should == "(foo < 1)"
    @ds.literal(s.sql_subscript(1)).should == "foo[1]"
    @ds.literal(s.like('a')).should == "(foo LIKE 'a')"
    @ds.literal(s.as(:a)).should == "foo AS a"
    @ds.literal(s.cast(Integer)).should == "CAST(foo AS integer)"
    @ds.literal(s.desc).should == "foo DESC"
    @ds.literal(s.sql_string + '1').should == "(foo || '1')"
  end

  specify "Numeric#sql_expr should wrap the object in a NumericExpression" do
    [1, 2.0, 2^70, BigDecimal.new('1.0')].each do |o|
      @ds.literal(o.sql_expr).should == @ds.literal(o)
      @ds.literal(o.sql_expr + 1).should == "(#{@ds.literal(o)} + 1)"
    end
  end

  specify "String#sql_expr should wrap the object in a StringExpression" do
    @ds.literal("".sql_expr).should == "''"
    @ds.literal("".sql_expr + :a).should == "('' || a)"
  end

  specify "NilClass, TrueClass, and FalseClass#sql_expr should wrap the object in a BooleanExpression" do
    [nil, true, false].each do |o|
      @ds.literal(o.sql_expr).should == @ds.literal(o)
      @ds.literal(o.sql_expr & :a).should == "(#{@ds.literal(o)} AND a)"
    end
  end

  specify "Proc#sql_expr should should treat the object as a virtual row block" do
    @ds.literal(proc{a}.sql_expr).should == "a"
    @ds.literal(proc{a__b}.sql_expr).should == "a.b"
    @ds.literal(proc{a(b)}.sql_expr).should == "a(b)"
  end

  specify "Proc#sql_expr should should wrap the object in a GenericComplexExpression if the object is not already an expression" do
    @ds.literal(proc{1}.sql_expr).should == "1"
    @ds.literal(proc{1}.sql_expr + 2).should == "(1 + 2)"
  end

  specify "Proc#sql_expr should should convert a hash or array of two element arrays to a BooleanExpression" do
    @ds.literal(proc{{a=>b}}.sql_expr).should == "(a = b)"
    @ds.literal(proc{[[a, b]]}.sql_expr & :a).should == "((a = b) AND a)"
  end
end
