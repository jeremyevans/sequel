require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')

describe "Sequel sql_expr extension" do
  specify "Object#sql_expr should wrap the object in a GenericComplexExpression" do
    o = Object.new
    s = o.sql_expr
    s.should == Sequel::SQL::GenericComplexExpression.new(:NOOP, o)
    (s+1).should == Sequel::SQL::NumericExpression.new(:+, s, 1)
    (s & true).should == Sequel::SQL::BooleanExpression.new(:AND, s, true)
    (s < 1).should == Sequel::SQL::BooleanExpression.new(:<, s, 1)
    s.sql_subscript(1).should == Sequel::SQL::Subscript.new(s, [1])
    s.like('a').should == Sequel::SQL::BooleanExpression.new(:LIKE, s, 'a')
    s.as(:a).should == Sequel::SQL::AliasedExpression.new(s, :a)
    s.cast(Integer).should == Sequel::SQL::Cast.new(s, Integer)
    s.desc.should == Sequel::SQL::OrderedExpression.new(s, true)
    s.sql_string.should == Sequel::SQL::StringExpression.new(:NOOP, s)
  end

  specify "Numeric#sql_expr should wrap the object in a NumericExpression" do
    [1, 2.0, 2^70, BigDecimal.new('1.0')].each do |o|
      o.sql_expr.should == Sequel::SQL::NumericExpression.new(:NOOP, o)
    end
  end

  specify "String#sql_expr should wrap the object in a StringExpression" do
    "".sql_expr.should == Sequel::SQL::StringExpression.new(:NOOP, "")
  end

  specify "NilClass, TrueClass, and FalseClass#sql_expr should wrap the object in a BooleanExpression" do
    [nil, true, false].each do |o|
      o.sql_expr.should == Sequel::SQL::BooleanExpression.new(:NOOP, o)
    end
  end

  specify "Proc#sql_expr should should treat the object as a virtual row block" do
    proc{a}.sql_expr.should == Sequel::SQL::Identifier.new(:a)
    proc{a__b}.sql_expr.should == Sequel::SQL::QualifiedIdentifier.new('a', 'b')
    proc{a(b)}.sql_expr.should == Sequel::SQL::Function.new(:a, Sequel::SQL::Identifier.new(:b))
  end

  specify "Proc#sql_expr should should wrap the object in a GenericComplexExpression if the object is not already an expression" do
    proc{1}.sql_expr.should == Sequel::SQL::GenericComplexExpression.new(:NOOP, 1)
  end

  specify "Proc#sql_expr should should convert a hash or array of two element arrays to a BooleanExpression" do
    proc{{a=>b}}.sql_expr.should == Sequel::SQL::BooleanExpression.new(:'=', Sequel::SQL::Identifier.new(:a), Sequel::SQL::Identifier.new(:b))
    proc{[[a,b]]}.sql_expr.should == Sequel::SQL::BooleanExpression.new(:'=', Sequel::SQL::Identifier.new(:a), Sequel::SQL::Identifier.new(:b))
  end
end
