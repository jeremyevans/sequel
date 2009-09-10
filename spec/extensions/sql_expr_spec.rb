require File.join(File.dirname(__FILE__), 'spec_helper')

context "Sequel sql_expr extension" do
  specify "Object#sql_expr should wrap the object in a GenericComplexExpression" do
    o = Object.new
    s = o.sql_expr
    s.should be_a_kind_of(Sequel::SQL::GenericComplexExpression)
    s.op.should == :NOOP
    s.args.should == [o]
    (s+1).should be_a_kind_of(Sequel::SQL::NumericExpression)
    (s & true).should be_a_kind_of(Sequel::SQL::BooleanExpression)
    (s < 1).should be_a_kind_of(Sequel::SQL::BooleanExpression)
    s.sql_subscript(1).should be_a_kind_of(Sequel::SQL::Subscript)
    s.like('a').should be_a_kind_of(Sequel::SQL::BooleanExpression)
    s.as(:a).should be_a_kind_of(Sequel::SQL::AliasedExpression)
    s.cast(Integer).should be_a_kind_of(Sequel::SQL::Cast)
    s.desc.should be_a_kind_of(Sequel::SQL::OrderedExpression)
    s.sql_string.should be_a_kind_of(Sequel::SQL::StringExpression)
  end

  specify "Numeric#sql_expr should wrap the object in a NumericExpression" do
    [1, 2.0, 2^40, BigDecimal.new('1.0')].each do |o|
      s = o.sql_expr
      s.should be_a_kind_of(Sequel::SQL::NumericExpression)
      s.op.should == :NOOP
      s.args.should == [o]
    end
  end

  specify "String#sql_expr should wrap the object in a StringExpression" do
    o = ""
    s = o.sql_expr
    s.should be_a_kind_of(Sequel::SQL::StringExpression)
    s.op.should == :NOOP
    s.args.should == [o]
  end

  specify "NilClass, TrueClass, and FalseClass#sql_expr should wrap the object in a BooleanExpression" do
    [nil, true, false].each do |o|
      s = o.sql_expr
      s.should be_a_kind_of(Sequel::SQL::BooleanExpression)
      s.op.should == :NOOP
      s.args.should == [o]
    end
  end
end
