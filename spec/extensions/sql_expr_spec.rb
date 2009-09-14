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

  specify "Proc#sql_expr should should treat the object as a virtual row block" do
    s = proc{a}.sql_expr
    s.should be_a_kind_of(Sequel::SQL::Identifier)
    s.value.should == :a

    s = proc{a__b}.sql_expr
    s.should be_a_kind_of(Sequel::SQL::QualifiedIdentifier)
    s.table.should == "a"
    s.column.should == "b"

    s = proc{a(b)}.sql_expr
    s.should be_a_kind_of(Sequel::SQL::Function)
    s.f.should == :a
    s.args.length.should == 1
    s.args.first.should be_a_kind_of(Sequel::SQL::Identifier)
    s.args.first.value.should == :b
  end

  specify "Proc#sql_expr should should wrap the object in a GenericComplexExpression if the object is not already an expression" do
    s = proc{1}.sql_expr
    s.should be_a_kind_of(Sequel::SQL::GenericComplexExpression)
    s.op.should == :NOOP
    s.args.should == [1]
  end

  specify "Proc#sql_expr should should convert a hash or array of two element arrays to a BooleanExpression" do
    s = proc{{a=>b}}.sql_expr
    s.should be_a_kind_of(Sequel::SQL::BooleanExpression)
    s.op.should == :"="
    s.args.first.should be_a_kind_of(Sequel::SQL::Identifier)
    s.args.first.value.should == :a
    s.args.last.should be_a_kind_of(Sequel::SQL::Identifier)
    s.args.last.value.should == :b

    s = proc{[[a,b]]}.sql_expr
    s.should be_a_kind_of(Sequel::SQL::BooleanExpression)
    s.op.should == :"="
    s.args.first.should be_a_kind_of(Sequel::SQL::Identifier)
    s.args.first.value.should == :a
    s.args.last.should be_a_kind_of(Sequel::SQL::Identifier)
    s.args.last.value.should == :b
  end
end
