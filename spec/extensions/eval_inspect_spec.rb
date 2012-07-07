require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "eval_inspect extension" do
  before do
    @ds = Sequel.mock.dataset
    @ds.meta_def(:supports_window_functions?){true}
    @ds.meta_def(:literal_blob_append){|sql, s| sql << "X'#{s}'"}
  end

  specify "should make eval(obj.inspect) == obj for all Sequel::SQL::Expression subclasses" do
    [
      # Objects with components where eval(inspect) == self
      Sequel::SQL::AliasedExpression.new(:b, :a),
      Sequel::SQL::CaseExpression.new({:b=>:a}, :c),
      Sequel::SQL::CaseExpression.new({:b=>:a}, :c, :d),
      Sequel::SQL::Cast.new(:a, :b),
      Sequel::SQL::ColumnAll.new(:a),
      Sequel::SQL::ComplexExpression.new(:'=', :b, :a),
      Sequel::SQL::Constant.new(:a),
      Sequel::SQL::Function.new(:a, :b, :c),
      Sequel::SQL::Identifier.new(:a),
      Sequel::SQL::JoinClause.new(:inner, :b, :c),
      Sequel::SQL::JoinOnClause.new({:d=>:a}, :inner, :b, :c),
      Sequel::SQL::JoinUsingClause.new([:a], :inner, :b, :c),
      Sequel::SQL::PlaceholderLiteralString.new('? = ?', [:a, :b]),
      Sequel::SQL::PlaceholderLiteralString.new(':a = :b', [{:a=>:b, :b=>42}]),
      Sequel::SQL::OrderedExpression.new(:a),
      Sequel::SQL::OrderedExpression.new(:a, false),
      Sequel::SQL::OrderedExpression.new(:a, false, :nulls=>:first),
      Sequel::SQL::OrderedExpression.new(:a, false, :nulls=>:last),
      Sequel::SQL::QualifiedIdentifier.new(:b, :a),
      Sequel::SQL::Subscript.new(:a, [1, 2]),
      Sequel::SQL::Window.new(:order=>:a, :partition=>:b),
      Sequel::SQL::WindowFunction.new(Sequel::SQL::Function.new(:a, :b, :c), Sequel::SQL::Window.new(:order=>:a, :partition=>:b)),
      Sequel::SQL::Wrapper.new(:a),

      # Objects with components where eval(inspect) != self
      Sequel::SQL::AliasedExpression.new(Sequel::SQL::Blob.new('s'), :a),
      Sequel::SQL::AliasedExpression.new(Sequel::LiteralString.new('s'), :a),
      Sequel::SQL::PlaceholderLiteralString.new('(a, b) IN ?', [Sequel::SQL::ValueList.new([[1, 2]])]),
      Sequel::SQL::CaseExpression.new({{:d=>Sequel::LiteralString.new('e')}=>:a}, :c, :d),
      Sequel::SQL::AliasedExpression.new(Date.new(2011, 10, 11), :a),
      Sequel::SQL::AliasedExpression.new(Sequel::SQLTime.create(10, 20, 30, 500000.125), :a),
      Sequel::SQL::AliasedExpression.new(DateTime.new(2011, 9, 11, 10, 20, 30), :a),
      Sequel::SQL::AliasedExpression.new(DateTime.new(2011, 9, 11, 10, 20, 30, 0.25), :a),
      Sequel::SQL::AliasedExpression.new(DateTime.new(2011, 9, 11, 10, 20, 30, -0.25), :a),
      Sequel::SQL::AliasedExpression.new(Time.local(2011, 9, 11, 10, 20, 30), :a),
      Sequel::SQL::AliasedExpression.new(Time.local(2011, 9, 11, 10, 20, 30, 500000.125), :a),
      Sequel::SQL::AliasedExpression.new(Time.utc(2011, 9, 11, 10, 20, 30), :a),
      Sequel::SQL::AliasedExpression.new(Time.utc(2011, 9, 11, 10, 20, 30, 500000.125), :a),
      Sequel::SQL::AliasedExpression.new(BigDecimal.new('1.000000000000000000000000000000000000000000000001'), :a),
    ].each do |o|
      v = eval(o.inspect)
      v.should == o
      @ds.literal(v).should == @ds.literal(o)
    end
  end
end
