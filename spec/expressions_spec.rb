require File.join(File.dirname(__FILE__), '../lib/sequel')

context "A Proc object containing a single comparison" do
  setup do
    @p1 = proc {a > 1}
    @e = @p1.to_expressions
  end
  
  specify "should compile into an array containing a single expression" do
    @e.should be_a_kind_of(Array)
    @e.size.should == 1
    
    expr = @e.first
    expr.left.should == :a
    expr.op.should == :gt
    expr.right.should == 1
  end
end

context "A Proc object containing numerous expressions" do
  setup do
    @p1 = proc {a > 1 && b < 5 && c <=> 4}
    @e = @p1.to_expressions
  end
  
  specify "should compile into a list of expressions" do
    @e.should be_a_kind_of(Array)
    @e.size.should == 3
    
    e1 = @e[0]
    e1.left.should == :a
    e1.op.should == :gt
    e1.right.should == 1
    
    e2 = @e[1]
    e2.left.should == :b
    e2.op.should == :lt
    e2.right.should == 5
    
    e3 = @e[2]
    e3.left.should == :c
    e3.op.should == :not
    e3.right.should == 4
  end
end

context "Expression" do
  setup do
    @e = Sequel::Dataset::Expression.new(:a)
  end

  specify "should support ==" do
    @e == 3
    @e.op.should == :eql
    @e.right.should == 3
  end

  specify "should support <=> (!=)" do
    @e <=> 3
    @e.op.should == :not
    @e.right.should == 3
  end

  specify "should support >" do
    @e > 3
    @e.op.should == :gt
    @e.right.should == 3
  end

  specify "should support <" do
    @e < 3
    @e.op.should == :lt
    @e.right.should == 3
  end

  specify "should support >=" do
    @e >= 3
    @e.op.should == :gte
    @e.right.should == 3
  end

  specify "should support <=" do
    @e <= 3
    @e.op.should == :lte
    @e.right.should == 3
  end

  specify "should support =~" do
    @e =~ 3
    @e.op.should == :like
    @e.right.should == 3
  end
  
  specify "should support nil?" do
    @e.nil?
    @e.op.should == :eql
    @e.right.should == nil
  end
  
  specify "should support in" do
    @e.in 1..5
    @e.op.should == :eql
    @e.right.should == (1..5)
  end
  
  specify "should support in?" do
    @e.in? 1..5
    @e.op.should == :eql
    @e.right.should == (1..5)
  end
  
  specify "should support like" do
    @e.like "1028%"
    @e.op.should == :like
    @e.right.should == "1028%"
  end
  
  specify "should support like?" do
    @e.like? "1028%"
    @e.op.should == :like
    @e.right.should == "1028%"
  end
  
  specify "should support is_not" do
    @e.is_not 5
    @e.op.should == :not
    @e.right.should == 5
  end
  
  specify "should turn an unknown operator into a qualified field name" do
    @e.id <=> 5
    @e.left.should == 'a.id'
    @e.op.should == :not
    @e.right.should == 5
  end
end

context "An invalid expression" do
  specify "should raise a SequelError" do
    proc {proc {abc < Object.vzxczs}.to_expressions}.should raise_error(SequelError)
  end
end

context "Expressions" do
  specify "should support SUM" do
    e = proc {SUM(:test) >= 100}.to_expressions.first
    e.left.should == 'sum(test)'
    e.op.should == :gte
    e.right.should == 100
  end
end