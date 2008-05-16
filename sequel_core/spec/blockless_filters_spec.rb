require File.join(File.dirname(__FILE__), 'spec_helper')

context "Blockless Ruby Filters" do
  before do
    db = Sequel::Database.new
    db.quote_identifiers = false
    @d = db[:items]
    def @d.l(*args)
      literal(filter_expr(*args))
    end
  end
  
  it "should support boolean columns directly" do
    @d.l(:x).should == 'x'
  end
  
  it "should support NOT via Symbol#~" do
    @d.l(~:x).should == 'NOT x'
  end
  
  it "should support qualified columns" do
    @d.l(:x__y).should == 'x.y'
    @d.l(~:x__y).should == 'NOT x.y'
  end

  it "should support NOT with SQL functions" do
    @d.l(~:is_blah[]).should == 'NOT is_blah()'
    @d.l(~:is_blah[:x]).should == 'NOT is_blah(x)'
    @d.l(~:is_blah[:x__y]).should == 'NOT is_blah(x.y)'
    @d.l(~:is_blah[:x, :x__y]).should == 'NOT is_blah(x, x.y)'
  end

  it "should handle multiple ~" do
    @d.l(~~:x).should == 'x'
    @d.l(~~~:x).should == 'NOT x'
    @d.l(~~(:x > 100)).should == '(x > 100)'
    @d.l(~~(:x & :y)).should == '(x AND y)'
    @d.l(~~(:x | :y)).should == '(x OR y)'
  end

  it "should support >, <, >=, and <= via Symbol#>,<,>=,<=" do
    @d.l(:x > 100).should == '(x > 100)'
    @d.l(:x < 100.01).should == '(x < 100.01)'
    @d.l(:x >= 100000000000000000000000000000000000).should == '(x >= 100000000000000000000000000000000000)'
    @d.l(:x <= 100).should == '(x <= 100)'
  end
  
  it "should support negation of >, <, >=, and <= via Symbol#~" do
    @d.l(~(:x > 100)).should == '(x <= 100)'
    @d.l(~(:x < 100.01)).should == '(x >= 100.01)'
    @d.l(~(:x >= 100000000000000000000000000000000000)).should == '(x < 100000000000000000000000000000000000)'
    @d.l(~(:x <= 100)).should == '(x > 100)'
  end

  it "should support = via Hash" do
    @d.l(:x => 100).should == '(x = 100)'
    @d.l(:x => 'a').should == '(x = \'a\')'
    @d.l(:x => true).should == '(x = \'t\')'
    @d.l(:x => false).should == '(x = \'f\')'
    @d.l(:x => nil).should == '(x IS NULL)'
  end
  
  it "should support != via Hash#~" do
    @d.l(~{:x => 100}).should == '(x != 100)'
    @d.l(~{:x => 'a'}).should == '(x != \'a\')'
    @d.l(~{:x => true}).should == '(x != \'t\')'
    @d.l(~{:x => false}).should == '(x != \'f\')'
    @d.l(~{:x => nil}).should == '(x IS NOT NULL)'
  end
  
  it "should support ~ via Hash and Regexp (if supported by database)" do
    @d.l(:x => /blah/).should == '(x ~ \'blah\')'
  end
  
  it "should support !~ via Hash#~ and Regexp" do
    @d.l(~{:x => /blah/}).should == '(x !~ \'blah\')'
  end
  
  it "should support LIKE via Symbol#like" do
    @d.l(:x.like('a')).should == '(x LIKE \'a\')'
    @d.l(:x.like(/a/)).should == '(x ~ \'a\')'
    @d.l(:x.like('a', 'b')).should == '((x LIKE \'a\') OR (x LIKE \'b\'))'
    @d.l(:x.like(/a/, /b/)).should == '((x ~ \'a\') OR (x ~ \'b\'))'
    @d.l(:x.like('a', /b/)).should == '((x LIKE \'a\') OR (x ~ \'b\'))'
  end

  it "should support NOT LIKE via Symbol#like and Symbol#~" do
    @d.l(~:x.like('a')).should == '(x NOT LIKE \'a\')'
    @d.l(~:x.like(/a/)).should == '(x !~ \'a\')'
    @d.l(~:x.like('a', 'b')).should == '((x NOT LIKE \'a\') AND (x NOT LIKE \'b\'))'
    @d.l(~:x.like(/a/, /b/)).should == '((x !~ \'a\') AND (x !~ \'b\'))'
    @d.l(~:x.like('a', /b/)).should == '((x NOT LIKE \'a\') AND (x !~ \'b\'))'
  end

  it "should support negating ranges via Hash#~ and Range" do
    @d.l(~{:x => 1..5}).should == '((x < 1) OR (x > 5))'
    @d.l(~{:x => 1...5}).should == '((x < 1) OR (x >= 5))'
  end
  
  it "should support negating NOT IN via Hash#~ and Dataset or Array" do
    @d.l(~{:x => @d.select(:i)}).should == '(x NOT IN (SELECT i FROM items))'
    @d.l(~{:x => [1,2,3]}).should == '(x NOT IN (1, 2, 3))'
  end

  it "should support + - * / via Symbol#+,-,*,/" do
    @d.l(:x + 1 > 100).should == '((x + 1) > 100)'
    @d.l((:x * :y) < 100.01).should == '((x * y) < 100.01)'
    @d.l((:x - :y/2) >= 100000000000000000000000000000000000).should == '((x - (y / 2)) >= 100000000000000000000000000000000000)'
    @d.l((((:x - :y)/(:x + :y))*:z) <= 100).should == '((((x - y) / (x + y)) * z) <= 100)'
    @d.l(~((((:x - :y)/(:x + :y))*:z) <= 100)).should == '((((x - y) / (x + y)) * z) > 100)'
  end
  
  it "should only allow negation of boolean expressions" do
    @d.l(~(:x + 1 > 100)).should == '((x + 1) <= 100)'
    proc{@d.l(~(:x + 1))}.should raise_error(Sequel::Error)
  end

  it "should support AND conditions via &" do
    @d.l(:x & :y).should == '(x AND y)'
    @d.l(:x & :y & :z).should == '((x AND y) AND z)'
    @d.l(:x & {:y => :z}).should == '(x AND (y = z))'
    @d.l({:y => :z} & :x).should == '((y = z) AND x)'
    @d.l({:x => :a} & {:y => :z}).should == '((x = a) AND (y = z))'
    @d.l((:x > 200) & (:y < 200)).should == '((x > 200) AND (y < 200))'
    @d.l(:x & ~:y).should == '(x AND NOT y)'
    @d.l(~:x & :y).should == '(NOT x AND y)'
    @d.l(~:x & ~:y).should == '(NOT x AND NOT y)'
  end
  
  it "should support OR conditions via |" do
    @d.l(:x | :y).should == '(x OR y)'
    @d.l(:x | :y | :z).should == '((x OR y) OR z)'
    @d.l(:x | {:y => :z}).should == '(x OR (y = z))'
    @d.l({:y => :z} | :x).should == '((y = z) OR x)'
    @d.l({:x => :a} | {:y => :z}).should == '((x = a) OR (y = z))'
    @d.l((:x > 200) | (:y < 200)).should == '((x > 200) OR (y < 200))'
  end
  
  it "should support & | combinations" do
    @d.l((:x | :y) & :z).should == '((x OR y) AND z)'
    @d.l(:x | (:y & :z)).should == '(x OR (y AND z))'
    @d.l((:x & :w) | (:y & :z)).should == '((x AND w) OR (y AND z))'
  end
  
  it "should support & | with ~" do
    @d.l(~((:x | :y) & :z)).should == '((NOT x AND NOT y) OR NOT z)'
    @d.l(~(:x | (:y & :z))).should == '(NOT x AND (NOT y OR NOT z))'
    @d.l(~((:x & :w) | (:y & :z))).should == '((NOT x OR NOT w) AND (NOT y OR NOT z))'
    @d.l(~((:x > 200) | (:y & :z))).should == '((x <= 200) AND (NOT y OR NOT z))'
  end
  
  it "should support LiteralString" do
    @d.l('x'.lit).should == '(x)'
    @d.l(~'x'.lit).should == 'NOT x'
    @d.l(~~'x'.lit).should == '(x)'
    @d.l(~(('x'.lit | :y) & :z)).should == '((NOT x AND NOT y) OR NOT z)'
    @d.l(~(:x | 'y'.lit)).should == '(NOT x AND NOT y)'
    @d.l(~('x'.lit & 'y'.lit)).should == '(NOT x OR NOT y)'
    @d.l({'y'.lit => 'z'.lit} & 'x'.lit).should == '((y = z) AND x)'
    @d.l(('x'.lit > 200) & ('y'.lit < 200)).should == '((x > 200) AND (y < 200))'
    @d.l(~('x'.lit + 1 > 100)).should == '((x + 1) <= 100)'
    @d.l('x'.lit.like(/a/)).should == '(x ~ \'a\')'
    @d.l('x'.lit + 1 > 100).should == '((x + 1) > 100)'
    @d.l(('x'.lit * :y) < 100.01).should == '((x * y) < 100.01)'
    @d.l(('x'.lit - :y/2) >= 100000000000000000000000000000000000).should == '((x - (y / 2)) >= 100000000000000000000000000000000000)'
    @d.l(('z'.lit * (('x'.lit / :y)/(:x + :y))) <= 100).should == '((z * ((x / y) / (x + y))) <= 100)'
    @d.l(~(((('x'.lit - :y)/(:x + :y))*:z) <= 100)).should == '((((x - y) / (x + y)) * z) > 100)'
  end
end
