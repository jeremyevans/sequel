require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')

describe "Blockless Ruby Filters" do
  before do
    db = Sequel::Database.new
    @d = db[:items]
    def @d.l(*args, &block)
      literal(filter_expr(*args, &block))
    end
    def @d.lit(*args)
      literal(*args)
    end
  end
  
  it "should support boolean columns directly" do
    @d.l(:x).should == 'x'
  end
  
  it "should support qualified columns" do
    @d.l(:x__y).should == 'x.y'
  end

  it "should support NOT with SQL functions" do
    @d.l(~Sequel.function(:is_blah)).should == 'NOT is_blah()'
    @d.l(~Sequel.function(:is_blah, :x)).should == 'NOT is_blah(x)'
    @d.l(~Sequel.function(:is_blah, :x__y)).should == 'NOT is_blah(x.y)'
    @d.l(~Sequel.function(:is_blah, :x, :x__y)).should == 'NOT is_blah(x, x.y)'
  end

  it "should handle multiple ~" do
    @d.l(~Sequel.~(:x)).should == 'x'
    @d.l(~~Sequel.~(:x)).should == 'NOT x'
    @d.l(~~Sequel.&(:x, :y)).should == '(x AND y)'
    @d.l(~~Sequel.|(:x, :y)).should == '(x OR y)'
  end

  it "should support = via Hash" do
    @d.l(:x => 100).should == '(x = 100)'
    @d.l(:x => 'a').should == '(x = \'a\')'
    @d.l(:x => true).should == '(x IS TRUE)'
    @d.l(:x => false).should == '(x IS FALSE)'
    @d.l(:x => nil).should == '(x IS NULL)'
    @d.l(:x => [1,2,3]).should == '(x IN (1, 2, 3))'
  end

  it "should use = 't' and != 't' OR IS NULL if IS TRUE is not supported" do
    meta_def(@d, :supports_is_true?){false}
    @d.l(:x => true).should == "(x = 't')"
    @d.l(~Sequel.expr(:x => true)).should == "((x != 't') OR (x IS NULL))"
    @d.l(:x => false).should == "(x = 'f')"
    @d.l(~Sequel.expr(:x => false)).should == "((x != 'f') OR (x IS NULL))"
  end
  
  it "should support != via inverted Hash" do
    @d.l(~Sequel.expr(:x => 100)).should == '(x != 100)'
    @d.l(~Sequel.expr(:x => 'a')).should == '(x != \'a\')'
    @d.l(~Sequel.expr(:x => true)).should == '(x IS NOT TRUE)'
    @d.l(~Sequel.expr(:x => false)).should == '(x IS NOT FALSE)'
    @d.l(~Sequel.expr(:x => nil)).should == '(x IS NOT NULL)'
  end
  
  it "should support ~ via Hash and Regexp (if supported by database)" do
    def @d.supports_regexp?; true end
    @d.l(:x => /blah/).should == '(x ~ \'blah\')'
  end
  
  it "should support !~ via inverted Hash and Regexp" do
    def @d.supports_regexp?; true end
    @d.l(~Sequel.expr(:x => /blah/)).should == '(x !~ \'blah\')'
  end
  
  it "should support negating ranges" do
    @d.l(~Sequel.expr(:x => 1..5)).should == '((x < 1) OR (x > 5))'
    @d.l(~Sequel.expr(:x => 1...5)).should == '((x < 1) OR (x >= 5))'
  end
  
  it "should support negating IN with Dataset or Array" do
    @d.l(~Sequel.expr(:x => @d.select(:i))).should == '(x NOT IN (SELECT i FROM items))'
    @d.l(~Sequel.expr(:x => [1,2,3])).should == '(x NOT IN (1, 2, 3))'
  end

  it "should not add ~ method to string expressions" do
    proc{~Sequel.expr(:x).sql_string}.should raise_error(NoMethodError) 
  end

  it "should allow mathematical or string operations on true, false, or nil" do
    @d.lit(Sequel.expr(:x) + 1).should == '(x + 1)'
    @d.lit(Sequel.expr(:x) - true).should == "(x - 't')"
    @d.lit(Sequel.expr(:x) / false).should == "(x / 'f')"
    @d.lit(Sequel.expr(:x) * nil).should == '(x * NULL)'
    @d.lit(Sequel.join([:x, nil])).should == '(x || NULL)'
  end

  it "should allow mathematical or string operations on boolean complex expressions" do
    @d.lit(Sequel.expr(:x) + (Sequel.expr(:y) + 1)).should == '(x + y + 1)'
    @d.lit(Sequel.expr(:x) - ~Sequel.expr(:y)).should == '(x - NOT y)'
    @d.lit(Sequel.expr(:x) / (Sequel.expr(:y) & :z)).should == '(x / (y AND z))'
    @d.lit(Sequel.expr(:x) * (Sequel.expr(:y) | :z)).should == '(x * (y OR z))'
    @d.lit(Sequel.expr(:x) + Sequel.expr(:y).like('a')).should == "(x + (y LIKE 'a' ESCAPE '\\'))"
    @d.lit(Sequel.expr(:x) - ~Sequel.expr(:y).like('a')).should == "(x - (y NOT LIKE 'a' ESCAPE '\\'))"
    @d.lit(Sequel.join([:x, ~Sequel.expr(:y).like('a')])).should == "(x || (y NOT LIKE 'a' ESCAPE '\\'))"
  end

  it "should support AND conditions via &" do
    @d.l(Sequel.expr(:x) & :y).should == '(x AND y)'
    @d.l(Sequel.expr(:x).sql_boolean & :y).should == '(x AND y)'
    @d.l(Sequel.expr(:x) & :y & :z).should == '(x AND y AND z)'
    @d.l(Sequel.expr(:x) & {:y => :z}).should == '(x AND (y = z))'
    @d.l((Sequel.expr(:x) + 200 < 0) & (Sequel.expr(:y) - 200 < 0)).should == '(((x + 200) < 0) AND ((y - 200) < 0))'
    @d.l(Sequel.expr(:x) & ~Sequel.expr(:y)).should == '(x AND NOT y)'
    @d.l(~Sequel.expr(:x) & :y).should == '(NOT x AND y)'
    @d.l(~Sequel.expr(:x) & ~Sequel.expr(:y)).should == '(NOT x AND NOT y)'
  end
  
  it "should support OR conditions via |" do
    @d.l(Sequel.expr(:x) | :y).should == '(x OR y)'
    @d.l(Sequel.expr(:x).sql_boolean | :y).should == '(x OR y)'
    @d.l(Sequel.expr(:x) | :y | :z).should == '(x OR y OR z)'
    @d.l(Sequel.expr(:x) | {:y => :z}).should == '(x OR (y = z))'
    @d.l((Sequel.expr(:x).sql_number > 200) | (Sequel.expr(:y).sql_number < 200)).should == '((x > 200) OR (y < 200))'
  end
  
  it "should support & | combinations" do
    @d.l((Sequel.expr(:x) | :y) & :z).should == '((x OR y) AND z)'
    @d.l(Sequel.expr(:x) | (Sequel.expr(:y) & :z)).should == '(x OR (y AND z))'
    @d.l((Sequel.expr(:x) & :w) | (Sequel.expr(:y) & :z)).should == '((x AND w) OR (y AND z))'
  end
  
  it "should support & | with ~" do
    @d.l(~((Sequel.expr(:x) | :y) & :z)).should == '((NOT x AND NOT y) OR NOT z)'
    @d.l(~(Sequel.expr(:x) | (Sequel.expr(:y) & :z))).should == '(NOT x AND (NOT y OR NOT z))'
    @d.l(~((Sequel.expr(:x) & :w) | (Sequel.expr(:y) & :z))).should == '((NOT x OR NOT w) AND (NOT y OR NOT z))'
    @d.l(~((Sequel.expr(:x).sql_number > 200) | (Sequel.expr(:y) & :z))).should == '((x <= 200) AND (NOT y OR NOT z))'
  end
  
  it "should support LiteralString" do
    @d.l(Sequel.lit('x')).should == '(x)'
    @d.l(~Sequel.lit('x')).should == 'NOT x'
    @d.l(~~Sequel.lit('x')).should == 'x'
    @d.l(~((Sequel.lit('x') | :y) & :z)).should == '((NOT x AND NOT y) OR NOT z)'
    @d.l(~(Sequel.expr(:x) | Sequel.lit('y'))).should == '(NOT x AND NOT y)'
    @d.l(~(Sequel.lit('x') & Sequel.lit('y'))).should == '(NOT x OR NOT y)'
    @d.l(Sequel.expr(Sequel.lit('y') => Sequel.lit('z')) & Sequel.lit('x')).should == '((y = z) AND x)'
    @d.l((Sequel.lit('x') > 200) & (Sequel.lit('y') < 200)).should == '((x > 200) AND (y < 200))'
    @d.l(~(Sequel.lit('x') + 1 > 100)).should == '((x + 1) <= 100)'
    @d.l(Sequel.lit('x').like('a')).should == '(x LIKE \'a\' ESCAPE \'\\\')'
    @d.l(Sequel.lit('x') + 1 > 100).should == '((x + 1) > 100)'
    @d.l((Sequel.lit('x') * :y) < 100.01).should == '((x * y) < 100.01)'
    @d.l((Sequel.lit('x') - Sequel.expr(:y)/2) >= 100000000000000000000000000000000000).should == '((x - (y / 2)) >= 100000000000000000000000000000000000)'
    @d.l((Sequel.lit('z') * ((Sequel.lit('x') / :y)/(Sequel.expr(:x) + :y))) <= 100).should == '((z * (x / y / (x + y))) <= 100)'
    @d.l(~((((Sequel.lit('x') - :y)/(Sequel.expr(:x) + :y))*:z) <= 100)).should == '((((x - y) / (x + y)) * z) > 100)'
  end

  it "should support hashes by ANDing the conditions" do
    @d.l(:x => 100, :y => 'a')[1...-1].split(' AND ').sort.should == ['(x = 100)', '(y = \'a\')']
    @d.l(:x => true, :y => false)[1...-1].split(' AND ').sort.should == ['(x IS TRUE)', '(y IS FALSE)']
    @d.l(:x => nil, :y => [1,2,3])[1...-1].split(' AND ').sort.should == ['(x IS NULL)', '(y IN (1, 2, 3))']
  end
  
  it "should support arrays with all two pairs the same as hashes" do
    @d.l([[:x, 100],[:y, 'a']]).should == '((x = 100) AND (y = \'a\'))'
    @d.l([[:x, true], [:y, false]]).should == '((x IS TRUE) AND (y IS FALSE))'
    @d.l([[:x, nil], [:y, [1,2,3]]]).should == '((x IS NULL) AND (y IN (1, 2, 3)))'
  end
  
  it "should emulate columns for array values" do
    @d.l([:x, :y]=>Sequel.value_list([[1,2], [3,4]])).should == '((x, y) IN ((1, 2), (3, 4)))'
    @d.l([:x, :y, :z]=>[[1,2,5], [3,4,6]]).should == '((x, y, z) IN ((1, 2, 5), (3, 4, 6)))'
  end
  
  it "should emulate multiple column in if not supported" do
    meta_def(@d, :supports_multiple_column_in?){false}
    @d.l([:x, :y]=>Sequel.value_list([[1,2], [3,4]])).should == '(((x = 1) AND (y = 2)) OR ((x = 3) AND (y = 4)))'
    @d.l([:x, :y, :z]=>[[1,2,5], [3,4,6]]).should == '(((x = 1) AND (y = 2) AND (z = 5)) OR ((x = 3) AND (y = 4) AND (z = 6)))'
  end
  
  it "should support StringExpression#+ for concatenation of SQL strings" do
    @d.lit(Sequel.expr(:x).sql_string + :y).should == '(x || y)'
    @d.lit(Sequel.join([:x]) + :y).should == '(x || y)'
    @d.lit(Sequel.join([:x, :z], ' ') + :y).should == "(x || ' ' || z || y)"
  end

  it "should be supported inside blocks" do
    @d.l{Sequel.or([[:x, nil], [:y, [1,2,3]]])}.should == '((x IS NULL) OR (y IN (1, 2, 3)))'
    @d.l{Sequel.~([[:x, nil], [:y, [1,2,3]]])}.should == '((x IS NOT NULL) OR (y NOT IN (1, 2, 3)))'
    @d.l{~((((Sequel.lit('x') - :y)/(Sequel.expr(:x) + :y))*:z) <= 100)}.should == '((((x - y) / (x + y)) * z) > 100)'
    @d.l{Sequel.&({:x => :a}, {:y => :z})}.should == '((x = a) AND (y = z))'
  end

  it "should support &, |, ^, ~, <<, and >> for NumericExpressions" do
    @d.l(Sequel.expr(:x).sql_number & 1 > 100).should == '((x & 1) > 100)'
    @d.l(Sequel.expr(:x).sql_number | 1 > 100).should == '((x | 1) > 100)'
    @d.l(Sequel.expr(:x).sql_number ^ 1 > 100).should == '((x ^ 1) > 100)'
    @d.l(~Sequel.expr(:x).sql_number > 100).should == '(~x > 100)'
    @d.l(Sequel.expr(:x).sql_number << 1 > 100).should == '((x << 1) > 100)'
    @d.l(Sequel.expr(:x).sql_number >> 1 > 100).should == '((x >> 1) > 100)'
    @d.l((Sequel.expr(:x) + 1) & 1 > 100).should == '(((x + 1) & 1) > 100)'
    @d.l((Sequel.expr(:x) + 1) | 1 > 100).should == '(((x + 1) | 1) > 100)'
    @d.l((Sequel.expr(:x) + 1) ^ 1 > 100).should == '(((x + 1) ^ 1) > 100)'
    @d.l(~(Sequel.expr(:x) + 1) > 100).should == '(~(x + 1) > 100)'
    @d.l((Sequel.expr(:x) + 1) << 1 > 100).should == '(((x + 1) << 1) > 100)'
    @d.l((Sequel.expr(:x) + 1) >> 1 > 100).should == '(((x + 1) >> 1) > 100)'
    @d.l((Sequel.expr(:x) + 1) & (Sequel.expr(:x) + 2) > 100).should == '(((x + 1) & (x + 2)) > 100)'
  end

  it "should allow using a Bitwise method on a ComplexExpression that isn't a NumericExpression" do
    @d.lit((Sequel.expr(:x) + 1) & (Sequel.expr(:x) + '2')).should == "((x + 1) & (x || '2'))"
  end

  it "should allow using a Boolean method on a ComplexExpression that isn't a BooleanExpression" do
    @d.l(Sequel.expr(:x) & (Sequel.expr(:x) + '2')).should == "(x AND (x || '2'))"
  end

  it "should raise an error if attempting to invert a ComplexExpression that isn't a BooleanExpression" do
    proc{Sequel::SQL::BooleanExpression.invert(Sequel.expr(:x) + 2)}.should raise_error(Sequel::Error)
  end

  it "should return self on .lit" do
    y = Sequel.expr(:x) + 1
    y.lit.should == y
  end

  it "should return have .sql_literal return the literal SQL for the expression" do
    y = Sequel.expr(:x) + 1
    y.sql_literal(@d).should == '(x + 1)'
    y.sql_literal(@d).should == @d.literal(y)
  end

  it "should support SQL::Constants" do
    @d.l({:x => Sequel::NULL}).should == '(x IS NULL)'
    @d.l({:x => Sequel::NOTNULL}).should == '(x IS NOT NULL)'
    @d.l({:x => Sequel::TRUE}).should == '(x IS TRUE)'
    @d.l({:x => Sequel::FALSE}).should == '(x IS FALSE)'
    @d.l({:x => Sequel::SQLTRUE}).should == '(x IS TRUE)'
    @d.l({:x => Sequel::SQLFALSE}).should == '(x IS FALSE)'
  end
  
  it "should support negation of SQL::Constants" do
    @d.l(Sequel.~(:x => Sequel::NULL)).should == '(x IS NOT NULL)'
    @d.l(Sequel.~(:x => Sequel::NOTNULL)).should == '(x IS NULL)'
    @d.l(Sequel.~(:x => Sequel::TRUE)).should == '(x IS NOT TRUE)'
    @d.l(Sequel.~(:x => Sequel::FALSE)).should == '(x IS NOT FALSE)'
    @d.l(Sequel.~(:x => Sequel::SQLTRUE)).should == '(x IS NOT TRUE)'
    @d.l(Sequel.~(:x => Sequel::SQLFALSE)).should == '(x IS NOT FALSE)'
  end
  
  it "should support direct negation of SQL::Constants" do
    @d.l({:x => ~Sequel::NULL}).should == '(x IS NOT NULL)'
    @d.l({:x => ~Sequel::NOTNULL}).should == '(x IS NULL)'
    @d.l({:x => ~Sequel::TRUE}).should == '(x IS FALSE)'
    @d.l({:x => ~Sequel::FALSE}).should == '(x IS TRUE)'
    @d.l({:x => ~Sequel::SQLTRUE}).should == '(x IS FALSE)'
    @d.l({:x => ~Sequel::SQLFALSE}).should == '(x IS TRUE)'
  end
  
  it "should raise an error if trying to invert an invalid SQL::Constant" do
    proc{~Sequel::CURRENT_DATE}.should raise_error(Sequel::Error)
  end

  it "should raise an error if trying to create an invalid complex expression" do
    proc{Sequel::SQL::ComplexExpression.new(:BANG, 1, 2)}.should raise_error(Sequel::Error)
  end

  it "should use a string concatentation for + if given a string" do
    @d.lit(Sequel.expr(:x) + '1').should == "(x || '1')"
    @d.lit(Sequel.expr(:x) + '1' + '1').should == "(x || '1' || '1')"
  end

  it "should use an addition for + if given a literal string" do
    @d.lit(Sequel.expr(:x) + Sequel.lit('1')).should == "(x + 1)"
    @d.lit(Sequel.expr(:x) + Sequel.lit('1') + Sequel.lit('1')).should == "(x + 1 + 1)"
  end

  it "should use a bitwise operator for & and | if given an integer" do
    @d.lit(Sequel.expr(:x) & 1).should == "(x & 1)"
    @d.lit(Sequel.expr(:x) | 1).should == "(x | 1)"
    @d.lit(Sequel.expr(:x) & 1 & 1).should == "(x & 1 & 1)"
    @d.lit(Sequel.expr(:x) | 1 | 1).should == "(x | 1 | 1)"
  end
  
  it "should allow adding a string to an integer expression" do
    @d.lit(Sequel.expr(:x) + 1 + 'a').should == "(x + 1 + 'a')"
  end

  it "should allow adding an integer to an string expression" do
    @d.lit(Sequel.expr(:x) + 'a' + 1).should == "(x || 'a' || 1)"
  end

  it "should allow adding a boolean to an integer expression" do
    @d.lit(Sequel.expr(:x) + 1 + true).should == "(x + 1 + 't')"
  end

  it "should allow adding a boolean to an string expression" do
    @d.lit(Sequel.expr(:x) + 'a' + true).should == "(x || 'a' || 't')"
  end

  it "should allow using a boolean operation with an integer on an boolean expression" do
    @d.lit(Sequel.expr(:x) & :a & 1).should == "(x AND a AND 1)"
  end

  it "should allow using a boolean operation with a string on an boolean expression" do
    @d.lit(Sequel.expr(:x) & :a & 'a').should == "(x AND a AND 'a')"
  end

  it "should allowing AND of boolean expression and literal string" do
   @d.lit(Sequel.expr(:x) & :a & Sequel.lit('a')).should == "(x AND a AND a)"
  end

  it "should allowing + of integer expression and literal string" do
   @d.lit(Sequel.expr(:x) + :a + Sequel.lit('a')).should == "(x + a + a)"
  end

  it "should allowing + of string expression and literal string" do
   @d.lit(Sequel.expr(:x) + 'a' + Sequel.lit('a')).should == "(x || 'a' || a)"
  end

  it "should allow sql_{string,boolean,number} methods on numeric expressions" do
   @d.lit((Sequel.expr(:x) + 1).sql_string + 'a').should == "((x + 1) || 'a')"
   @d.lit((Sequel.expr(:x) + 1).sql_boolean & 1).should == "((x + 1) AND 1)"
   @d.lit((Sequel.expr(:x) + 1).sql_number + 'a').should == "(x + 1 + 'a')"
  end

  it "should allow sql_{string,boolean,number} methods on string expressions" do
   @d.lit((Sequel.expr(:x) + 'a').sql_string + 'a').should == "(x || 'a' || 'a')"
   @d.lit((Sequel.expr(:x) + 'a').sql_boolean & 1).should == "((x || 'a') AND 1)"
   @d.lit((Sequel.expr(:x) + 'a').sql_number + 'a').should == "((x || 'a') + 'a')"
  end

  it "should allow sql_{string,boolean,number} methods on boolean expressions" do
   @d.lit((Sequel.expr(:x) & :y).sql_string + 'a').should == "((x AND y) || 'a')"
   @d.lit((Sequel.expr(:x) & :y).sql_boolean & 1).should == "(x AND y AND 1)"
   @d.lit((Sequel.expr(:x) & :y).sql_number + 'a').should == "((x AND y) + 'a')"
  end

  it "should raise an error if trying to literalize an invalid complex expression" do
    ce = Sequel.+(:x, 1)
    ce.instance_variable_set(:@op, :BANG)
    proc{@d.lit(ce)}.should raise_error(Sequel::Error)
  end

  it "should support equality comparison of two expressions" do
    e1 = ~Sequel.like(:comment, '%:hidden:%')
    e2 = ~Sequel.like(:comment, '%:hidden:%')
    e1.should == e2
  end

  it "should support expression filter methods on Datasets" do
    d = @d.select(:a)

    @d.lit(d + 1).should == '((SELECT a FROM items) + 1)'
    @d.lit(d - 1).should == '((SELECT a FROM items) - 1)'
    @d.lit(d * 1).should == '((SELECT a FROM items) * 1)'
    @d.lit(d / 1).should == '((SELECT a FROM items) / 1)'

    @d.lit(d => 1).should == '((SELECT a FROM items) = 1)'
    @d.lit(Sequel.~(d => 1)).should == '((SELECT a FROM items) != 1)'
    @d.lit(d > 1).should == '((SELECT a FROM items) > 1)'
    @d.lit(d < 1).should == '((SELECT a FROM items) < 1)'
    @d.lit(d >= 1).should == '((SELECT a FROM items) >= 1)'
    @d.lit(d <= 1).should == '((SELECT a FROM items) <= 1)'

    @d.lit(d.as(:b)).should == '(SELECT a FROM items) AS b'

    @d.lit(d & :b).should == '((SELECT a FROM items) AND b)'
    @d.lit(d | :b).should == '((SELECT a FROM items) OR b)'
    @d.lit(~d).should == 'NOT (SELECT a FROM items)'

    @d.lit(d.cast(Integer)).should == 'CAST((SELECT a FROM items) AS integer)'
    @d.lit(d.cast_numeric).should == 'CAST((SELECT a FROM items) AS integer)'
    @d.lit(d.cast_string).should == 'CAST((SELECT a FROM items) AS varchar(255))'
    @d.lit(d.cast_numeric << :b).should == '(CAST((SELECT a FROM items) AS integer) << b)'
    @d.lit(d.cast_string + :b).should == '(CAST((SELECT a FROM items) AS varchar(255)) || b)'

    @d.lit(d.extract(:year)).should == 'extract(year FROM (SELECT a FROM items))'
    @d.lit(d.sql_boolean & :b).should == '((SELECT a FROM items) AND b)'
    @d.lit(d.sql_number << :b).should == '((SELECT a FROM items) << b)'
    @d.lit(d.sql_string + :b).should == '((SELECT a FROM items) || b)'

    @d.lit(d.asc).should == '(SELECT a FROM items) ASC'
    @d.lit(d.desc).should == '(SELECT a FROM items) DESC'

    @d.lit(d.like(:b)).should == '((SELECT a FROM items) LIKE b ESCAPE \'\\\')'
    @d.lit(d.ilike(:b)).should == '(UPPER((SELECT a FROM items)) LIKE UPPER(b) ESCAPE \'\\\')'
  end

  it "should handled emulated char_length function" do
    @d.lit(Sequel.char_length(:a)).should == 'char_length(a)'
  end

  it "should handled emulated trim function" do
    @d.lit(Sequel.trim(:a)).should == 'trim(a)'
  end
end

describe Sequel::SQL::VirtualRow do
  before do
    db = Sequel::Database.new
    db.quote_identifiers = true
    @d = db[:items]
    meta_def(@d, :supports_window_functions?){true}
    def @d.l(*args, &block)
      literal(filter_expr(*args, &block))
    end
  end

  it "should treat methods without arguments as identifiers" do
    @d.l{column}.should == '"column"'
  end

  it "should treat methods without arguments that have embedded double underscores as qualified identifiers" do
    @d.l{table__column}.should == '"table"."column"'
  end

  it "should treat methods with arguments as functions with the arguments" do
    @d.l{function(arg1, 10, 'arg3')}.should == 'function("arg1", 10, \'arg3\')'
  end

  it "should treat methods with a block and no arguments as a function call with no arguments" do
    @d.l{version{}}.should == 'version()'
  end

  it "should treat methods with a block and a leading argument :* as a function call with the SQL wildcard" do
    @d.l{count(:*){}}.should == 'count(*)'
  end

  it "should treat methods with a block and a leading argument :distinct as a function call with DISTINCT and the additional method arguments" do
    @d.l{count(:distinct, column1){}}.should == 'count(DISTINCT "column1")'
    @d.l{count(:distinct, column1, column2){}}.should == 'count(DISTINCT "column1", "column2")'
  end

  it "should raise an error if an unsupported argument is used with a block" do
    proc{@d.l{count(:blah){}}}.should raise_error(Sequel::Error)
  end

  it "should treat methods with a block and a leading argument :over as a window function call" do
    @d.l{rank(:over){}}.should == 'rank() OVER ()'
  end

  it "should support :partition options for window function calls" do
    @d.l{rank(:over, :partition=>column1){}}.should == 'rank() OVER (PARTITION BY "column1")'
    @d.l{rank(:over, :partition=>[column1, column2]){}}.should == 'rank() OVER (PARTITION BY "column1", "column2")'
  end

  it "should support :args options for window function calls" do
    @d.l{avg(:over, :args=>column1){}}.should == 'avg("column1") OVER ()'
    @d.l{avg(:over, :args=>[column1, column2]){}}.should == 'avg("column1", "column2") OVER ()'
  end

  it "should support :order option for window function calls" do
    @d.l{rank(:over, :order=>column1){}}.should == 'rank() OVER (ORDER BY "column1")'
    @d.l{rank(:over, :order=>[column1, column2]){}}.should == 'rank() OVER (ORDER BY "column1", "column2")'
  end

  it "should support :window option for window function calls" do
    @d.l{rank(:over, :window=>:win){}}.should == 'rank() OVER ("win")'
  end

  it "should support :*=>true option for window function calls" do
    @d.l{count(:over, :* =>true){}}.should == 'count(*) OVER ()'
  end

  it "should support :frame=>:all option for window function calls" do
    @d.l{rank(:over, :frame=>:all){}}.should == 'rank() OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)'
  end

  it "should support :frame=>:rows option for window function calls" do
    @d.l{rank(:over, :frame=>:rows){}}.should == 'rank() OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)'
  end

  it "should support :frame=>'some string' option for window function calls" do
    @d.l{rank(:over, :frame=>'RANGE BETWEEN 3 PRECEDING AND CURRENT ROW'){}}.should == 'rank() OVER (RANGE BETWEEN 3 PRECEDING AND CURRENT ROW)'
  end

  it "should raise an error if an invalid :frame option is used" do
    proc{@d.l{rank(:over, :frame=>:blah){}}}.should raise_error(Sequel::Error)
  end

  it "should support all these options together" do
    @d.l{count(:over, :* =>true, :partition=>a, :order=>b, :window=>:win, :frame=>:rows){}}.should == 'count(*) OVER ("win" PARTITION BY "a" ORDER BY "b" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)'
  end

  it "should raise an error if window functions are not supported" do
    class << @d; remove_method :supports_window_functions? end
    meta_def(@d, :supports_window_functions?){false}
    proc{@d.l{count(:over, :* =>true, :partition=>a, :order=>b, :window=>:win, :frame=>:rows){}}}.should raise_error(Sequel::Error)
    proc{Sequel.mock.dataset.filter{count(:over, :* =>true, :partition=>a, :order=>b, :window=>:win, :frame=>:rows){}}.sql}.should raise_error(Sequel::Error)
  end
  
  it "should deal with classes without requiring :: prefix" do
    @d.l{date < Date.today}.should == "(\"date\" < '#{Date.today}')"
    @d.l{date < Sequel::CURRENT_DATE}.should == "(\"date\" < CURRENT_DATE)"
    @d.l{num < Math::PI.to_i}.should == "(\"num\" < 3)"
  end
  
  it "should deal with methods added to Object after requiring Sequel" do
    class Object
      def adsoiwemlsdaf; 42; end
    end
    Sequel::BasicObject.remove_methods!
    @d.l{a > adsoiwemlsdaf}.should == '("a" > "adsoiwemlsdaf")'
  end
  
  it "should deal with private methods added to Kernel after requiring Sequel" do
    module Kernel
      private
      def adsoiwemlsdaf2; 42; end
    end
    Sequel::BasicObject.remove_methods!
    @d.l{a > adsoiwemlsdaf2}.should == '("a" > "adsoiwemlsdaf2")'
  end

  it "should have operator methods defined that produce Sequel expression objects" do
    @d.l{|o| o.&({:a=>1}, :b)}.should == '(("a" = 1) AND "b")'
    @d.l{|o| o.|({:a=>1}, :b)}.should == '(("a" = 1) OR "b")'
    @d.l{|o| o.+(1, :b) > 2}.should == '((1 + "b") > 2)'
    @d.l{|o| o.-(1, :b) < 2}.should == '((1 - "b") < 2)'
    @d.l{|o| o.*(1, :b) >= 2}.should == '((1 * "b") >= 2)'
    @d.l{|o| o./(1, :b) <= 2}.should == '((1 / "b") <= 2)'
    @d.l{|o| o.~(:a=>1)}.should == '("a" != 1)'
    @d.l{|o| o.~([[:a, 1], [:b, 2]])}.should == '(("a" != 1) OR ("b" != 2))'
    @d.l{|o| o.<(1, :b)}.should == '(1 < "b")'
    @d.l{|o| o.>(1, :b)}.should == '(1 > "b")'
    @d.l{|o| o.<=(1, :b)}.should == '(1 <= "b")'
    @d.l{|o| o.>=(1, :b)}.should == '(1 >= "b")'
  end

  it "should have have ` produce literal strings" do
    @d.l{a > `some SQL`}.should == '("a" > some SQL)'
    @d.l{|o| o.a > o.`('some SQL')}.should == '("a" > some SQL)' #`
  end
end

describe "Sequel core extension replacements" do
  before do
    @db = Sequel::Database.new
    @ds = @db.dataset 
    def @ds.supports_regexp?; true end
    @o = Object.new
    def @o.sql_literal(ds) 'foo' end
  end

  def l(arg, should)
    @ds.literal(arg).should == should
  end

  it "Sequel.expr should return items wrapped in Sequel objects" do
    Sequel.expr(1).should be_a_kind_of(Sequel::SQL::NumericExpression)
    Sequel.expr('a').should be_a_kind_of(Sequel::SQL::StringExpression)
    Sequel.expr(true).should be_a_kind_of(Sequel::SQL::BooleanExpression)
    Sequel.expr(nil).should be_a_kind_of(Sequel::SQL::Wrapper)
    Sequel.expr({1=>2}).should be_a_kind_of(Sequel::SQL::BooleanExpression)
    Sequel.expr([[1, 2]]).should be_a_kind_of(Sequel::SQL::BooleanExpression)
    Sequel.expr([1]).should be_a_kind_of(Sequel::SQL::Wrapper)
    Sequel.expr{|o| o.a}.should be_a_kind_of(Sequel::SQL::Identifier)
    Sequel.expr{a}.should be_a_kind_of(Sequel::SQL::Identifier)
    Sequel.expr(:a).should be_a_kind_of(Sequel::SQL::Identifier)
    Sequel.expr(:a__b).should be_a_kind_of(Sequel::SQL::QualifiedIdentifier)
    Sequel.expr(:a___c).should be_a_kind_of(Sequel::SQL::AliasedExpression)
    Sequel.expr(:a___c).expression.should be_a_kind_of(Sequel::SQL::Identifier)
    Sequel.expr(:a__b___c).should be_a_kind_of(Sequel::SQL::AliasedExpression)
    Sequel.expr(:a__b___c).expression.should be_a_kind_of(Sequel::SQL::QualifiedIdentifier)
  end

  it "Sequel.expr should return an appropriate wrapped object" do
    l(Sequel.expr(1) + 1, "(1 + 1)")
    l(Sequel.expr('a') + 'b', "('a' || 'b')")
    l(Sequel.expr(:b) & nil, "(b AND NULL)")
    l(Sequel.expr(nil) & true, "(NULL AND 't')")
    l(Sequel.expr(false) & true, "('f' AND 't')")
    l(Sequel.expr(true) | false, "('t' OR 'f')")
    l(Sequel.expr(@o) + 1, "(foo + 1)")
  end

  it "Sequel.expr should handle condition specifiers" do
    l(Sequel.expr(:a=>1) & nil, "((a = 1) AND NULL)")
    l(Sequel.expr([[:a, 1]]) & nil, "((a = 1) AND NULL)")
    l(Sequel.expr([[:a, 1], [:b, 2]]) & nil, "((a = 1) AND (b = 2) AND NULL)")
  end

  it "Sequel.expr should handle arrays that are not condition specifiers" do
    l(Sequel.expr([1]), "(1)")
    l(Sequel.expr([1, 2]), "(1, 2)")
  end

  it "Sequel.expr should treat blocks/procs as virtual rows and wrap the output" do
    l(Sequel.expr{1} + 1, "(1 + 1)")
    l(Sequel.expr{o__a} + 1, "(o.a + 1)")
    l(Sequel.expr{[[:a, 1]]} & nil, "((a = 1) AND NULL)")
    l(Sequel.expr{|v| @o} + 1, "(foo + 1)")

    l(Sequel.expr(proc{1}) + 1, "(1 + 1)")
    l(Sequel.expr(proc{o__a}) + 1, "(o.a + 1)")
    l(Sequel.expr(proc{[[:a, 1]]}) & nil, "((a = 1) AND NULL)")
    l(Sequel.expr(proc{|v| @o}) + 1, "(foo + 1)")
  end

  it "Sequel.expr should handle lambda proc virtual rows" do
    l(Sequel.expr(&lambda{1}), "1")
    l(Sequel.expr(&lambda{|| 1}), "1")
  end

  it "Sequel.expr should raise an error if given an argument and a block" do
    proc{Sequel.expr(nil){}}.should raise_error(Sequel::Error)
  end

  it "Sequel.expr should raise an error if given neither an argument nor a block" do
    proc{Sequel.expr}.should raise_error(Sequel::Error)
  end

  it "Sequel.expr should return existing Sequel expressions directly" do
    o = Sequel.expr(1)
    Sequel.expr(o).should equal(o)
    o = Sequel.lit('1')
    Sequel.expr(o).should equal(o)
  end

  it "Sequel.~ should invert the given object" do
    l(Sequel.~(nil), 'NOT NULL')
    l(Sequel.~(:a=>1), "(a != 1)")
    l(Sequel.~([[:a, 1]]), "(a != 1)")
    l(Sequel.~([[:a, 1], [:b, 2]]), "((a != 1) OR (b != 2))")
    l(Sequel.~(Sequel.expr([[:a, 1], [:b, 2]]) & nil), "((a != 1) OR (b != 2) OR NOT NULL)")
  end

  it "Sequel.case should use a CASE expression" do
    l(Sequel.case({:a=>1}, 2), "(CASE WHEN a THEN 1 ELSE 2 END)")
    l(Sequel.case({:a=>1}, 2, :b), "(CASE b WHEN a THEN 1 ELSE 2 END)")
    l(Sequel.case([[:a, 1]], 2), "(CASE WHEN a THEN 1 ELSE 2 END)")
    l(Sequel.case([[:a, 1]], 2, :b), "(CASE b WHEN a THEN 1 ELSE 2 END)")
    l(Sequel.case([[:a, 1], [:c, 3]], 2), "(CASE WHEN a THEN 1 WHEN c THEN 3 ELSE 2 END)")
    l(Sequel.case([[:a, 1], [:c, 3]], 2, :b), "(CASE b WHEN a THEN 1 WHEN c THEN 3 ELSE 2 END)")
  end

  it "Sequel.case should raise an error if not given a condition specifier" do
    proc{Sequel.case(1, 2)}.should raise_error(Sequel::Error)
  end

  it "Sequel.value_list should use an SQL value list" do
    l(Sequel.value_list([[1, 2]]), "((1, 2))")
  end

  it "Sequel.value_list raise an error if not given an array" do
    proc{Sequel.value_list(1)}.should raise_error(Sequel::Error)
  end

  it "Sequel.negate should negate all entries in conditions specifier and join with AND" do
    l(Sequel.negate(:a=>1), "(a != 1)")
    l(Sequel.negate([[:a, 1]]), "(a != 1)")
    l(Sequel.negate([[:a, 1], [:b, 2]]), "((a != 1) AND (b != 2))")
  end

  it "Sequel.negate should raise an error if not given a conditions specifier" do
    proc{Sequel.negate(1)}.should raise_error(Sequel::Error)
  end

  it "Sequel.or should join all entries in conditions specifier with OR" do
    l(Sequel.or(:a=>1), "(a = 1)")
    l(Sequel.or([[:a, 1]]), "(a = 1)")
    l(Sequel.or([[:a, 1], [:b, 2]]), "((a = 1) OR (b = 2))")
  end

  it "Sequel.or should raise an error if not given a conditions specifier" do
    proc{Sequel.or(1)}.should raise_error(Sequel::Error)
  end

  it "Sequel.join should should use SQL string concatenation to join array" do
    l(Sequel.join([]), "''")
    l(Sequel.join(['a']), "('a')")
    l(Sequel.join(['a', 'b']), "('a' || 'b')")
    l(Sequel.join(['a', 'b'], 'c'), "('a' || 'c' || 'b')")
    l(Sequel.join([true, :b], :c), "('t' || c || b)")
    l(Sequel.join([false, nil], Sequel.lit('c')), "('f' || c || NULL)")
    l(Sequel.join([Sequel.expr('a'), Sequel.lit('d')], 'c'), "('a' || 'c' || d)")
  end

  it "Sequel.join should raise an error if not given an array" do
    proc{Sequel.join(1)}.should raise_error(Sequel::Error)
  end

  it "Sequel.& should join all arguments given with AND" do
    l(Sequel.&(:a), "(a)")
    l(Sequel.&(:a, :b=>:c), "(a AND (b = c))")
    l(Sequel.&(:a, {:b=>:c}, Sequel.lit('d')), "(a AND (b = c) AND d)")
  end

  it "Sequel.& should raise an error if given no arguments" do
    proc{Sequel.&}.should raise_error(Sequel::Error)
  end

  it "Sequel.| should join all arguments given with OR" do
    l(Sequel.|(:a), "(a)")
    l(Sequel.|(:a, :b=>:c), "(a OR (b = c))")
    l(Sequel.|(:a, {:b=>:c}, Sequel.lit('d')), "(a OR (b = c) OR d)")
  end

  it "Sequel.| should raise an error if given no arguments" do
    proc{Sequel.|}.should raise_error(Sequel::Error)
  end

  it "Sequel.as should return an aliased expression" do
    l(Sequel.as(:a, :b), "a AS b")
  end

  it "Sequel.cast should return a CAST expression" do
    l(Sequel.cast(:a, :int), "CAST(a AS int)")
    l(Sequel.cast(:a, Integer), "CAST(a AS integer)")
  end

  it "Sequel.cast_numeric should return a CAST expression treated as a number" do
    l(Sequel.cast_numeric(:a), "CAST(a AS integer)")
    l(Sequel.cast_numeric(:a, :int), "CAST(a AS int)")
    l(Sequel.cast_numeric(:a) << 2, "(CAST(a AS integer) << 2)")
  end

  it "Sequel.cast_string should return a CAST expression treated as a string" do
    l(Sequel.cast_string(:a), "CAST(a AS varchar(255))")
    l(Sequel.cast_string(:a, :text), "CAST(a AS text)")
    l(Sequel.cast_string(:a) + 'a', "(CAST(a AS varchar(255)) || 'a')")
  end

  it "Sequel.lit should return a literal string" do
    l(Sequel.lit('a'), "a")
  end

  it "Sequel.lit should return the argument if given a single literal string" do
    o = Sequel.lit('a')
    Sequel.lit(o).should equal(o)
  end

  it "Sequel.lit should accept multiple arguments for a placeholder literal string" do
    l(Sequel.lit('a = ?', 1), "a = 1")
    l(Sequel.lit('? = ?', :a, 1), "a = 1")
    l(Sequel.lit('a = :a', :a=>1), "a = 1")
  end

  it "Sequel.lit should work with an array for the placeholder string" do
    l(Sequel.lit(['a = '], 1), "a = 1")
    l(Sequel.lit(['', ' = '], :a, 1), "a = 1")
  end

  it "Sequel.blob should return an SQL::Blob" do
    l(Sequel.blob('a'), "'a'")
    Sequel.blob('a').should be_a_kind_of(Sequel::SQL::Blob)
  end

  it "Sequel.blob should return the given argument if given a blob" do
    o = Sequel.blob('a')
    Sequel.blob(o).should equal(o)
  end

  it "Sequel.qualify should return a qualified identifier" do
    l(Sequel.qualify(:t, :c), "t.c")
  end

  it "Sequel.identifier should return an identifier" do
    l(Sequel.identifier(:t__c), "t__c")
  end

  it "Sequel.asc should return an ASC ordered expression" do
    l(Sequel.asc(:a), "a ASC")
    l(Sequel.asc(:a, :nulls=>:first), "a ASC NULLS FIRST")
  end

  it "Sequel.desc should return a DESC ordered expression " do
    l(Sequel.desc(:a), "a DESC")
    l(Sequel.desc(:a, :nulls=>:last), "a DESC NULLS LAST")
  end

  it "Sequel.{+,-,*,/} should accept arguments and use the appropriate operator" do
    %w'+ - * /'.each do |op|
      l(Sequel.send(op, 1), '(1)')
      l(Sequel.send(op, 1, 2), "(1 #{op} 2)")
      l(Sequel.send(op, 1, 2, 3), "(1 #{op} 2 #{op} 3)")
    end
  end

  it "Sequel.{+,-,*,/} should raise if given no arguments" do
    %w'+ - * /'.each do |op|
      proc{Sequel.send(op)}.should raise_error(Sequel::Error)
    end
  end

  it "Sequel.like should use a LIKE expression" do
    l(Sequel.like('a', 'b'), "('a' LIKE 'b' ESCAPE '\\')")
    l(Sequel.like(:a, :b), "(a LIKE b ESCAPE '\\')")
    l(Sequel.like(:a, /b/), "(a ~ 'b')")
    l(Sequel.like(:a, 'c', /b/), "((a LIKE 'c' ESCAPE '\\') OR (a ~ 'b'))")
  end

  it "Sequel.ilike should use an ILIKE expression" do
    l(Sequel.ilike('a', 'b'), "(UPPER('a') LIKE UPPER('b') ESCAPE '\\')")
    l(Sequel.ilike(:a, :b), "(UPPER(a) LIKE UPPER(b) ESCAPE '\\')")
    l(Sequel.ilike(:a, /b/), "(a ~* 'b')")
    l(Sequel.ilike(:a, 'c', /b/), "((UPPER(a) LIKE UPPER('c') ESCAPE '\\') OR (a ~* 'b'))")
  end

  it "Sequel.subscript should use an SQL subscript" do
    l(Sequel.subscript(:a, 1), 'a[1]')
    l(Sequel.subscript(:a, 1, 2), 'a[1, 2]')
    l(Sequel.subscript(:a, [1, 2]), 'a[1, 2]')
  end

  it "Sequel.function should return an SQL function" do
    l(Sequel.function(:a), 'a()')
    l(Sequel.function(:a, 1), 'a(1)')
    l(Sequel.function(:a, :b, 2), 'a(b, 2)')
  end

  it "Sequel.extract should use a date/time extraction" do
    l(Sequel.extract(:year, :a), 'extract(year FROM a)')
  end

  it "#* with no arguments should use a ColumnAll for Identifier and QualifiedIdentifier" do
    l(Sequel.expr(:a).*, 'a.*')
    l(Sequel.expr(:a__b).*, 'a.b.*')
  end

  it "SQL::Blob should be aliasable and castable by default" do
    b = Sequel.blob('a')
    l(b.as(:a), "'a' AS a")
    l(b.cast(Integer), "CAST('a' AS integer)")
  end

  it "SQL::Blob should be convertable to a literal string by default" do
    b = Sequel.blob('a ?')
    l(b.lit, "a ?")
    l(b.lit(1), "a 1")
  end
end

describe "Sequel::SQL::Function#==" do
  specify "should be true for functions with the same name and arguments, false otherwise" do
    a = Sequel.function(:date, :t)
    b = Sequel.function(:date, :t)
    a.should == b
    (a == b).should == true
    c = Sequel.function(:date, :c)
    a.should_not == c
    (a == c).should == false
    d = Sequel.function(:time, :c)
    a.should_not == d
    c.should_not == d
    (a == d).should == false
    (c == d).should == false
  end
end

describe "Sequel::SQL::OrderedExpression" do
  specify "should #desc" do
    @oe = Sequel.asc(:column)
    @oe.descending.should == false
    @oe.desc.descending.should == true
  end

  specify "should #asc" do
    @oe = Sequel.desc(:column)
    @oe.descending.should == true
    @oe.asc.descending.should == false
  end

  specify "should #invert" do
    @oe = Sequel.desc(:column)
    @oe.invert.descending.should == false
    @oe.invert.invert.descending.should == true
  end
end

describe "Expression" do
  specify "should consider objects == only if they have the same attributes" do
    Sequel.qualify(:table, :column).cast(:type).*(:numeric_column).asc.should == Sequel.qualify(:table, :column).cast(:type).*(:numeric_column).asc
    Sequel.qualify(:table, :other_column).cast(:type).*(:numeric_column).asc.should_not == Sequel.qualify(:table, :column).cast(:type).*(:numeric_column).asc

    Sequel.qualify(:table, :column).cast(:type).*(:numeric_column).asc.should eql(Sequel.qualify(:table, :column).cast(:type).*(:numeric_column).asc)
    Sequel.qualify(:table, :other_column).cast(:type).*(:numeric_column).asc.should_not eql(Sequel.qualify(:table, :column).cast(:type).*(:numeric_column).asc)
  end

  specify "should use the same hash value for objects that have the same attributes" do
    Sequel.qualify(:table, :column).cast(:type).*(:numeric_column).asc.hash.should == Sequel.qualify(:table, :column).cast(:type).*(:numeric_column).asc.hash
    Sequel.qualify(:table, :other_column).cast(:type).*(:numeric_column).asc.hash.should_not == Sequel.qualify(:table, :column).cast(:type).*(:numeric_column).asc.hash

    h = {}
    a = Sequel.qualify(:table, :column).cast(:type).*(:numeric_column).asc
    b = Sequel.qualify(:table, :column).cast(:type).*(:numeric_column).asc
    h[a] = 1
    h[b] = 2
    h[a].should == 2
    h[b].should == 2
  end
end

describe "Sequel::SQLTime" do
  before do
    @db = Sequel.mock
  end

  specify ".create should create from hour, minutes, seconds and optional microseconds" do
    @db.literal(Sequel::SQLTime.create(1, 2, 3)).should == "'01:02:03.000000'"
    @db.literal(Sequel::SQLTime.create(1, 2, 3, 500000)).should == "'01:02:03.500000'"
  end
end

describe "Sequel::SQL::Wrapper" do
  before do
    @ds = Sequel.mock.dataset
  end

  specify "should wrap objects so they can be used by the Sequel DSL" do
    o = Object.new
    def o.sql_literal(ds) 'foo' end
    s = Sequel::SQL::Wrapper.new(o)
    @ds.literal(s).should == "foo"
    @ds.literal(s+1).should == "(foo + 1)"
    @ds.literal(s & true).should == "(foo AND 't')"
    @ds.literal(s < 1).should == "(foo < 1)"
    @ds.literal(s.sql_subscript(1)).should == "foo[1]"
    @ds.literal(s.like('a')).should == "(foo LIKE 'a' ESCAPE '\\')"
    @ds.literal(s.as(:a)).should == "foo AS a"
    @ds.literal(s.cast(Integer)).should == "CAST(foo AS integer)"
    @ds.literal(s.desc).should == "foo DESC"
    @ds.literal(s.sql_string + '1').should == "(foo || '1')"
  end
end

describe "Sequel::SQL::Blob#to_sequel_blob" do
  specify "should return self" do
    c = Sequel::SQL::Blob.new('a')
    c.to_sequel_blob.should equal(c)
  end
end

describe Sequel::SQL::Subscript do
  before do
    @s = Sequel::SQL::Subscript.new(:a, [1])
    @ds = Sequel.mock.dataset
  end

  specify "should have | return a new non-nested subscript" do
    s = (@s | 2)
    s.should_not equal(@s)
    @ds.literal(s).should == 'a[1, 2]'
  end

  specify "should have [] return a new nested subscript" do
    s = @s[2]
    s.should_not equal(@s)
    @ds.literal(s).should == 'a[1][2]'
  end
end

describe Sequel::SQL::CaseExpression, "#with_merged_expression" do
  specify "should return self if it has no expression" do
    c = Sequel.case({1=>0}, 3)
    c.with_merged_expression.should equal(c)
  end

  specify "should merge expression into conditions if it has an expression" do
    db = Sequel::Database.new
    c = Sequel.case({1=>0}, 3, 4)
    db.literal(c.with_merged_expression).should == db.literal(Sequel.case({{4=>1}=>0}, 3))
  end
end

describe "Sequel.recursive_map" do
  specify "should recursively convert an array using a callable" do
    Sequel.recursive_map(['1'], proc{|s| s.to_i}).should == [1]
    Sequel.recursive_map([['1']], proc{|s| s.to_i}).should == [[1]]
  end

  specify "should not call callable if value is nil" do
    Sequel.recursive_map([nil], proc{|s| s.to_i}).should == [nil]
    Sequel.recursive_map([[nil]], proc{|s| s.to_i}).should == [[nil]]
  end
end

describe "Sequel.delay" do
  before do
    @o = Class.new do
      def a
        @a ||= 0
        @a += 1
      end
      def _a
        @a if defined?(@a)
      end

      attr_accessor :b
    end.new
  end

  specify "should delay calling the block until literalization" do
    ds = Sequel.mock[:b].where(:a=>Sequel.delay{@o.a})
    @o._a.should be_nil
    ds.sql.should == "SELECT * FROM b WHERE (a = 1)"
    @o._a.should == 1
    ds.sql.should == "SELECT * FROM b WHERE (a = 2)"
    @o._a.should == 2
  end

  specify "should have the condition specifier handling respect delayed evaluations" do
    ds = Sequel.mock[:b].where(:a=>Sequel.delay{@o.b})
    ds.sql.should == "SELECT * FROM b WHERE (a IS NULL)"
    @o.b = 1
    ds.sql.should == "SELECT * FROM b WHERE (a = 1)"
    @o.b = [1, 2]
    ds.sql.should == "SELECT * FROM b WHERE (a IN (1, 2))"
  end

  specify "should raise if called without a block" do
    proc{Sequel.delay}.should raise_error(Sequel::Error)
  end
end

describe "Sequel.parse_json" do
  before do
    Sequel::JSON = Object.new
    def (Sequel::JSON).parse(json, opts={})
      [json, opts]
    end
  end
  after do
    Sequel.send(:remove_const, :JSON)
  end

  specify "should parse json correctly" do
    Sequel.parse_json('[]').should == ['[]', {:create_additions=>false}]
  end
end

describe "Sequel::LiteralString" do
  before do
    @s = Sequel::LiteralString.new("? = ?")
  end

  specify "should have lit return self if no arguments" do
    @s.lit.should equal(@s)
  end

  specify "should have lit return self if return a placeholder literal string if arguments" do
    @s.lit(1, 2).should be_a_kind_of(Sequel::SQL::PlaceholderLiteralString)
    Sequel.mock.literal(@s.lit(1, :a)).should == '1 = a'
  end

  specify "should have to_sequel_blob convert to blob" do
    @s.to_sequel_blob.should == @s
    @s.to_sequel_blob.should be_a_kind_of(Sequel::SQL::Blob)
  end
end

describe "Sequel core extensions" do
  specify "should have Sequel.core_extensions? be false by default" do
    Sequel.core_extensions?.should be_false
  end
end
