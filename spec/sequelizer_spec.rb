require File.join(File.dirname(__FILE__), 'spec_helper')

context "Proc#to_sql" do
  DB = Sequel::Database.new
  DS = DB[:items]

  class ::Proc
    def to_sql
      DS.proc_to_sql(self)
    end
  end

  specify "should support <sym> <op> <lit>" do
    proc {:x > 100}.to_sql.should == '(x > 100)'
    proc {:x < 100}.to_sql.should == '(x < 100)'
    proc {:x >= 100}.to_sql.should == '(x >= 100)'
    proc {:x <= 100}.to_sql.should == '(x <= 100)'
    proc {:x == 100}.to_sql.should == '(x = 100)'
  end
  
  specify "should support number literals" do
    proc {:x > 123.45}.to_sql.should == '(x > 123.45)'
    proc {:x > -30_000}.to_sql.should == '(x > -30000)'
  end

  specify "should support string literals" do
    proc {:x == 'abc'}.to_sql.should == "(x = 'abc')"
    proc {:y == "ab'cd"}.to_sql.should == "(y = 'ab''cd')"
  end
  
  specify "should support boolean literals" do
    proc {:x == false}.to_sql.should == "(x = 'f')"
    proc {:x == true}.to_sql.should == "(x = 't')"
  end
  
  specify "should support nil literal and nil?" do
    proc {:x == nil}.to_sql.should == "(x IS NULL)"
    proc {:x.nil?}.to_sql.should == "(x IS NULL)"
  end
  
  specify "should support local vars or method references" do
    proc {proc {:x == a}.to_sql}.should raise_error(NameError)
    b = 123
    proc {:x == b}.to_sql.should == "(x = 123)"
    def xyz; 321; end
    proc {:x == xyz}.to_sql.should == "(x = 321)"
    proc {:x == xyz.to_s}.to_sql.should == "(x = '321')"
    
    def y1(x); x; end
    def y2; 111; end
    
    proc {:x == y1(222)}.to_sql.should == "(x = 222)"
    proc {:x == y2}.to_sql.should == "(x = 111)"
  end
  
  specify "should support constants" do
    ZZZ = 444
    proc {:x == ZZZ}.to_sql.should == "(x = 444)"
    
    CCCD = Module.new
    CCCD::DDD = 'hi'
    proc {:x == CCCD::DDD}.to_sql.should == "(x = 'hi')"
  end
  
  specify "should support instance attributes" do
    @abc = 123
    proc {:x == @abc}.to_sql.should == "(x = 123)"
  end
  
  specify "should support class attributes" do
    @@abc = 321
    proc {:x == @@abc}.to_sql.should == "(x = 321)"
  end
  
  specify "should support like? pattern" do
    proc {:x.like? '%abc'}.to_sql.should == "(x LIKE '%abc')"
  end
  
  specify "should support =~ operator" do
    # stock SQL version does not know about regexps
    proc {:x =~ '123'}.to_sql.should == "(x LIKE '123')"
  end
  
  specify "should raise on =~ operator for unsupported types" do
    # stock SQL version does not know about regexps
    proc {proc {:x =~ /123/}.to_sql}.should raise_error(SequelError)
    proc {proc {:x =~ 123}.to_sql}.should raise_error(SequelError)
  end
  
  specify "should support != operator" do
    proc {:x != 100}.to_sql.should == "(NOT (x = 100))"
  end

  specify "should support !~ operator" do
    proc {:x !~ '123'}.to_sql.should == "(NOT (x LIKE '123'))"
  end
  
  specify "should support ! operator" do
    proc {!:x}.to_sql.should == "(x = 'f')"
    proc {!(:x > 100)}.to_sql.should == "(NOT (x > 100))"
  end
  
  specify "should support && operator" do
    proc {1 && 2}.to_sql.should == "(1 AND 2)"
    proc {:x > 100 && :y < 100}.to_sql.should == "((x > 100) AND (y < 100))"
    proc {:x && :y && :z}.to_sql.should == "(x AND (y AND z))"
  end
  
  specify "should concatenate separate statements using AND" do
    proc {:x == 20; :y == 30}.to_sql.should == "((x = 20) AND (y = 30))"
    proc {:x != 1; :y != 2; :z != 3}.to_sql.should == \
      "((NOT (x = 1)) AND (NOT (y = 2)) AND (NOT (z = 3)))"
  end
  
  specify "should support || operator" do
    proc {1 || 2}.to_sql.should == "(1 OR 2)"
    proc {:x > 100 || :y < 100}.to_sql.should == "((x > 100) OR (y < 100))"
    proc {:x || :y || :z}.to_sql.should == "(x OR (y OR z))"
  end
  
  specify "should support operator combinations" do
    proc {(:x > 1 || :y > 2) && (:z > 3)}.to_sql.should == "(((x > 1) OR (y > 2)) AND (z > 3))"
    proc {(1 && 2) || (3 || 4)}.to_sql.should == "((1 AND 2) OR (3 OR 4))"
    proc {(:x != 2) || (:y == 3) || !(:z == 4)}.to_sql.should == \
      "((NOT (x = 2)) OR ((y = 3) OR (NOT (z = 4))))"
  end
  
  specify "should support late bound column references" do
    def abc; :tttt; end
    proc {abc > 2}.to_sql.should == "(tttt > 2)"
  end
  
  specify "should support qualified column references" do
    proc {:x__y > 3}.to_sql.should == "(x.y > 3)"
  end
  
  specify "should support functions on columns" do
    proc {:x.MAX > 100}.to_sql.should == "(max(x) > 100)"
    proc {:x.COUNT > 100}.to_sql.should == "(count(x) > 100)"
  end
  
  specify "should support SQL functions" do
    proc {:MAX[:x] > 100}.to_sql.should == "(MAX(x) > 100)"

    proc {:MAX[:x__y] > 100}.to_sql.should == "(MAX(x.y) > 100)"
  end
  
  specify "should support SQL functions with multiple arguments" do
    proc {:sum[1, 2, 3] > 100}.to_sql.should == "(sum(1, 2, 3) > 100)"
    
    proc {:x[1, DB[:y].select(:z), "a'b"] > 100}.to_sql.should == \
      "(x(1, (SELECT z FROM y), 'a''b') > 100)"
  end
  
  specify "should do stuff like..." do
    proc {:price < 100 || :category != 'ruby'}.to_sql.should == \
      "((price < 100) OR (NOT (category = 'ruby')))"
    t = Time.now
    proc {:node_id == 1 && :stamp < t}.to_sql.should == \
      "((node_id = 1) AND (stamp < #{DS.literal(t)}))"
      
    proc {1 < :x}.to_sql.should == "(1 < x)"
  end
  
  specify "should complain if someone is crazy" do
    proc {proc {def x; 1; end}.to_sql}.should raise_error(SequelError)
    a = 1
    proc {proc {a = 1}.to_sql}.should raise_error(SequelError)
  end
  
  specify "should support comparison to Range objects" do
    proc {:x == (1..10)}.to_sql.should == \
      "(x >= 1 AND x <= 10)"

    proc {:x == (1...10)}.to_sql.should == \
      "(x >= 1 AND x < 10)"
      
    a, b = 3, 5
    proc {:x == (a..b)}.to_sql.should == \
      "(x >= 3 AND x <= 5)"

    proc {:x == (a...b)}.to_sql.should == \
      "(x >= 3 AND x < 5)"
      
    t1 = Time.now - 4000
    t2 = Time.now - 2000
    
    proc {:stamp == (t1..t2)}.to_sql.should == \
      "(stamp >= #{DS.literal(t1)} AND stamp <= #{DS.literal(t2)})"
  end

  specify "should support comparison to sub-queries" do
    @ds2 = DB[:test].select(:node_id)
    
    proc {:id == @ds2}.to_sql.should == \
      "(id IN (SELECT node_id FROM test))"
      
    proc {:id == DB[:test].select(:node_id)}.to_sql.should == \
    "(id IN (SELECT node_id FROM test))"

    proc {:id == DB[:test].select(:node_id).filter {:active == true}}.to_sql.should == \
      "(id IN (SELECT node_id FROM test WHERE (active = 't')))"
    
    proc {:price >= DB[:items].select(:price)}.to_sql.should == \
      "(price >= (SELECT price FROM items))"
  end

  specify "should support comparison to arrays" do
    proc {:id == [1, 3, 7, 15]}.to_sql.should == \
      "(id IN (1, 3, 7, 15))"
  end
  
  specify "should not literalize String#expr and String#lit" do
    proc {'x'.lit == 1}.to_sql.should == "(x = 1)"
    proc {'x.y'.expr == 1}.to_sql.should == "(x.y = 1)"
  end

  specify "should support in/in? operator" do
    proc {:x.in [3, 4, 5]}.to_sql.should == "(x IN (3, 4, 5))"
    proc {:x.in?(3, 4, 5)}.to_sql.should == "(x IN (3, 4, 5))"

    proc {:x.in(1..10)}.to_sql.should == "(x >= 1 AND x <= 10)"
    proc {:x.in?(1..10)}.to_sql.should == "(x >= 1 AND x <= 10)"

    @ds2 = DB[:test].select(:node_id)
    proc {:x.in @ds2}.to_sql.should == "(x IN (SELECT node_id FROM test))"
  end
  
  specify "should support nested procs" do
    proc {:x > 10 || proc{:y > 20}}.to_sql.should == \
      "((x > 10) OR (y > 20))"
    
    def pr(&block)
      proc {:x > 10 || block}
    end
    
    pr {:y > 20}.to_sql.should == \
      "((x > 10) OR (y > 20))"
  end
  
  specify "should support local arguments" do
    def t(x)
      proc {x > 10}.to_sql
    end
    t(:y).should == "(y > 10)"
  end
  
  specify "should support binary operators on local context" do
    XXX = 1
    YYY = 2
    proc {XXX || YYY}.to_sql.should == "(1 OR 2)"
    
    xxx = 1
    yyy = 2
    proc {xxx && yyy}.to_sql.should == "(1 AND 2)"
  end
  
  specify "should support arithmetics" do
    zzz = 300
    proc {(:x + 100) > zzz}.to_sql.should == "((x + 100) > 300)"
    
    proc {(:x + :y * 100) > zzz}.to_sql.should == "((x + (y * 100)) > 300)"
    
    proc {:units * :price}.to_sql.should == "(units * price)"
  end
  
  specify "should support globals" do
    $aaaa_zzzz = 400
    proc {:x > $aaaa_zzzz}.to_sql.should == "(x > 400)"
  end
  
  specify "should support Regexp macros" do
    "abc" =~ /(ab)/
    proc {:x == $1}.to_sql.should == "(x = 'ab')"
  end
  
  specify "should evaluate expression not referring to symbols or literal strings." do
    proc {:x > 2 * 3}.to_sql.should == "(x > 6)"
    y = 3
    proc {:x > y * 4}.to_sql.should == "(x > 12)"

    proc {:AVG[:x] > 4}.to_sql.should == "(AVG(x) > 4)"

    proc {:AVG[:x] > 4}.to_sql.should == "(AVG(x) > 4)"
    
    proc {:y == (1 > 2)}.to_sql.should == "(y = 'f')"
  end
  
  specify "should support ternary operator" do
    y = true
    proc {:x > (y ? 1 : 2)}.to_sql.should == "(x > 1)"
    
    proc {((1 > 2) ? :x : :y) > 3}.to_sql.should == "(y > 3)"
  end
end
