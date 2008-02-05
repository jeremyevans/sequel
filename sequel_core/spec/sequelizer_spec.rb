require File.join(File.dirname(__FILE__), 'spec_helper')

context "Sequelizer without ParseTree" do
  setup do
    module Kernel
      alias_method :orig_sq_require, :require
      def require(*args); raise LoadError; end
    end
    old_verbose = $VERBOSE
    $VERBOSE = nil
    load(File.join(File.dirname(__FILE__), '../lib/sequel_core/dataset/sequelizer.rb'))
    $VERBOSE = old_verbose
    @db = Sequel::Database.new
    @ds = @db[:items]
  end
  
  teardown do
    module Kernel
      alias_method :require, :orig_sq_require
    end
    old_verbose = $VERBOSE
    $VERBOSE = nil
    load(File.join(File.dirname(__FILE__), '../lib/sequel_core/dataset/sequelizer.rb'))
    $VERBOSE = old_verbose
  end
  
  specify "should raise error when converting proc to SQL" do
    proc {proc {:x > 1}.to_sql(@ds)}.should raise_error(Sequel::Error)
  end
end

context "Sequelizer without Ruby2Ruby" do
  setup do
    module Kernel
      alias_method :orig_sq_require, :require
      def require(name); raise LoadError if name == 'ruby2ruby'; end
    end
    old_verbose = $VERBOSE
    $VERBOSE = nil
    load(File.join(File.dirname(__FILE__), '../lib/sequel_core/dataset/sequelizer.rb'))
    $VERBOSE = old_verbose
    @db = Sequel::Database.new
    @ds = @db[:items]
  end
  
  teardown do
    module Kernel
      alias_method :require, :orig_sq_require
    end
    old_verbose = $VERBOSE
    $VERBOSE = nil
    load(File.join(File.dirname(__FILE__), '../lib/sequel_core/dataset/sequelizer.rb'))
    $VERBOSE = old_verbose
  end
  
  specify "should raise error only when using external expressions" do
    proc {proc {:x > 1}.to_sql(@ds)}.should_not raise_error(Sequel::Error)
    proc {proc {1 + 1}.to_sql(@ds)}.should raise_error(Sequel::Error)
  end
end

context "Proc without #to_sexp method (Ruby2Ruby missing)" do
  setup do
    class Proc
      alias_method :orig_to_sexp, :to_sexp
      remove_method :to_sexp
    end
    
    module Kernel
      alias_method :orig_sq_require, :require
      def require(name); raise LoadError if name == 'ruby2ruby'; end
    end
    old_verbose = $VERBOSE
    $VERBOSE = nil
    load(File.join(File.dirname(__FILE__), '../lib/sequel_core/dataset/sequelizer.rb'))
    $VERBOSE = old_verbose
    @db = Sequel::Database.new
    @ds = @db[:items]
  end
  
  teardown do
    module Kernel
      alias_method :require, :orig_sq_require
    end
    old_verbose = $VERBOSE
    $VERBOSE = nil
    load(File.join(File.dirname(__FILE__), '../lib/sequel_core/dataset/sequelizer.rb'))
    $VERBOSE = old_verbose
    
    class Proc
      alias_method :to_sexp, :orig_to_sexp
    end
  end
  
  specify "should define a replacement Proc#to_sexp implementation" do
    pr = proc {1 + 1}
    proc {pr.to_sexp}.should_not raise_error
    pr.to_sexp.should == [:bmethod, nil, [:call, [:lit, 1], :+, [:array, [:lit, 1]]]]
  end
end

context "Proc#to_sql" do
  DB = Sequel::Database.new
  DS = DB[:items]

  class ::Proc
    def sql
      to_sql(DS)
    end
    
    def sql_comma_separated
      to_sql(DS, :comma_separated => true)
    end
  end
  
  def DS.match_expr(l, r)
    case r
    when String
      "(#{literal(l)} LIKE #{literal(r)})"
    when Regexp
      "(#{literal(l)} ~ #{literal(r.source)})"
    else
      raise Sequel::Error, "Unsupported match pattern class (#{r.class})."
    end
  end
  
  specify "should support <sym> <op> <lit>" do
    proc {:x > 100}.sql.should == '(x > 100)'
    proc {:x < 100}.sql.should == '(x < 100)'
    proc {:x >= 100}.sql.should == '(x >= 100)'
    proc {:x <= 100}.sql.should == '(x <= 100)'
    proc {:x == 100}.sql.should == '(x = 100)'
  end
  
  specify "should support number literals" do
    proc {:x > 123.45}.sql.should == '(x > 123.45)'
    proc {:x > -30_000}.sql.should == '(x > -30000)'
  end

  specify "should support string literals" do
    proc {:x == 'abc'}.sql.should == "(x = 'abc')"
    proc {:y == "ab'cd"}.sql.should == "(y = 'ab''cd')"
  end
  
  specify "should support boolean literals" do
    proc {:x == false}.sql.should == "(x = 'f')"
    proc {:x == true}.sql.should == "(x = 't')"
  end
  
  specify "should support nil literal and nil?" do
    proc {:x == nil}.sql.should == "(x IS NULL)"
    proc {:x.nil?}.sql.should == "(x IS NULL)"
  end
  
  specify "should support local vars or method references" do
    proc {proc {:x == a}.sql}.should raise_error(NameError)
    b = 123
    proc {:x == b}.sql.should == "(x = 123)"
    def xyz; 321; end
    proc {:x == xyz}.sql.should == "(x = 321)"
    proc {:x == xyz.to_s}.sql.should == "(x = '321')"
    
    def y1(x); x; end
    def y2; 111; end
    
    proc {:x == y1(222)}.sql.should == "(x = 222)"
    proc {:x == y2}.sql.should == "(x = 111)"
  end
  
  specify "sould support subscript access on symbols" do
    proc {:x|1 > 0}.sql.should == "(x[1] > 0)"
    proc {:x|2|3 > 0}.sql.should == "(x[2, 3] > 0)"
    proc {:x|[4, 5] > 0}.sql.should == "(x[4, 5] > 0)"
  end
  
  specify "should support constants" do
    ZZZ = 444
    proc {:x == ZZZ}.sql.should == "(x = 444)"
    
    CCCD = Module.new
    CCCD::DDD = 'hi'
    proc {:x == CCCD::DDD}.sql.should == "(x = 'hi')"
  end
  
  specify "should support instance attributes" do
    @abc = 123
    proc {:x == @abc}.sql.should == "(x = 123)"
  end
  
  specify "should support class attributes" do
    @@abc = 321
    proc {:x == @@abc}.sql.should == "(x = 321)"
  end
  
  specify "should support like? pattern" do
    proc {:x.like? '%abc'}.sql.should == "(x LIKE '%abc')"
  end
  
  specify "should support =~ operator" do
    # stock SQL version does not know about regexps
    proc {:x =~ '123'}.sql.should == "(x LIKE '123')"

    proc {:x =~ /^123/}.sql.should == "(x ~ '^123')"
  end
  
  specify "should raise on =~ operator for unsupported types" do
    proc {proc {:x =~ 123}.sql}.should raise_error(Sequel::Error)
  end
  
  specify "should support != operator" do
    proc {:x != 100}.sql.should == "(NOT (x = 100))"
  end

  specify "should support !~ operator" do
    proc {:x !~ '123'}.sql.should == "(NOT (x LIKE '123'))"
  end
  
  specify "should support ! operator" do
    proc {!:x}.sql.should == "(x = 'f')"
    proc {!(:x > 100)}.sql.should == "(NOT (x > 100))"
  end
  
  specify "should support && operator" do
    proc {1 && 2}.sql.should == "(1 AND 2)"
    proc {:x > 100 && :y < 100}.sql.should == "((x > 100) AND (y < 100))"
    proc {:x && :y && :z}.sql.should == "(x AND (y AND z))"
  end
  
  specify "should support << operator for assignment" do
    proc {:x << 1}.sql.should == "x = 1"
  end
  
  specify "should concatenate separate statements using AND" do
    proc {:x == 20; :y == 30}.sql.should == "((x = 20) AND (y = 30))"
    proc {:x != 1; :y != 2; :z != 3}.sql.should == \
      "((NOT (x = 1)) AND (NOT (y = 2)) AND (NOT (z = 3)))"
  end
  
  specify "should concatenate separate statements using custom join argument" do
    proc {:x << 20; :y << 30}.sql_comma_separated.should == "x = 20, y = 30"
    z = 333
    proc {:x << :x + 1; :y << z}.sql_comma_separated.should == "x = (x + 1), y = 333"
  end
  
  specify "should support || operator" do
    proc {1 || 2}.sql.should == "(1 OR 2)"
    proc {:x > 100 || :y < 100}.sql.should == "((x > 100) OR (y < 100))"
    proc {:x || :y || :z}.sql.should == "(x OR (y OR z))"
  end
  
  specify "should support operator combinations" do
    proc {(:x > 1 || :y > 2) && (:z > 3)}.sql.should == "(((x > 1) OR (y > 2)) AND (z > 3))"
    proc {(1 && 2) || (3 || 4)}.sql.should == "((1 AND 2) OR (3 OR 4))"
    proc {(:x != 2) || (:y == 3) || !(:z == 4)}.sql.should == \
      "((NOT (x = 2)) OR ((y = 3) OR (NOT (z = 4))))"
  end
  
  specify "should support late bound column references" do
    def abc; :tttt; end
    proc {abc > 2}.sql.should == "(tttt > 2)"
  end
  
  specify "should support qualified column references" do
    proc {:x__y > 3}.sql.should == "(x.y > 3)"
  end
  
  specify "should support functions on columns" do
    proc {:x.MAX > 100}.sql.should == "(max(x) > 100)"
    proc {:x.COUNT > 100}.sql.should == "(count(x) > 100)"
  end
  
  specify "should support SQL functions" do
    proc {:MAX[:x] > 100}.sql.should == "(MAX(x) > 100)"

    proc {:MAX[:x__y] > 100}.sql.should == "(MAX(x.y) > 100)"
  end
  
  specify "should support SQL functions with multiple arguments" do
    proc {:sum[1, 2, 3] > 100}.sql.should == "(sum(1, 2, 3) > 100)"
    
    proc {:x[1, DB[:y].select(:z), "a'b"] > 100}.sql.should == \
      "(x(1, (SELECT z FROM y), 'a''b') > 100)"
  end
  
  specify "should support SQL functions without arguments" do
    proc {:abc[] > 100}.sql.should == "(abc() > 100)"
    
    proc {:now[] - :last_stamp > 100}.sql.should == \
      "((now() - last_stamp) > 100)"
  end
  
  specify "should do stuff like..." do
    proc {:price < 100 || :category != 'ruby'}.sql.should == \
      "((price < 100) OR (NOT (category = 'ruby')))"
    t = Time.now
    proc {:node_id == 1 && :stamp < t}.sql.should == \
      "((node_id = 1) AND (stamp < #{DS.literal(t)}))"
      
    proc {1 < :x}.sql.should == "(1 < x)"
  end
  
  specify "should complain if someone is crazy" do
    proc {proc {def x; 1; end}.sql}.should raise_error(Sequel::Error::InvalidExpression)
    a = 1
    proc {proc {a = 1}.sql}.should raise_error(Sequel::Error::InvalidExpression)
  end
  
  specify "should support comparison to Range objects" do
    proc {:x == (1..10)}.sql.should == \
      "(x >= 1 AND x <= 10)"

    proc {:x == (1...10)}.sql.should == \
      "(x >= 1 AND x < 10)"
      
    a, b = 3, 5
    proc {:x == (a..b)}.sql.should == \
      "(x >= 3 AND x <= 5)"

    proc {:x == (a...b)}.sql.should == \
      "(x >= 3 AND x < 5)"
      
    t1 = Time.now - 4000
    t2 = Time.now - 2000
    
    proc {:stamp == (t1..t2)}.sql.should == \
      "(stamp >= #{DS.literal(t1)} AND stamp <= #{DS.literal(t2)})"
  end

  specify "should support comparison to sub-queries" do
    @ds2 = DB[:test].select(:node_id)
    
    proc {:id == @ds2}.sql.should == \
      "(id IN (SELECT node_id FROM test))"
      
    proc {:id == DB[:test].select(:node_id)}.sql.should == \
    "(id IN (SELECT node_id FROM test))"

    proc {:id == DB[:test].select(:node_id).filter {:active == true}}.sql.should == \
      "(id IN (SELECT node_id FROM test WHERE (active = 't')))"
    
    proc {:price >= DB[:items].select(:price)}.sql.should == \
      "(price >= (SELECT price FROM items))"
  end

  specify "should support comparison to arrays" do
    proc {:id == [1, 3, 7, 15]}.sql.should == \
      "(id IN (1, 3, 7, 15))"
  end
  
  specify "should not literalize String#expr and String#lit" do
    proc {'x'.lit == 1}.sql.should == "(x = 1)"
    proc {'x.y'.expr == 1}.sql.should == "(x.y = 1)"
  end

  specify "should support in/in? operator" do
    proc {:x.in [3, 4, 5]}.sql.should == "(x IN (3, 4, 5))"
    proc {:x.in?(3, 4, 5)}.sql.should == "(x IN (3, 4, 5))"

    proc {:x.in(1..10)}.sql.should == "(x >= 1 AND x <= 10)"
    proc {:x.in?(1..10)}.sql.should == "(x >= 1 AND x <= 10)"

    @ds2 = DB[:test].select(:node_id)
    proc {:x.in @ds2}.sql.should == "(x IN (SELECT node_id FROM test))"
  end
  
  specify "should support nested procs" do
    proc {:x > 10 || proc{:y > 20}}.sql.should == \
      "((x > 10) OR (y > 20))"
    
    def pr(&block)
      proc {:x > 10 || block}
    end
    
    pr {:y > 20}.sql.should == \
      "((x > 10) OR (y > 20))"
  end
  
  specify "should support unfolding of calls to #each" do
    # from http://groups.google.com/group/sequel-talk/browse_thread/thread/54a660568515fbb7
    periods = [:day, :week, :month, :year, :alltime]
    idx = 1
    v = 2
    pr = proc do
      periods.each do |p|
        (p|idx) << (p|idx) + v
      end
    end
    pr.sql_comma_separated.should == \
      "day[1] = (day[1] + 2), week[1] = (week[1] + 2), month[1] = (month[1] + 2), year[1] = (year[1] + 2), alltime[1] = (alltime[1] + 2)"
  end
  
  specify "should support unfolding of calls to Hash#each" do
    periods = {:month => 3}
    idx = 1
    pr = proc do
      periods.each do |k, v|
        k << k + v
      end
    end
    pr.sql_comma_separated.should == "month = (month + 3)"
  end
  
  specify "should support local arguments" do
    def t(x)
      proc {x > 10}.sql
    end
    t(:y).should == "(y > 10)"
  end
  
  specify "should support binary operators on local context" do
    XXX = 1
    YYY = 2
    proc {XXX || YYY}.sql.should == "(1 OR 2)"
    
    xxx = 1
    yyy = 2
    proc {xxx && yyy}.sql.should == "(1 AND 2)"
  end
  
  specify "should support arithmetics" do
    zzz = 300
    proc {(:x + 100) > zzz}.sql.should == "((x + 100) > 300)"
    
    proc {(:x + :y * 100) > zzz}.sql.should == "((x + (y * 100)) > 300)"
    
    proc {:units * :price}.sql.should == "(units * price)"
  end
  
  specify "should support | operator" do
    proc {(:x | 1) > 0}.sql.should == "(x[1] > 0)"
    proc {10 | 1}.sql.should == 11
  end
  
  specify "should support globals" do
    $aaaa_zzzz = 400
    proc {:x > $aaaa_zzzz}.sql.should == "(x > 400)"
  end
  
  specify "should support Regexp macros" do
    "abc" =~ /(ab)/
    proc {:x == $1}.sql.should == "(x = 'ab')"
  end
  
  specify "should evaluate expression not referring to symbols or literal strings." do
    proc {:x > 2 * 3}.sql.should == "(x > 6)"
    y = 3
    proc {:x > y * 4}.sql.should == "(x > 12)"

    proc {:AVG[:x] > 4}.sql.should == "(AVG(x) > 4)"

    proc {:AVG[:x] > 4}.sql.should == "(AVG(x) > 4)"
    
    proc {:y == (1 > 2)}.sql.should == "(y = 'f')"
  end
  
  specify "should support ternary operator" do
    y = true
    proc {:x > (y ? 1 : 2)}.sql.should == "(x > 1)"
    
    proc {((1 > 2) ? :x : :y) > 3}.sql.should == "(y > 3)"
  end
  
  specify "should support strings with embedded Ruby code in them and literalize them" do
    proc {:n == "#{1+2}"}.sql.should == "(n = '3')"
    
    y = "12'34"
    
    proc {:x > "#{y}"}.sql.should == "(x > '12''34')"
  end
  
  specify "should support format strings and literalize the result" do
    prod = 1
    proc {:x == "abc%d" % prod}.sql.should == "(x = 'abc1')"
    
    proc {:x == ("%d" % prod).lit}.sql.should == "(x = 1)"
  end
  
  specify "should support conditional filters" do
    @criteria = nil
    proc {if @criteria; :x.like @criteria; end}.sql.should == nil
    
    @criteria = 'blah'
    proc {if @criteria; :x.like @criteria; end}.sql.should == "(x LIKE 'blah')"

    @criteria = nil
    proc {if @criteria; :x.like @criteria; else; :x.like 'ddd'; end}.sql.should == "(x LIKE 'ddd')"
  end
end

context "Proc#to_sql stock" do
  specify "should not support regexps" do
    db = Sequel::Database.new
    ds = db[:items]

    p = proc {:x =~ /abc/}
    proc {p.to_sql(ds)}.should raise_error(Sequel::Error)
  end
end

