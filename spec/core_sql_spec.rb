require File.join(File.dirname(__FILE__), 'spec_helper')

context "Array#to_sql" do
  specify "should concatenate multiple lines into a single string" do
    "SELECT * \r\nFROM items\r\n WHERE a = 1".split.to_sql. \
      should == 'SELECT * FROM items WHERE a = 1'
  end
  
  specify "should remove superfluous white space and line breaks" do
    "\tSELECT * \n FROM items    ".split.to_sql. \
      should == 'SELECT * FROM items'
  end
  
  specify "should remove ANSI SQL comments" do
    "SELECT *   --comment\r\n  FROM items\r\n  --comment".split.to_sql. \
      should == 'SELECT * FROM items'
  end
  
  specify "should remove C-style comments" do
    "SELECT * \r\n /* comment comment\r\n comment\r\n FROM items */\r\n FROM items\r\n--comment".split.to_sql. \
      should == 'SELECT * FROM items'
  end
end

context "String#to_sql" do
  specify "should concatenate multiple lines into a single string" do
    "SELECT * \r\nFROM items\r\nWHERE a = 1".to_sql. \
      should == 'SELECT * FROM items WHERE a = 1'
  end
  
  specify "should remove superfluous white space and line breaks" do
    "\tSELECT * \r\n FROM items    ".to_sql. \
      should == 'SELECT * FROM items'
  end
  
  specify "should remove ANSI SQL comments" do
    "SELECT *   --comment \r\n FROM items\r\n  --comment".to_sql. \
      should == 'SELECT * FROM items'
  end
  
  specify "should remove C-style comments" do
    "SELECT * \r\n/* comment comment\r\ncomment\r\nFROM items */\r\nFROM items\r\n--comment".to_sql. \
      should == 'SELECT * FROM items'
  end
end

context "String#lit" do
  specify "should return an LiteralString object" do
    'xyz'.lit.should be_a_kind_of(Sequel::LiteralString)
    'xyz'.lit.to_s.should == 'xyz'
  end
  
  specify "should inhibit string literalization" do
    db = Sequel::Database.new
    ds = db[:t]
    
    ds.update_sql(:stamp => "NOW()".lit).should == \
      "UPDATE t SET stamp = NOW()"
  end
end

context "String#expr" do
  specify "should return an LiteralString object" do
    'xyz'.expr.should be_a_kind_of(Sequel::LiteralString)
    'xyz'.expr.to_s.should == 'xyz'
  end

  specify "should inhibit string literalization" do
    db = Sequel::Database.new
    ds = db[:t]
    
    ds.update_sql(:stamp => "NOW()".expr).should == \
      "UPDATE t SET stamp = NOW()"
  end
end

context "String#split_sql" do
  specify "should split a string containing multiple statements" do
    "DROP TABLE a; DROP TABLE c".split_sql.should == \
      ['DROP TABLE a', 'DROP TABLE c']
  end
  
  specify "should remove comments from the string" do
    "DROP TABLE a;/* DROP TABLE b; DROP TABLE c;*/DROP TABLE d".split_sql.should == \
      ['DROP TABLE a', 'DROP TABLE d']
  end
end

context "#DESC/#desc" do
  setup do
    @ds = Sequel::Dataset.new(nil)
  end
  
  specify "should format a DESC clause for a column ref" do
    :test.DESC.to_s(@ds).should == 'test DESC'
    :test.desc.to_s(@ds).should == 'test DESC'
    
    :items__price.DESC.to_s(@ds).should == 'items.price DESC'
    :items__price.desc.to_s(@ds).should == 'items.price DESC'
  end

  specify "should format a DESC clause for a function" do
    :avg[:test].DESC.to_s(@ds).should == 'avg(test) DESC'
    :avg[:test].desc.to_s(@ds).should == 'avg(test) DESC'
  end
  
  specify "should format a DESC clause for a literal value" do
    1.DESC.to_s(@ds).should == '1 DESC'
    'abc'.desc.to_s(@ds).should == "'abc' DESC"
  end
end

context "#ASC/#asc" do
  setup do
    @ds = Sequel::Dataset.new(nil)
  end
  
  specify "should format a ASC clause for a column ref" do
    :test.ASC.to_s(@ds).should == 'test ASC'
    :test.asc.to_s(@ds).should == 'test ASC'
    
    :items__price.ASC.to_s(@ds).should == 'items.price ASC'
    :items__price.asc.to_s(@ds).should == 'items.price ASC'
  end

  specify "should format a ASC clause for a function" do
    :avg[:test].ASC.to_s(@ds).should == 'avg(test) ASC'
    :avg[:test].asc.to_s(@ds).should == 'avg(test) ASC'
  end
  
  specify "should format a ASC clause for a literal value" do
    1.ASC.to_s(@ds).should == '1 ASC'
    'abc'.asc.to_s(@ds).should == "'abc' ASC"
  end
end

context "#AS/#as" do
  setup do
    @ds = Sequel::Dataset.new(nil)
  end
  
  specify "should format a AS clause for a column ref" do
    :test.AS(:t).to_s(@ds).should == 'test AS t'
    :test.as(:t).to_s(@ds).should == 'test AS t'
    
    :items__price.AS(:p).to_s(@ds).should == 'items.price AS p'
    :items__price.as(:p).to_s(@ds).should == 'items.price AS p'
  end

  specify "should format a AS clause for a function" do
    :avg[:test].AS(:avg).to_s(@ds).should == 'avg(test) AS avg'
    :avg[:test].as(:avg).to_s(@ds).should == 'avg(test) AS avg'
  end
  
  specify "should format a AS clause for a literal value" do
    1.AS(:one).to_s(@ds).should == '1 AS one'
    'abc'.as(:abc).to_s(@ds).should == "'abc' AS abc"
  end
end

context "Column references" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      def quote_column_ref(c); "`#{c}`"; end
    end
    @ds = @c.new(nil)
  end
  
  specify "should be quoted properly" do
    @ds.literal(:xyz).should == "`xyz`"
    @ds.literal(:xyz__abc).should == "xyz.`abc`"

    @ds.literal(:xyz.as(:x)).should == "`xyz` AS `x`"
    @ds.literal(:xyz__abc.as(:x)).should == "xyz.`abc` AS `x`"

    @ds.literal(:xyz___x).should == "`xyz` AS `x`"
    @ds.literal(:xyz__abc___x).should == "xyz.`abc` AS `x`"
  end
  
  specify "should be quoted properly in SQL functions" do
    @ds.literal(:avg[:xyz]).should == "avg(`xyz`)"
    @ds.literal(:avg[:xyz, 1]).should == "avg(`xyz`, 1)"
    @ds.literal(:avg[:xyz].as(:a)).should == "avg(`xyz`) AS `a`"
  end

  specify "should be quoted properly in ASC/DESC clauses" do
    @ds.literal(:xyz.ASC).should == "`xyz` ASC"
    @ds.literal(:avg[:xyz, 1].desc).should == "avg(`xyz`, 1) DESC"
  end
  
  specify "should be quoted properly in a cast function" do
    @ds.literal(:x.cast_as(:integer)).should == "cast(`x` AS integer)"
    @ds.literal(:x__y.cast_as(:varchar[20])).should == "cast(x.`y` AS varchar(20))"
  end
end

context "Symbol#ALL/#all" do
  setup do
    @ds = Sequel::Dataset.new(nil)
  end
  
  specify "should format a qualified wildcard" do
    :xyz.ALL.to_s(@ds).should == 'xyz.*'
    :abc.all.to_s(@ds).should == 'abc.*'
  end
end

context "Symbol#to_column_ref" do
  setup do
    @ds = Sequel::Dataset.new(nil)
  end
  
  specify "should convert qualified symbol notation into dot notation" do
    :abc__def.to_column_ref(@ds).should == 'abc.def'
  end
  
  specify "should convert AS symbol notation into SQL AS notation" do
    :xyz___x.to_column_ref(@ds).should == 'xyz AS x'
    :abc__def___x.to_column_ref(@ds).should == 'abc.def AS x'
  end
  
  specify "should support names with digits" do
    :abc2.to_column_ref(@ds).should == 'abc2'
    :xx__yy3.to_column_ref(@ds).should == 'xx.yy3'
    :ab34__temp3_4ax.to_column_ref(@ds).should == 'ab34.temp3_4ax'
    :x1___y2.to_column_ref(@ds).should == 'x1 AS y2'
    :abc2__def3___ggg4.to_column_ref(@ds).should == 'abc2.def3 AS ggg4'
  end
  
  specify "should support upper case and lower case" do
    :ABC.to_column_ref(@ds).should == 'ABC'
    :Zvashtoy__aBcD.to_column_ref(@ds).should == 'Zvashtoy.aBcD'
  end
end

context "Symbol" do
  setup do
    @ds = Sequel::Dataset.new(nil)
  end
  
  specify "should support MIN for specifying min function" do
    :abc__def.MIN.to_s(@ds).should == 'min(abc.def)'
  end

  specify "should support MAX for specifying max function" do
    :abc__def.MAX.to_s(@ds).should == 'max(abc.def)'
  end

  specify "should support SUM for specifying sum function" do
    :abc__def.SUM.to_s(@ds).should == 'sum(abc.def)'
  end

  specify "should support AVG for specifying avg function" do
    :abc__def.AVG.to_s(@ds).should == 'avg(abc.def)'
  end
  
  specify "should support COUNT for specifying count function" do
    :abc__def.COUNT.to_s(@ds).should == 'count(abc.def)'
  end
  
  specify "should support any other function using upper case letters" do
    :abc__def.DADA.to_s(@ds).should == 'dada(abc.def)'
  end
  
  specify "should support upper case outer functions" do
    :COUNT['1'].to_s(@ds).should == "COUNT('1')"
  end
  
  specify "should inhibit string literalization" do
    db = Sequel::Database.new
    ds = db[:t]
    ds.select(:COUNT['1']).sql.should == "SELECT COUNT('1') FROM t"
  end
  
  specify "should support cast function" do
    :abc.cast_as(:integer).to_s(@ds).should == "cast(abc AS integer)"
  end
  
  specify "should raise NoMethodError for non-uppercase invalid methods" do
    proc {:abc.dfaxs}.should raise_error(NoMethodError)
  end
end

