require File.join(File.dirname(__FILE__), 'spec_helper')

context "Array#all_two_pairs?" do
  specify "should return false if empty" do
    [].all_two_pairs?.should == false
  end

  specify "should return false if any of the elements is not an array" do
    [1].all_two_pairs?.should == false
    [[1,2],1].all_two_pairs?.should == false
  end

  specify "should return false if any of the elements has a length other than two" do
    [[1,2],[]].all_two_pairs?.should == false
    [[1,2],[1]].all_two_pairs?.should == false
    [[1,2],[1,2,3]].all_two_pairs?.should == false
  end

  specify "should return true if all of the elements are arrays with a length of two" do
    [[1,2]].all_two_pairs?.should == true
    [[1,2],[1,2]].all_two_pairs?.should == true
    [[1,2],[1,2],[1,2]].all_two_pairs?.should == true
  end
end
  
context "Array#case and Hash#case" do
  setup do
    @d = Sequel::Dataset.new(nil)
  end

  specify "should return SQL CASE expression" do
    @d.literal({:x=>:y}.case(:z)).should == '(CASE WHEN x THEN y ELSE z END)'
    ['(CASE WHEN x THEN y WHEN a THEN b ELSE z END)',
     '(CASE WHEN a THEN b WHEN x THEN y ELSE z END)'].should(include(@d.literal({:x=>:y, :a=>:b}.case(:z))))
    @d.literal([[:x, :y]].case(:z)).should == '(CASE WHEN x THEN y ELSE z END)'
    @d.literal([[:x, :y], [:a, :b]].case(:z)).should == '(CASE WHEN x THEN y WHEN a THEN b ELSE z END)'
  end

  specify "should raise an error if an array that isn't all two pairs is used" do
    proc{[:b].case(:a)}.should raise_error(Sequel::Error)
    proc{[:b, :c].case(:a)}.should raise_error(Sequel::Error)
    proc{[[:b, :c], :d].case(:a)}.should raise_error(Sequel::Error)
  end

  specify "should raise an error if an empty array/hash is used" do
    proc{[].case(:a)}.should raise_error(Sequel::Error)
    proc{{}.case(:a)}.should raise_error(Sequel::Error)
  end
end

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
    Sequel::Database.new[:t].update_sql(:stamp => "NOW()".expr).should == \
      "UPDATE t SET stamp = NOW()"
  end

  specify "should be aliased as expr" do
    'xyz'.expr.should be_a_kind_of(Sequel::LiteralString)
    'xyz'.expr.to_s.should == 'xyz'
    Sequel::Database.new[:t].update_sql(:stamp => "NOW()".expr).should == \
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

context "#desc" do
  setup do
    @ds = Sequel::Dataset.new(nil)
  end
  
  specify "should format a DESC clause for a column ref" do
    :test.desc.to_s(@ds).should == 'test DESC'
    
    :items__price.desc.to_s(@ds).should == 'items.price DESC'
  end

  specify "should format a DESC clause for a function" do
    :avg[:test].desc.to_s(@ds).should == 'avg(test) DESC'
  end
  
  specify "should format a DESC clause for a literal value" do
    'abc'.desc.to_s(@ds).should == "'abc' DESC"
  end
end

context "#asc" do
  setup do
    @ds = Sequel::Dataset.new(nil)
  end
  
  specify "should format a ASC clause for a column ref" do
    :test.asc.to_s(@ds).should == 'test ASC'
    
    :items__price.asc.to_s(@ds).should == 'items.price ASC'
  end

  specify "should format a ASC clause for a function" do
    :avg[:test].asc.to_s(@ds).should == 'avg(test) ASC'
  end
  
  specify "should format a ASC clause for a literal value" do
    'abc'.asc.to_s(@ds).should == "'abc' ASC"
  end
end

context "#as" do
  setup do
    @ds = Sequel::Dataset.new(nil)
  end
  
  specify "should format a AS clause for a column ref" do
    :test.as(:t).to_s(@ds).should == 'test AS t'
    
    :items__price.as(:p).to_s(@ds).should == 'items.price AS p'
  end

  specify "should format a AS clause for a function" do
    :avg[:test].as(:avg).to_s(@ds).should == 'avg(test) AS avg'
  end
  
  specify "should format a AS clause for a literal value" do
    'abc'.as(:abc).to_s(@ds).should == "'abc' AS abc"
  end
end

context "Column references" do
  setup do
    @c = Class.new(Sequel::Dataset) do
      def quoted_identifier(c); "`#{c}`"; end
    end
    @ds = @c.new(nil)
    @ds.quote_identifiers = true
  end
  
  specify "should be quoted properly" do
    @ds.literal(:xyz).should == "`xyz`"
    @ds.literal(:xyz__abc).should == "`xyz`.`abc`"

    @ds.literal(:xyz.as(:x)).should == "`xyz` AS `x`"
    @ds.literal(:xyz__abc.as(:x)).should == "`xyz`.`abc` AS `x`"

    @ds.literal(:xyz___x).should == "`xyz` AS `x`"
    @ds.literal(:xyz__abc___x).should == "`xyz`.`abc` AS `x`"
  end
  
  specify "should be quoted properly in SQL functions" do
    @ds.literal(:avg[:xyz]).should == "avg(`xyz`)"
    @ds.literal(:avg[:xyz, 1]).should == "avg(`xyz`, 1)"
    @ds.literal(:avg[:xyz].as(:a)).should == "avg(`xyz`) AS `a`"
  end

  specify "should be quoted properly in ASC/DESC clauses" do
    @ds.literal(:xyz.asc).should == "`xyz` ASC"
    @ds.literal(:avg[:xyz, 1].desc).should == "avg(`xyz`, 1) DESC"
  end
  
  specify "should be quoted properly in a cast function" do
    @ds.literal(:x.cast_as(:integer)).should == "cast(`x` AS integer)"
    @ds.literal(:x__y.cast_as('varchar(20)')).should == "cast(`x`.`y` AS varchar(20))"
  end
end

context "Symbol#*" do
  setup do
    @ds = Sequel::Dataset.new(nil)
  end
  
  specify "should format a qualified wildcard if no argument" do
    :xyz.*.to_s(@ds).should == 'xyz.*'
    :abc.*.to_s(@ds).should == 'abc.*'
  end

  specify "should format a filter expression if an argument" do
    :xyz.*(3).to_s(@ds).should == '(xyz * 3)'
    :abc.*(5).to_s(@ds).should == '(abc * 5)'
  end
end

context "Symbol#to_column_ref" do
  setup do
    @ds = MockDataset.new(nil)
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

  specify "should support spaces inside column names" do
    @ds.quote_identifiers = true
    :"AB C".to_column_ref(@ds).should == '"AB C"'
    :"Zvas htoy__aB cD".to_column_ref(@ds).should == '"Zvas htoy"."aB cD"'
    :"aB cD___XX XX".to_column_ref(@ds).should == '"aB cD" AS "XX XX"'
    :"Zva shtoy__aB cD___XX XX".to_column_ref(@ds).should == '"Zva shtoy"."aB cD" AS "XX XX"'
  end
end

context "Symbol" do
  setup do
    @ds = Sequel::Dataset.new(nil)
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
  
  specify "should support subscript access using | operator" do
    (:abc|1).to_s(@ds).should == 'abc[1]'
    (:abc|[1]).to_s(@ds).should == 'abc[1]'
    (:abc|[1, 2]).to_s(@ds).should == 'abc[1, 2]'
    (:abc|1|2).to_s(@ds).should == 'abc[1, 2]'
  end

  specify "should support SQL EXTRACT function via #extract " do
    :abc.extract(:year).to_s(@ds).should == "extract(year FROM abc)"
  end
end

context "String#to_time" do
  specify "should convert the string into a Time object" do
    "2007-07-11".to_time.should == Time.parse("2007-07-11")
    "06:30".to_time.should == Time.parse("06:30")
  end
  
  specify "should raise Error::InvalidValue for an invalid time" do
    proc {'0000-00-00'.to_time}.should raise_error(Sequel::Error::InvalidValue)
  end
end

context "String#to_date" do
  specify "should convert the string into a Date object" do
    "2007-07-11".to_date.should == Date.parse("2007-07-11")
  end
  
  specify "should raise Error::InvalidValue for an invalid date" do
    proc {'0000-00-00'.to_date}.should raise_error(Sequel::Error::InvalidValue)
  end
end

context "String#to_datetime" do
  specify "should convert the string into a DateTime object" do
    "2007-07-11 10:11:12a".to_datetime.should == DateTime.parse("2007-07-11 10:11:12a")
  end
  
  specify "should raise Error::InvalidValue for an invalid date" do
    proc {'0000-00-00'.to_datetime}.should raise_error(Sequel::Error::InvalidValue)
  end
end

context "String#to_sequel_time" do
  after do
    Sequel.datetime_class = Time
  end

  specify "should convert the string into a Time object by default" do
    "2007-07-11 10:11:12a".to_sequel_time.class.should == Time
    "2007-07-11 10:11:12a".to_sequel_time.should == Time.parse("2007-07-11 10:11:12a")
  end
  
  specify "should convert the string into a DateTime object if that is set" do
    Sequel.datetime_class = DateTime
    "2007-07-11 10:11:12a".to_sequel_time.class.should == DateTime
    "2007-07-11 10:11:12a".to_sequel_time.should == DateTime.parse("2007-07-11 10:11:12a")
  end
  
  specify "should raise Error::InvalidValue for an invalid time" do
    proc {'0000-00-00'.to_sequel_time}.should raise_error(Sequel::Error::InvalidValue)
    Sequel.datetime_class = DateTime
    proc {'0000-00-00'.to_sequel_time}.should raise_error(Sequel::Error::InvalidValue)
  end
end

context "Sequel::SQL::Function#==" do
  specify "should be true for functions with the same name and arguments, false otherwise" do
    a = :date[:t]
    b = :date[:t]
    a.should == b
    (a == b).should == true
    c = :date[:c]
    a.should_not == c
    (a == c).should == false
    d = :time[:c]
    a.should_not == d
    c.should_not == d
    (a == d).should == false
    (c == d).should == false
  end
end
