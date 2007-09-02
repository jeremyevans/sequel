require File.join(File.dirname(__FILE__), 'spec_helper')

context "Enumerable#send_each" do
  specify "should send the supplied method to each item" do
    a = ['abbc', 'bbccdd', 'hebtre']
    a.send_each(:gsub!, 'b', '_')
    a.should == ['a__c', '__ccdd', 'he_tre']
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

context "String#to_time" do
  specify "should convert the string into a Time object" do
    "2007-07-11".to_time.should == Time.parse("2007-07-11")
    "06:30".to_time.should == Time.parse("06:30")
  end
end

context "Symbol#DESC" do
  specify "should append the symbol with DESC" do
    :hey.DESC.should == 'hey DESC'
  end
  
  specify "should support qualified symbol notation" do
    :abc__def.DESC.should == 'abc.def DESC'
  end
end

context "Symbol#AS" do
  specify "should append an AS clause" do
    :hey.AS(:ho).should == 'hey AS ho'
  end
  
  specify "should support qualified symbol notation" do
    :abc__def.AS(:x).should == 'abc.def AS x'
  end
end

context "Symbol#ALL" do
  specify "should format a qualified wildcard" do
    :xyz.ALL.should == 'xyz.*'
  end
end

context "Symbol#to_field_name" do
  specify "should convert qualified symbol notation into dot notation" do
    :abc__def.to_field_name.should == 'abc.def'
  end
  
  specify "should convert AS symbol notation into SQL AS notation" do
    :xyz___x.to_field_name.should == 'xyz AS x'
    :abc__def___x.to_field_name.should == 'abc.def AS x'
  end
end

context "FieldCompositionMethods#field_title" do
  specify "should return the field name for non aliased fields" do
    :xyz.field_title.should == 'xyz'
    :abc__xyz.field_title.should == 'xyz'
    
    'abc'.field_title.should == 'abc'
    'abc.def'.field_title.should == 'def'
  end
  
  specify "should return the field alias for aliased fields" do
    :xyz___x.field_title.should == 'x'
    :abc__xyz___y.field_title.should == 'y'
    
    'abc AS x'.field_title.should == 'x'
    'abc as y'.field_title.should == 'y'
    'abc.def AS d'.field_title.should == 'd'
  end
end

context "Symbol" do
  specify "should support MIN for specifying min function" do
    :abc__def.MIN.should == 'min(abc.def)'
  end

  specify "should support MAX for specifying max function" do
    :abc__def.MAX.should == 'max(abc.def)'
  end

  specify "should support SUM for specifying sum function" do
    :abc__def.SUM.should == 'sum(abc.def)'
  end

  specify "should support AVG for specifying avg function" do
    :abc__def.AVG.should == 'avg(abc.def)'
  end
  
  specify "should support COUNT for specifying count function" do
    :abc__def.COUNT.should == 'count(abc.def)'
  end
  
  specify "should support any other function using upper case letters" do
    :abc__def.DADA.should == 'dada(abc.def)'
  end
  
  specify "should support upper case outer functions" do
    :COUNT['1'].should == 'COUNT(1)'
  end
  
  specify "should inhibit string literalization" do
    db = Sequel::Database.new
    ds = db[:t]
    ds.select(:COUNT['1']).sql.should == "SELECT COUNT(1) FROM t"
  end
end