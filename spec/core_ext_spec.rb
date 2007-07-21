require File.join(File.dirname(__FILE__), '../lib/sequel')

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

context "String#expr" do
  specify "should return an ExpressionString object" do
    'xyz'.expr.should be_a_kind_of(Sequel::ExpressionString)
    'xyz'.expr.to_s.should == 'xyz'
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
end