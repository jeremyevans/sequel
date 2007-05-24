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
    "SELECT * 
     FROM items
     WHERE a = 1".split.to_sql.should == 'SELECT * FROM items WHERE a = 1'
  end
  
  specify "should remove superfluous white space and line breaks" do
    "\tSELECT * \r\n
     FROM items    ".split.to_sql.should == 'SELECT * FROM items'
  end
  
  specify "should remove ANSI SQL comments" do
    "SELECT *   --comment
     FROM items
     --comment".split.to_sql.should == 'SELECT * FROM items'
  end
  
  specify "should remove C-style comments" do
    "SELECT *
     /* comment comment
     comment
     FROM items */
     FROM items
     --comment".split.to_sql.should == 'SELECT * FROM items'
  end
end

context "String#to_sql" do
  specify "should concatenate multiple lines into a single string" do
    "SELECT * 
     FROM items
     WHERE a = 1".to_sql.should == 'SELECT * FROM items WHERE a = 1'
  end
  
  specify "should remove superfluous white space and line breaks" do
    "\tSELECT * \r\n
     FROM items    ".to_sql.should == 'SELECT * FROM items'
  end
  
  specify "should remove ANSI SQL comments" do
    "SELECT *   --comment
     FROM items
     --comment".to_sql.should == 'SELECT * FROM items'
  end
  
  specify "should remove C-style comments" do
    "SELECT *
     /* comment comment
     comment
     FROM items */
     FROM items
     --comment".to_sql.should == 'SELECT * FROM items'
  end
end

