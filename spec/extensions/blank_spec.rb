require File.join(File.dirname(__FILE__), 'spec_helper')

context "Object#blank?" do
  specify "it should be true if the object responds true to empty?" do
    [].blank?.should == true
    {}.blank?.should == true
    o = Object.new
    def o.empty?; true; end
    o.blank?.should == true
  end

  specify "it should be false if the object doesn't respond true to empty?" do
    [2].blank?.should == false
    {1=>2}.blank?.should == false
    Object.new.blank?.should == false
  end
end

context "Numeric#blank?" do
  specify "it should always be false" do
    1.blank?.should == false
    0.blank?.should == false
    -1.blank?.should == false
    1.0.blank?.should == false
    0.0.blank?.should == false
    -1.0.blank?.should == false
    10000000000000000.blank?.should == false
    -10000000000000000.blank?.should == false
    10000000000000000.0.blank?.should == false
    -10000000000000000.0.blank?.should == false
  end
end

context "NilClass#blank?" do
  specify "it should always be true" do
    nil.blank?.should == true
  end
end

context "TrueClass#blank?" do
  specify "it should always be false" do
    true.blank?.should == false
  end
end

context "FalseClass#blank?" do
  specify "it should always be true" do
    false.blank?.should == true
  end
end

context "String#blank?" do
  specify "it should be true if the string is empty" do
    ''.blank?.should == true
  end
  specify "it should be true if the string is composed of just whitespace" do
    ' '.blank?.should == true
    "\r\n\t".blank?.should == true
    (' '*4000).blank?.should == true
    ("\r\n\t"*4000).blank?.should == true
  end
  specify "it should be false if the string has any non whitespace characters" do
    '1'.blank?.should == false
    ("\r\n\t"*4000 + 'a').blank?.should == false
    ("\r\na\t"*4000).blank?.should == false
  end
end
