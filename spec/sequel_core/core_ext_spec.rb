require File.join(File.dirname(__FILE__), 'spec_helper')

context "Array#extract_options!" do
  specify "should pop the last item if it is a hash" do
    a = [1,2,{1=>2}]
    a.extract_options!.should == {1=>2}
    a.should == [1,2]
  end

  specify "should return an empty hash if the last item is not a hash" do
    a = [1,2]
    a.extract_options!.should == {}
    a.should == [1,2]
  end
end

context "Enumerable#send_each" do
  specify "should send the supplied method to each item" do
    a = ['abbc', 'bbccdd', 'hebtre']
    a.send_each(:gsub!, 'b', '_')
    a.should == ['a__c', '__ccdd', 'he_tre']
  end
end

context "Range#interval" do
  specify "should return the interval between the beginning and end for an inclusive range" do
    (1..10).interval.should == 9
    
    r = rand(100000) + 10
    t1 = Time.now.to_i; t2 = t1 + r
    (t1..t2).interval.should == r
  end

  specify "should return the interval between the beginning and end for an exclusive range" do
    (1...10).interval.should == 8
    
    r = rand(100000) + 10
    t1 = Time.now.to_i; t2 = t1 + r
    (t1...t2).interval.should == r - 1
  end
end

context "Module#class_attr_reader" do
  specify "it should create instance methods that call class methods of the same name" do
    @c = Class.new do
      def self.x; 1; end
      class_attr_reader :x
    end
    @c.new.x.should == 1
    def @c.x; 2; end
    @c.new.x.should == 2
  end
end

context "Module#metaalias" do
  specify "it should create aliases of singleton/class methods" do
    @c = Class.new do
      def self.x; 1; end
      metaalias :y, :x
    end
    @c.y.should == 1
    def @c.x; 2; end
    @c.y.should == 1
  end
end

context "Module#metaattr_reader" do
  specify "it should create attr_readers of singleton/class methods" do
    @c = Class.new do
      @y = 1
      @x = 2
      metaattr_reader :y, :x
    end
    @c.y.should == 1
    @c.x.should == 2
  end
end

context "Object#is_one_of?" do
  specify "it should be true if the object is one of the classes" do
    1.is_one_of?(Numeric, Array).should == true
    [].is_one_of?(Numeric, Array).should == true
    {}.is_one_of?(Numeric, Enumerable).should == true
  end

  specify "it should be false if the object is not one of the classes" do
    'a'.is_one_of?(Numeric, Array).should == false
    Object.new.is_one_of?(Numeric, Array).should == false
  end
end

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

context "FalseClass#blank?" do
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
