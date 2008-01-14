require File.join(File.dirname(__FILE__), 'spec_helper')

context "Enumerable#send_each" do
  specify "should send the supplied method to each item" do
    a = ['abbc', 'bbccdd', 'hebtre']
    a.send_each(:gsub!, 'b', '_')
    a.should == ['a__c', '__ccdd', 'he_tre']
  end
end

context "Range#interval" do
  specify "should return the interval between the beginning and end of the range" do
    (1..10).interval.should == 9
    
    r = rand(100000) + 10
    t1 = Time.now; t2 = t1 + r
    (t1..t2).interval.should == r
  end
end
