require File.join(File.dirname(__FILE__), 'spec_helper')

context "Enumerable#send_each" do
  specify "should send the supplied method to each item" do
    a = ['abbc', 'bbccdd', 'hebtre']
    a.send_each(:gsub!, 'b', '_')
    a.should == ['a__c', '__ccdd', 'he_tre']
  end
end

context "String#to_time" do
  specify "should convert the string into a Time object" do
    "2007-07-11".to_time.should == Time.parse("2007-07-11")
    "06:30".to_time.should == Time.parse("06:30")
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

context "Numeric extensions" do
  setup do
    Sequel::NumericExtensions.enable
  end
  
  specify "should support conversion of minutes to seconds" do
    1.minute.should == 60
    3.minutes.should == 180
  end
  
  specify "should support conversion of hours to seconds" do
    1.hour.should == 3600
    3.hours.should == 3600 * 3
  end

  specify "should support conversion of days to seconds" do
    1.day.should == 86400
    3.days.should == 86400 * 3
  end

  specify "should support conversion of weeks to seconds" do
    1.week.should == 86400 * 7
    3.weeks.should == 86400 * 7 * 3
  end
  
  specify "should provide #ago functionality" do
    t1 = Time.now
    t2 = 1.day.ago
    t1.should > t2
    ((t1 - t2).to_i - 86400).abs.should < 2
    
    t1 = Time.now
    t2 = 1.day.before(t1)
    t2.should == t1 - 1.day
  end

  specify "should provide #from_now functionality" do
    t1 = Time.now
    t2 = 1.day.from_now
    t1.should < t2
    ((t2 - t1).to_i - 86400).abs.should < 2
    
    t1 = Time.now
    t2 = 1.day.since(t1)
    t2.should == t1 + 1.day
  end
end