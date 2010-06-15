require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')

context "String#to_time" do
  specify "should convert the string into a Time object" do
    "2007-07-11".to_time.should == Time.parse("2007-07-11")
    "06:30".to_time.should == Time.parse("06:30")
  end
  
  specify "should raise InvalidValue for an invalid time" do
    proc {'0000-00-00'.to_time}.should raise_error(Sequel::InvalidValue)
  end
end

context "String#to_date" do
  after do
    Sequel.convert_two_digit_years = true
  end

  specify "should convert the string into a Date object" do
    "2007-07-11".to_date.should == Date.parse("2007-07-11")
  end
  
  specify "should convert 2 digit years by default" do
    "July 11, 07".to_date.should == Date.parse("2007-07-11")
  end

  specify "should not convert 2 digit years if set not to" do
    Sequel.convert_two_digit_years = false
    "July 11, 07".to_date.should == Date.parse("0007-07-11")
  end

  specify "should raise InvalidValue for an invalid date" do
    proc {'0000-00-00'.to_date}.should raise_error(Sequel::InvalidValue)
  end
end

context "String#to_datetime" do
  after do
    Sequel.convert_two_digit_years = true
  end

  specify "should convert the string into a DateTime object" do
    "2007-07-11 10:11:12a".to_datetime.should == DateTime.parse("2007-07-11 10:11:12a")
  end
  
  specify "should convert 2 digit years by default" do
    "July 11, 07 10:11:12a".to_datetime.should == DateTime.parse("2007-07-11 10:11:12a")
  end

  specify "should not convert 2 digit years if set not to" do
    Sequel.convert_two_digit_years = false
    "July 11, 07 10:11:12a".to_datetime.should == DateTime.parse("0007-07-11 10:11:12a")
  end

  specify "should raise InvalidValue for an invalid date" do
    proc {'0000-00-00'.to_datetime}.should raise_error(Sequel::InvalidValue)
  end
end

context "String#to_sequel_time" do
  after do
    Sequel.datetime_class = Time
    Sequel.convert_two_digit_years = true
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
  
  specify "should convert 2 digit years by default if using DateTime class" do
    Sequel.datetime_class = DateTime
    "July 11, 07 10:11:12a".to_sequel_time.should == DateTime.parse("2007-07-11 10:11:12a")
  end

  specify "should not convert 2 digit years if set not to when using DateTime class" do
    Sequel.datetime_class = DateTime
    Sequel.convert_two_digit_years = false
    "July 11, 07 10:11:12a".to_sequel_time.should == DateTime.parse("0007-07-11 10:11:12a")
  end

  specify "should raise InvalidValue for an invalid time" do
    proc {'0000-00-00'.to_sequel_time}.should raise_error(Sequel::InvalidValue)
    Sequel.datetime_class = DateTime
    proc {'0000-00-00'.to_sequel_time}.should raise_error(Sequel::InvalidValue)
  end
end
