require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')

describe "current_datetime_timestamp extension" do
  before do
    @ds = Sequel.mock[:table].extension(:current_datetime_timestamp)
  end
  after do
    Sequel.datetime_class = Time
  end

  specify "should have current_timestamp respect Sequel.datetime_class" do
    t = Sequel::Dataset.new(nil).current_datetime 
    t.should be_a_kind_of(Time)
    (Time.now - t < 0.1).should == true

    Sequel.datetime_class = DateTime
    t = Sequel::Dataset.new(nil).current_datetime 
    t.should be_a_kind_of(DateTime)
    (DateTime.now - t < (0.1/86400)).should == true
  end

  specify "should have current_timestamp value be literalized as CURRENT_TIMESTAMP" do
    @ds.literal(@ds.current_datetime).should == 'CURRENT_TIMESTAMP'
    Sequel.datetime_class = DateTime
    @ds.literal(@ds.current_datetime).should == 'CURRENT_TIMESTAMP'
  end
end
