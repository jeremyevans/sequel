require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

if RUBY_VERSION >= '1.9.0'
describe "Sequel::Dataset::RoundTimestamps" do
  before do
    @dataset = Sequel.mock.dataset.extension(:round_timestamps)
  end

  specify "should round times properly for databases supporting microsecond precision" do
    @dataset.literal(Sequel::SQLTime.create(1, 2, 3, 499999.5)).should == "'01:02:03.500000'"
    @dataset.literal(Time.local(2010, 1, 2, 3, 4, 5.4999995)).should == "'2010-01-02 03:04:05.500000'"
    @dataset.literal(DateTime.new(2010, 1, 2, 3, 4, Rational(54999995, 10000000))).should == "'2010-01-02 03:04:05.500000'"

    @dataset.literal(Sequel::SQLTime.create(1, 2, 3, 499999.4)).should == "'01:02:03.499999'"
    @dataset.literal(Time.local(2010, 1, 2, 3, 4, 5.4999994)).should == "'2010-01-02 03:04:05.499999'"
    @dataset.literal(DateTime.new(2010, 1, 2, 3, 4, Rational(54999994, 10000000))).should == "'2010-01-02 03:04:05.499999'"
  end
  
  specify "should round times properly for databases supporting millisecond precision" do
    def @dataset.timestamp_precision() 3 end
    @dataset.literal(Sequel::SQLTime.create(1, 2, 3, 499500)).should == "'01:02:03.500'"
    @dataset.literal(Time.local(2010, 1, 2, 3, 4, 5.4995)).should == "'2010-01-02 03:04:05.500'"
    @dataset.literal(DateTime.new(2010, 1, 2, 3, 4, Rational(54995, 10000))).should == "'2010-01-02 03:04:05.500'"

    @dataset.literal(Sequel::SQLTime.create(1, 2, 3, 499499)).should == "'01:02:03.499'"
    @dataset.literal(Time.local(2010, 1, 2, 3, 4, 5.4994)).should == "'2010-01-02 03:04:05.499'"
    @dataset.literal(DateTime.new(2010, 1, 2, 3, 4, Rational(54994, 10000))).should == "'2010-01-02 03:04:05.499'"
  end
  
  specify "should round times properly for databases supporting second precision" do
    def @dataset.supports_timestamp_usecs?() false end
    @dataset.literal(Sequel::SQLTime.create(1, 2, 3, 500000)).should == "'01:02:04'"
    @dataset.literal(Time.local(2010, 1, 2, 3, 4, 5.5)).should == "'2010-01-02 03:04:06'"
    @dataset.literal(DateTime.new(2010, 1, 2, 3, 4, Rational(55, 10))).should == "'2010-01-02 03:04:06'"

    @dataset.literal(Sequel::SQLTime.create(1, 2, 3, 499999)).should == "'01:02:03'"
    @dataset.literal(Time.local(2010, 1, 2, 3, 4, 5.4999999)).should == "'2010-01-02 03:04:05'"
    @dataset.literal(DateTime.new(2010, 1, 2, 3, 4, Rational(54999999, 10000000))).should == "'2010-01-02 03:04:05'"
  end
end
else
  skip_warn "round_timestamps extension: only works on ruby 1.9+"
end
