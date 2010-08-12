require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

begin
  require 'tzinfo'
rescue LoadError => e
  skip_warn "named_timezones_spec: can't load tzinfo (#{e.class}: #{e})"
else
Sequel.extension :named_timezones
Sequel.datetime_class = Time

describe "Sequel named_timezones extension" do
  before do
    @tz_in = TZInfo::Timezone.get('America/Los_Angeles')
    @tz_out = TZInfo::Timezone.get('America/New_York')
    @db = MockDatabase.new
    @dt = DateTime.civil(2009,6,1,10,20,30,0)
    Sequel.application_timezone = 'America/Los_Angeles'
    Sequel.database_timezone = 'America/New_York'
    Sequel.datetime_class = DateTime
  end
  after do
    Sequel.default_timezone = nil
    Sequel.datetime_class = Time
  end
  
  it "should convert string arguments to *_timezone= to TZInfo::Timezone instances" do
    Sequel.application_timezone.should == @tz_in
    Sequel.database_timezone.should == @tz_out
  end
    
  it "should accept TZInfo::Timezone instances in *_timezone=" do
    Sequel.application_timezone = @tz_in
    Sequel.database_timezone = @tz_out
    Sequel.application_timezone.should == @tz_in
    Sequel.database_timezone.should == @tz_out
  end
    
  unless RUBY_VERSION == '1.8.6' && RUBY_PLATFORM =~ /win/
    it "should convert datetimes going into the database to named database_timezone" do
      ds = @db[:a]
      def ds.supports_timestamp_timezones?; true; end
      def ds.supports_timestamp_usecs?; false; end
      ds.insert([@dt, DateTime.civil(2009,6,1,3,20,30,-7/24.0), DateTime.civil(2009,6,1,6,20,30,-1/6.0)])
      @db.sqls.should == ["INSERT INTO a VALUES ('2009-06-01 06:20:30-0400', '2009-06-01 06:20:30-0400', '2009-06-01 06:20:30-0400')"]
    end
  end
  
  it "should convert datetimes coming out of the database from database_timezone to application_timezone" do
    dt = Sequel.database_to_application_timestamp('2009-06-01 06:20:30-0400')
    dt.should == @dt
    dt.offset.should == -7/24.0
    
    dt = Sequel.database_to_application_timestamp('2009-06-01 10:20:30+0000')
    dt.should == @dt
    dt.offset.should == -7/24.0
  end
    
  it "should assume datetimes coming out of the database that don't have an offset as coming from database_timezone" do
    dt = Sequel.database_to_application_timestamp('2009-06-01 06:20:30')
    dt.should == @dt
    dt.offset.should == -7/24.0
    
    dt = Sequel.database_to_application_timestamp('2009-06-01 10:20:30')
    dt.should == @dt + 1/6.0
    dt.offset.should == -7/24.0
  end
  
  it "should work with the thread_local_timezones extension" do
    [Thread.new{Sequel.thread_application_timezone = 'America/New_York'; sleep 0.03; Sequel.application_timezone.should == @tz_out},
     Thread.new{sleep 0.01; Sequel.thread_application_timezone = 'America/Los_Angeles'; sleep 0.01; Sequel.application_timezone.should == @tz_in}].each{|x| x.join}
  end
end
end
