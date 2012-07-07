require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel thread_local_timezones extension" do
  after do
    Sequel.default_timezone = nil
    Sequel.thread_application_timezone = nil
    Sequel.thread_database_timezone = nil
    Sequel.thread_typecast_timezone = nil
  end

  it "should allow specifying thread local timezones via thread_*_timezone=" do
    proc{Sequel.thread_application_timezone = :local}.should_not raise_error
    proc{Sequel.thread_database_timezone = :utc}.should_not raise_error
    proc{Sequel.thread_typecast_timezone = nil}.should_not raise_error
  end

  it "should use thread local timezone if available" do
    Sequel.thread_application_timezone = :local
    Sequel.application_timezone.should == :local
    Sequel.thread_database_timezone = :utc
    Sequel.database_timezone.should == :utc
    Sequel.thread_typecast_timezone = nil
    Sequel.typecast_timezone.should == nil
  end

  it "should fallback to default timezone if no thread_local timezone" do
    Sequel.default_timezone = :utc
    Sequel.application_timezone.should == :utc
    Sequel.database_timezone.should == :utc
    Sequel.typecast_timezone.should == :utc
  end

  it "should use a nil thread_local_timezone if set instead of falling back to the default timezone if thread_local_timezone is set to :nil" do
    Sequel.typecast_timezone = :utc
    Sequel.thread_typecast_timezone = nil
    Sequel.typecast_timezone.should == :utc
    Sequel.thread_typecast_timezone = :nil
    Sequel.typecast_timezone.should == nil
  end

  it "should be thread safe" do
    q, q1, q2 = Queue.new, Queue.new, Queue.new
    tz1, tz2 = nil, nil
    t1 = Thread.new do
      Sequel.thread_application_timezone = :utc
      q2.push nil
      q.pop
      tz1 = Sequel.application_timezone
    end
    t2 = Thread.new do
      Sequel.thread_application_timezone = :local
      q2.push nil
      q1.pop
      tz2 = Sequel.application_timezone
    end
    q2.pop
    q2.pop
    q.push nil
    q1.push nil
    t1.join
    t2.join
    tz1.should == :utc
    tz2.should == :local
  end
end
