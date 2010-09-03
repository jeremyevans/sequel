require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

describe "Sequel timezone support" do
  def test_timezone
    # Tests should cover both DST and non-DST times.
    [Time.now, Time.local(2010,1,1,12), Time.local(2010,6,1,12)].each do |t|
      @db[:t].insert(t)
      t2 = @db[:t].single_value
      t2 = Sequel.database_to_application_timestamp(t2.to_s) unless t2.is_a?(Time)
      (t2 - t).should be_close(0, 2)
      t2.utc_offset.should == 0 if Sequel.application_timezone == :utc
      t2.utc_offset.should == t.getlocal.utc_offset if Sequel.application_timezone == :local
      @db[:t].delete
    end

    Sequel.datetime_class = DateTime
    local_dst_offset = Time.local(2010, 6).utc_offset/86400.0
    local_std_offset = Time.local(2010, 1).utc_offset/86400.0
    [DateTime.now, DateTime.civil(2010,1,1,12,0,0,local_std_offset), DateTime.civil(2010,6,1,12,0,0,local_dst_offset)].each do |dt|
      @db[:t].insert(dt)
      dt2 = @db[:t].single_value
      dt2 = Sequel.database_to_application_timestamp(dt2.to_s) unless dt2.is_a?(DateTime)
      (dt2 - dt).should be_close(0, 0.00002)
      dt2.offset.should == 0 if Sequel.application_timezone == :utc
      dt2.offset.should == dt.offset if Sequel.application_timezone == :local
      @db[:t].delete
    end
  end

  before do
    @db = INTEGRATION_DB
    @db.create_table!(:t){DateTime :t}
  end
  after do
    @db.drop_table(:t)
    Sequel.default_timezone = nil
    Sequel.datetime_class = Time
  end

  cspecify "should support using UTC for database storage and local time for the application", [:swift], [:do, proc{|db| db.database_type != :sqlite}] do
    Sequel.database_timezone = :utc
    Sequel.application_timezone = :local
    test_timezone
  end

  cspecify "should support using local time for database storage and UTC for the application", [:swift], [:do, proc{|db| db.database_type != :sqlite}] do
    Sequel.database_timezone = :local
    Sequel.application_timezone = :utc
    test_timezone
  end

  cspecify "should support using UTC for both database storage and for application", [:swift], [:do, proc{|db| db.database_type != :sqlite}] do
    Sequel.default_timezone = :utc
    test_timezone
  end

  cspecify "should support using local time for both database storage and for application", [:do, proc{|db| db.database_type != :sqlite}] do
    Sequel.default_timezone = :local
    test_timezone
  end
end
