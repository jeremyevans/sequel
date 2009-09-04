require File.join(File.dirname(__FILE__), 'spec_helper.rb')

describe Sequel::Database do
  specify "should provide disconnect functionality" do
    INTEGRATION_DB.test_connection
    INTEGRATION_DB.pool.size.should == 1
    INTEGRATION_DB.disconnect
    INTEGRATION_DB.pool.size.should == 0
  end

  specify "should raise Sequel::DatabaseError on invalid SQL" do
    proc{INTEGRATION_DB << "SELECT"}.should raise_error(Sequel::DatabaseError)
  end

  specify "should store underlying wrapped exception in Sequel::DatabaseError" do
    begin
      INTEGRATION_DB << "SELECT"
    rescue Sequel::DatabaseError=>e
      e.wrapped_exception.should be_a_kind_of(Exception)
    end
  end

  specify "should not have the connection pool swallow non-StandardError based exceptions" do
    proc{INTEGRATION_DB.pool.hold{raise Interrupt, "test"}}.should raise_error(Interrupt)
  end
end
