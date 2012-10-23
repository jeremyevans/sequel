require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

describe Sequel::Database do
  specify "should provide disconnect functionality" do
    INTEGRATION_DB.disconnect
    INTEGRATION_DB.pool.size.should == 0
    INTEGRATION_DB.test_connection
    INTEGRATION_DB.pool.size.should == 1
  end

  specify "should provide disconnect functionality after preparing a statement" do
    INTEGRATION_DB.create_table!(:items){Integer :i}
    INTEGRATION_DB[:items].prepare(:first, :a).call
    INTEGRATION_DB.disconnect
    INTEGRATION_DB.pool.size.should == 0
    INTEGRATION_DB.drop_table?(:items)
  end

  specify "should raise Sequel::DatabaseError on invalid SQL" do
    proc{INTEGRATION_DB << "SELECT"}.should raise_error(Sequel::DatabaseError)
  end

  specify "should store underlying wrapped exception in Sequel::DatabaseError" do
    begin
      INTEGRATION_DB << "SELECT"
    rescue Sequel::DatabaseError=>e
      if defined?(Java::JavaLang::Exception)
        (e.wrapped_exception.is_a?(Exception) || e.wrapped_exception.is_a?(Java::JavaLang::Exception)).should be_true
      else
        e.wrapped_exception.should be_a_kind_of(Exception)
      end
    end
  end

  specify "should not have the connection pool swallow non-StandardError based exceptions" do
    proc{INTEGRATION_DB.pool.hold{raise Interrupt, "test"}}.should raise_error(Interrupt)
  end

  specify "should provide ability to check connections for validity" do
    conn = INTEGRATION_DB.synchronize{|c| c}
    INTEGRATION_DB.valid_connection?(conn).should be_true
    INTEGRATION_DB.disconnect
    INTEGRATION_DB.valid_connection?(conn).should be_false
  end
end
