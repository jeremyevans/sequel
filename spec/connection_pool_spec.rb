require File.join(File.dirname(__FILE__), '../lib/sequel')

context "ConnectionPool#hold" do
  setup do
    @pool = Sequel::ConnectionPool.new {Array.new}
  end
  
  specify "should catch exceptions and reraise them as SequelConnectionError" do
    proc {@pool.hold {|c| c.basdfadfaf}}.should_raise SequelConnectionError
  end
  
  specify "should provide the original exception wrapped in a SequelConnectionError" do
    begin
      @pool.hold {raise "mau"}
    rescue => e
      e.should_be_a_kind_of SequelConnectionError
      e.original_error.should_be_a_kind_of RuntimeError
      e.original_error.message.should == 'mau'
      e.message.should == 'RuntimeError: mau'
    end
  end
  
  specify "should handle Exception errors (normally not caught be rescue)" do
    begin
      @pool.hold {raise Exception}
    rescue => e
      e.should_be_a_kind_of SequelConnectionError
      e.original_error.should_be_a_kind_of Exception
    end
  end
end