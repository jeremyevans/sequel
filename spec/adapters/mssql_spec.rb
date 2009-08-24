require File.join(File.dirname(__FILE__), 'spec_helper.rb')

unless defined?(MSSQL_DB)
  MSSQL_URL = 'jdbc:sqlserver://localhost;integratedSecurity=true;database=sandbox' unless defined? MSSQL_URL
  MSSQL_DB = Sequel.connect(ENV['SEQUEL_MSSQL_SPEC_DB']||MSSQL_URL)
end
INTEGRATION_DB = MSSQL_DB unless defined?(INTEGRATION_DB)

context "A MSSQL database" do
  before do
    @db = MSSQL_DB
  end

  specify "read milliseconds" do
    rs = @db["select getutcdate() as full_date, datepart(millisecond, getutcdate()) as milliseconds"].first
    rs[:milliseconds].should == rs[:full_date].usec/1000
  end

  specify "write milliseconds" do
    t = Time.utc(9999, 12, 31, 23, 59, 59, 997000)
    @db["select cast(datepart(millisecond, ?) as int) as milliseconds", t].get.should == t.usec/1000
  end
end
