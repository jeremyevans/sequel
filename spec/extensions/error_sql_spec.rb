require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "error_sql extension" do
  before do
    @db = Sequel.mock(:fetch=>proc{|sql| @db.log_yield(sql){raise StandardError}}).extension(:error_sql)
  end

  it "should have Sequel::DatabaseError#sql give the SQL causing the error" do
    @db["SELECT"].all rescue (e = $!)
    e.sql.must_equal "SELECT"
  end

  it "should have Sequel::DatabaseError#sql give the SQL causing the error when using a logger" do
    l = Object.new
    def l.method_missing(*) end
    @db.loggers = [l]
    @db["SELECT"].all rescue (e = $!)
    e.sql.must_equal "SELECT"
  end
end
