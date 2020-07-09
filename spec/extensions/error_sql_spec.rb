require_relative "spec_helper"

describe "error_sql extension" do
  before do
    @db = Sequel.mock(:fetch=>proc{|sql| @db.synchronize{|c| @db.log_connection_yield(sql, c){raise StandardError}}}).extension(:error_sql)
  end

  it "should have Sequel::DatabaseError#sql give the SQL causing the error" do
    @db["SELECT"].all rescue (e = $!)
    e.sql.must_equal "SELECT"
  end

  it "should include connection information in SQL information if logging connection info" do
    @db.log_connection_info = true
    @db["SELECT"].all rescue (e = $!)
    e.sql.must_match(/\A\(conn: -?\d+\) SELECT\z/)
  end

  it "should include arguments in SQL information if given" do
    @db["SELECT"].with_fetch(proc{|sql| @db.synchronize{|c| @db.log_connection_yield(sql, c, [1, 2]){raise StandardError}}}).all rescue (e = $!)
    e.sql.must_equal "SELECT; [1, 2]"
  end

  it "should have Sequel::DatabaseError#sql give the SQL causing the error when using a logger" do
    l = Object.new
    def l.method_missing(*) end
    @db.loggers = [l]
    @db["SELECT"].all rescue (e = $!)
    e.sql.must_equal "SELECT"
  end

  it "should have Sequel::DatabaseError#sql be nil if there is no wrapped exception" do
    @db["SELECT"].with_fetch(proc{|sql| @db.log_connection_yield(sql, nil){raise Sequel::DatabaseError}}).all rescue (e = $!)
    e.sql.must_be_nil
  end
end
