require_relative "spec_helper"

describe "dataset_run extension" do
  it "#run should run the SQL on the database" do
    db = Sequel.mock
    db["SQL with ?", "placeholder"].extension(:dataset_run).run.must_be_nil
    db.sqls.must_equal ["SQL with 'placeholder'"]
  end

  it "#run should respect current server" do
    db = Sequel.mock(:servers=>{:a=>{}})
    db["SQL with ?", "placeholder"].extension(:dataset_run).server(:a).run.must_be_nil
    db.sqls.must_equal ["SQL with 'placeholder' -- a"]
  end
end
