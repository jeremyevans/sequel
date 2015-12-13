require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "identifier_columns plugin" do
  before do
    @db = Sequel.mock(:numrows=>1, :fetch=>{:id=>1, :a__b=>2}, :autoid=>1)
    @c = Class.new(Sequel::Model(@db[:test]))
    @ds = @c.dataset
    @c.columns :id, :a__b
    @c.plugin :identifier_columns
    @db.sqls
  end

  it "should not use qualification when updating or inserting values" do
    @c.create(:a__b=>2).save
    @db.sqls.must_equal ["INSERT INTO test (a__b) VALUES (2)", "SELECT * FROM test WHERE (id = 1) LIMIT 1", "UPDATE test SET a__b = 2 WHERE (id = 1)"]
  end
end
