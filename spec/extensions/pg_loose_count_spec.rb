require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "pg_loose_count extension" do
  before do
    @db = Sequel.mock(:host=>'postgres', :fetch=>{:v=>1}).extension(:pg_loose_count)
  end

  specify "should add loose_count method getting fast count for entire table using table statistics" do
    @db.loose_count(:a).should == 1
    @db.sqls.should == ["SELECT CAST(reltuples AS integer) AS v FROM pg_class WHERE (oid = CAST(CAST('a' AS regclass) AS oid)) LIMIT 1"]
  end

  specify "should support schema qualified tables" do
    @db.loose_count(:a__b).should == 1
    @db.sqls.should == ["SELECT CAST(reltuples AS integer) AS v FROM pg_class WHERE (oid = CAST(CAST('a.b' AS regclass) AS oid)) LIMIT 1"]
  end
end
