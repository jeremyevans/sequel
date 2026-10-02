# frozen_string_literal: true
require_relative "spec_helper"

describe "pg_loose_count extension" do
  before do
    @db = Sequel.mock(:host=>'postgres', :fetch=>{:v=>1}).extension(:pg_loose_count)
    @db.extend_datasets{def quote_identifiers?; false end}
  end

  it "should add loose_count method getting fast count for entire table using table statistics" do
    @db.loose_count(:a).must_equal 1
    @db.sqls.must_equal ["SELECT CAST(reltuples AS integer) AS v FROM pg_class WHERE (oid = CAST(CAST('a' AS regclass) AS oid)) LIMIT 1"]
  end

  it "should support schema qualified tables" do
    @db.loose_count(Sequel[:a][:b]).must_equal 1
    @db.sqls.must_equal ["SELECT CAST(reltuples AS integer) AS v FROM pg_class WHERE (oid = CAST(CAST('a.b' AS regclass) AS oid)) LIMIT 1"]
  end

  with_symbol_splitting "should support schema qualified table symbols" do
    @db.loose_count(:a__b).must_equal 1
    @db.sqls.must_equal ["SELECT CAST(reltuples AS integer) AS v FROM pg_class WHERE (oid = CAST(CAST('a.b' AS regclass) AS oid)) LIMIT 1"]
  end

  it "should add loose_counts method getting fast counts for all tables in a schema" do
    ["schema", :schema, Sequel.identifier(:schema)].each do
      @db.fetch = {relname: "t", v: 2}
      @db.loose_counts(:schema).must_equal(t: 2)
      @db.sqls.must_equal ["SELECT relname, CAST(reltuples AS integer) AS v FROM pg_class WHERE ((relnamespace = CAST(to_regnamespace('schema') AS oid)) AND (relkind IN ('r', 'm', 'p'))) ORDER BY relname"]
    end
  end

  it "should have loose counts work on older postgres versions" do
    @db.server_version = 90400
    @db.fetch = {relname: "t", v: 2}
    @db.loose_counts(:schema).must_equal(t: 2)
    @db.sqls.must_equal ["SELECT relname, CAST(reltuples AS integer) AS v FROM pg_class WHERE ((relnamespace IN (SELECT oid FROM pg_namespace WHERE (nspname = 'schema'))) AND (relkind IN ('r', 'm', 'p'))) ORDER BY relname"]
  end

  it "should raise when giving unsupported argument type to loose_counts" do
    proc{@db.loose_counts(Object.new)}.must_raise Sequel::Error
  end
end
