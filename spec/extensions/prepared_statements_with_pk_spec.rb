require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "prepared_statements_with_pk plugin" do
  before do
    @db = Sequel.mock(:fetch=>{:id=>1, :name=>'foo', :i=>2}, :autoid=>proc{|sql| 1}, :numrows=>1, :servers=>{:read_only=>{}})
    @c = Class.new(Sequel::Model(@db[:people]))
    @c.columns :id, :name, :i
    @c.plugin :prepared_statements_with_pk
    @p = @c.load(:id=>1, :name=>'foo', :i=>2)
    @db.sqls
  end

  it "should load the prepared_statements plugin" do
    @c.plugins.must_include(Sequel::Plugins::PreparedStatements)
  end

  it "should correctly lookup by primary key from dataset" do
    @c.dataset.filter(:name=>'foo')[1].must_equal @p
    @c.db.sqls.must_equal ["SELECT * FROM people WHERE ((name = 'foo') AND (people.id = 1)) LIMIT 1 -- read_only"]
  end

  it "should still work correctly if there are multiple conflicting variables" do
    @c.dataset.filter(:name=>'foo').or(:name=>'bar')[1].must_equal @p
    @c.db.sqls.must_equal ["SELECT * FROM people WHERE (((name = 'foo') OR (name = 'bar')) AND (people.id = 1)) LIMIT 1 -- read_only"]
  end

  it "should still work correctly if the primary key is used elsewhere in the query" do
    @c.dataset.filter{id > 2}[1].must_equal @p
    @c.db.sqls.must_equal ["SELECT * FROM people WHERE ((id > 2) AND (people.id = 1)) LIMIT 1 -- read_only"]
  end
end
