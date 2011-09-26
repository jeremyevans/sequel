require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "prepared_statements_with_pk plugin" do
  before do
    @c = Class.new(Sequel::Model(:people))
    @c.columns :id, :name, :i
    @ds = ds = @c.dataset
    def ds.fetch_rows(sql)
      db << {:server=>@opts[:server] || :read_only}.merge(opts)[:server]
      super{|h|}
      yield(:id=>1, :name=>'foo', :i=>2)
    end
    @c.plugin :prepared_statements_with_pk
    @p = @c.load(:id=>1, :name=>'foo', :i=>2)
    @c.db.execute 'foo'
    @sqls = @c.db.sqls
    @sqls.clear
  end

  specify "should load the prepared_statements plugin" do
    @c.plugins.should include(Sequel::Plugins::PreparedStatements)
  end

  specify "should correctly lookup by primary key from dataset" do
    @c.dataset.filter(:name=>'foo')[1].should == @p
    @sqls.should == [:read_only, "SELECT * FROM people WHERE ((name = 'foo') AND (people.id = 1)) LIMIT 1"]
  end

  specify "should still work correctly if there are multiple conflicting variables" do
    @c.dataset.filter(:name=>'foo').or(:name=>'bar')[1].should == @p
    @sqls.should == [:read_only, "SELECT * FROM people WHERE (((name = 'foo') OR (name = 'bar')) AND (people.id = 1)) LIMIT 1"]
  end

  specify "should still work correctly if the primary key is used elsewhere in the query" do
    @c.dataset.filter{id > 2}[1].should == @p
    @sqls.should == [:read_only, "SELECT * FROM people WHERE ((id > 2) AND (people.id = 1)) LIMIT 1"]
  end
end
