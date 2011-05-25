require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "prepared_statements plugin" do
  before do
    @c = Class.new(Sequel::Model(:people))
    @c.columns :id, :name, :i
    @ds = ds = @c.dataset
    def ds.fetch_rows(sql)
      db << {:server=>@opts[:server] || :read_only}.merge(opts)[:server]
      super{|h|}
      yield(:id=>1, :name=>'foo', :i=>2)
    end
    def ds.update(*)
      db << default_server_opts(opts)[:server]
      super
    end
    def ds.insert(*)
      db << default_server_opts(opts)[:server]
      super
      1
    end
    def ds.delete(*)
      db << default_server_opts(opts)[:server]
      super
    end
    @c.plugin :prepared_statements
    @p = @c.load(:id=>1, :name=>'foo', :i=>2)
    @c.db.execute 'foo'
    @sqls = @c.db.sqls
    @sqls.clear
  end

  specify "should correctly lookup by primary key" do
    @c[1].should == @p
    @sqls.should == [:read_only, "SELECT * FROM people WHERE (id = 1) LIMIT 1"]
  end 

  specify "should correctly lookup by primary key from dataset" do
    @c.dataset.filter(:name=>'foo')[1].should == @p
    @sqls.should == [:read_only, "SELECT * FROM people WHERE ((name = 'foo') AND (id = 1)) LIMIT 1"]
  end

  specify "should correctly delete instance" do
    @p.destroy.should == @p
    @sqls.should == [:default, "DELETE FROM people WHERE (id = 1)"]
  end

  specify "should correctly update instance" do
    @p.update(:name=>'bar').should == @c.load(:id=>1, :name=>'bar', :i => 2)
    @sqls.should == [:default, "UPDATE people SET name = 'bar' WHERE (id = 1)"]
  end

  specify "should correctly create instance" do
    @c.create(:name=>'foo').should == @c.load(:id=>1, :name=>'foo', :i => 2)
    @sqls.should == [:default, "INSERT INTO people (name) VALUES ('foo')", :default, "SELECT * FROM people WHERE (id = 1) LIMIT 1"]
  end

  specify "should correctly create instance if dataset supports insert_select" do
    def @ds.supports_insert_select?
      true
    end
    def @ds.insert_select(h)
      return {:id=>1, :name=>'foo', :i => 2}
    end
    def @ds.insert_sql(*)
      "#{super}#{' RETURNING *' if opts.has_key?(:returning)}"
    end
    @c.create(:name=>'foo').should == @c.load(:id=>1, :name=>'foo', :i => 2)
    @sqls.should == [:default, "INSERT INTO people (name) VALUES ('foo') RETURNING *"]
  end

  specify "should work correctly when subclassing" do
    c = Class.new(@c)
    c[1].should == c.load(:id=>1, :name=>'foo', :i=>2)
    @sqls.should == [:read_only, "SELECT * FROM people WHERE (id = 1) LIMIT 1"]
  end 
end
