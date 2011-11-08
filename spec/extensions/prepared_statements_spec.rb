require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "prepared_statements plugin" do
  before do
    @db = Sequel.mock(:fetch=>{:id=>1, :name=>'foo', :i=>2}, :autoid=>proc{|sql| 1}, :numrows=>1, :servers=>{:read_only=>{}})
    @c = Class.new(Sequel::Model(@db[:people]))
    @c.columns :id, :name, :i
    @c.plugin :prepared_statements
    @p = @c.load(:id=>1, :name=>'foo', :i=>2)
    @ds = @c.dataset
    @db.sqls
  end

  specify "should correctly lookup by primary key" do
    @c[1].should == @p
    @db.sqls.should == ["SELECT * FROM people WHERE (id = 1) LIMIT 1 -- read_only"]
  end 

  specify "should correctly delete instance" do
    @p.destroy.should == @p
    @db.sqls.should == ["DELETE FROM people WHERE (id = 1)"]
  end

  specify "should correctly update instance" do
    @p.update(:name=>'bar').should == @c.load(:id=>1, :name=>'bar', :i => 2)
    @db.sqls.should == ["UPDATE people SET name = 'bar' WHERE (id = 1)"]
  end

  specify "should correctly create instance" do
    @c.create(:name=>'foo').should == @c.load(:id=>1, :name=>'foo', :i => 2)
    @db.sqls.should == ["INSERT INTO people (name) VALUES ('foo')", "SELECT * FROM people WHERE (id = 1) LIMIT 1"]
  end

  specify "should correctly create instance if dataset supports insert_select" do
    def @ds.supports_insert_select?
      true
    end
    def @ds.insert_select(h)
      {:id=>1, :name=>'foo', :i => 2}
    end
    def @ds.insert_sql(*)
      "#{super}#{' RETURNING *' if opts.has_key?(:returning)}"
    end
    @c.create(:name=>'foo').should == @c.load(:id=>1, :name=>'foo', :i => 2)
    @db.sqls.should == ["INSERT INTO people (name) VALUES ('foo') RETURNING *"]
  end

  specify "should work correctly when subclassing" do
    c = Class.new(@c)
    c[1].should == c.load(:id=>1, :name=>'foo', :i=>2)
    @db.sqls.should == ["SELECT * FROM people WHERE (id = 1) LIMIT 1 -- read_only"]
  end 
end
