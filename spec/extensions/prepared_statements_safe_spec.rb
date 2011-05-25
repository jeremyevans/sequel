require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "prepared_statements_safe plugin" do
  before do
    @c = Class.new(Sequel::Model(:people))
    @c.columns :id, :name, :i
    @ds = ds = @c.dataset
    @c.instance_variable_set(:@db_schema, {:i=>{}, :name=>{}, :id=>{:primary_key=>true}})
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
    @c.plugin :prepared_statements_safe
    @p = @c.load(:id=>1, :name=>'foo', :i=>2)
    @c.db.execute 'foo'
    @sqls = @c.db.sqls
    @sqls.clear
  end

  specify "should load the prepared_statements plugin" do
    @c.plugins.should include(Sequel::Plugins::PreparedStatements)
  end

  specify "should set default values correctly" do
    @c.prepared_statements_column_defaults.should == {:name=>nil, :i=>nil}
    @c.instance_variable_set(:@db_schema, {:i=>{:default=>'f(x)'}, :name=>{:ruby_default=>'foo'}, :id=>{:primary_key=>true}})
    Class.new(@c).prepared_statements_column_defaults.should == {:name=>'foo'}
  end

  specify "should set default values when creating" do
    @c.create
    @sqls[1].should =~ /INSERT INTO people \((i|name), (i|name)\) VALUES \(NULL, NULL\)/
    @sqls.clear
    @c.create(:name=>'foo')
    @sqls[1].should =~ /INSERT INTO people \((i|name), (i|name)\) VALUES \((NULL|'foo'), (NULL|'foo')\)/
    @sqls.clear
    @c.create(:name=>'foo', :i=>2)
    @sqls[1].should =~ /INSERT INTO people \((i|name), (i|name)\) VALUES \((2|'foo'), (2|'foo')\)/
  end 

  specify "should use database default values" do
    @c.instance_variable_set(:@db_schema, {:i=>{:ruby_default=>2}, :name=>{:ruby_default=>'foo'}, :id=>{:primary_key=>true}})
    c = Class.new(@c)
    c.create
    @sqls[1].should =~ /INSERT INTO people \((i|name), (i|name)\) VALUES \((2|'foo'), (2|'foo')\)/
  end

  specify "should not set defaults for unparseable dataset default values" do
    @c.instance_variable_set(:@db_schema, {:i=>{:default=>'f(x)'}, :name=>{:ruby_default=>'foo'}, :id=>{:primary_key=>true}})
    c = Class.new(@c)
    c.create
    @sqls[1].should == "INSERT INTO people (name) VALUES ('foo')"
  end

  specify "should save all fields when updating" do
    @p.update(:i=>3)
    @sqls[1].should =~ /UPDATE people SET (name = 'foo'|i = 3), (name = 'foo'|i = 3) WHERE \(id = 1\)/
  end
end
