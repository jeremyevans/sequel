require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "prepared_statements_safe plugin" do
  before do
    @db = Sequel.mock(:fetch=>{:id=>1, :name=>'foo', :i=>2}, :autoid=>proc{|sql| 1}, :numrows=>1, :servers=>{:read_only=>{}})
    @c = Class.new(Sequel::Model(@db[:people]))
    @c.columns :id, :name, :i
    @c.instance_variable_set(:@db_schema, {:i=>{}, :name=>{}, :id=>{:primary_key=>true}})
    @c.plugin :prepared_statements_safe
    @p = @c.load(:id=>1, :name=>'foo', :i=>2)
    @db.sqls
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
    @db.sqls.first.should =~ /INSERT INTO people \((i|name), (i|name)\) VALUES \(NULL, NULL\)/
    @c.create(:name=>'foo')
    @db.sqls.first.should =~ /INSERT INTO people \((i|name), (i|name)\) VALUES \((NULL|'foo'), (NULL|'foo')\)/
    @c.create(:name=>'foo', :i=>2)
    @db.sqls.first.should =~ /INSERT INTO people \((i|name), (i|name)\) VALUES \((2|'foo'), (2|'foo')\)/
  end

  specify "should use database default values" do
    @c.instance_variable_set(:@db_schema, {:i=>{:ruby_default=>2}, :name=>{:ruby_default=>'foo'}, :id=>{:primary_key=>true}})
    c = Class.new(@c)
    c.create
    @db.sqls.first.should =~ /INSERT INTO people \((i|name), (i|name)\) VALUES \((2|'foo'), (2|'foo')\)/
  end

  specify "should not set defaults for unparseable dataset default values" do
    @c.instance_variable_set(:@db_schema, {:i=>{:default=>'f(x)'}, :name=>{:ruby_default=>'foo'}, :id=>{:primary_key=>true}})
    c = Class.new(@c)
    c.create
    @db.sqls.first.should == "INSERT INTO people (name) VALUES ('foo')"
  end

  specify "should save all fields when updating" do
    @p.update(:i=>3)
    @db.sqls.first.should =~ /UPDATE people SET (name = 'foo'|i = 3), (name = 'foo'|i = 3) WHERE \(id = 1\)/
  end

  specify "should work with abstract classes" do
    c = Class.new(Sequel::Model)
    c.plugin :prepared_statements_safe
    c1 = Class.new(c)
    c1.meta_def(:get_db_schema){@db_schema = {:i=>{:default=>'f(x)'}, :name=>{:ruby_default=>'foo'}, :id=>{:primary_key=>true}}}
    c1.set_dataset(:people)
    c1.prepared_statements_column_defaults.should == {:name=>'foo'}
    Class.new(c1).prepared_statements_column_defaults.should == {:name=>'foo'}
  end
end
