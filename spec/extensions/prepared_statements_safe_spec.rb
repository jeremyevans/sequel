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

  it "should load the prepared_statements plugin" do
    @c.plugins.must_include(Sequel::Plugins::PreparedStatements)
  end

  it "should set default values correctly" do
    @c.prepared_statements_column_defaults.must_equal(:name=>nil, :i=>nil)
    @c.instance_variable_set(:@db_schema, {:i=>{:default=>'f(x)'}, :name=>{:ruby_default=>'foo'}, :id=>{:primary_key=>true}})
    Class.new(@c).prepared_statements_column_defaults.must_equal(:name=>'foo')
  end

  it "should set default values when creating" do
    @c.create
    @db.sqls.first.must_match(/INSERT INTO people \((i|name), (i|name)\) VALUES \(NULL, NULL\)/)
    @c.create(:name=>'foo')
    @db.sqls.first.must_match(/INSERT INTO people \((i|name), (i|name)\) VALUES \((NULL|'foo'), (NULL|'foo')\)/)
    @c.create(:name=>'foo', :i=>2)
    @db.sqls.first.must_match(/INSERT INTO people \((i|name), (i|name)\) VALUES \((2|'foo'), (2|'foo')\)/)
  end 

  it "should use database default values" do
    @c.instance_variable_set(:@db_schema, {:i=>{:ruby_default=>2}, :name=>{:ruby_default=>'foo'}, :id=>{:primary_key=>true}})
    c = Class.new(@c)
    c.create
    @db.sqls.first.must_match(/INSERT INTO people \((i|name), (i|name)\) VALUES \((2|'foo'), (2|'foo')\)/)
  end

  it "should not set defaults for unparseable dataset default values" do
    @c.instance_variable_set(:@db_schema, {:i=>{:default=>'f(x)'}, :name=>{:ruby_default=>'foo'}, :id=>{:primary_key=>true}})
    c = Class.new(@c)
    c.create
    @db.sqls.first.must_equal "INSERT INTO people (name) VALUES ('foo')"
  end

  it "should save all fields when updating" do
    @p.update(:i=>3)
    @db.sqls.first.must_match(/UPDATE people SET (name = 'foo'|i = 3), (name = 'foo'|i = 3) WHERE \(id = 1\)/)
  end

  it "should work with abstract classes" do
    c = Class.new(Sequel::Model)
    c.plugin :prepared_statements_safe
    c1 = Class.new(c)
    c1.meta_def(:get_db_schema){@db_schema = {:i=>{:default=>'f(x)'}, :name=>{:ruby_default=>'foo'}, :id=>{:primary_key=>true}}}
    c1.set_dataset(:people)
    c1.prepared_statements_column_defaults.must_equal(:name=>'foo')
    Class.new(c1).prepared_statements_column_defaults.must_equal(:name=>'foo')
  end
end
