require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "schema_caching extension" do
  before do
    @db = Sequel.mock.extension(:schema_caching)
    @schemas = {'"table"'=>[[:column, {:db_type=>"integer", :default=>"nextval('table_id_seq'::regclass)", :allow_null=>false, :primary_key=>true, :type=>:integer, :ruby_default=>nil}]]}
    @filename = "spec/files/test_schema_#$$.dump" 
    @db.instance_variable_set(:@schemas, @schemas)
  end
  after do
    File.delete(@filename) if File.exist?(@filename)
  end

  it "Database#dump_schema_cache should dump cached schema to the given file" do
    File.exist?(@filename).should be_false
    @db.dump_schema_cache(@filename)
    File.exist?(@filename).should be_true
    File.size(@filename).should > 0
  end

  it "Database#load_schema_cache should load cached schema from the given file dumped by #dump_schema_cache" do
    @db.dump_schema_cache(@filename)
    db = Sequel::Database.new.extension(:schema_caching)
    db.load_schema_cache(@filename)
    @db.instance_variable_get(:@schemas).should == @schemas
  end

  it "Database#dump_schema_cache? should dump cached schema to the given file unless the file exists" do
    File.open(@filename, 'wb'){|f|}
    File.size(@filename).should == 0
    @db.dump_schema_cache?(@filename)
    File.size(@filename).should == 0
  end

  it "Database#load_schema_cache? should load cached schema from the given file if it exists" do
    db = Sequel::Database.new.extension(:schema_caching)
    File.exist?(@filename).should be_false
    db.load_schema_cache?(@filename)
    db.instance_variable_get(:@schemas).should == {}
  end
end
