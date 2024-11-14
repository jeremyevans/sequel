require_relative "spec_helper"

describe "pg_schema_caching extension" do
  before do
    @db = Sequel.connect('mock://postgres').extension(:pg_schema_caching)
    @schemas = {
      '"table1"'=>[
        [:column1, {:oid=>11111, :db_type=>"custom_type", :default=>"nextval('table_id_seq'::regclass)", :allow_null=>false, :primary_key=>true, :type=>:integer, :ruby_default=>nil}],
        [:column2, {:oid=>1111, :db_type=>"integer", :default=>"nextval('table_id_seq'::regclass)", :allow_null=>false, :primary_key=>true, :type=>:integer, :ruby_default=>nil}],
      ],
      '"table2"'=>[
        [:column3, {:oid=>1111, :db_type=>"integer", :default=>"nextval('table_id_seq'::regclass)", :allow_null=>false, :primary_key=>true, :type=>:integer, :ruby_default=>nil}],
      ],
      '"table3"'=>[
        [:column4, {:oid=>11112, :db_type=>"custom_type2", :default=>"nextval('table_id_seq'::regclass)", :allow_null=>false, :primary_key=>true, :type=>:integer, :ruby_default=>nil}],
        [:column5, {:oid=>1111, :db_type=>"integer", :default=>"nextval('table_id_seq'::regclass)", :allow_null=>false, :primary_key=>true, :type=>:integer, :ruby_default=>nil}],
      ]
    }
    @filename = "spec/files/test_schema_#$$.dump" 
    @db.instance_variable_set(:@schemas, @schemas)
  end
  after do
    File.delete(@filename) if File.exist?(@filename)
  end

  it "Database#dump_schema_cache should dump cached schema to the given file without custom oids" do
    File.exist?(@filename).must_equal false
    @db.dump_schema_cache(@filename)
    File.exist?(@filename).must_equal true
    cache = Marshal.load(File.binread(@filename))
    cache['"table1"'][0][1][:oid].must_equal :custom
    cache['"table1"'][1][1][:oid].must_equal 1111
    cache['"table2"'][0][1][:oid].must_equal 1111
    cache['"table3"'][0][1][:oid].must_equal :custom
    cache['"table3"'][1][1][:oid].must_equal 1111
  end

  it "Database#load_schema_cache should load cached schema, using a single query for custom type oids" do
    @db.dump_schema_cache(@filename)
    @db.fetch = [{:typname=>"custom_type2", :oid=>22221}, {:typname=>"custom_type", :oid=>22222}]
    @db.load_schema_cache(@filename)
    @db.schema(:table1)[0][1][:oid].must_equal 22222
    @db.schema(:table1)[1][1][:oid].must_equal 1111
    @db.schema(:table2)[0][1][:oid].must_equal 1111
    @db.schema(:table3)[0][1][:oid].must_equal 22221
    @db.schema(:table3)[1][1][:oid].must_equal 1111
    @db.sqls.must_equal ["SELECT \"typname\", \"oid\" FROM \"pg_type\" WHERE (\"typname\" IN ('custom_type', 'custom_type2'))"]
  end

  it "Database#load_schema_cache should load cached schema without issuing a query if there are no custom type oids" do
    @schemas.delete('"table1"')
    @schemas.delete('"table3"')
    @db.dump_schema_cache(@filename)
    @db.load_schema_cache(@filename)
    @db.sqls.must_equal []
  end

  it "Database#load_schema_cache should warn if custom type oids present in cache are not found in the database, and remove schema entry from cache" do
    @db.dump_schema_cache(@filename)
    @db.fetch = [{:typname=>"custom_type2", :oid=>22221}]
    a = []
    @db.define_singleton_method(:warn){|*args| a.replace(args)}
    @db.load_schema_cache(@filename)
    a.must_equal ["Could not load OIDs for the following custom types: custom_type", {:uplevel=>3}]
    @db.instance_variable_get(:@schemas).keys.must_equal(%w'"table2" "table3"')
  end
end
