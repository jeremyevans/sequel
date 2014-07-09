require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "pg_inet extension" do
  before do
    @db = Sequel.connect('mock://postgres', :quote_identifiers=>false)
    @db.extend(Module.new do
      def schema_parse_table(*)
        [[:a, {:oid=>1}]]
      end
    end)
    @db.send(:metadata_dataset)._fetch = [[{:v=>1, :enumlabel=>'a'}, {:v=>1, :enumlabel=>'b'}, {:v=>1, :enumlabel=>'c'}],
      [{:typname=>'enum1', :v=>212389}]]
    @db.extension(:pg_array, :pg_enum)
    @db.sqls
  end

  it "should include enum information in the schema entry" do
    @db.schema(:a).should == [[:a, {:oid=>1, :ruby_default=>nil, :type=>:enum, :enum_values=>%w'a b c'}]]
  end

  it "should typecast objects to string" do
    @db.typecast_value(:enum, :a).should == 'a'
  end

  it "should add array parsers for enum values" do
    @db.conversion_procs[212389].call('{a,b,c}').should == %w'a b c'
  end

  it "should support #create_enum method for adding a new enum" do
    @db.create_enum(:foo, [:a, :b, :c])
    @db.sqls.first.should == "CREATE TYPE foo AS ENUM ('a', 'b', 'c')"
    @db.create_enum(:sch__foo, %w'a b c')
    @db.sqls.first.should == "CREATE TYPE sch.foo AS ENUM ('a', 'b', 'c')"
  end

  it "should support #drop_enum method for dropping an enum" do
    @db.drop_enum(:foo)
    @db.sqls.first.should == "DROP TYPE foo"
    @db.drop_enum(:sch__foo, :if_exists=>true)
    @db.sqls.first.should == "DROP TYPE IF EXISTS sch.foo"
    @db.drop_enum('foo', :cascade=>true)
    @db.sqls.first.should == "DROP TYPE foo CASCADE"
  end

  it "should support #add_enum_value method for adding value to an existing enum" do
    @db.add_enum_value(:foo, :a)
    @db.sqls.first.should == "ALTER TYPE foo ADD VALUE 'a'"
  end

  it "should support :before option for #add_enum_value method for adding value before an existing enum value" do
    @db.add_enum_value('foo', :a, :before=>:b)
    @db.sqls.first.should == "ALTER TYPE foo ADD VALUE 'a' BEFORE 'b'"
  end

  it "should support :after option for #add_enum_value method for adding value after an existing enum value" do
    @db.add_enum_value(:sch__foo, :a, :after=>:b)
    @db.sqls.first.should == "ALTER TYPE sch.foo ADD VALUE 'a' AFTER 'b'"
  end

  it "should support :if_not_exists option for #add_enum_value method for not adding the value if it exists" do
    @db.add_enum_value(:foo, :a, :if_not_exists=>true)
    @db.sqls.first.should == "ALTER TYPE foo ADD VALUE IF NOT EXISTS 'a'"
  end
end
