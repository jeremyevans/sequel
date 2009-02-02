require File.join(File.dirname(__FILE__), 'spec_helper.rb')

if INTEGRATION_DB.respond_to?(:schema_parse_table, true)
describe "Database schema parser" do
  before do
    @iom = INTEGRATION_DB.identifier_output_method
    @iim = INTEGRATION_DB.identifier_input_method
    @defsch = INTEGRATION_DB.default_schema
    clear_sqls
  end
  after do
    INTEGRATION_DB.drop_table(:items) if INTEGRATION_DB.table_exists?(:items)
    INTEGRATION_DB.identifier_output_method = @iom
    INTEGRATION_DB.identifier_input_method = @iim
    INTEGRATION_DB.default_schema = @defsch
  end

  specify "should handle a database with a identifier_output_method" do
    INTEGRATION_DB.identifier_output_method = :reverse
    INTEGRATION_DB.identifier_input_method = :reverse
    INTEGRATION_DB.default_schema = nil if INTEGRATION_DB.default_schema
    INTEGRATION_DB.create_table!(:items){integer :number}
    INTEGRATION_DB.schema(nil, :reload=>true)[:items].should be_a_kind_of(Array)
    INTEGRATION_DB.schema(:items, :reload=>true).first.first.should == :number
  end

  specify "should be a hash with table_names as symbols" do
    INTEGRATION_DB.create_table!(:items){integer :number}
    schema = INTEGRATION_DB.schema(nil, :reload=>true)
    schema.should be_a_kind_of(Hash)
    schema[:items].should_not == nil
  end

  specify "should not issue an sql query if the schema has been loaded unless :reload is true" do
    INTEGRATION_DB.create_table!(:items){integer :number}
    INTEGRATION_DB.schema(nil, :reload=>true)
    clear_sqls
    INTEGRATION_DB.schema
    sqls_should_be
  end

  specify "should give the same result for a single table regardless of whether schema was called for a single table" do
    INTEGRATION_DB.create_table!(:items){integer :number}
    INTEGRATION_DB.schema(:items, :reload=>true).should == INTEGRATION_DB.schema(nil, :reload=>true)[:items]
  end

  specify "should raise an error when the table doesn't exist" do
    proc{INTEGRATION_DB.schema(:no_table)}.should raise_error(Sequel::Error)
  end

  specify "should return the schema correctly" do
    INTEGRATION_DB.create_table!(:items){integer :number}
    schema = INTEGRATION_DB.schema(:items, :reload=>true)
    schema.should be_a_kind_of(Array)
    schema.length.should == 1
    col = schema.first
    col.should be_a_kind_of(Array)
    col.length.should == 2
    col.first.should == :number
    col_info = col.last
    col_info.should be_a_kind_of(Hash)
    col_info[:type].should == :integer
    clear_sqls
    INTEGRATION_DB.schema(:items)
    sqls_should_be
  end

  specify "should parse primary keys from the schema properly" do
    INTEGRATION_DB.create_table!(:items){integer :number}
    INTEGRATION_DB.schema(:items).collect{|k,v| k if v[:primary_key]}.compact.should == []
    INTEGRATION_DB.create_table!(:items){primary_key :number}
    INTEGRATION_DB.schema(:items).collect{|k,v| k if v[:primary_key]}.compact.should == [:number]
    INTEGRATION_DB.create_table!(:items){integer :number1; integer :number2; primary_key [:number1, :number2]}
    INTEGRATION_DB.schema(:items).collect{|k,v| k if v[:primary_key]}.compact.should == [:number1, :number2]
  end

  specify "should parse NULL/NOT NULL from the schema properly" do
    INTEGRATION_DB.create_table!(:items){integer :number, :null=>true}
    INTEGRATION_DB.schema(:items).first.last[:allow_null].should == true
    INTEGRATION_DB.create_table!(:items){integer :number, :null=>false}
    INTEGRATION_DB.schema(:items).first.last[:allow_null].should == false
  end

  specify "should parse defaults from the schema properly" do
    INTEGRATION_DB.create_table!(:items){integer :number}
    INTEGRATION_DB.schema(:items).first.last[:default].should == nil
    INTEGRATION_DB.create_table!(:items){integer :number, :default=>0}
    INTEGRATION_DB.schema(:items).first.last[:default].to_s.should == "0"
    INTEGRATION_DB.create_table!(:items){varchar :a, :default=>"blah", :size=>4}
    INTEGRATION_DB.schema(:items).first.last[:default].gsub(/::character varying\z/, '').gsub("'", '').should == "blah"
  end
end
end

describe "Database schema modifiers" do
  before do
    INTEGRATION_DB.create_table!(:items){integer :number}
    @ds = INTEGRATION_DB[:items]
    @ds.insert([10])
    clear_sqls
  end
  after do
    INTEGRATION_DB.drop_table(:items) if INTEGRATION_DB.table_exists?(:items)
  end

  specify "should create tables correctly" do
    INTEGRATION_DB.table_exists?(:items).should == true
    INTEGRATION_DB.schema(:items, :reload=>true).map{|x| x.first}.should == [:number]
    @ds.columns!.should == [:number]
  end

  specify "should add columns to tables correctly" do
    INTEGRATION_DB.schema(:items, :reload=>true).map{|x| x.first}.should == [:number]
    @ds.columns!.should == [:number]
    INTEGRATION_DB.alter_table(:items){add_column :name, :text}
    INTEGRATION_DB.schema(:items, :reload=>true).map{|x| x.first}.should == [:number, :name]
    @ds.columns!.should == [:number, :name]
    unless INTEGRATION_DB.url =~ /sqlite/
      INTEGRATION_DB.alter_table(:items){add_primary_key :id}
      INTEGRATION_DB.schema(:items, :reload=>true).map{|x| x.first}.should == [:number, :name, :id]
      @ds.columns!.should == [:number, :name, :id]
      INTEGRATION_DB.alter_table(:items){add_foreign_key :item_id, :items}
      INTEGRATION_DB.schema(:items, :reload=>true).map{|x| x.first}.should == [:number, :name, :id, :item_id]
      @ds.columns!.should == [:number, :name, :id, :item_id]
    end
  end

  specify "should remove columns from tables correctly" do
    INTEGRATION_DB.create_table!(:items) do
      primary_key :id
      text :name
      integer :number
      foreign_key :item_id, :items
    end
    @ds.insert(:number=>10)
    INTEGRATION_DB.schema(:items, :reload=>true).map{|x| x.first}.should == [:id, :name, :number, :item_id]
    @ds.columns!.should == [:id, :name, :number, :item_id]
    INTEGRATION_DB.drop_column(:items, :number)
    INTEGRATION_DB.schema(:items, :reload=>true).map{|x| x.first}.should == [:id, :name, :item_id]
    @ds.columns!.should == [:id, :name, :item_id]
    INTEGRATION_DB.drop_column(:items, :name)
    INTEGRATION_DB.schema(:items, :reload=>true).map{|x| x.first}.should == [:id, :item_id]
    @ds.columns!.should == [:id, :item_id]
    INTEGRATION_DB.drop_column(:items, :item_id)
    INTEGRATION_DB.schema(:items, :reload=>true).map{|x| x.first}.should == [:id]
    @ds.columns!.should == [:id]
  end
end

if INTEGRATION_DB.respond_to?(:tables)
describe "Database#tables" do
  before do
    class ::String
      @@xxxxx = 0
      def xxxxx
        "xxxxx#{@@xxxxx += 1}"
      end
    end
    @iom = INTEGRATION_DB.identifier_output_method
    @iim = INTEGRATION_DB.identifier_input_method
    clear_sqls
  end
  after do
    INTEGRATION_DB.identifier_output_method = @iom
    INTEGRATION_DB.identifier_input_method = @iim
  end

  specify "should return an array of symbols" do
    ts = INTEGRATION_DB.tables
    ts.should be_a_kind_of(Array)
    ts.each{|t| t.should be_a_kind_of(Symbol)}
  end

  specify "should respect the database's identifier_output_method" do
    INTEGRATION_DB.identifier_output_method = :xxxxx
    INTEGRATION_DB.identifier_input_method = :xxxxx
    INTEGRATION_DB.tables.each{|t| t.to_s.should =~ /\Ax{5}\d+\z/}
  end
end
end
