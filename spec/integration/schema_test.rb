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
    INTEGRATION_DB.create_table!(:items){Integer :number}
    INTEGRATION_DB.schema(:items, :reload=>true).should be_a_kind_of(Array)
    INTEGRATION_DB.schema(:items, :reload=>true).first.first.should == :number
  end

  specify "should not issue an sql query if the schema has been loaded unless :reload is true" do
    INTEGRATION_DB.create_table!(:items){Integer :number}
    INTEGRATION_DB.schema(:items, :reload=>true)
    clear_sqls
    INTEGRATION_DB.schema(:items)
    sqls_should_be
    clear_sqls
    INTEGRATION_DB.schema(:items, :reload=>true)
    sqls_should_be "PRAGMA table_info('items')"
  end

  specify "should raise an error when the table doesn't exist" do
    proc{INTEGRATION_DB.schema(:no_table)}.should raise_error(Sequel::Error)
  end

  specify "should return the schema correctly" do
    INTEGRATION_DB.create_table!(:items){Integer :number}
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
    INTEGRATION_DB.create_table!(:items){Integer :number}
    INTEGRATION_DB.schema(:items).collect{|k,v| k if v[:primary_key]}.compact.should == []
    INTEGRATION_DB.create_table!(:items){primary_key :number}
    INTEGRATION_DB.schema(:items).collect{|k,v| k if v[:primary_key]}.compact.should == [:number]
    INTEGRATION_DB.create_table!(:items){Integer :number1; Integer :number2; primary_key [:number1, :number2]}
    INTEGRATION_DB.schema(:items).collect{|k,v| k if v[:primary_key]}.compact.should == [:number1, :number2]
  end

  specify "should parse NULL/NOT NULL from the schema properly" do
    INTEGRATION_DB.create_table!(:items){Integer :number, :null=>true}
    INTEGRATION_DB.schema(:items).first.last[:allow_null].should == true
    INTEGRATION_DB.create_table!(:items){Integer :number, :null=>false}
    INTEGRATION_DB.schema(:items).first.last[:allow_null].should == false
  end

  specify "should parse defaults from the schema properly" do
    INTEGRATION_DB.create_table!(:items){Integer :number}
    INTEGRATION_DB.schema(:items).first.last[:default].should == nil
    INTEGRATION_DB.create_table!(:items){Integer :number, :default=>0}
    INTEGRATION_DB.schema(:items).first.last[:default].to_s.should == "0"
    INTEGRATION_DB.create_table!(:items){String :a, :default=>"blah"}
    INTEGRATION_DB.schema(:items).first.last[:default].should include('blah')
  end

  specify "should parse types from the schema properly" do
    INTEGRATION_DB.create_table!(:items){Integer :number}
    INTEGRATION_DB.schema(:items).first.last[:type].should == :integer
    INTEGRATION_DB.create_table!(:items){Fixnum :number}
    INTEGRATION_DB.schema(:items).first.last[:type].should == :integer
    INTEGRATION_DB.create_table!(:items){Bignum :number}
    INTEGRATION_DB.schema(:items).first.last[:type].should == :integer
    INTEGRATION_DB.create_table!(:items){Float :number}
    INTEGRATION_DB.schema(:items).first.last[:type].should == :float
    INTEGRATION_DB.create_table!(:items){BigDecimal :number}
    INTEGRATION_DB.schema(:items).first.last[:type].should == :decimal
    INTEGRATION_DB.create_table!(:items){Numeric :number}
    INTEGRATION_DB.schema(:items).first.last[:type].should == :decimal
    INTEGRATION_DB.create_table!(:items){String :number}
    INTEGRATION_DB.schema(:items).first.last[:type].should == :string
    INTEGRATION_DB.create_table!(:items){Date :number}
    INTEGRATION_DB.schema(:items).first.last[:type].should == :date
    INTEGRATION_DB.create_table!(:items){Time :number}
    INTEGRATION_DB.schema(:items).first.last[:type].should == :datetime
    INTEGRATION_DB.create_table!(:items){DateTime :number}
    INTEGRATION_DB.schema(:items).first.last[:type].should == :datetime
    INTEGRATION_DB.create_table!(:items){File :number}
    INTEGRATION_DB.schema(:items).first.last[:type].should == :blob
    INTEGRATION_DB.create_table!(:items){TrueClass :number}
    INTEGRATION_DB.schema(:items).first.last[:type].should == :boolean
    INTEGRATION_DB.create_table!(:items){FalseClass :number}
    INTEGRATION_DB.schema(:items).first.last[:type].should == :boolean
  end
end
end

if INTEGRATION_DB.respond_to?(:indexes)
describe "Database index parsing" do
  after do
    INTEGRATION_DB.drop_table(:items)
  end

  specify "should parse indexes into a hash" do
    INTEGRATION_DB.create_table!(:items){Integer :n; Integer :a}
    INTEGRATION_DB.indexes(:items).should == {}
    INTEGRATION_DB.add_index(:items, :n)
    INTEGRATION_DB.indexes(:items).should == {:items_n_index=>{:columns=>[:n], :unique=>false}}
    INTEGRATION_DB.drop_index(:items, :n)
    INTEGRATION_DB.indexes(:items).should == {}
    INTEGRATION_DB.add_index(:items, :n, :unique=>true, :name=>:blah_blah_index)
    INTEGRATION_DB.indexes(:items).should == {:blah_blah_index=>{:columns=>[:n], :unique=>true}}
    INTEGRATION_DB.add_index(:items, [:n, :a])
    INTEGRATION_DB.indexes(:items).should == {:blah_blah_index=>{:columns=>[:n], :unique=>true}, :items_n_a_index=>{:columns=>[:n, :a], :unique=>false}}
    INTEGRATION_DB.drop_index(:items, :n, :name=>:blah_blah_index)
    INTEGRATION_DB.indexes(:items).should == {:items_n_a_index=>{:columns=>[:n, :a], :unique=>false}}
    INTEGRATION_DB.drop_index(:items, [:n, :a])
    INTEGRATION_DB.indexes(:items).should == {}
  end
end
end

describe "Database schema modifiers" do
  before do
    INTEGRATION_DB.create_table!(:items){Integer :number}
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
    unless INTEGRATION_DB.url =~ /sqlite|amalgalite/
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
      String :name
      Integer :number
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
