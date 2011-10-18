require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

if INTEGRATION_DB.respond_to?(:schema_parse_table, true)
describe "Database schema parser" do
  before do
    @iom = INTEGRATION_DB.identifier_output_method
    @iim = INTEGRATION_DB.identifier_input_method
    @defsch = INTEGRATION_DB.default_schema
    @qi = INTEGRATION_DB.quote_identifiers?
  end
  after do
    INTEGRATION_DB.identifier_output_method = @iom
    INTEGRATION_DB.identifier_input_method = @iim
    INTEGRATION_DB.default_schema = @defsch
    INTEGRATION_DB.quote_identifiers = @qi
    INTEGRATION_DB.drop_table(:items) if INTEGRATION_DB.table_exists?(:items)
  end

  specify "should handle a database with a identifier_output_method" do
    INTEGRATION_DB.identifier_output_method = :reverse
    INTEGRATION_DB.identifier_input_method = :reverse
    INTEGRATION_DB.quote_identifiers = true
    INTEGRATION_DB.default_schema = nil if INTEGRATION_DB.default_schema
    INTEGRATION_DB.create_table!(:items){Integer :number}
    begin
      INTEGRATION_DB.schema(:items, :reload=>true).should be_a_kind_of(Array)
      INTEGRATION_DB.schema(:items, :reload=>true).first.first.should == :number
    ensure 
      INTEGRATION_DB.drop_table(:items)
    end
  end

  specify "should not issue an sql query if the schema has been loaded unless :reload is true" do
    INTEGRATION_DB.create_table!(:items){Integer :number}
    INTEGRATION_DB.schema(:items, :reload=>true)
    INTEGRATION_DB.schema(:items)
    INTEGRATION_DB.schema(:items, :reload=>true)
  end

  specify "Model schema should include columns in the table, even if they aren't selected" do
    INTEGRATION_DB.create_table!(:items){String :a; Integer :number}
    m = Sequel::Model(INTEGRATION_DB[:items].select(:a))
    m.columns.should == [:a]
    m.db_schema[:number][:type].should == :integer
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
    INTEGRATION_DB.schema(:items)
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
    INTEGRATION_DB.schema(:items).first.last[:ruby_default].should == nil
    INTEGRATION_DB.create_table!(:items){Integer :number, :default=>0}
    INTEGRATION_DB.schema(:items).first.last[:ruby_default].should == 0
    INTEGRATION_DB.create_table!(:items){String :a, :default=>"blah"}
    INTEGRATION_DB.schema(:items).first.last[:ruby_default].should == 'blah'
  end

  cspecify "should parse types from the schema properly", [:jdbc, :db2], :oracle do
    INTEGRATION_DB.create_table!(:items){Integer :number}
    INTEGRATION_DB.schema(:items).first.last[:type].should == :integer
    INTEGRATION_DB.create_table!(:items){Fixnum :number}
    INTEGRATION_DB.schema(:items).first.last[:type].should == :integer
    INTEGRATION_DB.create_table!(:items){Bignum :number}
    INTEGRATION_DB.schema(:items).first.last[:type].should == :integer
    INTEGRATION_DB.create_table!(:items){Float :number}
    INTEGRATION_DB.schema(:items).first.last[:type].should == :float
    INTEGRATION_DB.create_table!(:items){BigDecimal :number, :size=>[11, 2]}
    INTEGRATION_DB.schema(:items).first.last[:type].should == :decimal
    INTEGRATION_DB.create_table!(:items){Numeric :number, :size=>[12, 0]}
    INTEGRATION_DB.schema(:items).first.last[:type].should == :integer
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

begin
  INTEGRATION_DB.drop_table(:blah) rescue nil
  INTEGRATION_DB.indexes(:blah)
rescue Sequel::NotImplemented
rescue
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
  
  specify "should not include a primary key index" do
    INTEGRATION_DB.create_table!(:items){primary_key :n}
    INTEGRATION_DB.indexes(:items).should == {}
    INTEGRATION_DB.create_table!(:items){Integer :n; Integer :a; primary_key [:n, :a]}
    INTEGRATION_DB.indexes(:items).should == {}
  end
end
end

describe "Database schema modifiers" do
  before do
    @db = INTEGRATION_DB
    @ds = @db[:items]
  end
  after do
    @db.drop_table(:items) if @db.table_exists?(:items)
  end

  specify "should create tables correctly" do
    @db.create_table!(:items){Integer :number}
    @db.table_exists?(:items).should == true
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:number]
    @ds.insert([10])
    @ds.columns!.should == [:number]
  end
  
  specify "should create temporary tables without raising an exception" do
    @db.create_table!(:items, :temp=>true){Integer :number}
  end
  
  specify "should have create_table? only create the table if it doesn't already exist" do
    @db.create_table!(:items){String :a}
    @db.create_table?(:items){String :b}
    @db[:items].columns.should == [:a]
    @db.drop_table(:items) rescue nil
    @db.create_table?(:items){String :b}
    @db[:items].columns.should == [:b]
  end

  specify "should have create_table? work correctly with indexes" do
    @db.create_table!(:items){String :a, :index=>true}
    @db.create_table?(:items){String :b, :index=>true}
    @db[:items].columns.should == [:a]
    @db.drop_table(:items) rescue nil
    @db.create_table?(:items){String :b, :index=>true}
    @db[:items].columns.should == [:b]
  end

  specify "should rename tables correctly" do
    @db.drop_table(:items) rescue nil
    @db.create_table!(:items2){Integer :number}
    @db.rename_table(:items2, :items)
    @db.table_exists?(:items).should == true
    @db.table_exists?(:items2).should == false
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:number]
    @ds.insert([10])
    @ds.columns!.should == [:number]
  end
  
  specify "should allow creating indexes with tables" do
    @db.create_table!(:items){Integer :number; index :number}
    @db.table_exists?(:items).should == true
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:number]
    @ds.insert([10])
    @ds.columns!.should == [:number]
  end

  specify "should handle combination of default, unique, and not null" do
    @db.create_table!(:items){Integer :number, :default=>0, :null=>false, :unique=>true}
    @db.table_exists?(:items).should == true
    @db.schema(:items, :reload=>true).map{|x| x.last}.first.values_at(:ruby_default, :allow_null).should == [0, false]
    @ds.insert([10])
  end

  specify "should handle foreign keys correctly when creating tables" do
    @db.create_table!(:items) do 
      primary_key :id
      foreign_key :item_id, :items
      unique [:item_id, :id]
      foreign_key [:id, :item_id], :items, :key=>[:item_id, :id]
    end
    @db.table_exists?(:items).should == true
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id, :item_id]
    @ds.columns!.should == [:id, :item_id]
  end

  specify "should add columns to tables correctly" do
    @db.create_table!(:items){Integer :number}
    @ds.insert(:number=>10)
    @db.alter_table(:items){add_column :name, String}
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:number, :name]
    @ds.columns!.should == [:number, :name]
    @ds.all.should == [{:number=>10, :name=>nil}]
  end

  cspecify "should add primary key columns to tables correctly", :h2, :derby do
    @db.create_table!(:items){Integer :number}
    @ds.insert(:number=>10)
    @db.alter_table(:items){add_primary_key :id}
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:number, :id]
    @ds.columns!.should == [:number, :id]
    @ds.map(:number).should == [10]
    proc{@ds.insert(:id=>@ds.map(:id).first)}.should raise_error
  end

  specify "should drop primary key constraints from tables correctly" do
    @db.create_table!(:items){Integer :number; primary_key [:number], :name=>:items_pk}
    @ds.insert(:number=>10)
    @db.alter_table(:items){drop_constraint :items_pk, :type=>:primary_key}
    @ds.map(:number).should == [10]
    proc{@ds.insert(10)}.should_not raise_error
  end

  specify "should add foreign key columns to tables correctly" do
    @db.create_table!(:items){primary_key :id}
    @ds.insert
    i = @ds.get(:id)
    @db.alter_table(:items){add_foreign_key :item_id, :items}
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id, :item_id]
    @ds.columns!.should == [:id, :item_id]
    @ds.all.should == [{:id=>i, :item_id=>nil}]
  end

  specify "should rename columns correctly" do
    @db.create_table!(:items){Integer :id}
    @ds.insert(:id=>10)
    @db.alter_table(:items){rename_column :id, :id2}
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id2]
    @ds.columns!.should == [:id2]
    @ds.all.should == [{:id2=>10}]
  end

  specify "should rename columns with defaults correctly" do
    @db.create_table!(:items){String :n, :default=>'blah'}
    @ds.insert
    @db.alter_table(:items){rename_column :n, :n2}
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:n2]
    @ds.columns!.should == [:n2]
    @ds.insert
    @ds.all.should == [{:n2=>'blah'}, {:n2=>'blah'}]
  end

  specify "should rename columns with not null constraints" do
    @db.create_table!(:items, :engine=>:InnoDB){String :n, :null=>false}
    @ds.insert(:n=>'blah')
    @db.alter_table(:items){rename_column :n, :n2}
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:n2]
    @ds.columns!.should == [:n2]
    @ds.insert(:n2=>'blah')
    @ds.all.should == [{:n2=>'blah'}, {:n2=>'blah'}]
    proc{@ds.insert(:n=>nil)}.should raise_error(Sequel::DatabaseError)
  end

  cspecify "should set column NULL/NOT NULL correctly", [:jdbc, :db2] do
    @db.create_table!(:items, :engine=>:InnoDB){Integer :id}
    @ds.insert(:id=>10)
    @db.alter_table(:items){set_column_allow_null :id, false}
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id]
    @ds.columns!.should == [:id]
    proc{@ds.insert(:id=>nil)}.should raise_error(Sequel::DatabaseError)
    @db.alter_table(:items){set_column_allow_null :id, true}
    @ds.insert(:id=>nil)
    @ds.all.should == [{:id=>10}, {:id=>nil}]
  end

  specify "should set column defaults correctly" do
    @db.create_table!(:items){Integer :id}
    @ds.insert(:id=>10)
    @db.alter_table(:items){set_column_default :id, 20}
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id]
    @ds.columns!.should == [:id]
    @ds.insert
    @ds.all.should == [{:id=>10}, {:id=>20}]
  end

  cspecify "should set column types correctly", [:jdbc, :db2], :oracle do
    @db.create_table!(:items){Integer :id}
    @ds.insert(:id=>10)
    @db.alter_table(:items){set_column_type :id, String}
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id]
    @ds.columns!.should == [:id]
    @ds.insert(:id=>'20')
    @ds.all.should == [{:id=>"10"}, {:id=>"20"}]
  end

  specify "should add unnamed unique constraints and foreign key table constraints correctly" do
    @db.create_table!(:items, :engine=>:InnoDB){Integer :id; Integer :item_id}
    @db.alter_table(:items) do
      add_unique_constraint [:item_id, :id]
      add_foreign_key [:id, :item_id], :items, :key=>[:item_id, :id]
    end
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id, :item_id]
    @ds.columns!.should == [:id, :item_id]
    proc{@ds.insert(1, 1)}.should_not raise_error
    proc{@ds.insert(1, 1)}.should raise_error
    proc{@ds.insert(1, 2)}.should raise_error
  end

  specify "should add named unique constraints and foreign key table constraints correctly" do
    @db.create_table!(:items, :engine=>:InnoDB){Integer :id, :null=>false; Integer :item_id, :null=>false}
    @db.alter_table(:items) do
      add_unique_constraint [:item_id, :id], :name=>:unique_iii
      add_foreign_key [:id, :item_id], :items, :key=>[:item_id, :id], :name=>:fk_iii
    end
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id, :item_id]
    @ds.columns!.should == [:id, :item_id]
    proc{@ds.insert(1, 1)}.should_not raise_error
    proc{@ds.insert(1, 1)}.should raise_error
    proc{@ds.insert(1, 2)}.should raise_error
  end

  specify "should drop unique constraints and foreign key table constraints correctly" do
    @db.create_table!(:items) do
      Integer :id
      Integer :item_id
      unique [:item_id, :id], :name=>:items_uk
      foreign_key [:id, :item_id], :items, :key=>[:item_id, :id], :name=>:items_fk
    end
    @db.alter_table(:items) do
      drop_constraint(:items_fk, :type=>:foreign_key)
      drop_constraint(:items_uk, :type=>:unique)
    end
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id, :item_id]
    @ds.columns!.should == [:id, :item_id]
    proc{@ds.insert(1, 2)}.should_not raise_error
    proc{@ds.insert(1, 2)}.should_not raise_error
  end

  cspecify "should remove columns from tables correctly", :h2, :mssql, [:jdbc, :db2], :hsqldb do
    @db.create_table!(:items) do
      primary_key :id
      String :name
      Integer :number
      foreign_key :item_id, :items
    end
    @ds.insert(:number=>10)
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id, :name, :number, :item_id]
    @ds.columns!.should == [:id, :name, :number, :item_id]
    @db.drop_column(:items, :number)
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id, :name, :item_id]
    @ds.columns!.should == [:id, :name, :item_id]
    @db.drop_column(:items, :name)
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id, :item_id]
    @ds.columns!.should == [:id, :item_id]
    @db.drop_column(:items, :item_id)
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id]
    @ds.columns!.should == [:id]
  end

  cspecify "should remove multiple columns in a single alter_table block", [:jdbc, :db2] do
    @db.create_table!(:items) do
      primary_key :id
      String :name
      Integer :number
    end
    @ds.insert(:number=>10)
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id, :name, :number]
    @ds.columns!.should == [:id, :name, :number]
    @db.alter_table(:items) do
      drop_column :name
      drop_column :number
    end
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id]
    @ds.columns!.should == [:id]
  end
end

begin
  INTEGRATION_DB.tables
rescue Sequel::NotImplemented
rescue
describe "Database#tables" do
  before do
    class ::String
      @@xxxxx = 0
      def xxxxx
        "xxxxx#{@@xxxxx += 1}"
      end
    end
    @db = INTEGRATION_DB
    @db.create_table(:sequel_test_table){Integer :a}
    @db.create_view :sequel_test_view, @db[:sequel_test_table]
    @iom = @db.identifier_output_method
    @iim = @db.identifier_input_method
  end
  after do
    @db.identifier_output_method = @iom
    @db.identifier_input_method = @iim
    @db.drop_view :sequel_test_view
    @db.drop_table :sequel_test_table
  end

  specify "should return an array of symbols" do
    ts = @db.tables
    ts.should be_a_kind_of(Array)
    ts.each{|t| t.should be_a_kind_of(Symbol)}
    ts.should include(:sequel_test_table)
    ts.should_not include(:sequel_test_view)
  end

  specify "should respect the database's identifier_output_method" do
    @db.identifier_output_method = :xxxxx
    @db.identifier_input_method = :xxxxx
    @db.tables.each{|t| t.to_s.should =~ /\Ax{5}\d+\z/}
  end
end
end

begin
  INTEGRATION_DB.views
rescue Sequel::NotImplemented
rescue
describe "Database#views" do
  before do
    class ::String
      @@xxxxx = 0
      def xxxxx
        "xxxxx#{@@xxxxx += 1}"
      end
    end
    @db = INTEGRATION_DB
    @db.create_table(:sequel_test_table){Integer :a}
    @db.create_view :sequel_test_view, @db[:sequel_test_table]
    @iom = @db.identifier_output_method
    @iim = @db.identifier_input_method
  end
  after do
    @db.identifier_output_method = @iom
    @db.identifier_input_method = @iim
    @db.drop_view :sequel_test_view
    @db.drop_table :sequel_test_table
  end

  specify "should return an array of symbols" do
    ts = @db.views
    ts.should be_a_kind_of(Array)
    ts.each{|t| t.should be_a_kind_of(Symbol)}
    ts.should_not include(:sequel_test_table)
    ts.should include(:sequel_test_view)
  end

  specify "should respect the database's identifier_output_method" do
    @db.identifier_output_method = :xxxxx
    @db.identifier_input_method = :xxxxx
    @db.views.each{|t| t.to_s.should =~ /\Ax{5}\d+\z/}
  end
end
end
