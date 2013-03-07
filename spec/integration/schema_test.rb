require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

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
    INTEGRATION_DB.drop_table?(:items)
  end

  specify "should handle a database with a identifier methods" do
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

  specify "should handle a dataset with identifier methods different than the database's" do
    INTEGRATION_DB.identifier_output_method = :reverse
    INTEGRATION_DB.identifier_input_method = :reverse
    INTEGRATION_DB.quote_identifiers = true
    INTEGRATION_DB.default_schema = nil if INTEGRATION_DB.default_schema
    INTEGRATION_DB.create_table!(:items){Integer :number}
    INTEGRATION_DB.identifier_output_method = @iom
    INTEGRATION_DB.identifier_input_method = @iim
    ds = INTEGRATION_DB[:items]
    ds.identifier_output_method = :reverse
    ds.identifier_input_method = :reverse
    begin
      INTEGRATION_DB.schema(ds, :reload=>true).should be_a_kind_of(Array)
      INTEGRATION_DB.schema(ds, :reload=>true).first.first.should == :number
    ensure 
      INTEGRATION_DB.identifier_output_method = :reverse
      INTEGRATION_DB.identifier_input_method = :reverse
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

  specify "should parse current timestamp defaults from the schema properly" do
    INTEGRATION_DB.create_table!(:items){Time :a, :default=>Sequel::CURRENT_TIMESTAMP}
    INTEGRATION_DB.schema(:items).first.last[:ruby_default].should == Sequel::CURRENT_TIMESTAMP
  end

  cspecify "should parse current date defaults from the schema properly", :mysql, :oracle do
    INTEGRATION_DB.create_table!(:items){Date :a, :default=>Sequel::CURRENT_DATE}
    INTEGRATION_DB.schema(:items).first.last[:ruby_default].should == Sequel::CURRENT_DATE
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
end if INTEGRATION_DB.respond_to?(:schema_parse_table, true)

test_indexes = begin
  INTEGRATION_DB.drop_table?(:blah)
  INTEGRATION_DB.indexes(:blah)
  true
rescue Sequel::NotImplemented
  false
rescue
  true
end
describe "Database index parsing" do
  after do
    INTEGRATION_DB.drop_table?(:items)
  end

  specify "should parse indexes into a hash" do
    # Delete :deferrable entry, since not all adapters implement it
    f = lambda{h = INTEGRATION_DB.indexes(:items); h.values.each{|h2| h2.delete(:deferrable)}; h}

    INTEGRATION_DB.create_table!(:items){Integer :n; Integer :a}
    f.call.should == {}
    INTEGRATION_DB.add_index(:items, :n)
    f.call.should == {:items_n_index=>{:columns=>[:n], :unique=>false}}
    INTEGRATION_DB.drop_index(:items, :n)
    f.call.should == {}
    INTEGRATION_DB.add_index(:items, :n, :unique=>true, :name=>:blah_blah_index)
    f.call.should == {:blah_blah_index=>{:columns=>[:n], :unique=>true}}
    INTEGRATION_DB.add_index(:items, [:n, :a])
    f.call.should == {:blah_blah_index=>{:columns=>[:n], :unique=>true}, :items_n_a_index=>{:columns=>[:n, :a], :unique=>false}}
    INTEGRATION_DB.drop_index(:items, :n, :name=>:blah_blah_index)
    f.call.should == {:items_n_a_index=>{:columns=>[:n, :a], :unique=>false}}
    INTEGRATION_DB.drop_index(:items, [:n, :a])
    f.call.should == {}
  end
  
  specify "should not include a primary key index" do
    INTEGRATION_DB.create_table!(:items){primary_key :n}
    INTEGRATION_DB.indexes(:items).should == {}
    INTEGRATION_DB.create_table!(:items){Integer :n; Integer :a; primary_key [:n, :a]}
    INTEGRATION_DB.indexes(:items).should == {}
  end
end if test_indexes

test_foreign_key_list = begin
  INTEGRATION_DB.drop_table?(:blah)
  INTEGRATION_DB.foreign_key_list(:blah)
  true
rescue Sequel::NotImplemented
  false
rescue
  true
end
describe "Database foreign key parsing" do
  before do
    @db = INTEGRATION_DB
    @pr = lambda do |table, *expected|
      actual = @db.foreign_key_list(table).sort_by{|c| c[:columns].map{|s| s.to_s}.join << (c[:key]||[]).map{|s| s.to_s}.join}.map{|v| v.values_at(:columns, :table, :key)}
      actual.zip(expected).each do |a, e|
        if e.last.first == :pk
          if a.last == nil
            a.pop
            e.pop
          else
           e.last.shift
          end
        end
        a.should == e
      end
      actual.length.should == expected.length
    end
  end
  after do
    @db.drop_table?(:b, :a)
  end

  specify "should parse foreign key information into an array of hashes" do
    @db.create_table!(:a, :engine=>:InnoDB){primary_key :c; Integer :d; index :d, :unique=>true}
    @db.create_table!(:b, :engine=>:InnoDB){foreign_key :e, :a}
    @pr[:a]
    @pr[:b, [[:e], :a, [:pk, :c]]]

    @db.alter_table(:b){add_foreign_key :f, :a, :key=>[:d]}
    @pr[:b, [[:e], :a, [:pk, :c]], [[:f], :a, [:d]]]

    @db.alter_table(:b){add_foreign_key [:f], :a, :key=>[:c]}
    @pr[:b, [[:e], :a, [:pk, :c]], [[:f], :a, [:c]], [[:f], :a, [:d]]]

    @db.alter_table(:a){add_index [:d, :c], :unique=>true}
    @db.alter_table(:b){add_foreign_key [:f, :e], :a, :key=>[:d, :c]}
    @pr[:b, [[:e], :a, [:pk, :c]], [[:f], :a, [:c]], [[:f], :a, [:d]], [[:f, :e], :a, [:d, :c]]]

    @db.alter_table(:b){drop_foreign_key [:f, :e]}
    @pr[:b, [[:e], :a, [:pk, :c]], [[:f], :a, [:c]], [[:f], :a, [:d]]]

    @db.alter_table(:b){drop_foreign_key :e}
    @pr[:b, [[:f], :a, [:c]], [[:f], :a, [:d]]]

    proc{@db.alter_table(:b){drop_foreign_key :f}}.should raise_error(Sequel::Error)
    @pr[:b, [[:f], :a, [:c]], [[:f], :a, [:d]]]
  end

  specify "should handle composite foreign and primary keys" do
    @db.create_table!(:a, :engine=>:InnoDB){Integer :b; Integer :c; primary_key [:b, :c]; index [:c, :b], :unique=>true}
    @db.create_table!(:b, :engine=>:InnoDB){Integer :e; Integer :f; foreign_key [:e, :f], :a; foreign_key [:f, :e], :a, :key=>[:c, :b]}
    @pr[:b, [[:e, :f], :a, [:pk, :b, :c]], [[:f, :e], :a, [:c, :b]]]
  end
end if test_foreign_key_list

describe "Database schema modifiers" do
  before do
    @db = INTEGRATION_DB
    @ds = @db[:items]
  end
  after do
    # Use instead of drop_table? to work around issues on jdbc/db2
    @db.drop_table(:items) rescue nil
    @db.drop_table(:items2) rescue nil
  end

  specify "should create tables correctly" do
    @db.create_table!(:items){Integer :number}
    @db.table_exists?(:items).should == true
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:number]
    @ds.insert([10])
    @ds.columns!.should == [:number]
  end
  
  specify "should create tables from select statements correctly" do
    @db.create_table!(:items){Integer :number}
    @ds.insert([10])
    @db.create_table(:items2, :as=>@db[:items])
    @db.schema(:items2, :reload=>true).map{|x| x.first}.should == [:number]
    @db[:items2].columns.should == [:number]
    @db[:items2].all.should == [{:number=>10}]
  end
  
  describe "views" do
    before do
      @db.drop_view(:items_view) rescue nil
      @db.create_table(:items){Integer :number}
      @ds.insert(:number=>1)
      @ds.insert(:number=>2)
    end
    after do
      @db.drop_view(:items_view)
    end

    specify "should create views correctly" do
      @db.create_view(:items_view, @ds.where(:number=>1))
      @db[:items_view].map(:number).should == [1]
    end

    specify "should create or replace views correctly" do
      @db.create_or_replace_view(:items_view, @ds.where(:number=>1))
      @db[:items_view].map(:number).should == [1]
      @db.create_or_replace_view(:items_view, @ds.where(:number=>2))
      @db[:items_view].map(:number).should == [2]
    end
  end
  
  specify "should handle create table in a rolled back transaction" do
    @db.drop_table?(:items)
    @db.transaction(:rollback=>:always){@db.create_table(:items){Integer :number}}
    @db.table_exists?(:items).should be_false
  end if INTEGRATION_DB.supports_transactional_ddl?
  
  describe "join tables" do
    after do
      @db.drop_join_table(:cat_id=>:cats, :dog_id=>:dogs) if @db.table_exists?(:cats_dogs)
      @db.drop_table(:cats, :dogs)
      @db.table_exists?(:cats_dogs).should == false
    end

    specify "should create join tables correctly" do
      @db.create_table!(:cats){primary_key :id}
      @db.create_table!(:dogs){primary_key :id}
      @db.create_join_table(:cat_id=>:cats, :dog_id=>:dogs)
      @db.table_exists?(:cats_dogs).should == true
    end
  end

  specify "should create temporary tables without raising an exception" do
    @db.create_table!(:items, :temp=>true){Integer :number}
  end

  specify "should have create_table? only create the table if it doesn't already exist" do
    @db.create_table!(:items){String :a}
    @db.create_table?(:items){String :b}
    @db[:items].columns.should == [:a]
    @db.drop_table?(:items)
    @db.create_table?(:items){String :b}
    @db[:items].columns.should == [:b]
  end

  specify "should have create_table? work correctly with indexes" do
    @db.create_table!(:items){String :a, :index=>true}
    @db.create_table?(:items){String :b, :index=>true}
    @db[:items].columns.should == [:a]
    @db.drop_table?(:items)
    @db.create_table?(:items){String :b, :index=>true}
    @db[:items].columns.should == [:b]
  end

  specify "should rename tables correctly" do
    @db.drop_table?(:items)
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

  cspecify "should add foreign key columns to tables correctly", :hsqldb do
    @db.create_table!(:items){primary_key :id}
    @ds.insert
    i = @ds.get(:id)
    @db.alter_table(:items){add_foreign_key :item_id, :items}
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id, :item_id]
    @ds.columns!.should == [:id, :item_id]
    @ds.all.should == [{:id=>i, :item_id=>nil}]
  end

  specify "should not allow NULLs in a primary key" do
    @db.create_table!(:items){String :id, :primary_key=>true}
    proc{@ds.insert(:id=>nil)}.should raise_error(Sequel::DatabaseError)
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

  specify "should rename columns when the table is referenced by a foreign key" do
    @db.create_table!(:items2){primary_key :id; Integer :a}
    @db.create_table!(:items){Integer :id, :primary_key=>true; foreign_key :items_id, :items2}
    @db[:items2].insert(:a=>10)
    @ds.insert(:id=>1)
    @db.alter_table(:items2){rename_column :a, :b}
    @db[:items2].insert(:b=>20)
    @ds.insert(:id=>2)
    @db[:items2].select_order_map([:id, :b]).should == [[1, 10], [2, 20]]
  end

  cspecify "should rename primary_key columns correctly", :db2 do
    @db.create_table!(:items){Integer :id, :primary_key=>true}
    @ds.insert(:id=>10)
    @db.alter_table(:items){rename_column :id, :id2}
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id2]
    @ds.columns!.should == [:id2]
    @ds.all.should == [{:id2=>10}]
  end

  cspecify "should set column NULL/NOT NULL correctly", [:jdbc, :db2], [:db2] do
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

  cspecify "should set column types correctly", [:jdbc, :db2], [:db2], :oracle do
    @db.create_table!(:items){Integer :id}
    @ds.insert(:id=>10)
    @db.alter_table(:items){set_column_type :id, String}
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id]
    @ds.columns!.should == [:id]
    @ds.insert(:id=>'20')
    @ds.all.should == [{:id=>"10"}, {:id=>"20"}]
  end

  cspecify "should set column types without modifying NULL/NOT NULL", [:jdbc, :db2], [:db2], :oracle, :derby do
    @db.create_table!(:items){Integer :id, :null=>false, :default=>2}
    proc{@ds.insert(:id=>nil)}.should raise_error(Sequel::DatabaseError)
    @db.alter_table(:items){set_column_type :id, String}
    proc{@ds.insert(:id=>nil)}.should raise_error(Sequel::DatabaseError)

    @db.create_table!(:items){Integer :id}
    @ds.insert(:id=>nil)
    @db.alter_table(:items){set_column_type :id, String}
    @ds.insert(:id=>nil)
    @ds.map(:id).should == [nil, nil]
  end

  cspecify "should set column types without modifying defaults", [:jdbc, :db2], [:db2], :oracle, :derby do
    @db.create_table!(:items){Integer :id, :default=>0}
    @ds.insert
    @ds.map(:id).should == [0]
    @db.alter_table(:items){set_column_type :id, String}
    @ds.insert
    @ds.map(:id).should == ['0', '0']

    @db.create_table!(:items){String :id, :default=>'a'}
    @ds.insert
    @ds.map(:id).should == %w'a'
    @db.alter_table(:items){set_column_type :id, String, :size=>1}
    @ds.insert
    @ds.map(:id).should == %w'a a'
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

  specify "should remove columns from tables correctly" do
    @db.create_table!(:items) do
      primary_key :id
      Integer :i
    end
    @ds.insert(:i=>10)
    @db.drop_column(:items, :i)
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id]
  end

  specify "should remove columns with defaults from tables correctly" do
    @db.create_table!(:items) do
      primary_key :id
      Integer :i, :default=>20
    end
    @ds.insert(:i=>10)
    @db.drop_column(:items, :i)
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id]
  end

  cspecify "should remove foreign key columns from tables correctly", :h2, :mssql, :hsqldb do
    # MySQL with InnoDB cannot drop foreign key columns unless you know the
    # name of the constraint, see Bug #14347
    @db.create_table!(:items, :engine=>:MyISAM) do
      primary_key :id
      Integer :i
      foreign_key :item_id, :items
    end
    @ds.insert(:i=>10)
    @db.drop_column(:items, :item_id)
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id, :i]
  end

  specify "should remove multiple columns in a single alter_table block" do
    @db.create_table!(:items) do
      primary_key :id
      String :name
      Integer :number
    end
    @ds.insert(:number=>10)
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id, :name, :number]
    @db.alter_table(:items) do
      drop_column :name
      drop_column :number
    end
    @db.schema(:items, :reload=>true).map{|x| x.first}.should == [:id]
  end

  cspecify "should work correctly with many operations in a single alter_table call", [:jdbc, :db2], [:db2] do
    @db.create_table!(:items) do
      primary_key :id
      String :name2
      String :number2
      constraint :bar, Sequel.~(:id=>nil)
    end
    @ds.insert(:name2=>'A12')
    @db.alter_table(:items) do
      add_column :number, Integer
      drop_column :number2
      rename_column :name2, :name
      drop_constraint :bar
      set_column_not_null :name
      set_column_default :name, 'A13'
      add_constraint :foo, Sequel.like(:name, 'A%')
    end
    @db[:items].first.should == {:id=>1, :name=>'A12', :number=>nil}
    @db[:items].delete
    proc{@db[:items].insert(:name=>nil)}.should raise_error(Sequel::DatabaseError)
    @db[:items].insert(:number=>1)
    @db[:items].get(:name).should == 'A13'
  end

  specify "should support deferrable foreign key constraints" do
    @db.create_table!(:items2){Integer :id, :primary_key=>true}
    @db.create_table!(:items){foreign_key :id, :items2, :deferrable=>true}
    proc{@db[:items].insert(1)}.should raise_error(Sequel::DatabaseError)
    proc{@db.transaction{proc{@db[:items].insert(1)}.should_not raise_error}}.should raise_error(Sequel::DatabaseError)
  end if INTEGRATION_DB.supports_deferrable_foreign_key_constraints?

  specify "should support deferrable unique constraints when creating or altering tables" do
    @db.create_table!(:items){Integer :t; unique [:t], :name=>:atest_def, :deferrable=>true, :using=>:btree}
    @db[:items].insert(1)
    @db[:items].insert(2)
    proc{@db[:items].insert(2)}.should raise_error(Sequel::DatabaseError)
    proc{@db.transaction{proc{@db[:items].insert(2)}.should_not raise_error}}.should raise_error(Sequel::DatabaseError)

    @db.create_table!(:items){Integer :t}
    @db.alter_table(:items){add_unique_constraint [:t], :name=>:atest_def, :deferrable=>true, :using=>:btree}
    @db[:items].insert(1)
    @db[:items].insert(2)
    proc{@db[:items].insert(2)}.should raise_error(Sequel::DatabaseError)
    proc{@db.transaction{proc{@db[:items].insert(2)}.should_not raise_error}}.should raise_error(Sequel::DatabaseError)
  end if INTEGRATION_DB.supports_deferrable_constraints?
end

test_tables = begin
  INTEGRATION_DB.tables
  true
rescue Sequel::NotImplemented
  false
end
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
end if test_tables

test_views = begin
  INTEGRATION_DB.views
  true
rescue Sequel::NotImplemented
  false
end
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
end if test_views
