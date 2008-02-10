require File.join(File.dirname(__FILE__), 'spec_helper')

describe Sequel::Schema::Generator do
  before :all do
    @generator = Sequel::Schema::Generator.new(SchemaDummyDatabase.new) do
      string :title
      column :body, :text
      foreign_key :parent_id
      primary_key :id
      check 'price > 100'
      constraint(:xxx) {:yyy == :zzz}
      index :title
      index [:title, :body]
    end
    @columns, @indexes = @generator.create_info
  end
  
  {:name => :id, :primary_key => true}.each do |column, expected|
    it "uses default primary key #{column}" do
      @columns.first[column].should == expected
    end
  end
  
  it "counts primary key, column and constraint definitions as columns" do
    @columns.size.should == 6
  end
  
  it "places primary key first" do
    @columns[0][:primary_key].should     be_true
    @columns[1][:primary_key].should_not be_true
    @columns[2][:primary_key].should_not be_true
  end

  it "retrieves primary key name" do
    @generator.primary_key_name.should == :id
  end

  it "keeps columns in order" do
    @columns[1][:name].should == :title
    @columns[1][:type].should == :string
    @columns[2][:name].should == :body
    @columns[2][:type].should == :text
  end
  
  it "creates foreign key column" do
    @columns[3][:name].should == :parent_id
    @columns[3][:type].should == :integer
  end
  
  it "finds columns" do
    [:title, :body, :parent_id, :id].each do |col|
      @generator.has_column?(col).should be_true
    end
    @generator.has_column?(:foo).should_not be_true
  end
  
  it "creates constraints" do
    @columns[4][:name].should == nil
    @columns[4][:type].should == :check
    @columns[4][:check].should == ['price > 100']

    @columns[5][:name].should == :xxx
    @columns[5][:type].should == :check
    @columns[5][:check].should be_a_kind_of(Proc)
  end
  
  it "creates indexes" do
    @indexes[0][:columns].should include(:title)
    @indexes[1][:columns].should include(:title)
    @indexes[1][:columns].should include(:body)
  end
end

describe Sequel::Schema::AlterTableGenerator do
  before :all do
    @generator = Sequel::Schema::AlterTableGenerator.new(SchemaDummyDatabase.new) do
      add_column :aaa, :text
      drop_column :bbb
      rename_column :ccc, :ho
      set_column_type :ddd, :float
      set_column_default :eee, 1
      add_index [:fff, :ggg]
      drop_index :hhh
      add_full_text_index :blah
    end
  end
  
  specify "should generate operation records" do
    @generator.operations.should == [
      {:op => :add_column, :name => :aaa, :type => :text},
      {:op => :drop_column, :name => :bbb},
      {:op => :rename_column, :name => :ccc, :new_name => :ho},
      {:op => :set_column_type, :name => :ddd, :type => :float},
      {:op => :set_column_default, :name => :eee, :default => 1},
      {:op => :add_index, :columns => [:fff, :ggg]},
      {:op => :drop_index, :columns => [:hhh]},
      {:op => :add_index, :columns => [:blah], :full_text => true}
    ]
  end
end