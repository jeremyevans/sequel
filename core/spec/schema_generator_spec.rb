require File.join(File.dirname(__FILE__), 'spec_helper')

describe Sequel::Schema::Generator do
  before :all do
    @generator = Sequel::Schema::Generator.new(SchemaDummyDatabase.new) do
      string :title
      column :body, :text
      foreign_key :parent_id
      primary_key :id
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
  
  it "counts primary key as column" do
    @columns.size.should == 4
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
  
  it "creates indexes" do
    @indexes[0][:columns].should include(:title)
    @indexes[1][:columns].should include(:title)
    @indexes[1][:columns].should include(:body)
  end
end