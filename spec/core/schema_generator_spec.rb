require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')

describe Sequel::Schema::Generator do
  before do
    @generator = Sequel::Schema::Generator.new(SchemaDummyDatabase.new) do
      string :title
      column :body, :text
      foreign_key :parent_id
      primary_key :id
      check 'price > 100'
      constraint(:xxx) {:yyy == :zzz}
      index :title
      index [:title, :body], :unique => true
      foreign_key :node_id, :nodes
      foreign_key :deferrable_node_id, :nodes, :deferrable => true
      primary_key [:title, :parent_id], :name => :cpk
      foreign_key [:node_id, :prop_id], :nodes_props, :name => :cfk
    end
    @columns, @indexes, @constraints = @generator.columns, @generator.indexes, @generator.constraints
  end
  
  it "should primary key column first" do
    @columns.first[:name].should == :id
    @columns.first[:primary_key].should == true
    @columns[3][:name].should == :parent_id
    @columns[3][:primary_key].should == nil
  end
  
  it "counts definitions correctly" do
    @columns.size.should == 6
    @indexes.size.should == 2
    @constraints.size.should == 4
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
    @columns[3][:type].should == Integer
    @columns[4][:name].should == :node_id
    @columns[4][:type].should == Integer
  end

  it "creates deferrable altered foreign key column" do
    @columns[5][:name].should == :deferrable_node_id
    @columns[5][:type].should == Integer
    @columns[5][:deferrable].should == true
  end
  
  it "uses table for foreign key columns, if specified" do
    @columns[3][:table].should == nil
    @columns[4][:table].should == :nodes
    @constraints[3][:table].should == :nodes_props
  end
  
  it "finds columns" do
    [:title, :body, :parent_id, :id].each do |col|
      @generator.has_column?(col).should be_true
    end
    @generator.has_column?(:foo).should_not be_true
  end
  
  it "creates constraints" do
    @constraints[0][:name].should == nil
    @constraints[0][:type].should == :check
    @constraints[0][:check].should == ['price > 100']

    @constraints[1][:name].should == :xxx
    @constraints[1][:type].should == :check
    @constraints[1][:check].should be_a_kind_of(Proc)

    @constraints[2][:name].should == :cpk
    @constraints[2][:type].should == :primary_key
    @constraints[2][:columns].should == [ :title, :parent_id ]

    @constraints[3][:name].should == :cfk
    @constraints[3][:type].should == :foreign_key
    @constraints[3][:columns].should == [ :node_id, :prop_id ]
    @constraints[3][:table].should == :nodes_props
  end
  
  it "creates indexes" do
    @indexes[0][:columns].should include(:title)
    @indexes[1][:columns].should include(:title)
    @indexes[1][:columns].should include(:body)
  end
end

describe Sequel::Schema::AlterTableGenerator do
  before do
    @generator = Sequel::Schema::AlterTableGenerator.new(SchemaDummyDatabase.new) do
      add_column :aaa, :text
      drop_column :bbb
      rename_column :ccc, :ho
      set_column_type :ddd, :float
      set_column_default :eee, 1
      add_index [:fff, :ggg]
      drop_index :hhh
      drop_index :hhh, :name=>:blah_blah
      add_full_text_index :blah
      add_spatial_index :geom
      add_index :blah, :type => :hash
      add_index :blah, :where => {:something => true}
      add_constraint :con1, 'fred > 100'
      drop_constraint :con2
      add_unique_constraint [:aaa, :bbb, :ccc], :name => :con3
      add_primary_key :id
      add_foreign_key :node_id, :nodes
      add_primary_key [:aaa, :bbb]
      add_foreign_key [:node_id, :prop_id], :nodes_props
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
      {:op => :drop_index, :columns => [:hhh], :name=>:blah_blah},
      {:op => :add_index, :columns => [:blah], :type => :full_text},
      {:op => :add_index, :columns => [:geom], :type => :spatial},
      {:op => :add_index, :columns => [:blah], :type => :hash},
      {:op => :add_index, :columns => [:blah], :where => {:something => true}},
      {:op => :add_constraint, :type => :check, :name => :con1, :check => ['fred > 100']},
      {:op => :drop_constraint, :name => :con2},
      {:op => :add_constraint, :type => :unique, :name => :con3, :columns => [:aaa, :bbb, :ccc]},
      {:op => :add_column, :name => :id, :type => Integer, :primary_key=>true, :auto_increment=>true},
      {:op => :add_column, :name => :node_id, :type => Integer, :table=>:nodes},
      {:op => :add_constraint, :type => :primary_key, :columns => [:aaa, :bbb]},
      {:op => :add_constraint, :type => :foreign_key, :columns => [:node_id, :prop_id], :table => :nodes_props}
    ]
  end
end

describe "Sequel::Schema::Generator generic type methods" do
  it "should store the type class in :type for each column" do
    Sequel::Schema::Generator.new(SchemaDummyDatabase.new) do
      String :a
      Integer :b
      Fixnum :c
      Bignum :d
      Float :e
      BigDecimal :f
      Date :g
      DateTime :h
      Time :i
      Numeric :j
      File :k
      TrueClass :l
      FalseClass :m
    end.columns.map{|c| c[:type]}.should == [String, Integer, Fixnum, Bignum, Float, BigDecimal, Date, DateTime, Time, Numeric, File, TrueClass, FalseClass]
  end
end
