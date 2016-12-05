require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')

describe Sequel::Schema::Generator do
  before do
    @generator = Sequel::Schema::Generator.new(Sequel.mock) do
      string :title
      column :body, :text
      foreign_key :parent_id
      primary_key :id
      check 'price > 100'
      constraint(:xxx) {{:yyy => :zzz}}
      index :title
      index [:title, :body], :unique => true
      foreign_key :node_id, :nodes
      foreign_key :deferrable_node_id, :nodes, :deferrable => true
      primary_key [:title, :parent_id], :name => :cpk
      foreign_key [:node_id, :prop_id], :nodes_props, :name => :cfk
    end
    @columns, @indexes, @constraints = @generator.columns, @generator.indexes, @generator.constraints
  end
  
  it "should respond to everything" do
    @generator.respond_to?(:foo).must_equal true
  end if RUBY_VERSION >= '1.9'

  it "should primary key column first" do
    @columns.first[:name].must_equal :id
    @columns.first[:primary_key].must_equal true
    @columns[3][:name].must_equal :parent_id
    @columns[3][:primary_key].must_be_nil
  end
  
  it "should respect existing column order if primary_key :keep_order is used" do
    generator = Sequel::Schema::Generator.new(Sequel.mock) do
      string :title
      primary_key :id, :keep_order=>true
    end

    columns = generator.columns
    columns.last[:name].must_equal :id
    columns.last[:primary_key].must_equal true
    columns.first[:name].must_equal :title
    columns.first[:primary_key].must_be_nil
  end
  
  it "should handle SQL::Identifier and SQL::QualifiedIdentifier as foreign_key arguments" do
    generator = Sequel::Schema::Generator.new(Sequel.mock) do
      foreign_key :a_id, Sequel.identifier(:as)
      foreign_key :b_id, Sequel.qualify(:c, :b)
    end

    columns = generator.columns
    columns.first.values_at(:name, :table).must_equal [:a_id, Sequel.identifier(:as)]
    columns.last.values_at(:name, :table).must_equal [:b_id, Sequel.qualify(:c, :b)]
  end
  
  it "counts definitions correctly" do
    @columns.size.must_equal 6
    @indexes.size.must_equal 2
    @constraints.size.must_equal 4
  end
  
  it "retrieves primary key name" do
    @generator.primary_key_name.must_equal :id
  end

  it "keeps columns in order" do
    @columns[1][:name].must_equal :title
    @columns[1][:type].must_equal :string
    @columns[2][:name].must_equal :body
    @columns[2][:type].must_equal :text
  end
  
  it "creates foreign key column" do
    @columns[3][:name].must_equal :parent_id
    @columns[3][:type].must_equal Integer
    @columns[4][:name].must_equal :node_id
    @columns[4][:type].must_equal Integer
  end

  it "creates deferrable altered foreign key column" do
    @columns[5][:name].must_equal :deferrable_node_id
    @columns[5][:type].must_equal Integer
    @columns[5][:deferrable].must_equal true
  end
  
  it "uses table for foreign key columns, if specified" do
    @columns[3][:table].must_be_nil
    @columns[4][:table].must_equal :nodes
    @constraints[3][:table].must_equal :nodes_props
  end
  
  it "finds columns" do
    [:title, :body, :parent_id, :id].each do |col|
      @generator.has_column?(col).must_equal true
    end
    @generator.has_column?(:foo).wont_equal true
  end
  
  it "creates constraints" do
    @constraints[0][:name].must_be_nil
    @constraints[0][:type].must_equal :check
    @constraints[0][:check].must_equal ['price > 100']

    @constraints[1][:name].must_equal :xxx
    @constraints[1][:type].must_equal :check
    @constraints[1][:check].must_be_kind_of Proc

    @constraints[2][:name].must_equal :cpk
    @constraints[2][:type].must_equal :primary_key
    @constraints[2][:columns].must_equal [ :title, :parent_id ]

    @constraints[3][:name].must_equal :cfk
    @constraints[3][:type].must_equal :foreign_key
    @constraints[3][:columns].must_equal [ :node_id, :prop_id ]
    @constraints[3][:table].must_equal :nodes_props
  end
  
  it "creates indexes" do
    @indexes[0][:columns].must_include(:title)
    @indexes[1][:columns].must_include(:title)
    @indexes[1][:columns].must_include(:body)
  end
end

describe Sequel::Schema::AlterTableGenerator do
  before do
    @generator = Sequel::Schema::AlterTableGenerator.new(Sequel.mock) do
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
      add_foreign_key [:node_id, :prop_id], :nodes_props, :name => :fkey
      drop_foreign_key :node_id
      drop_foreign_key [:node_id, :prop_id]
      drop_foreign_key [:node_id, :prop_id], :name => :fkey
    end
  end
  
  it "should generate operation records" do
    @generator.operations.must_equal [
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
      {:op => :add_constraint, :type => :foreign_key, :columns => [:node_id, :prop_id], :table => :nodes_props},
      {:op => :add_constraint, :type => :foreign_key, :columns => [:node_id, :prop_id], :table => :nodes_props, :name => :fkey},
      {:op => :drop_constraint, :type => :foreign_key, :columns => [:node_id]},
      {:op => :drop_column, :name => :node_id},
      {:op => :drop_constraint, :type => :foreign_key, :columns => [:node_id, :prop_id]},
      {:op => :drop_constraint, :type => :foreign_key, :columns => [:node_id, :prop_id], :name => :fkey},
    ]
  end
end

describe "Sequel::Schema::Generator generic type methods" do
  it "should store the type class in :type for each column" do
    Sequel::Schema::Generator.new(Sequel.mock) do
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
    end.columns.map{|c| c[:type]}.must_equal [String, Integer, Integer, :Bignum, Float, BigDecimal, Date, DateTime, Time, Numeric, File, TrueClass, FalseClass]
  end
end
