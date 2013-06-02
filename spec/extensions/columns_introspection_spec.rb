require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')

Sequel.extension :columns_introspection

describe "columns_introspection extension" do
  before do
    @db = Sequel.mock
    @ds = @db[:a]
    @ds.extend(Sequel::ColumnsIntrospection.dup) # dup to allow multiple places in class hierarchy
    @db.sqls
  end

  specify "should not issue a database query if the columns are already loaded" do
    @ds.instance_variable_set(:@columns, [:x])
    @ds.columns.should == [:x]
    @db.sqls.length.should == 0
  end
  
  specify "should handle plain symbols without a database query" do
    @ds.select(:x).columns.should == [:x]
    @db.sqls.length.should == 0
  end

  specify "should handle qualified symbols without a database query" do
    @ds.select(:t__x).columns.should == [:x]
    @db.sqls.length.should == 0
  end

  specify "should handle aliased symbols without a database query" do
    @ds.select(:x___a).columns.should == [:a]
    @db.sqls.length.should == 0
  end

  specify "should handle qualified and aliased symbols without a database query" do
    @ds.select(:t__x___a).columns.should == [:a]
    @db.sqls.length.should == 0
  end

  specify "should handle SQL::Identifiers " do
    @ds.select(Sequel.identifier(:x)).columns.should == [:x]
    @db.sqls.length.should == 0
  end

  specify "should handle SQL::QualifiedIdentifiers" do
    @ds.select(Sequel.qualify(:t, :x)).columns.should == [:x]
    @ds.select(Sequel.identifier(:x).qualify(:t)).columns.should == [:x]
    @db.sqls.length.should == 0
  end

  specify "should handle SQL::AliasedExpressions" do
    @ds.select(Sequel.as(:x, :a)).columns.should == [:a]
    @ds.select(Sequel.as(:x, Sequel.identifier(:a))).columns.should == [:a]
    @db.sqls.length.should == 0
  end

  specify "should handle selecting * from a single subselect with no joins without a database query if the subselect's columns can be handled" do
    @ds.select(:x).from_self.columns.should == [:x]
    @db.sqls.length.should == 0
    @ds.select(:x).from_self.from_self.columns.should == [:x]
    @db.sqls.length.should == 0
  end

  specify "should handle selecting * from a single table with no joins without a database query if the database has cached schema columns for the table" do
    @db.instance_variable_set(:@schemas, "a"=>[[:x, {}]])
    @ds.columns.should == [:x]
    @db.sqls.length.should == 0
  end

  specify "should issue a database query for multiple subselects or joins" do
    @ds.from(@ds.select(:x), @ds.select(:y)).columns
    @db.sqls.length.should == 1
    @ds.select(:x).from_self.natural_join(:a).columns
    @db.sqls.length.should == 1
  end

  specify "should issue a database query when common table expressions are used" do
    @db.instance_variable_set(:@schemas, "a"=>[[:x, {}]])
    @ds.with(:a, @ds).columns
    @db.sqls.length.should == 1
  end

  specify "should issue a database query if the wildcard is selected" do
    @ds.columns
    @db.sqls.length.should == 1
  end

  specify "should issue a database query if an unsupported type is used" do
    @ds.select(1).columns
    @db.sqls.length.should == 1
  end
end
