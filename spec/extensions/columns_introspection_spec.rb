require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')

describe "Sequel::Dataset.introspect_all_columns" do
  before do
    @db = MODEL_DB
    @ds = @db[:a]
    class Sequel::Dataset
      # Handle case where introspect_all_columns has already been called
      alias columns columns_without_introspection unless instance_methods(false).map{|x| x.to_s}.include?('columns')
    end
    Sequel::Dataset.introspect_all_columns
    @db.reset
  end
  after do
    class Sequel::Dataset
      alias columns columns_without_introspection
    end
  end

  specify "should turn on column introspection by default" do
    @ds.select(:x).columns.should == [:x]
    @db.sqls.length.should == 0
  end
end

describe "columns_introspection extension" do
  before do
    @db = MODEL_DB
    @ds = @db[:a]
    @ds.extend(Sequel::ColumnsIntrospection.dup) # dup to allow multiple places in class hierarchy
    @db.reset
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
    @ds.select(:x.identifier).columns.should == [:x]
    @db.sqls.length.should == 0
  end

  specify "should handle SQL::QualifiedIdentifiers" do
    @ds.select(:x.qualify(:t)).columns.should == [:x]
    @ds.select(:x.identifier.qualify(:t)).columns.should == [:x]
    @db.sqls.length.should == 0
  end

  specify "should handle SQL::AliasedExpressions" do
    @ds.select(:x.as(:a)).columns.should == [:a]
    @ds.select(:x.as(:a.identifier)).columns.should == [:a]
    @db.sqls.length.should == 0
  end

  specify "should issue a database query if the wildcard is selected" do
    @ds.columns
    @db.sqls.length.should == 1
  end

  specify "should issue a database query if an unsupported type is used" do
    @ds.select(1).columns
    @db.sqls.length.should == 1
  end

  specify "should not have column introspection on by default" do
    @db[:a].select(:x).columns
    @db.sqls.length.should == 1
  end
end
