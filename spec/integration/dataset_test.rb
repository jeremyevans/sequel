require File.join(File.dirname(__FILE__), 'spec_helper.rb')

describe "Simple Dataset operations" do
  before do
    INTEGRATION_DB.create_table!(:items) do
      primary_key :id
      integer :number
    end
    @ds = INTEGRATION_DB[:items]
    @ds.insert(:number=>10)
    clear_sqls
  end
  after do
    INTEGRATION_DB.drop_table(:items)
  end

  specify "should insert with a primary key specified" do
    @ds.insert(:id=>100, :number=>20)
    sqls_should_be(/INSERT INTO items \((number, id|id, number)\) VALUES \((100, 20|20, 100)\)/)
    @ds.count.should == 2
    @ds.order(:id).all.should == [{:id=>1, :number=>10}, {:id=>100, :number=>20}]
  end

  specify "should have insert return primary key value" do
    @ds.insert(:number=>20).should == 2
    sqls_should_be('INSERT INTO items (number) VALUES (20)')
    @ds.filter(:id=>2).first[:number].should == 20
  end

  specify "should delete correctly" do
    @ds.filter(1=>1).delete.should == 1
    sqls_should_be('DELETE FROM items WHERE (1 = 1)')
    @ds.count.should == 0
  end

  specify "should update correctly" do
    @ds.update(:number=>:number+1).should == 1
    sqls_should_be('UPDATE items SET number = (number + 1)')
    @ds.all.should == [{:id=>1, :number=>11}]
  end

  specify "should fetch all results correctly" do
    @ds.all.should == [{:id=>1, :number=>10}]
    sqls_should_be('SELECT * FROM items')
  end

  specify "should fetch a single row correctly" do
    @ds.first.should == {:id=>1, :number=>10}
    sqls_should_be('SELECT * FROM items LIMIT 1')
  end
end

describe "Simple Dataset operations" do
  before do
    INTEGRATION_DB.create_table!(:items) do
      Integer :number
      TrueClass :flag
    end
    @ds = INTEGRATION_DB[:items]
  end
  after do
    INTEGRATION_DB.drop_table(:items)
  end

  specify "should deal with boolean conditions correctly" do
    @ds.insert(:number=>1, :flag=>true)
    @ds.insert(:number=>2, :flag=>false)
    @ds.insert(:number=>3, :flag=>nil)
    @ds.order!(:number)
    @ds.filter(:flag=>true).map(:number).should == [1]
    @ds.filter(:flag=>false).map(:number).should == [2]
    @ds.filter(:flag=>nil).map(:number).should == [3]
    @ds.exclude(:flag=>true).map(:number).should == [2, 3]
    @ds.exclude(:flag=>false).map(:number).should == [1, 3]
    @ds.exclude(:flag=>nil).map(:number).should == [1, 2]
  end
end

describe "Simple Dataset operations in transactions" do
  before do
    INTEGRATION_DB.create_table!(:items_insert_in_transaction) do
      primary_key :id
      integer :number
    end
    @ds = INTEGRATION_DB[:items_insert_in_transaction]
    clear_sqls
  end
  after do
    INTEGRATION_DB.drop_table(:items_insert_in_transaction)
  end

  specify "should insert correctly with a primary key specified inside a transaction" do
    INTEGRATION_DB.transaction do
      @ds.insert(:id=>100, :number=>20)
      sqls_should_be('Transaction.begin', /INSERT INTO items_insert_in_transaction \((number, id|id, number)\) VALUES \((100, 20|20, 100)\)/)
      @ds.count.should == 1
      @ds.order(:id).all.should == [{:id=>100, :number=>20}]
    end
  end
  
  specify "should have insert return primary key value inside a transaction" do
    INTEGRATION_DB.transaction do
      @ds.insert(:number=>20).should == 1
      sqls_should_be('Transaction.begin', /INSERT INTO items_insert_in_transaction \(number\) VALUES \(20\)/)
      @ds.count.should == 1
      @ds.order(:id).all.should == [{:id=>1, :number=>20}]
    end
  end
end

describe "Dataset UNION, EXCEPT, and INTERSECT" do
  before do
    INTEGRATION_DB.create_table!(:i1){integer :number}
    INTEGRATION_DB.create_table!(:i2){integer :number}
    INTEGRATION_DB.create_table!(:i3){integer :number}
    @ds1 = INTEGRATION_DB[:i1]
    @ds1.insert(:number=>10)
    @ds1.insert(:number=>20)
    @ds2 = INTEGRATION_DB[:i2]
    @ds2.insert(:number=>10)
    @ds2.insert(:number=>30)
    @ds3 = INTEGRATION_DB[:i3]
    @ds3.insert(:number=>10)
    @ds3.insert(:number=>40)
    clear_sqls
  end
  
  specify "should give the correct results for simple UNION, EXCEPT, and INTERSECT" do
    @ds1.union(@ds2).order(:number).map{|x| x[:number].to_s}.should == %w'10 20 30'
    unless defined?(Sequel::Dataset::UnsupportedIntersectExcept) and @ds1.class.ancestors.include?(Sequel::Dataset::UnsupportedIntersectExcept)
      @ds1.except(@ds2).order(:number).map{|x| x[:number].to_s}.should == %w'20'
      @ds1.intersect(@ds2).order(:number).map{|x| x[:number].to_s}.should == %w'10'
    end
  end
  
  specify "should give the correct results for compound UNION, EXCEPT, and INTERSECT" do
    @ds1.union(@ds2).union(@ds3).order(:number).map{|x| x[:number].to_s}.should == %w'10 20 30 40'
    @ds1.union(@ds2.union(@ds3)).order(:number).map{|x| x[:number].to_s}.should == %w'10 20 30 40'
    unless defined?(Sequel::Dataset::UnsupportedIntersectExcept) and @ds1.class.ancestors.include?(Sequel::Dataset::UnsupportedIntersectExcept)
      @ds1.union(@ds2).except(@ds3).order(:number).map{|x| x[:number].to_s}.should == %w'20 30'
      @ds1.union(@ds2.except(@ds3)).order(:number).map{|x| x[:number].to_s}.should == %w'10 20 30'
      @ds1.union(@ds2).intersect(@ds3).order(:number).map{|x| x[:number].to_s}.should == %w'10 '
      @ds1.union(@ds2.intersect(@ds3)).order(:number).map{|x| x[:number].to_s}.should == %w'10 20'
      
      @ds1.except(@ds2).union(@ds3).order(:number).map{|x| x[:number].to_s}.should == %w'10 20 40'
      @ds1.except(@ds2.union(@ds3)).order(:number).map{|x| x[:number].to_s}.should == %w'20'
      @ds1.except(@ds2).except(@ds3).order(:number).map{|x| x[:number].to_s}.should == %w'20'
      @ds1.except(@ds2.except(@ds3)).order(:number).map{|x| x[:number].to_s}.should == %w'10 20'
      @ds1.except(@ds2).intersect(@ds3).order(:number).map{|x| x[:number].to_s}.should == %w''
      @ds1.except(@ds2.intersect(@ds3)).order(:number).map{|x| x[:number].to_s}.should == %w'20'
      
      @ds1.intersect(@ds2).union(@ds3).order(:number).map{|x| x[:number].to_s}.should == %w'10 40'
      @ds1.intersect(@ds2.union(@ds3)).order(:number).map{|x| x[:number].to_s}.should == %w'10'
      @ds1.intersect(@ds2).except(@ds3).order(:number).map{|x| x[:number].to_s}.should == %w''
      @ds1.intersect(@ds2.except(@ds3)).order(:number).map{|x| x[:number].to_s}.should == %w''
      @ds1.intersect(@ds2).intersect(@ds3).order(:number).map{|x| x[:number].to_s}.should == %w'10'
      @ds1.intersect(@ds2.intersect(@ds3)).order(:number).map{|x| x[:number].to_s}.should == %w'10'
    end
  end
end
