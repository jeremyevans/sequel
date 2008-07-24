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
