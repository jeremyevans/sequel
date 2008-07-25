require File.join(File.dirname(__FILE__), 'spec_helper.rb')

describe "Supported types" do
  def create_items_table_with_column(name, type)
    INTEGRATION_DB.create_table!(:items){column name, type}
    INTEGRATION_DB[:items]
  end

  specify "should support NULL correctly" do
    ds = create_items_table_with_column(:number, :integer)
    ds.insert(:number => nil)
    ds.all.should == [{:number=>nil}]
  end

  specify "should support integer type" do
    ds = create_items_table_with_column(:number, :integer)
    ds.insert(:number => 2)
    ds.all.should == [{:number=>2}]
  end

  specify "should support varchar type" do
    ds = create_items_table_with_column(:name, 'varchar(255)'.lit)
    ds.insert(:name => 'Test User')
    ds.all.should == [{:name=>'Test User'}]
  end
  
  specify "should support date type" do
    ds = create_items_table_with_column(:dat, :date)
    d = Date.today
    ds.insert(:dat => d)
    x = ds.first[:dat]
    x = x.iso8601.to_date if Time === x
    x.to_s.should == d.to_s
  end
  
  specify "should support time type" do
    ds = create_items_table_with_column(:tim, :time)
    t = Time.now
    ds.insert(:tim => t)
    x = ds.first[:tim]
    [t.strftime('%H:%M:%S'), t.iso8601].should include(x.respond_to?(:strftime) ? x.strftime('%H:%M:%S') : x.to_s)
  end
end
