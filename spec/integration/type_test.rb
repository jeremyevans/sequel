require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

describe "Supported types" do
  def create_items_table_with_column(name, type, opts={})
    INTEGRATION_DB.create_table!(:items){column name, type, opts}
    INTEGRATION_DB[:items]
  end

  cspecify "should support casting correctly", [:sqlite, :sqlite] do
    ds = create_items_table_with_column(:number, Integer)
    ds.insert(:number => 1)
    ds.select(:number.cast_string.as(:n)).map(:n).should == %w'1'
    ds = create_items_table_with_column(:name, String)
    ds.insert(:name=> '1')
    ds.select(:name.cast_numeric.as(:n)).map(:n).should == [1]
  end

  specify "should support NULL correctly" do
    ds = create_items_table_with_column(:number, Integer)
    ds.insert(:number => nil)
    ds.all.should == [{:number=>nil}]
  end

  specify "should support generic integer type" do
    ds = create_items_table_with_column(:number, Integer)
    ds.insert(:number => 2)
    ds.all.should == [{:number=>2}]
  end
  
  specify "should support generic fixnum type" do
    ds = create_items_table_with_column(:number, Fixnum)
    ds.insert(:number => 2)
    ds.all.should == [{:number=>2}]
  end
  
  specify "should support generic bignum type" do
    ds = create_items_table_with_column(:number, Bignum)
    ds.insert(:number => 2**34)
    ds.all.should == [{:number=>2**34}]
  end
  
  specify "should support generic float type" do
    ds = create_items_table_with_column(:number, Float)
    ds.insert(:number => 2.1)
    ds.all.should == [{:number=>2.1}]
  end
  
  cspecify "should support generic numeric type", [:odbc, :mssql] do
    ds = create_items_table_with_column(:number, Numeric, :size=>[15, 10])
    ds.insert(:number => BigDecimal.new('2.123456789'))
    ds.all.should == [{:number=>BigDecimal.new('2.123456789')}]
    ds = create_items_table_with_column(:number, BigDecimal, :size=>[15, 10])
    ds.insert(:number => BigDecimal.new('2.123456789'))
    ds.all.should == [{:number=>BigDecimal.new('2.123456789')}]
  end

  specify "should support generic string type" do
    ds = create_items_table_with_column(:name, String)
    ds.insert(:name => 'Test User')
    ds.all.should == [{:name=>'Test User'}]
  end
  
  cspecify "should support generic date type", [:do, :sqlite], [:jdbc, :sqlite], :mssql do
    ds = create_items_table_with_column(:dat, Date)
    d = Date.today
    ds.insert(:dat => d)
    ds.first[:dat].should == d
  end
  
  cspecify "should support generic datetime type", [:do, :sqlite], [:jdbc, :sqlite] do
    ds = create_items_table_with_column(:tim, DateTime)
    t = DateTime.now
    ds.insert(:tim => t)
    ds.first[:tim].strftime('%Y%m%d%H%M%S').should == t.strftime('%Y%m%d%H%M%S')
    ds = create_items_table_with_column(:tim, Time)
    t = Time.now
    ds.insert(:tim => t)
    ds.first[:tim].strftime('%Y%m%d%H%M%S').should == t.strftime('%Y%m%d%H%M%S')
  end
  
  cspecify "should support generic file type", [:do], [:odbc, :mssql], [:mysql2] do
    ds = create_items_table_with_column(:name, File)
    ds.insert(:name => ("a\0"*300).to_sequel_blob)
    ds.all.should == [{:name=>("a\0"*300).to_sequel_blob}]
    ds.first[:name].should be_a_kind_of(::Sequel::SQL::Blob)
  end
  
  cspecify "should support generic boolean type", [:do, :sqlite], [:jdbc, :sqlite], [:odbc, :mssql] do
    ds = create_items_table_with_column(:number, TrueClass)
    ds.insert(:number => true)
    ds.all.should == [{:number=>true}]
    ds = create_items_table_with_column(:number, FalseClass)
    ds.insert(:number => true)
    ds.all.should == [{:number=>true}]
  end
end
