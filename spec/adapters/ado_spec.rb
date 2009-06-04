require File.join(File.dirname(__FILE__), 'spec_helper.rb')

unless defined?(ADO_DB)
  ADO_DB = Sequel.ado(:host => 'MY_SQL_SERVER', :database => 'MyDB', :user => 'my_usr', :password => 'my_pwd')
end
INTEGRATION_DB = ADO_DB unless defined?(INTEGRATION_DB)

context "An ADO dataset" do
  before(:each) do
    ADO_DB.create_table!(:items) { text :name }
  end
  
  specify "should not raise exceptions when working with empty datasets" do
    lambda {
      ADO_DB[:items].all
    }.should_not raise_error
  end

  describe 'setting the :command_timeout option' do
    before(:each) do
      @conn_options = {:host => 'MY_SQL_SERVER',
            :database => 'MyDB',
            :user => 'my_usr',
            :password => 'my_pwd',
            :command_timeout => 120}
    end

    specify 'it should set the CommandTimeout parameter on the ADO handle' do
      db = Sequel::ADO::Database.new(@conn_options)
      db.connect(@conn_options).CommandTimeout.should == 120
    end
  end

  describe 'when the :command_timeout option is not implicitly set' do
    before(:each) do
      @conn_options = {:host => 'MY_SQL_SERVER',
            :database => 'MyDB',
            :user => 'my_usr',
            :password => 'my_pwd'}
    end

    specify 'it should remain as the default of 30 seconds' do
      db = Sequel::ADO::Database.new(@conn_options)
      db.connect(@conn_options).CommandTimeout.should == 30
    end
  end

  describe 'setting the :provider option' do
    before(:each) do
      @conn_options = {:host => 'MY_SQL_SERVER',
            :database => 'MyDB',
            :user => 'my_usr',
            :password => 'my_pwd',
            :provider => "SQLOLEDB"}
    end

    specify 'it should set the CommandTimeout parameter on the ADO handle' do
      db = Sequel::ADO::Database.new(@conn_options)
      db.connect(@conn_options).Provider.should match /sqloledb/i
    end
  end


end

context "An MSSQL dataset" do
  before(:each) do
    ADO_DB.create_table!(:items) { text :name }
  end

  specify "should assign a default name to anonymous columns" do
    col = ADO_DB.fetch('SELECT COUNT(*) FROM items').columns[0]
    col.to_s.should == '(no column name)'
  end

  specify "should support counting" do
    ADO_DB[:items] << {:name => 'my name' }
    ADO_DB[:items].count.should == 1
  end

  specify "should support first" do
    ADO_DB[:items] << {:name => 'x' }
    ADO_DB[:items] << {:name => 'y' }
    ADO_DB[:items].first[:name].should == 'x'
  end

  specify "should support limit" do
    3.times do
      ADO_DB[:items] << {:name => 'my name' }
    end
    ADO_DB[:items].limit(2).all.size.should == 2
  end
end
