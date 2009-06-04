require File.join(File.dirname(__FILE__), 'spec_helper.rb')

unless defined?(INFORMIX_DB)
  INFORMIX_DB = Sequel.connect('informix://localhost/mydb')
end
INTEGRATION_DB = INFORMIX_DB unless defined?(INTEGRATION_DB)

if INFORMIX_DB.table_exists?(:test)
  INFORMIX_DB.drop_table :test
end
INFORMIX_DB.create_table :test do
  text :name
  integer :value
  
  index :value
end

context "A Informix database" do
  specify "should provide disconnect functionality" do
    INFORMIX_DB.execute("select user from dual")
    INFORMIX_DB.pool.size.should == 1
    INFORMIX_DB.disconnect
    INFORMIX_DB.pool.size.should == 0
  end
end

context "A Informix dataset" do
  before do
    @d = INFORMIX_DB[:test]
    @d.delete # remove all records
  end
  
  specify "should return the correct record count" do
    @d.count.should == 0
    @d << {:name => 'abc', :value => 123}
    @d << {:name => 'abc', :value => 456}
    @d << {:name => 'def', :value => 789}
    @d.count.should == 3
  end
  
  specify "should return the correct records" do
    @d.to_a.should == []
    @d << {:name => 'abc', :value => 123}
    @d << {:name => 'abc', :value => 456}
    @d << {:name => 'def', :value => 789}

    @d.order(:value).to_a.should == [
      {:name => 'abc', :value => 123},
      {:name => 'abc', :value => 456},
      {:name => 'def', :value => 789}
    ]
  end
  
  specify "should update records correctly" do
    @d << {:name => 'abc', :value => 123}
    @d << {:name => 'abc', :value => 456}
    @d << {:name => 'def', :value => 789}
    @d.filter(:name => 'abc').update(:value => 530)
    
    # the third record should stay the same
    # floating-point precision bullshit
    @d[:name => 'def'][:value].should == 789
    @d.filter(:value => 530).count.should == 2
  end
  
  specify "should delete records correctly" do
    @d << {:name => 'abc', :value => 123}
    @d << {:name => 'abc', :value => 456}
    @d << {:name => 'def', :value => 789}
    @d.filter(:name => 'abc').delete
    
    @d.count.should == 1
    @d.first[:name].should == 'def'
  end
  
  specify "should be able to literalize booleans" do
    proc {@d.literal(true)}.should_not raise_error
    proc {@d.literal(false)}.should_not raise_error
  end
  
  specify "should support transactions" do
    INFORMIX_DB.transaction do
      @d << {:name => 'abc', :value => 1}
    end

    @d.count.should == 1
  end
  
  specify "should support #first and #last" do
    @d << {:name => 'abc', :value => 123}
    @d << {:name => 'abc', :value => 456}
    @d << {:name => 'def', :value => 789}
    
    @d.order(:value).first.should == {:name => 'abc', :value => 123}
    @d.order(:value).last.should == {:name => 'def', :value => 789}
  end
end
