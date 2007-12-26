require File.join(File.dirname(__FILE__), '../../lib/sequel')

ORACLE_DB = Sequel('oracle://hr:hr@loalhost/XE')
if ORACLE_DB.table_exists?(:test)
  ORACLE_DB.drop_table :test
end
ORACLE_DB.create_table :test do
  text :name
  integer :value
  
  index :value
end

context "A Oracle database" do
  specify "should provide disconnect functionality" do
    ORACLE_DB.execute("select user from dual")
    ORACLE_DB.pool.size.should == 1
    ORACLE_DB.disconnect
    ORACLE_DB.pool.size.should == 0
  end
end

context "A Oracle dataset" do
  setup do
    @d = ORACLE_DB[:test]
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
    ORACLE_DB.transaction do
      @d << {:name => 'abc', :value => 1}
    end

    @d.count.should == 1
  end
end

context "A Oracle dataset in array tuples mode" do
  setup do
    @d = ORACLE_DB[:test]
    @d.delete # remove all records
    Sequel.use_array_tuples
  end
  
  teardown do
    Sequel.use_hash_tuples
  end
  
  specify "should return the correct records" do
    @d.to_a.should == []
    @d << {:name => 'abc', :value => 123}
    @d << {:name => 'abc', :value => 456}
    @d << {:name => 'def', :value => 789}

    @d.order(:value).select(:name, :value).to_a.should == [
      ['abc', 123],
      ['abc', 456],
      ['def', 789]
    ]
  end
  
  specify "should work correctly with transforms" do
    @d.transform(:value => [proc {|v| v.to_s}, proc {|v| v.to_i}])

    @d.to_a.should == []
    @d << {:name => 'abc', :value => 123}
    @d << {:name => 'abc', :value => 456}
    @d << {:name => 'def', :value => 789}

    @d.order(:value).select(:name, :value).to_a.should == [
      ['abc', '123'],
      ['abc', '456'],
      ['def', '789']
    ]
    
    a = @d.order(:value).first
    a.values.should == ['abc', '123']
    a.keys.should == [:name, :value]
    a[:name].should == 'abc'
    a[:value].should == '123'
  end
end
