SEQUEL_ADAPTER_TEST = :informix

require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

if DB.table_exists?(:test)
  DB.drop_table :test
end
DB.create_table :test do
  text :name
  integer :value
  
  index :value
end

describe "A Informix database" do
  it "should provide disconnect functionality" do
    DB.execute("select user from dual")
    DB.pool.size.must_equal 1
    DB.disconnect
    DB.pool.size.must_equal 0
  end
end

describe "A Informix dataset" do
  before do
    @d = DB[:test]
    @d.delete # remove all records
  end
  
  it "should return the correct record count" do
    @d.count.must_equal 0
    @d << {:name => 'abc', :value => 123}
    @d << {:name => 'abc', :value => 456}
    @d << {:name => 'def', :value => 789}
    @d.count.must_equal 3
  end
  
  it "should return the correct records" do
    @d.to_a.must_equal []
    @d << {:name => 'abc', :value => 123}
    @d << {:name => 'abc', :value => 456}
    @d << {:name => 'def', :value => 789}

    @d.order(:value).to_a.must_equal [
      {:name => 'abc', :value => 123},
      {:name => 'abc', :value => 456},
      {:name => 'def', :value => 789}
    ]
  end
  
  it "should update records correctly" do
    @d << {:name => 'abc', :value => 123}
    @d << {:name => 'abc', :value => 456}
    @d << {:name => 'def', :value => 789}
    @d.filter(:name => 'abc').update(:value => 530)
    
    # the third record should stay the same
    # floating-point precision bullshit
    @d[:name => 'def'][:value].must_equal 789
    @d.filter(:value => 530).count.must_equal 2
  end
  
  it "should delete records correctly" do
    @d << {:name => 'abc', :value => 123}
    @d << {:name => 'abc', :value => 456}
    @d << {:name => 'def', :value => 789}
    @d.filter(:name => 'abc').delete
    
    @d.count.must_equal 1
    @d.first[:name].must_equal 'def'
  end
  
  it "should be able to literalize booleans" do
    @d.literal(true)
    @d.literal(false)
  end
  
  it "should support transactions" do
    DB.transaction do
      @d << {:name => 'abc', :value => 1}
    end

    @d.count.must_equal 1
  end
  
  it "should support #first and #last" do
    @d << {:name => 'abc', :value => 123}
    @d << {:name => 'abc', :value => 456}
    @d << {:name => 'def', :value => 789}
    
    @d.order(:value).first.must_equal(:name => 'abc', :value => 123)
    @d.order(:value).last.must_equal(:name => 'def', :value => 789)
  end

  it "should return last inserted id" do
    first = @d.insert :name => 'abc', :value => 123
    second = @d.insert :name => 'abc', :value => 123
    (second - first).must_equal 1
  end
end
