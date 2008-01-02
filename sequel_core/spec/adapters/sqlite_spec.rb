require File.join(File.dirname(__FILE__), '../../lib/sequel')

SQLITE_DB = Sequel('sqlite:/')
SQLITE_DB.create_table :items do
  integer :id, :primary_key => true, :auto_increment => true
  text :name
  float :value
end
SQLITE_DB.create_table :test2 do
  text :name
  integer :value
end
SQLITE_DB.create_table(:time) {timestamp :t}

context "An SQLite database" do
  setup do
    @db = Sequel('sqlite:/')
  end
  
  specify "should provide a list of existing tables" do
    @db.tables.should == []
    
    @db.create_table :testing do
      text :name
    end
    @db.tables.should include(:testing)
  end
  
  specify "should support getting pragma values" do
    @db.pragma_get(:auto_vacuum).should == '0'
  end
  
  specify "should support setting pragma values" do
    @db.pragma_set(:auto_vacuum, '1')
    @db.pragma_get(:auto_vacuum).should == '1'
  end
  
  specify "should support getting and setting the auto_vacuum pragma" do
    @db.auto_vacuum = :full
    @db.auto_vacuum.should == :full
    @db.auto_vacuum = :none
    @db.auto_vacuum.should == :none
    
    proc {@db.auto_vacuum = :invalid}.should raise_error(Sequel::Error)
  end

  specify "should support getting and setting the synchronous pragma" do
    @db.synchronous = :off
    @db.synchronous.should == :off
    @db.synchronous = :normal
    @db.synchronous.should == :normal
    @db.synchronous = :full
    @db.synchronous.should == :full
    
    proc {@db.synchronous = :invalid}.should raise_error(Sequel::Error)
  end
  
  specify "should support getting and setting the temp_store pragma" do
    @db.temp_store = :default
    @db.temp_store.should == :default
    @db.temp_store = :file
    @db.temp_store.should == :file
    @db.temp_store = :memory
    @db.temp_store.should == :memory
    
    proc {@db.temp_store = :invalid}.should raise_error(Sequel::Error)
  end
  
  specify "should be able to execute multiple statements at once" do
    @db.create_table :t do
      text :name
    end
    
    @db << "insert into t (name) values ('abc');insert into t (name) values ('def')"

    @db[:t].count.should == 2
    
    @db[:t].order(:name).map(:name).should == ['abc', 'def']
  end
  
  specify "should be able to execute transactions" do
    @db.transaction do
      @db.create_table(:t) {text :name}
    end
    
    @db.tables.should == [:t]

    proc {@db.transaction do
      @db.create_table(:u) {text :name}
      raise ArgumentError
    end}.should raise_error(ArgumentError)
    # no commit
    @db.tables.should == [:t]

    proc {@db.transaction do
      @db.create_table(:v) {text :name}
      rollback!
    end}.should_not raise_error
    # no commit
    @db.tables.should == [:t]
  end

  specify "should support nested transactions" do
    @db.transaction do
      @db.transaction do
        @db.create_table(:t) {text :name}
      end
    end
    
    @db.tables.should == [:t]

    proc {@db.transaction do
      @db.create_table(:v) {text :name}
      @db.transaction do
        rollback! # should roll back the top-level transaction
      end
    end}.should_not raise_error
    # no commit
    @db.tables.should == [:t]
  end
  
  specify "should provide disconnect functionality" do
    @db.tables
    @db.pool.size.should == 1
    @db.disconnect
    @db.pool.size.should == 0
  end

  specify "should support timestamps" do
    t1 = Time.at(Time.now.to_i) #normalize time
    
    SQLITE_DB[:time] << {:t => t1}
    SQLITE_DB[:time].first[:t].should == t1
  end
end

context "An SQLite dataset" do
  setup do
    @d = SQLITE_DB[:items]
    @d.delete # remove all records
  end
  
  specify "should return the correct records" do
    @d.to_a.should == []
    @d << {:name => 'abc', :value => 1.23}
    @d << {:name => 'abc', :value => 4.56}
    @d << {:name => 'def', :value => 7.89}
    @d.select(:name, :value).to_a.sort_by {|h| h[:value]}.should == [
      {:name => 'abc', :value => 1.23},
      {:name => 'abc', :value => 4.56},
      {:name => 'def', :value => 7.89}
    ]
  end
  
  specify "should return the correct record count" do
    @d.count.should == 0
    @d << {:name => 'abc', :value => 1.23}
    @d << {:name => 'abc', :value => 4.56}
    @d << {:name => 'def', :value => 7.89}
    @d.count.should == 3
  end

  specify "should return the last inserted id when inserting records" do
    id = @d << {:name => 'abc', :value => 1.23}
    id.should == @d.first[:id]
  end
  
  specify "should update records correctly" do
    @d << {:name => 'abc', :value => 1.23}
    @d << {:name => 'abc', :value => 4.56}
    @d << {:name => 'def', :value => 7.89}
    @d.filter(:name => 'abc').update(:value => 5.3)
    
    # the third record should stay the same
    @d[:name => 'def'][:value].should == 7.89
    @d.filter(:value => 5.3).count.should == 2
  end
  
  specify "should delete records correctly" do
    @d << {:name => 'abc', :value => 1.23}
    @d << {:name => 'abc', :value => 4.56}
    @d << {:name => 'def', :value => 7.89}
    @d.filter(:name => 'abc').delete
    
    @d.count.should == 1
    @d.first[:name].should == 'def'
  end
end

context "An SQLite dataset" do
  setup do
    @d = SQLITE_DB[:items]
    @d.delete # remove all records
    @d << {:name => 'abc', :value => 1.23}
    @d << {:name => 'def', :value => 4.56}
    @d << {:name => 'ghi', :value => 7.89}
  end
  
  specify "should correctly return avg" do
    @d.avg(:value).should == ((1.23 + 4.56 + 7.89) / 3).to_s
  end
  
  specify "should correctly return sum" do
    @d.sum(:value).should == (1.23 + 4.56 + 7.89).to_s
  end
  
  specify "should correctly return max" do
    @d.max(:value).should == 7.89.to_s
  end
  
  specify "should correctly return min" do
    @d.min(:value).should == 1.23.to_s
  end
end

context "SQLite::Dataset#delete" do
  setup do
    @d = SQLITE_DB[:items]
    @d.delete # remove all records
    @d << {:name => 'abc', :value => 1.23}
    @d << {:name => 'def', :value => 4.56}
    @d << {:name => 'ghi', :value => 7.89}
  end
  
  specify "should return the number of records affected when filtered" do
    @d.count.should == 3
    @d.filter {:value < 3}.delete.should == 1
    @d.count.should == 2

    @d.filter {:value < 3}.delete.should == 0
    @d.count.should == 2
  end
  
  specify "should return the number of records affected when unfiltered" do
    @d.count.should == 3
    @d.delete.should == 3
    @d.count.should == 0

    @d.delete.should == 0
  end
end

context "SQLite::Dataset#update" do
  setup do
    @d = SQLITE_DB[:items]
    @d.delete # remove all records
    @d << {:name => 'abc', :value => 1.23}
    @d << {:name => 'def', :value => 4.56}
    @d << {:name => 'ghi', :value => 7.89}
  end
  
  specify "should return the number of records affected" do
    @d.filter(:name => 'abc').update(:value => 2).should == 1
    
    @d.update(:value => 10).should == 3
    
    @d.filter(:name => 'xxx').update(:value => 23).should == 0
  end
end

context "An SQLite dataset in array tuples mode" do
  setup do
    @d = SQLITE_DB[:items]
    @d.delete # remove all records
    
    Sequel.use_array_tuples
  end
  
  teardown do
    Sequel.use_hash_tuples
  end
  
  specify "should return the correct records" do
    @d.to_a.should == []
    @d << {:name => 'abc', :value => 1.23}
    @d << {:name => 'abc', :value => 4.56}
    @d << {:name => 'def', :value => 7.89}
    @d.select(:name, :value).to_a.sort_by {|h| h[:value]}.should == [
      ['abc', 1.23],
      ['abc', 4.56],
      ['def', 7.89]
    ]
  end
end  

context "SQLite dataset" do
  setup do
    SQLITE_DB.create_table :test do
      integer :id, :primary_key => true, :auto_increment => true
      text :name
      float :value
    end

    @d = SQLITE_DB[:items]
    @d.delete # remove all records
    @d << {:name => 'abc', :value => 1.23}
    @d << {:name => 'def', :value => 4.56}
    @d << {:name => 'ghi', :value => 7.89}
  end
  
  teardown do
    SQLITE_DB.drop_table :test
  end
  
  specify "should be able to insert from a subquery" do
    SQLITE_DB[:test] << @d
    SQLITE_DB[:test].count.should == 3
    SQLITE_DB[:test].select(:name, :value).order(:value).to_a.should == \
      @d.select(:name, :value).order(:value).to_a
  end
end

context "A SQLite database" do
  setup do
    @db = SQLITE_DB
  end

  specify "should support add_column operations" do
    @db.add_column :test2, :xyz, :text
    
    @db[:test2].columns.should == [:name, :value, :xyz]
    @db[:test2] << {:name => 'mmm', :value => 111}
    @db[:test2].first[:xyz].should == '000'
  end
  
  specify "should not support drop_column operations" do
    proc {@db.drop_column :test2, :xyz}.should raise_error(SequelError)
  end
  
  specify "should not support rename_column operations" do
    @db[:test2].delete
    @db.add_column :test2, :xyz, :text, :default => '000'
    @db[:test2] << {:name => 'mmm', :value => 111, :xyz => 'qqqq'}

    @db[:test2].columns.should == [:name, :value, :xyz]
    proc {@db.rename_column :test2, :xyz, :zyx}.should raise_error(SequelError)
  end
  
  specify "should not support set_column_type operations" do
    @db.add_column :test2, :xyz, :float
    @db[:test2].delete
    @db[:test2] << {:name => 'mmm', :value => 111, :xyz => 56.78}
    proc {@db.set_column_type :test2, :xyz, :integer}.should raise_error(SequelError)
  end
end  
