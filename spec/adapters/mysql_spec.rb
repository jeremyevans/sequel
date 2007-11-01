require File.join(File.dirname(__FILE__), '../../lib/sequel/mysql')

MYSQL_DB = Sequel('mysql://root@localhost/sandbox')
if MYSQL_DB.table_exists?(:items)
  MYSQL_DB.drop_table :items
end
MYSQL_DB.create_table :items do
  text :name
  integer :value
  
  index :value
end

context "A MySQL database" do
  setup do
    @db = MYSQL_DB
  end
  
  specify "should provide disconnect functionality" do
    @db.tables
    @db.pool.size.should == 1
    @db.disconnect
    @db.pool.size.should == 0
  end
end

context "A MySQL dataset" do
  setup do
    @d = MYSQL_DB[:items]
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
  
  specify "should quote columns using back-ticks" do
    @d.select(:name).sql.should == \
      'SELECT `name` FROM items'
      
    @d.select('COUNT(*)'.lit).sql.should == \
      'SELECT COUNT(*) FROM items'

    @d.select(:value.MAX).sql.should == \
      'SELECT max(`value`) FROM items'
      
    @d.select(:NOW[]).sql.should == \
    'SELECT NOW() FROM items'

    @d.select(:items__value.MAX).sql.should == \
      'SELECT max(items.`value`) FROM items'

    @d.order(:name.DESC).sql.should == \
      'SELECT * FROM items ORDER BY `name` DESC'
      
    @d.select('items.name AS item_name'.to_sym).sql.should == \
      'SELECT items.`name` AS `item_name` FROM items'
      
    @d.select('`name`'.lit).sql.should == \
      'SELECT `name` FROM items'

    @d.select('max(items.`name`) AS `max_name`'.lit).sql.should == \
      'SELECT max(items.`name`) AS `max_name` FROM items'

    @d.insert_sql(:value => 333).should == \
      'INSERT INTO items (`value`) VALUES (333);'
  end
  
  specify "should support ORDER clause in UPDATE statements" do
    @d.order(:name).update_sql(:value => 1).should == \
      'UPDATE items SET `value` = 1 ORDER BY `name`'
  end
  
  specify "should support LIMIT clause in UPDATE statements" do
    @d.limit(10).update_sql(:value => 1).should == \
      'UPDATE items SET `value` = 1 LIMIT 10'
  end
  
  specify "should support transactions" do
    MYSQL_DB.transaction do
      @d << {:name => 'abc', :value => 1}
    end

    @d.count.should == 1
  end
  
  specify "should support regexps" do
    @d << {:name => 'abc', :value => 1}
    @d << {:name => 'bcd', :value => 2}
    @d.filter(:name => /bc/).count.should == 2
    @d.filter(:name => /^bc/).count.should == 1
  end
  
  specify "should correctly literalize strings with comment backslashes in them" do
    @d.delete
    proc {@d << {:name => ':\\'}}.should_not raise_error
    
    @d.first[:name].should == ':\\'
  end
end

context "A MySQL dataset in array tuples mode" do
  setup do
    @d = MYSQL_DB[:items]
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
