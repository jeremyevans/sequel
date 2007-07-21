require File.join(File.dirname(__FILE__), '../../lib/sequel/mysql')

MYSQL_DB = Sequel('mysql://root@localhost/sandbox')
if MYSQL_DB.table_exists?(:items)
  MYSQL_DB.drop_table :items
end
MYSQL_DB.create_table :items do
  text :name
  integer :value
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
  
  # specify "should return the last inserted id when inserting records" do
  #   id = @d << {:name => 'abc', :value => 1.23}
  #   id.should == @d.first[:id]
  # end
  #
  
  specify "should return all records" do
    @d << {:name => 'abc', :value => 123}
    @d << {:name => 'abc', :value => 456}
    @d << {:name => 'def', :value => 789}
    
    @d.order(:value).all.should == [
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
  
  specify "should quote fields using back-ticks" do
    @d.select(:name).sql.should == \
      'SELECT `name` FROM items'
      
    @d.select('COUNT(*)').sql.should == \
      'SELECT COUNT(*) FROM items'

    @d.select(:value.MAX).sql.should == \
      'SELECT max(`value`) FROM items'

    @d.select(:items__value.MAX).sql.should == \
      'SELECT max(items.`value`) FROM items'

    @d.order(:name.DESC).sql.should == \
      'SELECT * FROM items ORDER BY `name` DESC'
      
    @d.select('items.name AS item_name').sql.should == \
      'SELECT items.`name` AS `item_name` FROM items'
      
    @d.select('`name`').sql.should == \
      'SELECT `name` FROM items'

    @d.select('max(items.`name`) as `max_name`').sql.should == \
      'SELECT max(items.`name`) AS `max_name` FROM items'

    @d.insert_sql(:value => 333).should == \
      'INSERT INTO items (`value`) VALUES (333)'
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
end