require File.join(File.dirname(__FILE__), 'spec_helper.rb')

describe "Simple Dataset operations" do
  before do
    INTEGRATION_DB.create_table!(:items) do
      primary_key :id
      Integer :number
    end
    @ds = INTEGRATION_DB[:items]
    @ds.insert(:number=>10)
    clear_sqls
  end
  after do
    INTEGRATION_DB.drop_table(:items)
  end

  specify "should support sequential primary keys" do
    @ds << {:number=>20}
    @ds << {:number=>30}
    @ds.order(:number).all.should == [
      {:id => 1, :number=>10},
      {:id => 2, :number=>20},
      {:id => 3, :number=>30} ]   
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
  
  specify "should fetch correctly with a limit" do
    @ds.order(:id).limit(2).all.should == [{:id=>1, :number=>10}]
    @ds.insert(:number=>20)
    @ds.order(:id).limit(1).all.should == [{:id=>1, :number=>10}]
    @ds.order(:id).limit(2).all.should == [{:id=>1, :number=>10}, {:id=>2, :number=>20}]
  end
  
  specify "should fetch correctly with a limit and offset" do
    @ds.order(:id).limit(2, 0).all.should == [{:id=>1, :number=>10}]
    @ds.order(:id).limit(2, 1).all.should == []
    @ds.insert(:number=>20)
    @ds.order(:id).limit(1, 1).all.should == [{:id=>2, :number=>20}]
    @ds.order(:id).limit(2, 0).all.should == [{:id=>1, :number=>10}, {:id=>2, :number=>20}]
    @ds.order(:id).limit(2, 1).all.should == [{:id=>2, :number=>20}]
  end
  
  specify "should fetch correctly with a limit and offset without an order" do
    @ds.limit(2, 1).all.should == []
  end

  specify "should alias columns correctly" do
    @ds.select(:id___x, :number___n).first.should == {:x=>1, :n=>10}
    sqls_should_be("SELECT id AS 'x', number AS 'n' FROM items LIMIT 1")
  end
end

describe Sequel::Dataset do
  before do
    INTEGRATION_DB.create_table!(:test) do
      String :name
      Integer :value
    end
    @d = INTEGRATION_DB[:test]
    clear_sqls
  end
  after do
    INTEGRATION_DB.drop_table(:test)
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
end

describe Sequel::Database do
  specify "should correctly escape strings" do
    INTEGRATION_DB.get("\\dingo".as(:a)) == "\\dingo"
  end

  specify "should correctly escape strings with quotes" do
    INTEGRATION_DB.get("\\'dingo".as(:a)) == "\\'dingo"
  end

  specify "should properly escape binary data" do
    INTEGRATION_DB.get("\1\2\3".to_sequel_blob.as(:a)) == "\1\2\3"
  end

  specify "should have a working table_exists?" do
    t = :basdfdsafsaddsaf
    INTEGRATION_DB.drop_table(t) rescue nil
    INTEGRATION_DB.table_exists?(t).should == false
    INTEGRATION_DB.create_table(t){Integer :a}
    INTEGRATION_DB.table_exists?(t).should == true
  end
end

context Sequel::Dataset do
  before do
    INTEGRATION_DB.create_table! :items do
      primary_key :id 
      Integer :value
    end 
    @d = INTEGRATION_DB[:items]
    @d << {:value => 123}
    @d << {:value => 456}
    @d << {:value => 789}
  end 
  after do
    INTEGRATION_DB.drop_table(:items)
  end 
  
  specify "should correctly return avg" do
    @d.avg(:value).to_i.should == 456
  end 
  
  specify "should correctly return sum" do
    @d.sum(:value).to_i.should == 1368
  end 
  
  specify "should correctly return max" do
    @d.max(:value).to_i.should == 789 
  end 
  
  specify "should correctly return min" do
    @d.min(:value).to_i.should == 123 
  end 
end

describe "Simple Dataset operations" do
  before do
    INTEGRATION_DB.create_table!(:items) do
      Integer :number
      TrueClass :flag
    end
    @ds = INTEGRATION_DB[:items]
  end
  after do
    INTEGRATION_DB.drop_table(:items)
  end

  specify "should deal with boolean conditions correctly" do
    @ds.insert(:number=>1, :flag=>true)
    @ds.insert(:number=>2, :flag=>false)
    @ds.insert(:number=>3, :flag=>nil)
    @ds.order!(:number)
    @ds.filter(:flag=>true).map(:number).should == [1]
    @ds.filter(:flag=>false).map(:number).should == [2]
    @ds.filter(:flag=>nil).map(:number).should == [3]
    @ds.exclude(:flag=>true).map(:number).should == [2, 3]
    @ds.exclude(:flag=>false).map(:number).should == [1, 3]
    @ds.exclude(:flag=>nil).map(:number).should == [1, 2]
  end
end

describe "Simple Dataset operations in transactions" do
  before do
    INTEGRATION_DB.create_table!(:items_insert_in_transaction) do
      primary_key :id
      integer :number
    end
    @ds = INTEGRATION_DB[:items_insert_in_transaction]
    clear_sqls
  end
  after do
    INTEGRATION_DB.drop_table(:items_insert_in_transaction)
  end

  specify "should insert correctly with a primary key specified inside a transaction" do
    INTEGRATION_DB.transaction do
      @ds.insert(:id=>100, :number=>20)
      sqls_should_be('BEGIN', /INSERT INTO items_insert_in_transaction \((number, id|id, number)\) VALUES \((100, 20|20, 100)\)/)
      @ds.count.should == 1
      @ds.order(:id).all.should == [{:id=>100, :number=>20}]
    end
  end
  
  specify "should have insert return primary key value inside a transaction" do
    INTEGRATION_DB.transaction do
      @ds.insert(:number=>20).should == 1
      sqls_should_be('BEGIN', /INSERT INTO items_insert_in_transaction \(number\) VALUES \(20\)/)
      @ds.count.should == 1
      @ds.order(:id).all.should == [{:id=>1, :number=>20}]
    end
  end
end

describe "Dataset UNION, EXCEPT, and INTERSECT" do
  before do
    INTEGRATION_DB.create_table!(:i1){integer :number}
    INTEGRATION_DB.create_table!(:i2){integer :number}
    @ds1 = INTEGRATION_DB[:i1]
    @ds1.insert(:number=>10)
    @ds1.insert(:number=>20)
    @ds2 = INTEGRATION_DB[:i2]
    @ds2.insert(:number=>10)
    @ds2.insert(:number=>30)
    clear_sqls
  end
  
  specify "should give the correct results for simple UNION, EXCEPT, and INTERSECT" do
    @ds1.union(@ds2).order(:number).map{|x| x[:number].to_s}.should == %w'10 20 30'
    if @ds1.supports_intersect_except?
      @ds1.except(@ds2).order(:number).map{|x| x[:number].to_s}.should == %w'20'
      @ds1.intersect(@ds2).order(:number).map{|x| x[:number].to_s}.should == %w'10'
    end
  end
  
  specify "should give the correct results for UNION, EXCEPT, and INTERSECT when used with ordering and limits" do
    @ds1.insert(:number=>8)
    @ds2.insert(:number=>9)
    @ds1.insert(:number=>38)
    @ds2.insert(:number=>39)

    @ds1.order(:number.desc).union(@ds2).order(:number).map{|x| x[:number].to_s}.should == %w'8 9 10 20 30 38 39'
    @ds1.union(@ds2.order(:number.desc)).order(:number).map{|x| x[:number].to_s}.should == %w'8 9 10 20 30 38 39'

    @ds1.order(:number.desc).limit(1).union(@ds2).order(:number).map{|x| x[:number].to_s}.should == %w'9 10 30 38 39'
    @ds2.order(:number.desc).limit(1).union(@ds1).order(:number).map{|x| x[:number].to_s}.should == %w'8 10 20 38 39'

    @ds1.union(@ds2.order(:number).limit(1)).order(:number).map{|x| x[:number].to_s}.should == %w'8 9 10 20 38'
    @ds2.union(@ds1.order(:number).limit(1)).order(:number).map{|x| x[:number].to_s}.should == %w'8 9 10 30 39'

    @ds1.union(@ds2).limit(2).order(:number).map{|x| x[:number].to_s}.should == %w'8 9'
    @ds2.union(@ds1).order(:number.desc).limit(2).map{|x| x[:number].to_s}.should == %w'39 38'

    @ds1.order(:number.desc).limit(2).union(@ds2.order(:number.desc).limit(2)).order(:number).limit(3).map{|x| x[:number].to_s}.should == %w'20 30 38'
    @ds2.order(:number).limit(2).union(@ds1.order(:number).limit(2)).order(:number.desc).limit(3).map{|x| x[:number].to_s}.should == %w'10 9 8'
  end

  specify "should give the correct results for compound UNION, EXCEPT, and INTERSECT" do
    INTEGRATION_DB.create_table!(:i3){integer :number}
    @ds3 = INTEGRATION_DB[:i3]
    @ds3.insert(:number=>10)
    @ds3.insert(:number=>40)

    @ds1.union(@ds2).union(@ds3).order(:number).map{|x| x[:number].to_s}.should == %w'10 20 30 40'
    @ds1.union(@ds2.union(@ds3)).order(:number).map{|x| x[:number].to_s}.should == %w'10 20 30 40'
    if @ds1.supports_intersect_except?
      @ds1.union(@ds2).except(@ds3).order(:number).map{|x| x[:number].to_s}.should == %w'20 30'
      @ds1.union(@ds2.except(@ds3)).order(:number).map{|x| x[:number].to_s}.should == %w'10 20 30'
      @ds1.union(@ds2).intersect(@ds3).order(:number).map{|x| x[:number].to_s}.should == %w'10 '
      @ds1.union(@ds2.intersect(@ds3)).order(:number).map{|x| x[:number].to_s}.should == %w'10 20'
      
      @ds1.except(@ds2).union(@ds3).order(:number).map{|x| x[:number].to_s}.should == %w'10 20 40'
      @ds1.except(@ds2.union(@ds3)).order(:number).map{|x| x[:number].to_s}.should == %w'20'
      @ds1.except(@ds2).except(@ds3).order(:number).map{|x| x[:number].to_s}.should == %w'20'
      @ds1.except(@ds2.except(@ds3)).order(:number).map{|x| x[:number].to_s}.should == %w'10 20'
      @ds1.except(@ds2).intersect(@ds3).order(:number).map{|x| x[:number].to_s}.should == %w''
      @ds1.except(@ds2.intersect(@ds3)).order(:number).map{|x| x[:number].to_s}.should == %w'20'
      
      @ds1.intersect(@ds2).union(@ds3).order(:number).map{|x| x[:number].to_s}.should == %w'10 40'
      @ds1.intersect(@ds2.union(@ds3)).order(:number).map{|x| x[:number].to_s}.should == %w'10'
      @ds1.intersect(@ds2).except(@ds3).order(:number).map{|x| x[:number].to_s}.should == %w''
      @ds1.intersect(@ds2.except(@ds3)).order(:number).map{|x| x[:number].to_s}.should == %w''
      @ds1.intersect(@ds2).intersect(@ds3).order(:number).map{|x| x[:number].to_s}.should == %w'10'
      @ds1.intersect(@ds2.intersect(@ds3)).order(:number).map{|x| x[:number].to_s}.should == %w'10'
    end
  end
end

if INTEGRATION_DB.dataset.supports_cte?
  describe "Common Table Expressions" do
    before do
      @db = INTEGRATION_DB
      @db.create_table!(:i1){Integer :id; Integer :parent_id}
      @ds = @db[:i1]
      @ds.insert(:id=>1)
      @ds.insert(:id=>2)
      @ds.insert(:id=>3, :parent_id=>1)
      @ds.insert(:id=>4, :parent_id=>1)
      @ds.insert(:id=>5, :parent_id=>3)
      @ds.insert(:id=>6, :parent_id=>5)
    end
    after do
      @db.drop_table(:i1)
    end
    
    specify "should give correct results for WITH" do
      @db[:t].with(:t, @ds.filter(:parent_id=>nil).select(:id)).order(:id).map(:id).should == [1, 2]
    end
    
    specify "should give correct results for recursive WITH" do
      ds = @db[:t].select(:i___id, :pi___parent_id).with_recursive(:t, @ds.filter(:parent_id=>nil), @ds.join(:t, :i=>:parent_id).select(:i1__id, :i1__parent_id), :args=>[:i, :pi])
      ds.all.should == [{:parent_id=>nil, :id=>1}, {:parent_id=>nil, :id=>2}, {:parent_id=>1, :id=>3}, {:parent_id=>1, :id=>4}, {:parent_id=>3, :id=>5}, {:parent_id=>5, :id=>6}]
      ps = @db[:t].select(:i___id, :pi___parent_id).with_recursive(:t, @ds.filter(:parent_id=>:$n), @ds.join(:t, :i=>:parent_id).filter(:t__i=>:parent_id).select(:i1__id, :i1__parent_id), :args=>[:i, :pi]).prepare(:select, :cte_sel)
      ps.call(:n=>1).should == [{:id=>3, :parent_id=>1}, {:id=>4, :parent_id=>1}, {:id=>5, :parent_id=>3}, {:id=>6, :parent_id=>5}]
      ps.call(:n=>3).should == [{:id=>5, :parent_id=>3}, {:id=>6, :parent_id=>5}]
      ps.call(:n=>5).should == [{:id=>6, :parent_id=>5}]
    end
  end
end

if INTEGRATION_DB.dataset.supports_window_functions?
  describe "Window Functions" do
    before do
      @db = INTEGRATION_DB
      @db.create_table!(:i1){Integer :id; Integer :group_id; Integer :amount}
      @ds = @db[:i1].order(:id)
      @ds.insert(:id=>1, :group_id=>1, :amount=>1)
      @ds.insert(:id=>2, :group_id=>1, :amount=>10)
      @ds.insert(:id=>3, :group_id=>1, :amount=>100)
      @ds.insert(:id=>4, :group_id=>2, :amount=>1000)
      @ds.insert(:id=>5, :group_id=>2, :amount=>10000)
      @ds.insert(:id=>6, :group_id=>2, :amount=>100000)
    end
    after do
      @db.drop_table(:i1)
    end
    
    specify "should give correct results for aggregate window functions" do
      @ds.select(:id){sum(:over, :args=>amount, :partition=>group_id){}.as(:sum)}.all.should ==
        [{:sum=>111, :id=>1}, {:sum=>111, :id=>2}, {:sum=>111, :id=>3}, {:sum=>111000, :id=>4}, {:sum=>111000, :id=>5}, {:sum=>111000, :id=>6}]
      @ds.select(:id){sum(:over, :args=>amount){}.as(:sum)}.all.should ==
        [{:sum=>111111, :id=>1}, {:sum=>111111, :id=>2}, {:sum=>111111, :id=>3}, {:sum=>111111, :id=>4}, {:sum=>111111, :id=>5}, {:sum=>111111, :id=>6}]
    end
      
    specify "should give correct results for ranking window functions with orders" do
      @ds.select(:id){rank(:over, :partition=>group_id, :order=>id){}.as(:rank)}.all.should ==
        [{:rank=>1, :id=>1}, {:rank=>2, :id=>2}, {:rank=>3, :id=>3}, {:rank=>1, :id=>4}, {:rank=>2, :id=>5}, {:rank=>3, :id=>6}]
      @ds.select(:id){rank(:over, :order=>id){}.as(:rank)}.all.should ==
        [{:rank=>1, :id=>1}, {:rank=>2, :id=>2}, {:rank=>3, :id=>3}, {:rank=>4, :id=>4}, {:rank=>5, :id=>5}, {:rank=>6, :id=>6}]
    end
      
    specify "should give correct results for aggregate window functions with orders" do
      @ds.select(:id){sum(:over, :args=>amount, :partition=>group_id, :order=>id){}.as(:sum)}.all.should ==
        [{:sum=>1, :id=>1}, {:sum=>11, :id=>2}, {:sum=>111, :id=>3}, {:sum=>1000, :id=>4}, {:sum=>11000, :id=>5}, {:sum=>111000, :id=>6}]
      @ds.select(:id){sum(:over, :args=>amount, :order=>id){}.as(:sum)}.all.should ==
        [{:sum=>1, :id=>1}, {:sum=>11, :id=>2}, {:sum=>111, :id=>3}, {:sum=>1111, :id=>4}, {:sum=>11111, :id=>5}, {:sum=>111111, :id=>6}]
    end
    
    specify "should give correct results for aggregate window functions with frames" do
      @ds.select(:id){sum(:over, :args=>amount, :partition=>group_id, :order=>id, :frame=>:all){}.as(:sum)}.all.should ==
        [{:sum=>111, :id=>1}, {:sum=>111, :id=>2}, {:sum=>111, :id=>3}, {:sum=>111000, :id=>4}, {:sum=>111000, :id=>5}, {:sum=>111000, :id=>6}]
      @ds.select(:id){sum(:over, :args=>amount, :partition=>group_id, :frame=>:all){}.as(:sum)}.all.should ==
        [{:sum=>111, :id=>1}, {:sum=>111, :id=>2}, {:sum=>111, :id=>3}, {:sum=>111000, :id=>4}, {:sum=>111000, :id=>5}, {:sum=>111000, :id=>6}]
      @ds.select(:id){sum(:over, :args=>amount, :order=>id, :frame=>:all){}.as(:sum)}.all.should ==
        [{:sum=>111111, :id=>1}, {:sum=>111111, :id=>2}, {:sum=>111111, :id=>3}, {:sum=>111111, :id=>4}, {:sum=>111111, :id=>5}, {:sum=>111111, :id=>6}]
      @ds.select(:id){sum(:over, :args=>amount, :frame=>:all){}.as(:sum)}.all.should ==
        [{:sum=>111111, :id=>1}, {:sum=>111111, :id=>2}, {:sum=>111111, :id=>3}, {:sum=>111111, :id=>4}, {:sum=>111111, :id=>5}, {:sum=>111111, :id=>6}]
        
      @ds.select(:id){sum(:over, :args=>amount, :partition=>group_id, :order=>id, :frame=>:rows){}.as(:sum)}.all.should ==
        [{:sum=>1, :id=>1}, {:sum=>11, :id=>2}, {:sum=>111, :id=>3}, {:sum=>1000, :id=>4}, {:sum=>11000, :id=>5}, {:sum=>111000, :id=>6}]
      @ds.select(:id){sum(:over, :args=>amount, :partition=>group_id, :frame=>:rows){}.as(:sum)}.all.should ==
        [{:sum=>1, :id=>1}, {:sum=>11, :id=>2}, {:sum=>111, :id=>3}, {:sum=>1000, :id=>4}, {:sum=>11000, :id=>5}, {:sum=>111000, :id=>6}]
      @ds.select(:id){sum(:over, :args=>amount, :order=>id, :frame=>:rows){}.as(:sum)}.all.should ==
        [{:sum=>1, :id=>1}, {:sum=>11, :id=>2}, {:sum=>111, :id=>3}, {:sum=>1111, :id=>4}, {:sum=>11111, :id=>5}, {:sum=>111111, :id=>6}]
      @ds.select(:id){sum(:over, :args=>amount, :frame=>:rows){}.as(:sum)}.all.should ==
        [{:sum=>1, :id=>1}, {:sum=>11, :id=>2}, {:sum=>111, :id=>3}, {:sum=>1111, :id=>4}, {:sum=>11111, :id=>5}, {:sum=>111111, :id=>6}]
    end
  end
end

describe Sequel::SQL::Constants do
  before do
    @db = INTEGRATION_DB
    @ds = @db[:constants]
    @c = proc do |v|
      case v
      when Time
        v
      when DateTime, String
        Time.parse(v.to_s)
      else
        v
      end
    end
    @c2 = proc{|v| v.is_a?(Date) ? v : Date.parse(v) }
  end
  after do
    @db.drop_table(:constants)
  end
  
  it "should have working CURRENT_DATE" do
    @db.create_table!(:constants){Date :d}
    @ds.insert(:d=>Sequel::CURRENT_DATE)
    Date.today.should == @c2[@ds.get(:d)]
  end

  it "should have working CURRENT_TIME" do
    @db.create_table!(:constants){Time :t, :only_time=>true}
    @ds.insert(:t=>Sequel::CURRENT_TIME)
    (Time.now - @c[@ds.get(:t)]).should be_close(0, 1)
  end

  it "should have working CURRENT_TIMESTAMP" do
    @db.create_table!(:constants){DateTime :ts}
    @ds.insert(:ts=>Sequel::CURRENT_TIMESTAMP)
    (Time.now - @c[@ds.get(:ts)]).should be_close(0, 1)
  end
end

describe "Sequel::Dataset#import and #multi_insert" do
  before do
    @db = INTEGRATION_DB
    @db.create_table!(:imp){Integer :i}
    @db.create_table!(:exp){Integer :i}
    @ids = @db[:imp].order(:i)
    @eds = @db[:exp]
  end
  after do
    @db.drop_table(:imp, :exp)
  end

  it "should import with multi_insert and an array of hashes" do
    @ids.multi_insert([{:i=>10}, {:i=>20}])
    @ids.all.should == [{:i=>10}, {:i=>20}]
  end

  it "should import with an array of arrays of values" do
    @ids.import([:i], [[10], [20]])
    @ids.all.should == [{:i=>10}, {:i=>20}]
  end

  it "should import with a dataset" do
    @eds.import([:i], [[10], [20]])
    @ids.import([:i], @eds)
    @ids.all.should == [{:i=>10}, {:i=>20}]
  end
end
