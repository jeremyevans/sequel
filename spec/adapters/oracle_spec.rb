require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')
require "timeout"

unless defined?(ORACLE_DB)
  ORACLE_DB = Sequel.connect('oracle://hr:hr@localhost/XE')
end
INTEGRATION_DB = ORACLE_DB unless defined?(INTEGRATION_DB)

if ORACLE_DB.table_exists?(:items)
  ORACLE_DB.drop_table :items
end
ORACLE_DB.create_table :items do
  varchar2 :name, :size => 50
  number :value, :size => 38
  date :date_created
  index :value
end

if ORACLE_DB.table_exists?(:books)
  ORACLE_DB.drop_table :books
end
ORACLE_DB.create_table :books do
  number :id, :size => 38
  varchar2 :title, :size => 50
  number :category_id, :size => 38
end

if ORACLE_DB.table_exists?(:categories)
  ORACLE_DB.drop_table :categories
end
ORACLE_DB.create_table :categories do
  number :id, :size => 38
  varchar2 :cat_name, :size => 50
end

describe "An Oracle database" do
  specify "should provide disconnect functionality" do
    ORACLE_DB.execute("select user from dual")
    ORACLE_DB.pool.size.should == 1
    ORACLE_DB.disconnect
    ORACLE_DB.pool.size.should == 0
  end
  
  specify "should provide schema information" do
    books_schema = [
      [:id, {:char_size=>0, :type=>:number, :allow_null=>true, :type_string=>"NUMBER(38)", :data_size=>22, :precision=>38, :char_used=>false, :scale=>0, :charset_form=>nil, :fsprecision=>38, :lfprecision=>0, :db_type=>"NUMBER(38)", :ruby_default => nil}], 
      [:title, {:char_size=>50, :type=>:varchar2, :allow_null=>true, :type_string=>"VARCHAR2(50)", :data_size=>50, :precision=>0, :char_used=>false, :scale=>0, :charset_form=>:implicit, :fsprecision=>0, :lfprecision=>0, :db_type=>"VARCHAR2(50)", :ruby_default => nil}], 
      [:category_id, {:char_size=>0, :type=>:number, :allow_null=>true, :type_string=>"NUMBER(38)", :data_size=>22, :precision=>38, :char_used=>false, :scale=>0, :charset_form=>nil, :fsprecision=>38, :lfprecision=>0, :db_type=>"NUMBER(38)", :ruby_default => nil}]]
    categories_schema = [
      [:id, {:char_size=>0, :type=>:number, :allow_null=>true, :type_string=>"NUMBER(38)", :data_size=>22, :precision=>38, :char_used=>false, :scale=>0, :charset_form=>nil, :fsprecision=>38, :lfprecision=>0, :db_type=>"NUMBER(38)", :ruby_default => nil}], 
      [:cat_name, {:char_size=>50, :type=>:varchar2, :allow_null=>true, :type_string=>"VARCHAR2(50)", :data_size=>50, :precision=>0, :char_used=>false, :scale=>0, :charset_form=>:implicit, :fsprecision=>0, :lfprecision=>0, :db_type=>"VARCHAR2(50)", :ruby_default => nil}]]
    items_schema = [
      [:name, {:char_size=>50, :type=>:varchar2, :allow_null=>true, :type_string=>"VARCHAR2(50)", :data_size=>50, :precision=>0, :char_used=>false, :scale=>0, :charset_form=>:implicit, :fsprecision=>0, :lfprecision=>0, :db_type=>"VARCHAR2(50)", :ruby_default => nil}], 
      [:value, {:char_size=>0, :type=>:number, :allow_null=>true, :type_string=>"NUMBER(38)", :data_size=>22, :precision=>38, :char_used=>false, :scale=>0, :charset_form=>nil, :fsprecision=>38, :lfprecision=>0, :db_type=>"NUMBER(38)", :ruby_default => nil}],
      [:date_created, {:charset_form=>nil, :type=>:date, :type_string=>"DATE", :fsprecision=>0, :data_size=>7, :lfprecision=>0, :precision=>0, :db_type=>"DATE", :char_used=>false, :char_size=>0, :scale=>0, :allow_null=>true, :ruby_default => nil}]]
     
    {:books => books_schema, :categories => categories_schema, :items => items_schema}.each_pair do |table, expected_schema|
      schema = ORACLE_DB.schema(table)
      schema.should_not be_nil
      schema.should == expected_schema
    end
  end
  
  specify "should create a temporary table" do
    ORACLE_DB.create_table! :test_tmp, :temp => true do
      varchar2 :name, :size => 50
      primary_key :id, :integer, :null => false
      index :name, :unique => true
    end
  end
end

describe "An Oracle dataset" do
  before do
    @d = ORACLE_DB[:items]
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
      {:date_created=>nil, :name => 'abc', :value => 123},
      {:date_created=>nil, :name => 'abc', :value => 456},
      {:date_created=>nil, :name => 'def', :value => 789}
    ]

    @d.select(:name).distinct.order_by(:name).to_a.should == [
      {:name => 'abc'},
      {:name => 'def'}
    ]
           
    @d.order(:value.desc).limit(1).to_a.should == [
      {:date_created=>nil, :name => 'def', :value => 789}                                        
    ]

    @d.filter(:name => 'abc').to_a.should == [
      {:date_created=>nil, :name => 'abc', :value => 123},
      {:date_created=>nil, :name => 'abc', :value => 456} 
    ]
    
    @d.order(:value.desc).filter(:name => 'abc').to_a.should == [
      {:date_created=>nil, :name => 'abc', :value => 456},
      {:date_created=>nil, :name => 'abc', :value => 123} 
    ]

    @d.filter(:name => 'abc').limit(1).to_a.should == [
      {:date_created=>nil, :name => 'abc', :value => 123}                                        
    ]
        
    @d.filter(:name => 'abc').order(:value.desc).limit(1).to_a.should == [
      {:date_created=>nil, :name => 'abc', :value => 456}                                        
    ]
    
    @d.filter(:name => 'abc').order(:value).limit(1).to_a.should == [
      {:date_created=>nil, :name => 'abc', :value => 123}                                        
    ]
        
    @d.order(:value).limit(1).to_a.should == [
      {:date_created=>nil, :name => 'abc', :value => 123}                                        
    ]

    @d.order(:value).limit(1, 1).to_a.should == [
      {:date_created=>nil, :name => 'abc', :value => 456}
    ]

    @d.order(:value).limit(1, 2).to_a.should == [
      {:date_created=>nil, :name => 'def', :value => 789}
    ]    
    
    @d.avg(:value).to_i.should == (789+123+456)/3
    
    @d.max(:value).to_i.should == 789
    
    @d.select(:name, :AVG.sql_function(:value)).filter(:name => 'abc').group(:name).to_a.should == [
      {:name => 'abc', :"avg(value)" => (456+123)/2.0}
    ]

    @d.select(:AVG.sql_function(:value)).group(:name).order(:name).limit(1).to_a.should == [
      {:"avg(value)" => (456+123)/2.0}
    ]
        
    @d.select(:name, :AVG.sql_function(:value)).group(:name).order(:name).to_a.should == [
      {:name => 'abc', :"avg(value)" => (456+123)/2.0},
      {:name => 'def', :"avg(value)" => 789*1.0}
    ]
    
    @d.select(:name, :AVG.sql_function(:value)).group(:name).order(:name).to_a.should == [
      {:name => 'abc', :"avg(value)" => (456+123)/2.0},
      {:name => 'def', :"avg(value)" => 789*1.0}
    ]

    @d.select(:name, :AVG.sql_function(:value)).group(:name).having(:name => ['abc', 'def']).order(:name).to_a.should == [
      {:name => 'abc', :"avg(value)" => (456+123)/2.0},
      {:name => 'def', :"avg(value)" => 789*1.0}
    ]
    
    @d.select(:name, :value).filter(:name => 'abc').union(@d.select(:name, :value).filter(:name => 'def')).order(:value).to_a.should == [
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

  specify "should translate values correctly" do
    @d << {:name => 'abc', :value => 456}
    @d << {:name => 'def', :value => 789}
    @d.filter(:value > 500).update(:date_created => "to_timestamp('2009-09-09', 'YYYY-MM-DD')".lit)
    
    @d[:name => 'def'][:date_created].should == Time.parse('2009-09-09')
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

describe "Joined Oracle dataset" do
  before do
    @d1 = ORACLE_DB[:books]
    @d1.delete # remove all records
    @d1 << {:id => 1, :title => 'aaa', :category_id => 100}
    @d1 << {:id => 2, :title => 'bbb', :category_id => 100}
    @d1 << {:id => 3, :title => 'ccc', :category_id => 101}
    @d1 << {:id => 4, :title => 'ddd', :category_id => 102}
    
    @d2 = ORACLE_DB[:categories]
    @d2.delete # remove all records
    @d2 << {:id => 100, :cat_name => 'ruby'}
    @d2 << {:id => 101, :cat_name => 'rails'}
  end
  
  specify "should return correct result" do
    @d1.join(:categories, :id => :category_id).select(:books__id, :title, :cat_name).order(:books__id).to_a.should == [
      {:id => 1, :title => 'aaa', :cat_name => 'ruby'},
      {:id => 2, :title => 'bbb', :cat_name => 'ruby'},
      {:id => 3, :title => 'ccc', :cat_name => 'rails'}
    ]

    @d1.join(:categories, :id => :category_id).select(:books__id, :title, :cat_name).order(:books__id).limit(2, 1).to_a.should == [
      {:id => 2, :title => 'bbb', :cat_name => 'ruby'},
      {:id => 3, :title => 'ccc', :cat_name => 'rails'},
    ]
   
    @d1.left_outer_join(:categories, :id => :category_id).select(:books__id, :title, :cat_name).order(:books__id).to_a.should == [
      {:id => 1, :title => 'aaa', :cat_name => 'ruby'},
      {:id => 2, :title => 'bbb', :cat_name => 'ruby'},
      {:id => 3, :title => 'ccc', :cat_name => 'rails'},
      {:id => 4, :title => 'ddd', :cat_name => nil} 
    ]
    
    @d1.left_outer_join(:categories, :id => :category_id).select(:books__id, :title, :cat_name).order(:books__id.desc).limit(2, 0).to_a.should == [      
      {:id => 4, :title => 'ddd', :cat_name => nil}, 
      {:id => 3, :title => 'ccc', :cat_name => 'rails'}
    ]      
  end  
end

describe "Oracle aliasing" do
  before do
    @d1 = ORACLE_DB[:books]
    @d1.delete # remove all records
    @d1 << {:id => 1, :title => 'aaa', :category_id => 100}
    @d1 << {:id => 2, :title => 'bbb', :category_id => 100}
    @d1 << {:id => 3, :title => 'bbb', :category_id => 100}
  end

  specify "should allow columns to be renamed" do
    @d1.select(:title.as(:name)).order_by(:id).to_a.should == [
      { :name => 'aaa' },
      { :name => 'bbb' },
      { :name => 'bbb' },
    ]
  end

  specify "nested queries should work" do
    @d1.select(:title).group_by(:title).count.should == 2
  end
end

describe "Row locks in Oracle" do
  before do
    @d1 = ORACLE_DB[:books]
    @d1.delete # remove all records
    @d1 << {:id => 1, :title => 'aaa'}
  end

  specify "#for_update should use FOR UPDATE" do
    @d1.for_update.sql.should == "SELECT * FROM BOOKS FOR UPDATE"
  end

  specify "#lock_style should accept symbols" do
    @d1.lock_style(:update).sql.should == "SELECT * FROM BOOKS FOR UPDATE"
  end

  specify "should not update during row lock" do
    ORACLE_DB.transaction do
      @d1.filter(:id => 1).for_update.to_a
      proc do
        t1 = Thread.start do
          # wait for unlock
          Timeout::timeout(0.02) do
            ORACLE_DB[:books].filter(:id => 1).update(:title => "bbb")
          end
        end
        t1.join
      end.should raise_error
      @d1.filter(:id => 1).first[:title].should == "aaa"
    end
    t2 = Thread.start { ORACLE_DB[:books].filter(:id => 1).update(:title => "bbb") }
    t2.join
    @d1.filter(:id => 1).first[:title].should == "bbb"
  end
end
