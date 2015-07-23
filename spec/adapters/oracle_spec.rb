SEQUEL_ADAPTER_TEST = :oracle

require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

describe "An Oracle database" do
  before(:all) do
    DB.create_table!(:items) do
      String :name, :size => 50
      Integer :value
      Date :date_created
      index :value
    end

    DB.create_table!(:books) do
      Integer :id
      String :title, :size => 50
      Integer :category_id
    end

    DB.create_table!(:categories) do
      Integer :id
      String :cat_name, :size => 50
    end

    DB.create_table!(:notes) do
      Integer :id
      String :title, :size => 50
      String :content, :text => true
    end
    @d = DB[:items]
  end
  after do
    @d.delete
  end
  after(:all) do
    DB.drop_table?(:items, :books, :categories, :notes)
  end

  it "should allow limit and offset with clob columns" do
    notes = []
    notes << {:id => 1, :title => 'abc', :content => 'zyx'}
    notes << {:id => 2, :title => 'def', :content => 'wvu'}
    notes << {:id => 3, :title => 'ghi', :content => 'tsr'}
    notes << {:id => 4, :title => 'jkl', :content => 'qpo'}
    notes << {:id => 5, :title => 'mno', :content => 'nml'}
    DB[:notes].multi_insert(notes)

    DB[:notes].sort_by{|x| x[:id]}.must_equal notes
    rows = DB[:notes].limit(3, 0).all
    rows.length.must_equal 3
    rows.all?{|v| notes.must_include(v)}
  end

  it "should provide disconnect functionality" do
    DB.execute("select user from dual")
    DB.pool.size.must_equal 1
    DB.disconnect
    DB.pool.size.must_equal 0
  end

  it "should have working view_exists?" do
    begin
      DB.view_exists?(:cats).must_equal false
      DB.create_view(:cats, DB[:categories])
      DB.view_exists?(:cats).must_equal true
      om = DB.identifier_output_method
      im = DB.identifier_input_method
      DB.identifier_output_method = :reverse
      DB.identifier_input_method = :reverse
      DB.view_exists?(:STAC).must_equal true
      DB.view_exists?(:cats).must_equal false
    ensure
      DB.identifier_output_method = om
      DB.identifier_input_method = im
      DB.drop_view(:cats)
    end
  end

  it "should be able to get current sequence value with SQL" do
    begin
      DB.create_table!(:foo){primary_key :id}
      DB.fetch('SELECT seq_foo_id.nextval FROM DUAL').single_value.must_equal 1
    ensure
      DB.drop_table(:foo)
    end
  end
  
  it "should provide schema information" do
    books_schema = [[:id, [:integer, false, true, nil]],
      [:title, [:string, false, true, nil]],
      [:category_id, [:integer, false, true, nil]]]
    categories_schema = [[:id, [:integer, false, true, nil]],
      [:cat_name, [:string, false, true, nil]]]
    items_schema = [[:name, [:string, false, true, nil]],
      [:value, [:integer, false, true, nil]],
      [:date_created, [:datetime, false, true, nil]]]
     
    {:books => books_schema, :categories => categories_schema, :items => items_schema}.each_pair do |table, expected_schema|
      schema = DB.schema(table)
      schema.wont_equal nil
      schema.map{|c, s| [c, s.values_at(:type, :primary_key, :allow_null, :ruby_default)]}.must_equal expected_schema
    end
  end
  
  it "should create a temporary table" do
    DB.create_table! :test_tmp, :temp => true do
      varchar2 :name, :size => 50
      primary_key :id, :integer, :null => false
      index :name, :unique => true
    end
    DB.drop_table?(:test_tmp)
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
      {:date_created=>nil, :name => 'abc', :value => 123},
      {:date_created=>nil, :name => 'abc', :value => 456},
      {:date_created=>nil, :name => 'def', :value => 789}
    ]

    @d.select(:name).distinct.order_by(:name).to_a.must_equal [
      {:name => 'abc'},
      {:name => 'def'}
    ]
           
    @d.order(Sequel.desc(:value)).limit(1).to_a.must_equal [
      {:date_created=>nil, :name => 'def', :value => 789}                                        
    ]

    @d.filter(:name => 'abc').order(:value).to_a.must_equal [
      {:date_created=>nil, :name => 'abc', :value => 123},
      {:date_created=>nil, :name => 'abc', :value => 456} 
    ]
    
    @d.order(Sequel.desc(:value)).filter(:name => 'abc').to_a.must_equal [
      {:date_created=>nil, :name => 'abc', :value => 456},
      {:date_created=>nil, :name => 'abc', :value => 123} 
    ]

    @d.filter(:name => 'abc').order(:value).limit(1).to_a.must_equal [
      {:date_created=>nil, :name => 'abc', :value => 123}                                        
    ]
        
    @d.filter(:name => 'abc').order(Sequel.desc(:value)).limit(1).to_a.must_equal [
      {:date_created=>nil, :name => 'abc', :value => 456}                                        
    ]
    
    @d.filter(:name => 'abc').order(:value).limit(1).to_a.must_equal [
      {:date_created=>nil, :name => 'abc', :value => 123}                                        
    ]
        
    @d.order(:value).limit(1).to_a.must_equal [
      {:date_created=>nil, :name => 'abc', :value => 123}                                        
    ]

    @d.order(:value).limit(1, 1).to_a.must_equal [
      {:date_created=>nil, :name => 'abc', :value => 456}
    ]

    @d.order(:value).limit(1, 2).to_a.must_equal [
      {:date_created=>nil, :name => 'def', :value => 789}
    ]    
    
    @d.avg(:value).to_i.must_equal((789+123+456)/3)
    
    @d.max(:value).to_i.must_equal 789
    
    @d.select(:name, Sequel.function(:AVG, :value).as(:avg)).filter(:name => 'abc').group(:name).to_a.must_equal [
      {:name => 'abc', :avg => (456+123)/2.0}
    ]

    @d.select(Sequel.function(:AVG, :value).as(:avg)).group(:name).order(:name).limit(1).to_a.must_equal [
      {:avg => (456+123)/2.0}
    ]
        
    @d.select(:name, Sequel.function(:AVG, :value).as(:avg)).group(:name).order(:name).to_a.must_equal [
      {:name => 'abc', :avg => (456+123)/2.0},
      {:name => 'def', :avg => 789*1.0}
    ]
    
    @d.select(:name, Sequel.function(:AVG, :value).as(:avg)).group(:name).order(:name).to_a.must_equal [
      {:name => 'abc', :avg => (456+123)/2.0},
      {:name => 'def', :avg => 789*1.0}
    ]

    @d.select(:name, Sequel.function(:AVG, :value).as(:avg)).group(:name).having(:name => ['abc', 'def']).order(:name).to_a.must_equal [
      {:name => 'abc', :avg => (456+123)/2.0},
      {:name => 'def', :avg => 789*1.0}
    ]
    
    @d.select(:name, :value).filter(:name => 'abc').union(@d.select(:name, :value).filter(:name => 'def')).order(:value).to_a.must_equal [
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
    
    @d[:name => 'def'][:value].must_equal 789
    @d.filter(:value => 530).count.must_equal 2
  end

  it "should translate values correctly" do
    @d << {:name => 'abc', :value => 456}
    @d << {:name => 'def', :value => 789}
    @d.filter('value > 500').update(:date_created => Sequel.lit("to_timestamp('2009-09-09', 'YYYY-MM-DD')"))
    
    @d[:name => 'def'][:date_created].strftime('%F').must_equal '2009-09-09'
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

  it "should return correct result" do
    @d1 = DB[:books]
    @d1.delete
    @d1 << {:id => 1, :title => 'aaa', :category_id => 100}
    @d1 << {:id => 2, :title => 'bbb', :category_id => 100}
    @d1 << {:id => 3, :title => 'ccc', :category_id => 101}
    @d1 << {:id => 4, :title => 'ddd', :category_id => 102}
    
    @d2 = DB[:categories]
    @d2.delete
    @d2 << {:id => 100, :cat_name => 'ruby'}
    @d2 << {:id => 101, :cat_name => 'rails'}
  
    @d1.join(:categories, :id => :category_id).select(:books__id, :title, :cat_name).order(:books__id).to_a.must_equal [
      {:id => 1, :title => 'aaa', :cat_name => 'ruby'},
      {:id => 2, :title => 'bbb', :cat_name => 'ruby'},
      {:id => 3, :title => 'ccc', :cat_name => 'rails'}
    ]

    @d1.join(:categories, :id => :category_id).select(:books__id, :title, :cat_name).order(:books__id).limit(2, 1).to_a.must_equal [
      {:id => 2, :title => 'bbb', :cat_name => 'ruby'},
      {:id => 3, :title => 'ccc', :cat_name => 'rails'},
    ]
   
    @d1.left_outer_join(:categories, :id => :category_id).select(:books__id, :title, :cat_name).order(:books__id).to_a.must_equal [
      {:id => 1, :title => 'aaa', :cat_name => 'ruby'},
      {:id => 2, :title => 'bbb', :cat_name => 'ruby'},
      {:id => 3, :title => 'ccc', :cat_name => 'rails'},
      {:id => 4, :title => 'ddd', :cat_name => nil} 
    ]
    
    @d1.left_outer_join(:categories, :id => :category_id).select(:books__id, :title, :cat_name).reverse_order(:books__id).limit(2, 0).to_a.must_equal [      
      {:id => 4, :title => 'ddd', :cat_name => nil}, 
      {:id => 3, :title => 'ccc', :cat_name => 'rails'}
    ]      
  end  

  it "should allow columns to be renamed" do
    @d1 = DB[:books]
    @d1.delete
    @d1 << {:id => 1, :title => 'aaa', :category_id => 100}
    @d1 << {:id => 2, :title => 'bbb', :category_id => 100}
    @d1 << {:id => 3, :title => 'bbb', :category_id => 100}

    @d1.select(Sequel.as(:title, :name)).order_by(:id).to_a.must_equal [
      { :name => 'aaa' },
      { :name => 'bbb' },
      { :name => 'bbb' },
    ]

    DB[:books].select(:title).group_by(:title).count.must_equal 2
  end

  it "#for_update should use FOR UPDATE" do
    DB[:books].for_update.sql.must_equal 'SELECT * FROM "BOOKS" FOR UPDATE'
  end

  it "#lock_style should accept symbols" do
    DB[:books].lock_style(:update).sql.must_equal 'SELECT * FROM "BOOKS" FOR UPDATE'
  end
end
