require File.join(File.dirname(__FILE__), "../spec_helper")

Sequel::Model.db = MODEL_DB = MockDatabase.new

describe Sequel::Model, "caching" do

  before(:each) do
    MODEL_DB.reset
    
    @cache_class = Class.new(Hash) do
      attr_accessor :ttl
      def set(k, v, ttl); self[k] = v; @ttl = ttl; end
      def get(k); self[k]; end
    end
    cache = @cache_class.new
    @cache = cache
    
    @c = Class.new(Sequel::Model(:items)) do
      set_cache cache
      
      def self.columns
        [:name, :id]
      end
    end
    
    $cache_dataset_row = {:name => 'sharon', :id => 1}
    @dataset = @c.dataset
    $sqls = []
    @dataset.extend(Module.new {
      def fetch_rows(sql)
        $sqls << sql
        yield $cache_dataset_row
      end
      
      def update(values)
        $sqls << update_sql(values)
        $cache_dataset_row.merge!(values)
      end
      
      def delete
        $sqls << delete_sql
      end
    })
  end
  
  it "should set the model's cache store" do
    @c.cache_store.should be(@cache)
  end
  
  it "should have a default ttl of 3600" do
    @c.cache_ttl.should == 3600
  end
  
  it "should take a ttl option" do
    @c.set_cache @cache, :ttl => 1234
    @c.cache_ttl.should == 1234
  end
  
  it "should offer a set_cache_ttl method for setting the ttl" do
    @c.cache_ttl.should == 3600
    @c.set_cache_ttl 1234
    @c.cache_ttl.should == 1234
  end
  
  it "should generate a cache key appropriate to the class" do
    m = @c.new
    m.values[:id] = 1
    m.cache_key.should == "#{m.class}:1"
    
    # custom primary key
    @c.set_primary_key :ttt
    m = @c.new
    m.values[:ttt] = 333
    m.cache_key.should == "#{m.class}:333"
    
    # composite primary key
    @c.set_primary_key [:a, :b, :c]
    m = @c.new
    m.values[:a] = 123
    m.values[:c] = 456
    m.values[:b] = 789
    m.cache_key.should == "#{m.class}:123,789,456"
  end
  
  it "should raise error if attempting to generate cache_key and primary key value is null" do
    m = @c.new
    proc {m.cache_key}.should raise_error(Sequel::Error)
    
    m.values[:id] = 1
    proc {m.cache_key}.should_not raise_error(Sequel::Error)
  end
  
  it "should set the cache when reading from the database" do
    $sqls.should == []
    @cache.should be_empty
    
    m = @c[1]
    $sqls.should == ['SELECT * FROM items WHERE (id = 1) LIMIT 1']
    m.values.should == $cache_dataset_row
    @cache[m.cache_key].should == m
    
    # read from cache
    m2 = @c[1]
    $sqls.should == ['SELECT * FROM items WHERE (id = 1) LIMIT 1']
    m2.should == m
    m2.values.should == $cache_dataset_row
  end
  
  it "should delete the cache when writing to the database" do
    # fill the cache
    m = @c[1]
    @cache[m.cache_key].should == m
    
    m.set(:name => 'tutu')
    @cache.has_key?(m.cache_key).should be_false
    $sqls.last.should == "UPDATE items SET name = 'tutu' WHERE (id = 1)"
    
    m = @c[1]
    @cache[m.cache_key].should == m
    m.name = 'hey'
    m.save
    @cache.has_key?(m.cache_key).should be_false
    $sqls.last.should == "UPDATE items SET name = 'hey', id = 1 WHERE (id = 1)"
  end
  
  it "should delete the cache when deleting the record" do
    # fill the cache
    m = @c[1]
    @cache[m.cache_key].should == m
    
    m.delete
    @cache.has_key?(m.cache_key).should be_false
    $sqls.last.should == "DELETE FROM items WHERE (id = 1)"
  end
  
  it "should support #[] as a shortcut to #find with hash" do
    m = @c[:id => 3]
    @cache[m.cache_key].should be_nil
    $sqls.last.should == "SELECT * FROM items WHERE (id = 3) LIMIT 1"
    
    m = @c[1]
    @cache[m.cache_key].should == m
    $sqls.should == ["SELECT * FROM items WHERE (id = 3) LIMIT 1", \
      "SELECT * FROM items WHERE (id = 1) LIMIT 1"]
    
    @c[:id => 4]
    $sqls.should == ["SELECT * FROM items WHERE (id = 3) LIMIT 1", \
      "SELECT * FROM items WHERE (id = 1) LIMIT 1", \
      "SELECT * FROM items WHERE (id = 4) LIMIT 1"]
  end
  
end
