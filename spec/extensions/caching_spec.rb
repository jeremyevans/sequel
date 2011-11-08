require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model, "caching" do
  before do
    @cache_class = Class.new(Hash) do
      attr_accessor :ttl
      def set(k, v, ttl); self[k] = v; @ttl = ttl; end
      def get(k); self[k]; end
    end
    cache = @cache_class.new
    @cache = cache
    
    @memcached_class = Class.new(Hash) do
      attr_accessor :ttl
      def set(k, v, ttl); self[k] = v; @ttl = ttl; end
      def get(k); if self[k] then return self[k]; else raise ArgumentError; end end
    end
    cache2 = @memcached_class.new
    @memcached = cache2
    
    @c = Class.new(Sequel::Model(:items))
    @c.class_eval do
      plugin :caching, cache
      def self.name; 'Item' end
      
      columns :name, :id
    end
  
    @c3 = Class.new(Sequel::Model(:items))
    @c3.class_eval do
      plugin :caching, cache2
      def self.name; 'Item' end
      
      columns :name, :id
    end

    @c4 = Class.new(Sequel::Model(:items))
    @c4.class_eval do
      plugin :caching, cache2, :ignore_exceptions => true
      def self.name; 'Item' end
      
      columns :name, :id
    end
   
    @dataset = @c.dataset = @c3.dataset = @c4.dataset
    @dataset._fetch = {:name => 'sharon', :id => 1}
    @dataset.numrows = 1
    
    @c2 = Class.new(@c) do
      def self.name; 'SubItem' end
    end    
    @c.db.reset
  end
  
  it "should set the model's cache store" do
    @c.cache_store.should be(@cache)
    @c2.cache_store.should be(@cache)
  end
  
  it "should have a default ttl of 3600" do
    @c.cache_ttl.should == 3600
    @c2.cache_ttl.should == 3600
  end
  
  it "should take a ttl option" do
    c = Class.new(Sequel::Model(:items))
    c.plugin :caching, @cache, :ttl => 1234
    c.cache_ttl.should == 1234
    Class.new(c).cache_ttl.should == 1234
  end
  
  it "should allow overriding the ttl option via a plugin :caching call" do
    @c.plugin :caching, @cache, :ttl => 1234
    @c.cache_ttl.should == 1234
    Class.new(@c).cache_ttl.should == 1234
  end
  
  it "should offer a set_cache_ttl method for setting the ttl" do
    @c.cache_ttl.should == 3600
    @c.set_cache_ttl 1234
    @c.cache_ttl.should == 1234
    Class.new(@c).cache_ttl.should == 1234
  end
  
  it "should generate a cache key appropriate to the class" do
    m = @c.new
    m.values[:id] = 1
    m.cache_key.should == "#{m.class}:1"
    m = @c2.new
    m.values[:id] = 1
    m.cache_key.should == "#{m.class}:1"
    
    # custom primary key
    @c.set_primary_key :ttt
    m = @c.new
    m.values[:ttt] = 333
    m.cache_key.should == "#{m.class}:333"
    c = Class.new(@c)
    m = c.new
    m.values[:ttt] = 333
    m.cache_key.should == "#{m.class}:333"
    
    # composite primary key
    @c.set_primary_key [:a, :b, :c]
    m = @c.new
    m.values[:a] = 123
    m.values[:c] = 456
    m.values[:b] = 789
    m.cache_key.should == "#{m.class}:123,789,456"
    c = Class.new(@c)
    m = c.new
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

    m = @c2.new
    proc {m.cache_key}.should raise_error(Sequel::Error)
    m.values[:id] = 1
    proc {m.cache_key}.should_not raise_error(Sequel::Error)
  end
  
  it "should not raise error if trying to save a new record" do
    proc {@c.new(:name=>'blah').save}.should_not raise_error
    proc {@c.create(:name=>'blah')}.should_not raise_error
    proc {@c2.new(:name=>'blah').save}.should_not raise_error
    proc {@c2.create(:name=>'blah')}.should_not raise_error
  end
  
  it "should set the cache when reading from the database" do
    @c.db.sqls.should == []
    @cache.should be_empty
    
    m = @c[1]
    @c.db.sqls.should == ['SELECT * FROM items WHERE id = 1']
    m.values.should == {:name=>"sharon", :id=>1}
    @cache[m.cache_key].should == m
    m2 = @c[1]
    @c.db.sqls.should == []
    m2.should == m
    m2.values.should == {:name=>"sharon", :id=>1}

    m = @c2[1]
    @c.db.sqls.should == ['SELECT * FROM items WHERE id = 1']
    m.values.should == {:name=>"sharon", :id=>1}
    @cache[m.cache_key].should == m
    m2 = @c2[1]
    @c.db.sqls.should == []
    m2.should == m
    m2.values.should == {:name=>"sharon", :id=>1}
  end
  
  it "should delete the cache when writing to the database" do
    m = @c[1]
    @cache[m.cache_key].should == m
    m.name = 'hey'
    m.save
    @cache.has_key?(m.cache_key).should be_false
    @c.db.sqls.should == ["SELECT * FROM items WHERE id = 1", "UPDATE items SET name = 'hey' WHERE (id = 1)"]

    m = @c2[1]
    @cache[m.cache_key].should == m
    m.name = 'hey'
    m.save
    @cache.has_key?(m.cache_key).should be_false
    @c.db.sqls.should == ["SELECT * FROM items WHERE id = 1", "UPDATE items SET name = 'hey' WHERE (id = 1)"]
  end

  it "should delete the cache when deleting the record" do
    m = @c[1]
    @cache[m.cache_key].should == m
    m.delete
    @cache.has_key?(m.cache_key).should be_false
    @c.db.sqls.should == ["SELECT * FROM items WHERE id = 1", "DELETE FROM items WHERE (id = 1)"]

    m = @c2[1]
    @cache[m.cache_key].should == m
    m.delete
    @cache.has_key?(m.cache_key).should be_false
    @c.db.sqls.should == ["SELECT * FROM items WHERE id = 1", "DELETE FROM items WHERE (id = 1)"]
  end
  
  it "should support #[] as a shortcut to #find with hash" do
    m = @c[:id => 3]
    @cache[m.cache_key].should be_nil
    @c.db.sqls.should == ["SELECT * FROM items WHERE (id = 3) LIMIT 1"]
    m = @c[1]
    @cache[m.cache_key].should == m
    @c.db.sqls.should == ["SELECT * FROM items WHERE id = 1"]
    @c[:id => 4]
    @c.db.sqls.should == ["SELECT * FROM items WHERE (id = 4) LIMIT 1"]

    m = @c2[:id => 3]
    @cache[m.cache_key].should be_nil
    @c.db.sqls.should == ["SELECT * FROM items WHERE (id = 3) LIMIT 1"]
    m = @c2[1]
    @cache[m.cache_key].should == m
    @c.db.sqls.should == ["SELECT * FROM items WHERE id = 1"]
    @c2[:id => 4]
    @c.db.sqls.should == ["SELECT * FROM items WHERE (id = 4) LIMIT 1"]
  end
  
  it "should support ignore_exception option" do
    c = Class.new(Sequel::Model(:items))
    c.plugin :caching, @cache, :ignore_exceptions => true
    Class.new(c).cache_ignore_exceptions.should == true
  end
  
  it "should raise an exception if cache_store is memcached and ignore_exception is not enabled" do
    proc{@c3[1]}.should raise_error
  end
  
  it "should rescue an exception if cache_store is memcached and ignore_exception is enabled" do
    @c4[1].values.should == {:name => 'sharon', :id => 1}
  end
end
