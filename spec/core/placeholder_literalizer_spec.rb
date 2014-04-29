require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Dataset::PlaceholderLiteralizer" do
  before do
    @c = Sequel::Dataset::PlaceholderLiteralizer
    @db = Sequel.mock
    @ds = @db[:items]
    @h = {:id=>1}
    @ds.db.fetch = @h
  end
  
  specify "should handle calls with no placeholders" do
    loader = @c.loader(@ds){|pl, ds| ds.where(:a=>1)}
    loader.first.should == @h
    @db.sqls.should == ["SELECT * FROM items WHERE (a = 1)"]
  end
  
  specify "should handle calls with a single placeholder" do
    loader = @c.loader(@ds){|pl, ds| ds.where(:a=>pl.arg)}
    loader.first(1).should == @h
    loader.first(2).should == @h
    @db.sqls.should == ["SELECT * FROM items WHERE (a = 1)", "SELECT * FROM items WHERE (a = 2)"]
  end
  
  specify "should handle calls with multiple placeholders" do
    loader = @c.loader(@ds){|pl, ds| ds.where(:a=>pl.arg).where(:b=>Sequel.+(pl.arg, 1)).where(pl.arg)}
    loader.first(1, :c, :id=>1).should == @h
    loader.first(2, :d, :id=>2).should == @h
    @db.sqls.should == ["SELECT * FROM items WHERE ((a = 1) AND (b = (c + 1)) AND (id = 1))", "SELECT * FROM items WHERE ((a = 2) AND (b = (d + 1)) AND (id = 2))"]
  end
  
  specify "should handle calls with placeholders and delayed arguments" do
    h = :h
    s = :s
    d = @ds.having(Sequel.delay{h}).select(Sequel.delay{s})
    loader = @c.loader(d){|pl, ds| ds.where(:a=>pl.arg).where(:b=>Sequel.+(pl.arg, 1)).where(pl.arg)}
    loader.first(1, :c, :id=>1).should == @h
    h = :h2
    s = :s2
    loader.first(2, :d, :id=>2).should == @h
    @db.sqls.should == ["SELECT s FROM items WHERE ((a = 1) AND (b = (c + 1)) AND (id = 1)) HAVING h", "SELECT s2 FROM items WHERE ((a = 2) AND (b = (d + 1)) AND (id = 2)) HAVING h2"]
  end
  
  specify "should handle calls with a placeholders used as filter arguments" do
    loader = @c.loader(@ds){|pl, ds| ds.where(pl.arg)}
    loader.first(:id=>1).should == @h
    loader.first(proc{a(b)}).should == @h
    loader.first("a = 1").should == @h
    @db.sqls.should == ["SELECT * FROM items WHERE (id = 1)", "SELECT * FROM items WHERE a(b)", "SELECT * FROM items WHERE (a = 1)"]
  end
  
  specify "should handle calls with a placeholders used as right hand side of condition specifiers" do
    loader = @c.loader(@ds){|pl, ds| ds.where(:a=>pl.arg)}
    loader.first(1).should == @h
    loader.first([1, 2]).should == @h
    loader.first(nil).should == @h
    @db.sqls.should == ["SELECT * FROM items WHERE (a = 1)", "SELECT * FROM items WHERE (a IN (1, 2))", "SELECT * FROM items WHERE (a IS NULL)"]
  end
  
  specify "should handle calls with a placeholder used multiple times" do
    loader = @c.loader(@ds){|pl, ds| a = pl.arg; ds.where(:a=>a).where(:b=>a)}
    loader.first(1).should == @h
    loader.first(2).should == @h
    @db.sqls.should == ["SELECT * FROM items WHERE ((a = 1) AND (b = 1))", "SELECT * FROM items WHERE ((a = 2) AND (b = 2))"]
  end
  
  specify "should handle calls with a placeholder used multiple times in different capacities" do
    loader = @c.loader(@ds){|pl, ds| a = pl.arg; ds.where(a).where(:b=>a)}
    loader.first("a = 1").should == @h
    loader.first(["a = ?", 2]).should == @h
    @db.sqls.should == ["SELECT * FROM items WHERE ((a = 1) AND (b = 'a = 1'))", "SELECT * FROM items WHERE ((a = 2) AND (b IN ('a = ?', 2)))"]
  end
  
  specify "should handle calls with manually specified argument positions" do
    loader = @c.loader(@ds){|pl, ds| ds.where(:a=>pl.arg(1)).where(:b=>pl.arg(0))}
    loader.first(1, 2).should == @h
    loader.first(2, 1).should == @h
    @db.sqls.should == ["SELECT * FROM items WHERE ((a = 2) AND (b = 1))", "SELECT * FROM items WHERE ((a = 1) AND (b = 2))"]
  end
  
  specify "should handle dataset with row procs" do
    @ds.row_proc = proc{|r| {:foo=>r[:id]+1}}
    loader = @c.loader(@ds){|pl, ds| ds.where(:a=>pl.arg)}
    loader.first(1).should == {:foo=>2}
    @db.sqls.should == ["SELECT * FROM items WHERE (a = 1)"]
  end
  
  specify "should return all rows for #all" do
    loader = @c.loader(@ds){|pl, ds| ds.where(:a=>pl.arg)}
    loader.all(1).should == [@h]
    @db.sqls.should == ["SELECT * FROM items WHERE (a = 1)"]
  end
  
  specify "should iterate over block for #all" do
    a = []
    loader = @c.loader(@ds){|pl, ds| ds.where(:a=>pl.arg)}
    loader.all(1){|r| a << r}.should == [@h]
    a.should == [@h]
    @db.sqls.should == ["SELECT * FROM items WHERE (a = 1)"]
  end
  
  specify "should iterate over block for #each" do
    a = []
    loader = @c.loader(@ds){|pl, ds| ds.where(:a=>pl.arg)}
    loader.each(1){|r| a << r}
    a.should == [@h]
    @db.sqls.should == ["SELECT * FROM items WHERE (a = 1)"]
  end
  
  specify "should return first value for #get" do
    loader = @c.loader(@ds){|pl, ds| ds.where(:a=>pl.arg)}
    loader.get(2).should == 1
    @db.sqls.should == ["SELECT * FROM items WHERE (a = 2)"]
  end

  specify "should literalize args as NULL if :placeholder_literal_null is set" do
    loader = @c.loader(@ds){|pl, ds| ds.where(pl.arg=>:a).clone(:placeholder_literal_null=>true)}
    loader.sql(1).should == "SELECT * FROM items WHERE (NULL = a)"
  end
  
  specify "should raise an error if called with an incorrect number of arguments" do
    loader = @c.loader(@ds){|pl, ds| ds.where(:a=>pl.arg)}
    proc{loader.first}.should raise_error(Sequel::Error)
    proc{loader.first(1, 2)}.should raise_error(Sequel::Error)
  end

  specify "should raise an error if called with an incorrect number of arguments when manually providing argument positions" do
    loader = @c.loader(@ds){|pl, ds| ds.where(:a=>pl.arg(1))}
    proc{loader.first}.should raise_error(Sequel::Error)
    proc{loader.first(1)}.should raise_error(Sequel::Error)
    proc{loader.first(1, 2, 3)}.should raise_error(Sequel::Error)
  end

  specify "should raise an error if argument literalized into a different string than returned by query" do
    o = Object.new
    def o.wrap(v)
      @v = v
      self
    end
    def o.sql_literal(ds)
      ds.literal(@v)
    end
    proc{@c.loader(@ds){|pl, ds| ds.where(o.wrap(pl.arg))}}.should raise_error(Sequel::Error)
  end
end
