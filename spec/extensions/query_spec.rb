require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Database#dataset" do
  before do
    @db = Sequel::Database.new
  end

  specify "should delegate to Dataset#query if block is provided" do
    @d = @db.query {select :x; from :y}
    @d.should be_a_kind_of(Sequel::Dataset)
    @d.sql.should == "SELECT x FROM y"
  end
end

describe "Dataset#query" do
  before do
    @d = Sequel::Dataset.new(nil)
  end
  
  specify "should allow cloning without arguments" do
    q = @d.query {clone}
    q.class.should == @d.class
    q.sql.should == "SELECT *"
  end
  
  specify "should support #from" do
    q = @d.query {from :xxx}
    q.class.should == @d.class
    q.sql.should == "SELECT * FROM xxx"
  end
  
  specify "should support #select" do
    q = @d.query do
      select :a, :b___mongo
      from :yyy
    end
    q.class.should == @d.class
    q.sql.should == "SELECT a, b AS mongo FROM yyy"
  end
  
  specify "should support #where" do
    q = @d.query do
      from :zzz
      where{x + 2 > Sequel.expr(:y) + 3}
    end
    q.class.should == @d.class
    q.sql.should == "SELECT * FROM zzz WHERE ((x + 2) > (y + 3))"

    q = @d.from(:zzz).query do
      where{(x > 1) & (Sequel.expr(:y) > 2)}
    end
    q.class.should == @d.class
    q.sql.should == "SELECT * FROM zzz WHERE ((x > 1) AND (y > 2))"

    q = @d.from(:zzz).query do
      where :x => 33
    end
    q.class.should == @d.class
    q.sql.should == "SELECT * FROM zzz WHERE (x = 33)"
  end
  
  specify "should support #group_by and #having" do
    q = @d.query do
      from :abc
      group_by :id
      having{x >= 2}
    end
    q.class.should == @d.class
    q.sql.should == "SELECT * FROM abc GROUP BY id HAVING (x >= 2)"
  end
  
  specify "should support #order, #order_by" do
    q = @d.query do
      from :xyz
      order_by :stamp
    end
    q.class.should == @d.class
    q.sql.should == "SELECT * FROM xyz ORDER BY stamp"
  end

  specify "should support blocks that end in nil" do
    condition = false
    q = @d.query do
      from :xyz
      order_by :stamp if condition
    end
    q.sql.should == "SELECT * FROM xyz"
  end
  
  specify "should raise on non-chainable method calls" do
    proc {@d.query {row_proc}}.should raise_error(Sequel::Error)
    proc {@d.query {all}}.should raise_error(Sequel::Error)
  end
end
