require File.join(File.dirname(File.expand_path(__FILE__)), '/spec_helper')

describe "List plugin" do
  def klass(opts={})
    @db = MODEL_DB
    c = Class.new(Sequel::Model(@db[:items]))
    c.class_eval do
      columns :id, :position, :scope_id, :pos
      plugin :list, opts
      self.use_transactions = false
    end
    c
  end

  before do
    @c = klass
    @o = @c.load(:id=>7, :position=>3)
    @sc = klass(:scope=>:scope_id)
    @so = @sc.load(:id=>7, :position=>3, :scope_id=>5)
    @db.reset
  end

  it "should default to using :position as the position field" do
    @c.position_field.should == :position
    @c.new.list_dataset.sql.should == 'SELECT * FROM items ORDER BY position'
  end

  it "should accept a :field option to modify the position field" do
    klass(:field=>:pos).position_field.should == :pos
  end

  it "should accept a :scope option with a symbol for a single scope column" do
    @sc.new(:scope_id=>4).list_dataset.sql.should == 'SELECT * FROM items WHERE (scope_id = 4) ORDER BY scope_id, position'
  end

  it "should accept a :scope option with an array of symbols for multiple scope columns" do
    ['SELECT * FROM items WHERE ((scope_id = 4) AND (pos = 3)) ORDER BY scope_id, pos, position',
     'SELECT * FROM items WHERE ((pos = 3) AND (scope_id = 4)) ORDER BY scope_id, pos, position'].
     should include(klass(:scope=>[:scope_id, :pos]).new(:scope_id=>4, :pos=>3).list_dataset.sql)
  end

  it "should accept a :scope option with a proc for a custom list scope" do
    klass(:scope=>proc{|o| o.model.dataset.filter(:active).filter(:scope_id=>o.scope_id)}).new(:scope_id=>4).list_dataset.sql.should == 'SELECT * FROM items WHERE (active AND (scope_id = 4)) ORDER BY position'
  end

  it "should modify the order when using the plugin" do
    c = Class.new(Sequel::Model(:items))
    c.model.dataset.sql.should == 'SELECT * FROM items'
    c.plugin :list
    c.model.dataset.sql.should == 'SELECT * FROM items ORDER BY position'
  end

  it "should be able to access the position field as a class attribute" do
    @c.position_field.should == :position
    klass(:field=>:pos).position_field.should == :pos
  end

  it "should be able to access the scope proc as a class attribute" do
    @c.scope_proc.should == nil
    @sc.scope_proc[@sc.new(:scope_id=>4)].sql.should == 'SELECT * FROM items WHERE (scope_id = 4) ORDER BY scope_id, position'
  end

  it "should work correctly in subclasses" do
    c = Class.new(klass(:scope=>:scope_id))
    c.position_field.should == :position
    c.scope_proc[c.new(:scope_id=>4)].sql.should == 'SELECT * FROM items WHERE (scope_id = 4) ORDER BY scope_id, position'
  end

  it "should have at_position return the model object at the given position" do
    @c.dataset._fetch = {:id=>1, :position=>1}
    @o.at_position(10).should == @c.load(:id=>1, :position=>1)
    @sc.dataset._fetch = {:id=>2, :position=>2, :scope_id=>5}
    @so.at_position(20).should == @sc.load(:id=>2, :position=>2, :scope_id=>5)
    @db.sqls.should == ["SELECT * FROM items WHERE (position = 10) ORDER BY position LIMIT 1",
      "SELECT * FROM items WHERE ((scope_id = 5) AND (position = 20)) ORDER BY scope_id, position LIMIT 1"]
  end

  it "should have position field set to max+1 when creating if not already set" do
    @c.dataset._fetch = [[{:pos=>nil}], [{:id=>1, :position=>1}], [{:pos=>1}], [{:id=>2, :position=>2}]]
    @c.dataset.autoid = 1
    @c.create.values.should == {:id=>1, :position=>1}
    @c.create.values.should == {:id=>2, :position=>2}
    @db.sqls.should == ["SELECT max(position) FROM items LIMIT 1",
      "INSERT INTO items (position) VALUES (1)", 
      "SELECT * FROM items WHERE (id = 1) ORDER BY position LIMIT 1",
      "SELECT max(position) FROM items LIMIT 1",
      "INSERT INTO items (position) VALUES (2)", 
      "SELECT * FROM items WHERE (id = 2) ORDER BY position LIMIT 1"]
  end

  it "should have position field set to max+1 in scope when creating if not already set" do
    @sc.dataset._fetch = [[{:pos=>nil}], [{:id=>1, :scope_id=>1, :position=>1}], [{:pos=>1}], [{:id=>2, :scope_id=>1, :position=>2}], [{:pos=>nil}], [{:id=>3, :scope_id=>2, :position=>1}]]
    @sc.dataset.autoid = 1
    @sc.create(:scope_id=>1).values.should == {:id=>1, :scope_id=>1, :position=>1}
    @sc.create(:scope_id=>1).values.should == {:id=>2, :scope_id=>1, :position=>2}
    @sc.create(:scope_id=>2).values.should == {:id=>3, :scope_id=>2, :position=>1}
    sqls = @db.sqls
    sqls.slice!(7).should =~ /INSERT INTO items \((scope_id|position), (scope_id|position)\) VALUES \([12], [12]\)/
    sqls.slice!(4).should =~ /INSERT INTO items \((scope_id|position), (scope_id|position)\) VALUES \([12], [12]\)/
    sqls.slice!(1).should =~ /INSERT INTO items \((scope_id|position), (scope_id|position)\) VALUES \(1, 1\)/
    sqls.should == ["SELECT max(position) FROM items WHERE (scope_id = 1) LIMIT 1",
      "SELECT * FROM items WHERE (id = 1) ORDER BY scope_id, position LIMIT 1",
      "SELECT max(position) FROM items WHERE (scope_id = 1) LIMIT 1",
      "SELECT * FROM items WHERE (id = 2) ORDER BY scope_id, position LIMIT 1",
      "SELECT max(position) FROM items WHERE (scope_id = 2) LIMIT 1",
      "SELECT * FROM items WHERE (id = 3) ORDER BY scope_id, position LIMIT 1"]
  end

  it "should have last_position return the last position in the list" do
    @c.dataset._fetch  = {:max=>10}
    @o.last_position.should == 10
    @sc.dataset._fetch = {:max=>20}
    @so.last_position.should == 20
    @db.sqls.should == ["SELECT max(position) FROM items LIMIT 1",
      "SELECT max(position) FROM items WHERE (scope_id = 5) LIMIT 1"]
  end

  it "should have list_dataset return the model's dataset for non scoped lists" do
    @o.list_dataset.sql.should == 'SELECT * FROM items ORDER BY position'
  end

  it "should have list dataset return a scoped dataset for scoped lists" do
    @so.list_dataset.sql.should == 'SELECT * FROM items WHERE (scope_id = 5) ORDER BY scope_id, position'
  end

  it "should have move_down without an argument move down a single position" do
    @c.dataset._fetch = {:max=>10}
    @o.move_down.should == @o
    @o.position.should == 4
    @db.sqls.should == ["SELECT max(position) FROM items LIMIT 1",
      "UPDATE items SET position = (position - 1) WHERE ((position >= 4) AND (position <= 4))",
      "UPDATE items SET position = 4 WHERE (id = 7)"]
  end

  it "should have move_down with an argument move down the given number of positions" do
    @c.dataset._fetch = {:max=>10}
    @o.move_down(3).should == @o
    @o.position.should == 6
    @db.sqls.should == ["SELECT max(position) FROM items LIMIT 1",
      "UPDATE items SET position = (position - 1) WHERE ((position >= 4) AND (position <= 6))",
      "UPDATE items SET position = 6 WHERE (id = 7)"]
  end

  it "should have move_down with a negative argument move up the given number of positions" do
    @o.move_down(-1).should == @o
    @o.position.should == 2
    @db.sqls.should == ["UPDATE items SET position = (position + 1) WHERE ((position >= 2) AND (position < 3))",
      "UPDATE items SET position = 2 WHERE (id = 7)"]
  end

  it "should have move_to raise an error if an invalid target is used" do
    proc{@o.move_to(0)}.should raise_error(Sequel::Error)
    @c.dataset._fetch = {:max=>10}
    proc{@o.move_to(11)}.should raise_error(Sequel::Error)
  end

  it "should have move_to use a transaction if the instance is configured to use transactions" do
    @o.use_transactions = true
    @o.move_to(2)
    @db.sqls.should == ["BEGIN",
      "UPDATE items SET position = (position + 1) WHERE ((position >= 2) AND (position < 3))",
      "UPDATE items SET position = 2 WHERE (id = 7)",
      "COMMIT"]
  end

  it "should have move_to do nothing if the target position is the same as the current position" do
    @o.use_transactions = true
    @o.move_to(@o.position).should == @o
    @o.position.should == 3
    @db.sqls.should == []
  end

  it "should have move to shift entries correctly between current and target if moving up" do
    @o.move_to(2)
    @db.sqls.first.should == "UPDATE items SET position = (position + 1) WHERE ((position >= 2) AND (position < 3))"
  end

  it "should have move to shift entries correctly between current and target if moving down" do
    @c.dataset._fetch = {:max=>10}
    @o.move_to(4)
    @db.sqls[1].should == "UPDATE items SET position = (position - 1) WHERE ((position >= 4) AND (position <= 4))"
  end

  it "should have move_to_bottom move the item to the last position" do
    @c.dataset._fetch = {:max=>10}
    @o.move_to_bottom
    @db.sqls.should == ["SELECT max(position) FROM items LIMIT 1",
      "UPDATE items SET position = (position - 1) WHERE ((position >= 4) AND (position <= 10))",
      "UPDATE items SET position = 10 WHERE (id = 7)"]
  end

  it "should have move_to_top move the item to the first position" do
    @o.move_to_top
    @db.sqls.should == ["UPDATE items SET position = (position + 1) WHERE ((position >= 1) AND (position < 3))",
      "UPDATE items SET position = 1 WHERE (id = 7)"]
  end

  it "should have move_up without an argument move up a single position" do
    @o.move_up.should == @o
    @o.position.should == 2
    @db.sqls.should == ["UPDATE items SET position = (position + 1) WHERE ((position >= 2) AND (position < 3))",
      "UPDATE items SET position = 2 WHERE (id = 7)"]
  end

  it "should have move_up with an argument move up the given number of positions" do
    @o.move_up(2).should == @o
    @o.position.should == 1
    @db.sqls.should == ["UPDATE items SET position = (position + 1) WHERE ((position >= 1) AND (position < 3))",
      "UPDATE items SET position = 1 WHERE (id = 7)"]
  end

  it "should have move_up with a negative argument move down the given number of positions" do
    @c.dataset._fetch = {:max=>10}
    @o.move_up(-1).should == @o
    @o.position.should == 4
    @db.sqls.should == ["SELECT max(position) FROM items LIMIT 1",
      "UPDATE items SET position = (position - 1) WHERE ((position >= 4) AND (position <= 4))",
      "UPDATE items SET position = 4 WHERE (id = 7)"]
  end

  it "should have next return the next entry in the list if not given an argument" do
    @c.dataset._fetch = {:id=>9, :position=>4}
    @o.next.should == @c.load(:id=>9, :position=>4)
    @db.sqls.should == ["SELECT * FROM items WHERE (position = 4) ORDER BY position LIMIT 1"]
  end

  it "should have next return the entry the given number of positions below the instance if given an argument" do
    @c.dataset._fetch = {:id=>9, :position=>5}
    @o.next(2).should == @c.load(:id=>9, :position=>5)
    @db.sqls.should == ["SELECT * FROM items WHERE (position = 5) ORDER BY position LIMIT 1"]
  end

  it "should have next return a previous entry if given a negative argument" do
    @c.dataset._fetch = {:id=>9, :position=>2}
    @o.next(-1).should == @c.load(:id=>9, :position=>2)
    @db.sqls.should == ["SELECT * FROM items WHERE (position = 2) ORDER BY position LIMIT 1"]
  end

  it "should have position_value return the value of the position field" do
    @o.position_value.should == 3
  end

  it "should have prev return the previous entry in the list if not given an argument" do
    @c.dataset._fetch = {:id=>9, :position=>2}
    @o.prev.should == @c.load(:id=>9, :position=>2)
    @db.sqls.should == ["SELECT * FROM items WHERE (position = 2) ORDER BY position LIMIT 1"]
  end

  it "should have prev return the entry the given number of positions above the instance if given an argument" do
    @c.dataset._fetch = {:id=>9, :position=>1}
    @o.prev(2).should == @c.load(:id=>9, :position=>1)
    @db.sqls.should == ["SELECT * FROM items WHERE (position = 1) ORDER BY position LIMIT 1"]
  end

  it "should have prev return a following entry if given a negative argument" do
    @c.dataset._fetch = {:id=>9, :position=>4}
    @o.prev(-1).should == @c.load(:id=>9, :position=>4)
    @db.sqls.should == ["SELECT * FROM items WHERE (position = 4) ORDER BY position LIMIT 1"]
  end
end
