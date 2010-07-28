require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model, "tree plugin" do
  def klass(opts={})
    @db = MODEL_DB
    c = Class.new(Sequel::Model(@db[:nodes]))
    c.class_eval do
      def self.name; 'Node'; end
      columns :id, :name, :parent_id, :i, :pi
    end
    ds = c.dataset
    class << ds
      attr_accessor :row_sets
      def fetch_rows(sql)
        @db << sql
        if rs = row_sets.shift
          rs.each{|row| yield row}
        end
      end
    end
    c.plugin :tree, opts
    c
  end

  def y(c, *hs)
    ds = c.dataset
    ds.row_sets = hs
  end

  before do
    @c = klass
    @ds = @c.dataset
    @o = @c.load(:id=>2, :parent_id=>1, :name=>'AA', :i=>3, :pi=>4)
    @db.reset
  end

  it "should define the correct associations" do
    @c.associations.sort_by{|x| x.to_s}.should == [:children, :parent]
  end
  
  it "should define the correct associations when giving options" do
    klass(:children=>{:name=>:cs}, :parent=>{:name=>:p}).associations.sort_by{|x| x.to_s}.should == [:cs, :p]
  end

  it "should use the correct SQL for lazy associations" do
    @o.parent_dataset.sql.should == 'SELECT * FROM nodes WHERE (nodes.id = 1) LIMIT 1'
    @o.children_dataset.sql.should == 'SELECT * FROM nodes WHERE (nodes.parent_id = 2)'
  end
  
  it "should use the correct SQL for lazy associations when giving options" do
    o = klass(:primary_key=>:i, :key=>:pi, :order=>:name, :children=>{:name=>:cs}, :parent=>{:name=>:p}).load(:id=>2, :parent_id=>1, :name=>'AA', :i=>3, :pi=>4)
    o.p_dataset.sql.should == 'SELECT * FROM nodes WHERE (nodes.i = 4) ORDER BY name LIMIT 1'
    o.cs_dataset.sql.should == 'SELECT * FROM nodes WHERE (nodes.pi = 3) ORDER BY name'
  end

  it "should have parent_column give the symbol of the parent column" do
    @c.parent_column.should == :parent_id
    klass(:key=>:p_id).parent_column.should == :p_id
  end

  it "should have tree_order give the order of the association" do
    @c.tree_order.should == nil
    klass(:order=>:name).tree_order.should == :name
    klass(:order=>[:parent_id, :name]).tree_order.should == [:parent_id, :name]
  end

  it "should work correctly in subclasses" do
    o = Class.new(klass(:primary_key=>:i, :key=>:pi, :order=>:name, :children=>{:name=>:cs}, :parent=>{:name=>:p})).load(:id=>2, :parent_id=>1, :name=>'AA', :i=>3, :pi=>4)
    o.p_dataset.sql.should == 'SELECT * FROM nodes WHERE (nodes.i = 4) ORDER BY name LIMIT 1'
    o.cs_dataset.sql.should == 'SELECT * FROM nodes WHERE (nodes.pi = 3) ORDER BY name'
  end

  it "should have roots return an array of the tree's roots" do
    y(@c, [{:id=>1, :parent_id=>nil, :name=>'r'}])
    @c.roots.should == [@c.load(:id=>1, :parent_id=>nil, :name=>'r')]
    @db.sqls.should == ["SELECT * FROM nodes WHERE (parent_id IS NULL)"]
  end

  it "should have roots_dataset be a dataset representing the tree's roots" do
    @c.roots_dataset.sql.should == "SELECT * FROM nodes WHERE (parent_id IS NULL)"
  end

  it "should have ancestors return the ancestors of the current node" do
    y(@c, [{:id=>1, :parent_id=>5, :name=>'r'}], [{:id=>5, :parent_id=>nil, :name=>'r2'}])
    @o.ancestors.should == [@c.load(:id=>1, :parent_id=>5, :name=>'r'), @c.load(:id=>5, :parent_id=>nil, :name=>'r2')]
    @db.sqls.should == ["SELECT * FROM nodes WHERE (nodes.id = 1) LIMIT 1",
      "SELECT * FROM nodes WHERE (nodes.id = 5) LIMIT 1"]
  end

  it "should have descendants return the descendants of the current node" do
    y(@c, [{:id=>3, :parent_id=>2, :name=>'r'}, {:id=>4, :parent_id=>2, :name=>'r2'}], [{:id=>5, :parent_id=>4, :name=>'r3'}], [])
    @o.descendants.should == [@c.load(:id=>3, :parent_id=>2, :name=>'r'), @c.load(:id=>4, :parent_id=>2, :name=>'r2'), @c.load(:id=>5, :parent_id=>4, :name=>'r3')] 
    @db.sqls.should == ["SELECT * FROM nodes WHERE (nodes.parent_id = 2)",
      "SELECT * FROM nodes WHERE (nodes.parent_id = 3)",
      "SELECT * FROM nodes WHERE (nodes.parent_id = 5)",
      "SELECT * FROM nodes WHERE (nodes.parent_id = 4)"]
  end

  it "should have root return the root of the current node" do
    y(@c, [{:id=>1, :parent_id=>5, :name=>'r'}], [{:id=>5, :parent_id=>nil, :name=>'r2'}])
    @o.root.should == @c.load(:id=>5, :parent_id=>nil, :name=>'r2')
    @db.sqls.should == ["SELECT * FROM nodes WHERE (nodes.id = 1) LIMIT 1",
      "SELECT * FROM nodes WHERE (nodes.id = 5) LIMIT 1"]
  end

  it "should have root? return true for a root node and false for a child node" do
    @c.new(:parent_id => nil).root?.should be_true
    @c.new(:parent_id => 1).root?.should be_false
  end

  it "should have self_and_siblings return the children of the current node's parent" do
    y(@c, [{:id=>1, :parent_id=>3, :name=>'r'}], [{:id=>7, :parent_id=>1, :name=>'r2'}, @o.values.dup])
    @o.self_and_siblings.should == [@c.load(:id=>7, :parent_id=>1, :name=>'r2'), @o] 
    @db.sqls.should == ["SELECT * FROM nodes WHERE (nodes.id = 1) LIMIT 1",
      "SELECT * FROM nodes WHERE (nodes.parent_id = 1)"]
  end

  it "should have siblings return the children of the current node's parent, except for the current node" do
    y(@c, [{:id=>1, :parent_id=>3, :name=>'r'}], [{:id=>7, :parent_id=>1, :name=>'r2'}, @o.values.dup])
    @o.siblings.should == [@c.load(:id=>7, :parent_id=>1, :name=>'r2')] 
    @db.sqls.should == ["SELECT * FROM nodes WHERE (nodes.id = 1) LIMIT 1",
      "SELECT * FROM nodes WHERE (nodes.parent_id = 1)"]
  end
end
