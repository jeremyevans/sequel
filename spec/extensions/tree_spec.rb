require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model, "tree plugin" do
  def klass(opts={})
    @db = DB
    c = Class.new(Sequel::Model(@db[:nodes]))
    c.class_eval do
      def self.name; 'Node'; end
      columns :id, :name, :parent_id, :i, :pi
      plugin :tree, opts
    end
    c
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
    @ds._fetch = [{:id=>1, :parent_id=>nil, :name=>'r'}]
    @c.roots.should == [@c.load(:id=>1, :parent_id=>nil, :name=>'r')]
    @db.sqls.should == ["SELECT * FROM nodes WHERE (parent_id IS NULL)"]
  end

  it "should have roots_dataset be a dataset representing the tree's roots" do
    @c.roots_dataset.sql.should == "SELECT * FROM nodes WHERE (parent_id IS NULL)"
  end

  it "should have ancestors return the ancestors of the current node" do
    @ds._fetch = [[{:id=>1, :parent_id=>5, :name=>'r'}], [{:id=>5, :parent_id=>nil, :name=>'r2'}]]
    @o.ancestors.should == [@c.load(:id=>1, :parent_id=>5, :name=>'r'), @c.load(:id=>5, :parent_id=>nil, :name=>'r2')]
    @db.sqls.should == ["SELECT * FROM nodes WHERE id = 1",
      "SELECT * FROM nodes WHERE id = 5"]
  end
  
  it "should have self_and_ancestors return current node and it ancestors" do
    @ds._fetch = [[{:id=>1, :parent_id=>5, :name=>'r'}], [{:id=>5, :parent_id=>nil, :name=>'r2'}]]
    @o.self_and_ancestors.should == [@c.load(:id=>2, :parent_id=>1, :name=>'AA', :i=>3, :pi=>4), @c.load(:id=>1, :parent_id=>5, :name=>'r'), @c.load(:id=>5, :parent_id=>nil, :name=>'r2')]
    @db.sqls.should == ["SELECT * FROM nodes WHERE id = 1",
      "SELECT * FROM nodes WHERE id = 5"]
  end

  it "should have descendants return the descendants of the current node" do
    @ds._fetch = [[{:id=>3, :parent_id=>2, :name=>'r'}, {:id=>4, :parent_id=>2, :name=>'r2'}], [{:id=>5, :parent_id=>4, :name=>'r3'}], []]
    @o.descendants.should == [@c.load(:id=>3, :parent_id=>2, :name=>'r'), @c.load(:id=>4, :parent_id=>2, :name=>'r2'), @c.load(:id=>5, :parent_id=>4, :name=>'r3')] 
    @db.sqls.should == ["SELECT * FROM nodes WHERE (nodes.parent_id = 2)",
      "SELECT * FROM nodes WHERE (nodes.parent_id = 3)",
      "SELECT * FROM nodes WHERE (nodes.parent_id = 5)",
      "SELECT * FROM nodes WHERE (nodes.parent_id = 4)"]
  end

  it "should have root return the root of the current node" do
    @ds._fetch = [[{:id=>1, :parent_id=>5, :name=>'r'}], [{:id=>5, :parent_id=>nil, :name=>'r2'}]]
    @o.root.should == @c.load(:id=>5, :parent_id=>nil, :name=>'r2')
    @db.sqls.should == ["SELECT * FROM nodes WHERE id = 1",
      "SELECT * FROM nodes WHERE id = 5"]
  end

  it "should have root? return true for a root node and false for a child node" do
    @c.load(:parent_id => nil).root?.should == true
    @c.load(:parent_id => 1).root?.should == false
  end

  it "should have root? return false for an new node" do
    @c.new.root?.should == false
  end

  it "should have self_and_siblings return the children of the current node's parent" do
    @ds._fetch = [[{:id=>1, :parent_id=>3, :name=>'r'}], [{:id=>7, :parent_id=>1, :name=>'r2'}, @o.values.dup]]
    @o.self_and_siblings.should == [@c.load(:id=>7, :parent_id=>1, :name=>'r2'), @o] 
    @db.sqls.should == ["SELECT * FROM nodes WHERE id = 1",
      "SELECT * FROM nodes WHERE (nodes.parent_id = 1)"]
  end

  it "should have siblings return the children of the current node's parent, except for the current node" do
    @ds._fetch = [[{:id=>1, :parent_id=>3, :name=>'r'}], [{:id=>7, :parent_id=>1, :name=>'r2'}, @o.values.dup]]
    @o.siblings.should == [@c.load(:id=>7, :parent_id=>1, :name=>'r2')] 
    @db.sqls.should == ["SELECT * FROM nodes WHERE id = 1",
      "SELECT * FROM nodes WHERE (nodes.parent_id = 1)"]
  end

  describe ":single_root option" do
    before do
      @c = klass(:single_root => true)
    end

    it "should have root class method return the root" do
      @c.dataset._fetch = [{:id=>1, :parent_id=>nil, :name=>'r'}]
      @c.root.should == @c.load(:id=>1, :parent_id=>nil, :name=>'r')
    end

    it "prevents creating a second root" do
      @c.dataset._fetch = [{:id=>1, :parent_id=>nil, :name=>'r'}]
      lambda { @c.create }.should raise_error(Sequel::Plugins::Tree::TreeMultipleRootError)
    end

    it "errors when promoting an existing record to a second root" do
      @c.dataset._fetch = [{:id=>1, :parent_id=>nil, :name=>'r'}]
      n = @c.load(:id => 2, :parent_id => 1)
      lambda { n.update(:parent_id => nil) }.should raise_error(Sequel::Plugins::Tree::TreeMultipleRootError)
    end

    it "allows updating existing root" do
      @c.dataset._fetch = [{:id=>1, :parent_id=>nil, :name=>'r'}]
      lambda { @c.root.update(:name => 'fdsa') }.should_not raise_error
    end
  end
end

describe Sequel::Model, "tree plugin with composite keys" do
  def klass(opts={})
    @db = DB
    c = Class.new(Sequel::Model(@db[:nodes]))
    c.class_eval do
      def self.name; 'Node'; end
      columns :id, :id2, :name, :parent_id, :parent_id2, :i, :pi
      set_primary_key [:id, :id2]
      plugin :tree, opts.merge(:key=>[:parent_id, :parent_id2])
    end
    c
  end

  before do
    @c = klass
    @ds = @c.dataset
    @o = @c.load(:id=>2, :id2=>5, :parent_id=>1, :parent_id2=>6, :name=>'AA', :i=>3, :pi=>4)
    @db.reset
  end

  
  it "should use the correct SQL for lazy associations" do
    @o.parent_dataset.sql.should == 'SELECT * FROM nodes WHERE ((nodes.id = 1) AND (nodes.id2 = 6)) LIMIT 1'
    @o.children_dataset.sql.should == 'SELECT * FROM nodes WHERE ((nodes.parent_id = 2) AND (nodes.parent_id2 = 5))'
  end
  
  it "should have parent_column give an array of symbols of the parent column" do
    @c.parent_column.should == [:parent_id, :parent_id2]
  end

  it "should have roots return an array of the tree's roots" do
    @ds._fetch = [{:id=>1, :parent_id=>nil, :parent_id2=>nil, :name=>'r'}]
    @c.roots.should == [@c.load(:id=>1, :parent_id=>nil, :parent_id2=>nil, :name=>'r')]
    @db.sqls.should == ["SELECT * FROM nodes WHERE ((parent_id IS NULL) OR (parent_id2 IS NULL))"]
  end

  it "should have roots_dataset be a dataset representing the tree's roots" do
    @c.roots_dataset.sql.should == "SELECT * FROM nodes WHERE ((parent_id IS NULL) OR (parent_id2 IS NULL))"
  end

  it "should have ancestors return the ancestors of the current node" do
    @ds._fetch = [[{:id=>1, :id2=>6, :parent_id=>5, :parent_id2=>7, :name=>'r'}], [{:id=>5, :id2=>7, :parent_id=>nil, :parent_id2=>nil, :name=>'r2'}]]
    @o.ancestors.should == [@c.load(:id=>1, :id2=>6, :parent_id=>5, :parent_id2=>7, :name=>'r'), @c.load(:id=>5, :id2=>7, :parent_id=>nil, :parent_id2=>nil, :name=>'r2')]
    sqls = @db.sqls
    sqls.length.should == 2
    ["SELECT * FROM nodes WHERE ((id = 1) AND (id2 = 6)) LIMIT 1", "SELECT * FROM nodes WHERE ((id2 = 6) AND (id = 1)) LIMIT 1"].should include(sqls[0])
    ["SELECT * FROM nodes WHERE ((id = 5) AND (id2 = 7)) LIMIT 1", "SELECT * FROM nodes WHERE ((id2 = 7) AND (id = 5)) LIMIT 1"].should include(sqls[1])
  end

  it "should have descendants return the descendants of the current node" do
    @ds._fetch = [[{:id=>3, :id2=>7, :parent_id=>2, :parent_id2=>5, :name=>'r'}, {:id=>4, :id2=>8, :parent_id=>2, :parent_id2=>5, :name=>'r2'}], [{:id=>5, :id2=>9, :parent_id=>4, :parent_id2=>8, :name=>'r3'}], []]
    @o.descendants.should == [@c.load(:id=>3, :id2=>7, :parent_id=>2, :parent_id2=>5, :name=>'r'), @c.load(:id=>4, :id2=>8, :parent_id=>2, :parent_id2=>5, :name=>'r2'), @c.load(:id=>5, :id2=>9, :parent_id=>4, :parent_id2=>8, :name=>'r3')] 
    @db.sqls.should == ["SELECT * FROM nodes WHERE ((nodes.parent_id = 2) AND (nodes.parent_id2 = 5))",
      "SELECT * FROM nodes WHERE ((nodes.parent_id = 3) AND (nodes.parent_id2 = 7))",
      "SELECT * FROM nodes WHERE ((nodes.parent_id = 5) AND (nodes.parent_id2 = 9))",
      "SELECT * FROM nodes WHERE ((nodes.parent_id = 4) AND (nodes.parent_id2 = 8))"]
  end

  it "should have root return the root of the current node" do
    @ds._fetch = [[{:id=>1, :id2=>6, :parent_id=>5, :parent_id2=>7, :name=>'r'}], [{:id=>5, :id2=>7, :parent_id=>nil, :parent_id2=>nil, :name=>'r2'}]]
    @o.root.should == @c.load(:id=>5, :id2=>7, :parent_id=>nil, :parent_id2=>nil, :name=>'r2')
    sqls = @db.sqls
    sqls.length.should == 2
    ["SELECT * FROM nodes WHERE ((id = 1) AND (id2 = 6)) LIMIT 1", "SELECT * FROM nodes WHERE ((id2 = 6) AND (id = 1)) LIMIT 1"].should include(sqls[0])
    ["SELECT * FROM nodes WHERE ((id = 5) AND (id2 = 7)) LIMIT 1", "SELECT * FROM nodes WHERE ((id2 = 7) AND (id = 5)) LIMIT 1"].should include(sqls[1])
  end

  it "should have root? return true for a root node and false for a child node" do
    @c.load(:parent_id => nil, :parent_id2=>nil).root?.should == true
    @c.load(:parent_id => 1, :parent_id2=>nil).root?.should == true
    @c.load(:parent_id => nil, :parent_id2=>2).root?.should == true
    @c.load(:parent_id => 1, :parent_id2=>2).root?.should == false
  end

  it "should have root? return false for an new node" do
    @c.new.root?.should == false
  end

  it "should have self_and_siblings return the children of the current node's parent" do
    @ds._fetch = [[{:id=>1, :id2=>6, :parent_id=>3, :parent_id2=>7, :name=>'r'}], [{:id=>7, :id2=>9, :parent_id=>1, :parent_id2=>6, :name=>'r2'}, @o.values.dup]]
    @o.self_and_siblings.should == [@c.load(:id=>7, :id2=>9, :parent_id=>1, :parent_id2=>6, :name=>'r2'), @o] 
    sqls = @db.sqls
    sqls.length.should == 2
    ["SELECT * FROM nodes WHERE ((id = 1) AND (id2 = 6)) LIMIT 1", "SELECT * FROM nodes WHERE ((id2 = 6) AND (id = 1)) LIMIT 1"].should include(sqls[0])
    sqls[1].should == "SELECT * FROM nodes WHERE ((nodes.parent_id = 1) AND (nodes.parent_id2 = 6))"
  end

  it "should have siblings return the children of the current node's parent, except for the current node" do
    @ds._fetch = [[{:id=>1, :id2=>6, :parent_id=>3, :parent_id2=>7, :name=>'r'}], [{:id=>7, :id2=>9, :parent_id=>1, :parent_id2=>6, :name=>'r2'}, @o.values.dup]]
    @o.siblings.should == [@c.load(:id=>7, :id2=>9, :parent_id=>1, :parent_id2=>6, :name=>'r2')] 
    sqls = @db.sqls
    sqls.length.should == 2
    ["SELECT * FROM nodes WHERE ((id = 1) AND (id2 = 6)) LIMIT 1", "SELECT * FROM nodes WHERE ((id2 = 6) AND (id = 1)) LIMIT 1"].should include(sqls[0])
    sqls[1].should == "SELECT * FROM nodes WHERE ((nodes.parent_id = 1) AND (nodes.parent_id2 = 6))"
  end

  describe ":single_root option" do
    before do
      @c = klass(:single_root => true)
    end

    it "prevents creating a second root" do
      @c.dataset._fetch = [{:id=>1, :id2=>6, :parent_id=>nil, :parent_id2=>nil, :name=>'r'}]
      lambda { @c.create }.should raise_error(Sequel::Plugins::Tree::TreeMultipleRootError)
      @c.dataset._fetch = [{:id=>1, :id2=>6, :parent_id=>1, :parent_id2=>nil, :name=>'r'}]
      lambda { @c.create(:parent_id2=>1) }.should raise_error(Sequel::Plugins::Tree::TreeMultipleRootError)
      @c.dataset._fetch = [{:id=>1, :id2=>6, :parent_id=>nil, :parent_id2=>2, :name=>'r'}]
      lambda { @c.create(:parent_id=>2) }.should raise_error(Sequel::Plugins::Tree::TreeMultipleRootError)
    end

    it "errors when promoting an existing record to a second root" do
      @c.dataset._fetch = [{:id=>1, :id2=>6, :parent_id=>nil, :parent_id2=>nil, :name=>'r'}]
      lambda { @c.load(:id => 2, :id2=>7, :parent_id => 1, :parent_id2=>2).update(:parent_id => nil, :parent_id2=>nil) }.should raise_error(Sequel::Plugins::Tree::TreeMultipleRootError)
      @c.dataset._fetch = [{:id=>1, :id2=>6, :parent_id=>1, :parent_id2=>nil, :name=>'r'}]
      lambda { @c.load(:id => 2, :id2=>7, :parent_id => 1, :parent_id2=>2).update(:parent_id => nil) }.should raise_error(Sequel::Plugins::Tree::TreeMultipleRootError)
      @c.dataset._fetch = [{:id=>1, :id2=>6, :parent_id=>nil, :parent_id2=>2, :name=>'r'}]
      lambda { @c.load(:id => 2, :id2=>7, :parent_id => 1, :parent_id2=>2).update(:parent_id2 => nil) }.should raise_error(Sequel::Plugins::Tree::TreeMultipleRootError)
    end

    it "allows updating existing root" do
      @c.dataset._fetch = {:id=>1, :id2=>6, :parent_id=>nil, :parent_id2=>nil, :name=>'r'}
      lambda { @c.root.update(:name => 'fdsa') }.should_not raise_error
      @c.dataset._fetch = {:id=>1, :id2=>6, :parent_id=>1, :parent_id2=>nil, :name=>'r'}
      lambda { @c.root.update(:name => 'fdsa') }.should_not raise_error
      @c.dataset._fetch = {:id=>1, :id2=>6, :parent_id=>nil, :parent_id2=>2, :name=>'r'}
      lambda { @c.root.update(:name => 'fdsa') }.should_not raise_error
    end
  end
end
