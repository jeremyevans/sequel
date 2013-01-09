require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model, "rcte_tree" do
  before do
    @c = Class.new(Sequel::Model(MODEL_DB[:nodes]))
    @c.class_eval do
      def self.name; 'Node'; end
      columns :id, :name, :parent_id, :i, :pi
    end
    @ds = @c.dataset
    @o = @c.load(:id=>2, :parent_id=>1, :name=>'AA', :i=>3, :pi=>4)
    MODEL_DB.reset
  end

  it "should define the correct associations" do
    @c.plugin :rcte_tree
    @c.associations.sort_by{|x| x.to_s}.should == [:ancestors, :children, :descendants, :parent]
  end
  
  it "should define the correct associations when giving options" do
    @c.plugin :rcte_tree, :ancestors=>{:name=>:as}, :children=>{:name=>:cs}, :descendants=>{:name=>:ds}, :parent=>{:name=>:p}
    @c.associations.sort_by{|x| x.to_s}.should == [:as, :cs, :ds, :p]
  end

  it "should use the correct SQL for lazy associations" do
    @c.plugin :rcte_tree
    @o.parent_dataset.sql.should == 'SELECT * FROM nodes WHERE (nodes.id = 1) LIMIT 1'
    @o.children_dataset.sql.should == 'SELECT * FROM nodes WHERE (nodes.parent_id = 2)'
    @o.ancestors_dataset.sql.should == 'WITH t AS (SELECT * FROM nodes WHERE (id = 1) UNION ALL SELECT nodes.* FROM nodes INNER JOIN t ON (t.parent_id = nodes.id)) SELECT * FROM t AS nodes'
    @o.descendants_dataset.sql.should == 'WITH t AS (SELECT * FROM nodes WHERE (parent_id = 2) UNION ALL SELECT nodes.* FROM nodes INNER JOIN t ON (t.id = nodes.parent_id)) SELECT * FROM t AS nodes'
  end
  
  it "should use the correct SQL for lazy associations when recursive CTEs require column aliases" do
    @c.dataset.meta_def(:recursive_cte_requires_column_aliases?){true}
    @c.plugin :rcte_tree
    @o.parent_dataset.sql.should == 'SELECT * FROM nodes WHERE (nodes.id = 1) LIMIT 1'
    @o.children_dataset.sql.should == 'SELECT * FROM nodes WHERE (nodes.parent_id = 2)'
    @o.ancestors_dataset.sql.should == 'WITH t(id, name, parent_id, i, pi) AS (SELECT id, name, parent_id, i, pi FROM nodes WHERE (id = 1) UNION ALL SELECT nodes.id, nodes.name, nodes.parent_id, nodes.i, nodes.pi FROM nodes INNER JOIN t ON (t.parent_id = nodes.id)) SELECT * FROM t AS nodes'
    @o.descendants_dataset.sql.should == 'WITH t(id, name, parent_id, i, pi) AS (SELECT id, name, parent_id, i, pi FROM nodes WHERE (parent_id = 2) UNION ALL SELECT nodes.id, nodes.name, nodes.parent_id, nodes.i, nodes.pi FROM nodes INNER JOIN t ON (t.id = nodes.parent_id)) SELECT * FROM t AS nodes'
  end
  
  it "should use the correct SQL for lazy associations when giving options" do
    @c.plugin :rcte_tree, :primary_key=>:i, :key=>:pi, :cte_name=>:cte, :order=>:name, :ancestors=>{:name=>:as}, :children=>{:name=>:cs}, :descendants=>{:name=>:ds}, :parent=>{:name=>:p}
    @o.p_dataset.sql.should == 'SELECT * FROM nodes WHERE (nodes.i = 4) ORDER BY name LIMIT 1'
    @o.cs_dataset.sql.should == 'SELECT * FROM nodes WHERE (nodes.pi = 3) ORDER BY name'
    @o.as_dataset.sql.should == 'WITH cte AS (SELECT * FROM nodes WHERE (i = 4) UNION ALL SELECT nodes.* FROM nodes INNER JOIN cte ON (cte.pi = nodes.i)) SELECT * FROM cte AS nodes ORDER BY name'
    @o.ds_dataset.sql.should == 'WITH cte AS (SELECT * FROM nodes WHERE (pi = 3) UNION ALL SELECT nodes.* FROM nodes INNER JOIN cte ON (cte.i = nodes.pi)) SELECT * FROM cte AS nodes ORDER BY name'
  end

  it "should use the correct SQL for lazy associations with :conditions option" do
    @c.plugin :rcte_tree, :conditions => {:i => 1}
    @o.parent_dataset.sql.should == 'SELECT * FROM nodes WHERE ((i = 1) AND (nodes.id = 1)) LIMIT 1'
    @o.children_dataset.sql.should == 'SELECT * FROM nodes WHERE ((i = 1) AND (nodes.parent_id = 2))'
    @o.ancestors_dataset.sql.should == 'WITH t AS (SELECT * FROM nodes WHERE ((id = 1) AND (i = 1)) UNION ALL SELECT nodes.* FROM nodes INNER JOIN t ON (t.parent_id = nodes.id) WHERE (i = 1)) SELECT * FROM t AS nodes WHERE (i = 1)'
    @o.descendants_dataset.sql.should == 'WITH t AS (SELECT * FROM nodes WHERE ((parent_id = 2) AND (i = 1)) UNION ALL SELECT nodes.* FROM nodes INNER JOIN t ON (t.id = nodes.parent_id) WHERE (i = 1)) SELECT * FROM t AS nodes WHERE (i = 1)'
  end
  
  it "should add all parent associations when lazily loading ancestors" do
    @c.plugin :rcte_tree
    @ds._fetch = [[{:id=>1, :name=>'A', :parent_id=>3}, {:id=>4, :name=>'B', :parent_id=>nil}, {:id=>3, :name=>'?', :parent_id=>4}]]
    @o.ancestors.should == [@c.load(:id=>1, :name=>'A', :parent_id=>3), @c.load(:id=>4, :name=>'B', :parent_id=>nil), @c.load(:id=>3, :name=>'?', :parent_id=>4)]
    @o.associations[:parent].should == @c.load(:id=>1, :name=>'A', :parent_id=>3)
    @o.associations[:parent].associations[:parent].should == @c.load(:id=>3, :name=>'?', :parent_id=>4)
    @o.associations[:parent].associations[:parent].associations[:parent].should == @c.load(:id=>4, :name=>'B', :parent_id=>nil)
    @o.associations[:parent].associations[:parent].associations[:parent].associations.fetch(:parent, 1).should == nil
  end
  
  it "should add all parent associations when lazily loading ancestors and giving options" do
    @c.plugin :rcte_tree, :primary_key=>:i, :key=>:pi, :ancestors=>{:name=>:as}, :parent=>{:name=>:p}
    @ds._fetch = [[{:i=>4, :name=>'A', :pi=>5}, {:i=>6, :name=>'B', :pi=>nil}, {:i=>5, :name=>'?', :pi=>6}]]
    @o.as.should == [@c.load(:i=>4, :name=>'A', :pi=>5), @c.load(:i=>6, :name=>'B', :pi=>nil), @c.load(:i=>5, :name=>'?', :pi=>6)]
    @o.associations[:p].should == @c.load(:i=>4, :name=>'A', :pi=>5)
    @o.associations[:p].associations[:p].should == @c.load(:i=>5, :name=>'?', :pi=>6)
    @o.associations[:p].associations[:p].associations[:p].should == @c.load(:i=>6, :name=>'B', :pi=>nil)
    @o.associations[:p].associations[:p].associations[:p].associations.fetch(:p, 1).should == nil
  end
  
  it "should add all children associations when lazily loading descendants" do
    @c.plugin :rcte_tree
    @ds._fetch = [[{:id=>3, :name=>'??', :parent_id=>1}, {:id=>1, :name=>'A', :parent_id=>2}, {:id=>4, :name=>'B', :parent_id=>2}, {:id=>5, :name=>'?', :parent_id=>3}]]
    @o.descendants.should == [@c.load(:id=>3, :name=>'??', :parent_id=>1), @c.load(:id=>1, :name=>'A', :parent_id=>2), @c.load(:id=>4, :name=>'B', :parent_id=>2), @c.load(:id=>5, :name=>'?', :parent_id=>3)]
    @o.associations[:children].should == [@c.load(:id=>1, :name=>'A', :parent_id=>2), @c.load(:id=>4, :name=>'B', :parent_id=>2)]
    @o.associations[:children].map{|c1| c1.associations[:children]}.should == [[@c.load(:id=>3, :name=>'??', :parent_id=>1)], []]
    @o.associations[:children].map{|c1| c1.associations[:children].map{|c2| c2.associations[:children]}}.should == [[[@c.load(:id=>5, :name=>'?', :parent_id=>3)]], []]
    @o.associations[:children].map{|c1| c1.associations[:children].map{|c2| c2.associations[:children].map{|c3| c3.associations[:children]}}}.should == [[[[]]], []]
  end
  
  it "should add all children associations when lazily loading descendants and giving options" do
    @c.plugin :rcte_tree, :primary_key=>:i, :key=>:pi, :children=>{:name=>:cs}, :descendants=>{:name=>:ds}
    @ds._fetch = [[{:i=>7, :name=>'??', :pi=>5}, {:i=>5, :name=>'A', :pi=>3}, {:i=>6, :name=>'B', :pi=>3}, {:i=>8, :name=>'?', :pi=>7}]]
    @o.ds.should == [@c.load(:i=>7, :name=>'??', :pi=>5), @c.load(:i=>5, :name=>'A', :pi=>3), @c.load(:i=>6, :name=>'B', :pi=>3), @c.load(:i=>8, :name=>'?', :pi=>7)]
    @o.associations[:cs].should == [@c.load(:i=>5, :name=>'A', :pi=>3), @c.load(:i=>6, :name=>'B', :pi=>3)]
    @o.associations[:cs].map{|c1| c1.associations[:cs]}.should == [[@c.load(:i=>7, :name=>'??', :pi=>5)], []]
    @o.associations[:cs].map{|c1| c1.associations[:cs].map{|c2| c2.associations[:cs]}}.should == [[[@c.load(:i=>8, :name=>'?', :pi=>7)]], []]
    @o.associations[:cs].map{|c1| c1.associations[:cs].map{|c2| c2.associations[:cs].map{|c3| c3.associations[:cs]}}}.should == [[[[]]], []]
  end
  
  it "should eagerly load ancestors" do
    @c.plugin :rcte_tree
    @ds._fetch = [[{:id=>2, :parent_id=>1, :name=>'AA'}, {:id=>6, :parent_id=>2, :name=>'C'}, {:id=>7, :parent_id=>1, :name=>'D'}, {:id=>9, :parent_id=>nil, :name=>'E'}],
      [{:id=>2, :name=>'AA', :parent_id=>1, :x_root_x=>2},
       {:id=>1, :name=>'00', :parent_id=>8, :x_root_x=>1}, {:id=>1, :name=>'00', :parent_id=>8, :x_root_x=>2},
       {:id=>8, :name=>'?', :parent_id=>nil, :x_root_x=>2}, {:id=>8, :name=>'?', :parent_id=>nil, :x_root_x=>1}]]
    os = @ds.eager(:ancestors).all
    sqls = MODEL_DB.sqls
    sqls.first.should == "SELECT * FROM nodes"
    sqls.last.should =~ /WITH t AS \(SELECT id AS x_root_x, nodes\.\* FROM nodes WHERE \(id IN \([12], [12]\)\) UNION ALL SELECT t\.x_root_x, nodes\.\* FROM nodes INNER JOIN t ON \(t\.parent_id = nodes\.id\)\) SELECT \* FROM t AS nodes/
    os.should == [@c.load(:id=>2, :parent_id=>1, :name=>'AA'), @c.load(:id=>6, :parent_id=>2, :name=>'C'), @c.load(:id=>7, :parent_id=>1, :name=>'D'), @c.load(:id=>9, :parent_id=>nil, :name=>'E')]
    os.map{|o| o.ancestors}.should == [[@c.load(:id=>1, :name=>'00', :parent_id=>8), @c.load(:id=>8, :name=>'?', :parent_id=>nil)],
      [@c.load(:id=>2, :name=>'AA', :parent_id=>1), @c.load(:id=>1, :name=>'00', :parent_id=>8), @c.load(:id=>8, :name=>'?', :parent_id=>nil)],
      [@c.load(:id=>1, :name=>'00', :parent_id=>8), @c.load(:id=>8, :name=>'?', :parent_id=>nil)],
      []]
    os.map{|o| o.parent}.should == [@c.load(:id=>1, :name=>'00', :parent_id=>8), @c.load(:id=>2, :name=>'AA', :parent_id=>1), @c.load(:id=>1, :name=>'00', :parent_id=>8), nil]
    os.map{|o| o.parent.parent if o.parent}.should == [@c.load(:id=>8, :name=>'?', :parent_id=>nil), @c.load(:id=>1, :name=>'00', :parent_id=>8), @c.load(:id=>8, :name=>'?', :parent_id=>nil), nil]
    os.map{|o| o.parent.parent.parent if o.parent and o.parent.parent}.should == [nil, @c.load(:id=>8, :name=>'?', :parent_id=>nil), nil, nil]
    os.map{|o| o.parent.parent.parent.parent if o.parent and o.parent.parent and o.parent.parent.parent}.should == [nil, nil, nil, nil]
    MODEL_DB.sqls.should == []
  end
  
  it "should eagerly load ancestors when giving options" do
    @c.plugin :rcte_tree, :primary_key=>:i, :key=>:pi, :key_alias=>:kal, :cte_name=>:cte, :ancestors=>{:name=>:as}, :parent=>{:name=>:p}
    @ds._fetch = [[{:i=>2, :pi=>1, :name=>'AA'}, {:i=>6, :pi=>2, :name=>'C'}, {:i=>7, :pi=>1, :name=>'D'}, {:i=>9, :pi=>nil, :name=>'E'}],
      [{:i=>2, :name=>'AA', :pi=>1, :kal=>2},
       {:i=>1, :name=>'00', :pi=>8, :kal=>1}, {:i=>1, :name=>'00', :pi=>8, :kal=>2},
       {:i=>8, :name=>'?', :pi=>nil, :kal=>2}, {:i=>8, :name=>'?', :pi=>nil, :kal=>1}]]
    os = @ds.eager(:as).all
    sqls = MODEL_DB.sqls
    sqls.first.should == "SELECT * FROM nodes"
    sqls.last.should =~ /WITH cte AS \(SELECT i AS kal, nodes\.\* FROM nodes WHERE \(i IN \([12], [12]\)\) UNION ALL SELECT cte\.kal, nodes\.\* FROM nodes INNER JOIN cte ON \(cte\.pi = nodes\.i\)\) SELECT \* FROM cte/
    os.should == [@c.load(:i=>2, :pi=>1, :name=>'AA'), @c.load(:i=>6, :pi=>2, :name=>'C'), @c.load(:i=>7, :pi=>1, :name=>'D'), @c.load(:i=>9, :pi=>nil, :name=>'E')]
    os.map{|o| o.as}.should == [[@c.load(:i=>1, :name=>'00', :pi=>8), @c.load(:i=>8, :name=>'?', :pi=>nil)],
      [@c.load(:i=>2, :name=>'AA', :pi=>1), @c.load(:i=>1, :name=>'00', :pi=>8), @c.load(:i=>8, :name=>'?', :pi=>nil)],
      [@c.load(:i=>1, :name=>'00', :pi=>8), @c.load(:i=>8, :name=>'?', :pi=>nil)],
      []]
    os.map{|o| o.p}.should == [@c.load(:i=>1, :name=>'00', :pi=>8), @c.load(:i=>2, :name=>'AA', :pi=>1), @c.load(:i=>1, :name=>'00', :pi=>8), nil]
    os.map{|o| o.p.p if o.p}.should == [@c.load(:i=>8, :name=>'?', :pi=>nil), @c.load(:i=>1, :name=>'00', :pi=>8), @c.load(:i=>8, :name=>'?', :pi=>nil), nil]
    os.map{|o| o.p.p.p if o.p and o.p.p}.should == [nil, @c.load(:i=>8, :name=>'?', :pi=>nil), nil, nil]
    os.map{|o| o.p.p.p.p if o.p and o.p.p and o.p.p.p}.should == [nil, nil, nil, nil]
  end

  it "should eagerly load ancestors respecting association option :conditions" do
    @c.plugin :rcte_tree, :conditions => {:i => 1}
    @ds._fetch = [[{:id=>2, :parent_id=>1, :name=>'AA'}, {:id=>6, :parent_id=>2, :name=>'C'}, {:id=>7, :parent_id=>1, :name=>'D'}, {:id=>9, :parent_id=>nil, :name=>'E'}],
      [{:id=>2, :name=>'AA', :parent_id=>1, :x_root_x=>2},
       {:id=>1, :name=>'00', :parent_id=>8, :x_root_x=>1}, {:id=>1, :name=>'00', :parent_id=>8, :x_root_x=>2},
       {:id=>8, :name=>'?', :parent_id=>nil, :x_root_x=>2}, {:id=>8, :name=>'?', :parent_id=>nil, :x_root_x=>1}]]
    os = @ds.eager(:ancestors).all
    sqls = MODEL_DB.sqls
    sqls.first.should == "SELECT * FROM nodes"
    sqls.last.should =~ /WITH t AS \(SELECT id AS x_root_x, nodes\.\* FROM nodes WHERE \(\(id IN \([12], [12]\)\) AND \(i = 1\)\) UNION ALL SELECT t\.x_root_x, nodes\.\* FROM nodes INNER JOIN t ON \(t\.parent_id = nodes\.id\) WHERE \(i = 1\)\) SELECT \* FROM t AS nodes WHERE \(i = 1\)/
  end

  it "should eagerly load descendants" do
    @c.plugin :rcte_tree
    @ds._fetch = [[{:id=>2, :parent_id=>1, :name=>'AA'}, {:id=>6, :parent_id=>2, :name=>'C'}, {:id=>7, :parent_id=>1, :name=>'D'}],
      [{:id=>6, :parent_id=>2, :name=>'C', :x_root_x=>2}, {:id=>9, :parent_id=>2, :name=>'E', :x_root_x=>2},
       {:id=>3, :name=>'00', :parent_id=>6, :x_root_x=>6}, {:id=>3, :name=>'00', :parent_id=>6, :x_root_x=>2},
       {:id=>4, :name=>'?', :parent_id=>7, :x_root_x=>7}, {:id=>5, :name=>'?', :parent_id=>4, :x_root_x=>7}]]
    os = @ds.eager(:descendants).all
    sqls = MODEL_DB.sqls
    sqls.first.should == "SELECT * FROM nodes"
    sqls.last.should =~ /WITH t AS \(SELECT parent_id AS x_root_x, nodes\.\* FROM nodes WHERE \(parent_id IN \([267], [267], [267]\)\) UNION ALL SELECT t\.x_root_x, nodes\.\* FROM nodes INNER JOIN t ON \(t\.id = nodes\.parent_id\)\) SELECT \* FROM t AS nodes/
    os.should == [@c.load(:id=>2, :parent_id=>1, :name=>'AA'), @c.load(:id=>6, :parent_id=>2, :name=>'C'), @c.load(:id=>7, :parent_id=>1, :name=>'D')]
    os.map{|o| o.descendants}.should == [[@c.load(:id=>6, :parent_id=>2, :name=>'C'), @c.load(:id=>9, :parent_id=>2, :name=>'E'), @c.load(:id=>3, :name=>'00', :parent_id=>6)],
      [@c.load(:id=>3, :name=>'00', :parent_id=>6)],
      [@c.load(:id=>4, :name=>'?', :parent_id=>7), @c.load(:id=>5, :name=>'?', :parent_id=>4)]]
    os.map{|o| o.children}.should == [[@c.load(:id=>6, :parent_id=>2, :name=>'C'), @c.load(:id=>9, :parent_id=>2, :name=>'E')], [@c.load(:id=>3, :name=>'00', :parent_id=>6)], [@c.load(:id=>4, :name=>'?', :parent_id=>7)]]
    os.map{|o1| o1.children.map{|o2| o2.children}}.should == [[[@c.load(:id=>3, :name=>'00', :parent_id=>6)], []], [[]], [[@c.load(:id=>5, :name=>'?', :parent_id=>4)]]]
    os.map{|o1| o1.children.map{|o2| o2.children.map{|o3| o3.children}}}.should == [[[[]], []], [[]], [[[]]]]
    MODEL_DB.sqls.should == []
  end
  
  it "should eagerly load descendants when giving options" do
    @c.plugin :rcte_tree, :primary_key=>:i, :key=>:pi, :key_alias=>:kal, :cte_name=>:cte, :children=>{:name=>:cs}, :descendants=>{:name=>:ds}
    @ds._fetch = [[{:i=>2, :pi=>1, :name=>'AA'}, {:i=>6, :pi=>2, :name=>'C'}, {:i=>7, :pi=>1, :name=>'D'}],
      [{:i=>6, :pi=>2, :name=>'C', :kal=>2}, {:i=>9, :pi=>2, :name=>'E', :kal=>2},
       {:i=>3, :name=>'00', :pi=>6, :kal=>6}, {:i=>3, :name=>'00', :pi=>6, :kal=>2},
       {:i=>4, :name=>'?', :pi=>7, :kal=>7}, {:i=>5, :name=>'?', :pi=>4, :kal=>7}]]
    os = @ds.eager(:ds).all
    sqls = MODEL_DB.sqls
    sqls.first.should == "SELECT * FROM nodes"
    sqls.last.should =~ /WITH cte AS \(SELECT pi AS kal, nodes\.\* FROM nodes WHERE \(pi IN \([267], [267], [267]\)\) UNION ALL SELECT cte\.kal, nodes\.\* FROM nodes INNER JOIN cte ON \(cte\.i = nodes\.pi\)\) SELECT \* FROM cte/
    os.should == [@c.load(:i=>2, :pi=>1, :name=>'AA'), @c.load(:i=>6, :pi=>2, :name=>'C'), @c.load(:i=>7, :pi=>1, :name=>'D')]
    os.map{|o| o.ds}.should == [[@c.load(:i=>6, :pi=>2, :name=>'C'), @c.load(:i=>9, :pi=>2, :name=>'E'), @c.load(:i=>3, :name=>'00', :pi=>6)],
      [@c.load(:i=>3, :name=>'00', :pi=>6)],
      [@c.load(:i=>4, :name=>'?', :pi=>7), @c.load(:i=>5, :name=>'?', :pi=>4)]]
    os.map{|o| o.cs}.should == [[@c.load(:i=>6, :pi=>2, :name=>'C'), @c.load(:i=>9, :pi=>2, :name=>'E')], [@c.load(:i=>3, :name=>'00', :pi=>6)], [@c.load(:i=>4, :name=>'?', :pi=>7)]]
    os.map{|o1| o1.cs.map{|o2| o2.cs}}.should == [[[@c.load(:i=>3, :name=>'00', :pi=>6)], []], [[]], [[@c.load(:i=>5, :name=>'?', :pi=>4)]]]
    os.map{|o1| o1.cs.map{|o2| o2.cs.map{|o3| o3.cs}}}.should == [[[[]], []], [[]], [[[]]]]
    MODEL_DB.sqls.should == []
  end
  
  it "should eagerly load descendants to a given level" do
    @c.plugin :rcte_tree
    @ds._fetch = [[{:id=>2, :parent_id=>1, :name=>'AA'}, {:id=>6, :parent_id=>2, :name=>'C'}, {:id=>7, :parent_id=>1, :name=>'D'}],
      [{:id=>6, :parent_id=>2, :name=>'C', :x_root_x=>2, :x_level_x=>0}, {:id=>9, :parent_id=>2, :name=>'E', :x_root_x=>2, :x_level_x=>0},
       {:id=>3, :name=>'00', :parent_id=>6, :x_root_x=>6, :x_level_x=>0}, {:id=>3, :name=>'00', :parent_id=>6, :x_root_x=>2, :x_level_x=>1},
       {:id=>4, :name=>'?', :parent_id=>7, :x_root_x=>7, :x_level_x=>0}, {:id=>5, :name=>'?', :parent_id=>4, :x_root_x=>7, :x_level_x=>1}]]
    os = @ds.eager(:descendants=>2).all
    sqls = MODEL_DB.sqls
    sqls.first.should == "SELECT * FROM nodes"
    sqls.last.should =~ /WITH t AS \(SELECT parent_id AS x_root_x, nodes\.\*, 0 AS x_level_x FROM nodes WHERE \(parent_id IN \([267], [267], [267]\)\) UNION ALL SELECT t\.x_root_x, nodes\.\*, \(t\.x_level_x \+ 1\) AS x_level_x FROM nodes INNER JOIN t ON \(t\.id = nodes\.parent_id\) WHERE \(t\.x_level_x < 1\)\) SELECT \* FROM t AS nodes/
    os.should == [@c.load(:id=>2, :parent_id=>1, :name=>'AA'), @c.load(:id=>6, :parent_id=>2, :name=>'C'), @c.load(:id=>7, :parent_id=>1, :name=>'D')]
    os.map{|o| o.descendants}.should == [[@c.load(:id=>6, :parent_id=>2, :name=>'C'), @c.load(:id=>9, :parent_id=>2, :name=>'E'), @c.load(:id=>3, :name=>'00', :parent_id=>6)],
      [@c.load(:id=>3, :name=>'00', :parent_id=>6)],
      [@c.load(:id=>4, :name=>'?', :parent_id=>7), @c.load(:id=>5, :name=>'?', :parent_id=>4)]]
    os.map{|o| o.associations[:children]}.should == [[@c.load(:id=>6, :parent_id=>2, :name=>'C'), @c.load(:id=>9, :parent_id=>2, :name=>'E')], [@c.load(:id=>3, :name=>'00', :parent_id=>6)], [@c.load(:id=>4, :name=>'?', :parent_id=>7)]]
    os.map{|o1| o1.associations[:children].map{|o2| o2.associations[:children]}}.should == [[[@c.load(:id=>3, :name=>'00', :parent_id=>6)], []], [[]], [[@c.load(:id=>5, :name=>'?', :parent_id=>4)]]]
    os.map{|o1| o1.associations[:children].map{|o2| o2.associations[:children].map{|o3| o3.associations[:children]}}}.should == [[[[]], []], [[]], [[nil]]]
    MODEL_DB.sqls.should == []
  end
  
  it "should eagerly load descendants to a given level when giving options" do
    @c.plugin :rcte_tree, :primary_key=>:i, :key=>:pi, :key_alias=>:kal, :level_alias=>:lal, :cte_name=>:cte, :children=>{:name=>:cs}, :descendants=>{:name=>:ds}
    @ds._fetch = [[{:i=>2, :pi=>1, :name=>'AA'}, {:i=>6, :pi=>2, :name=>'C'}, {:i=>7, :pi=>1, :name=>'D'}],
      [{:i=>6, :pi=>2, :name=>'C', :kal=>2, :lal=>0}, {:i=>9, :pi=>2, :name=>'E', :kal=>2, :lal=>0},
       {:i=>3, :name=>'00', :pi=>6, :kal=>6, :lal=>0}, {:i=>3, :name=>'00', :pi=>6, :kal=>2, :lal=>1},
       {:i=>4, :name=>'?', :pi=>7, :kal=>7, :lal=>0}, {:i=>5, :name=>'?', :pi=>4, :kal=>7, :lal=>1}]]
    os = @ds.eager(:ds=>2).all
    sqls = MODEL_DB.sqls
    sqls.first.should == "SELECT * FROM nodes"
    sqls.last.should =~ /WITH cte AS \(SELECT pi AS kal, nodes\.\*, 0 AS lal FROM nodes WHERE \(pi IN \([267], [267], [267]\)\) UNION ALL SELECT cte\.kal, nodes\.\*, \(cte\.lal \+ 1\) AS lal FROM nodes INNER JOIN cte ON \(cte\.i = nodes\.pi\) WHERE \(cte\.lal < 1\)\) SELECT \* FROM cte/
    os.should == [@c.load(:i=>2, :pi=>1, :name=>'AA'), @c.load(:i=>6, :pi=>2, :name=>'C'), @c.load(:i=>7, :pi=>1, :name=>'D')]
    os.map{|o| o.ds}.should == [[@c.load(:i=>6, :pi=>2, :name=>'C'), @c.load(:i=>9, :pi=>2, :name=>'E'), @c.load(:i=>3, :name=>'00', :pi=>6)],
      [@c.load(:i=>3, :name=>'00', :pi=>6)],
      [@c.load(:i=>4, :name=>'?', :pi=>7), @c.load(:i=>5, :name=>'?', :pi=>4)]]
    os.map{|o| o.associations[:cs]}.should == [[@c.load(:i=>6, :pi=>2, :name=>'C'), @c.load(:i=>9, :pi=>2, :name=>'E')], [@c.load(:i=>3, :name=>'00', :pi=>6)], [@c.load(:i=>4, :name=>'?', :pi=>7)]]
    os.map{|o1| o1.associations[:cs].map{|o2| o2.associations[:cs]}}.should == [[[@c.load(:i=>3, :name=>'00', :pi=>6)], []], [[]], [[@c.load(:i=>5, :name=>'?', :pi=>4)]]]
    os.map{|o1| o1.associations[:cs].map{|o2| o2.associations[:cs].map{|o3| o3.associations[:cs]}}}.should == [[[[]], []], [[]], [[nil]]]
    MODEL_DB.sqls.should == []
  end

  it "should eagerly load descendants respecting association option :conditions" do
    @c.plugin :rcte_tree, :conditions => {:i => 1}
    @ds._fetch = [[{:id=>2, :parent_id=>1, :name=>'AA'}, {:id=>6, :parent_id=>2, :name=>'C'}, {:id=>7, :parent_id=>1, :name=>'D'}],
      [{:id=>6, :parent_id=>2, :name=>'C', :x_root_x=>2}, {:id=>9, :parent_id=>2, :name=>'E', :x_root_x=>2},
       {:id=>3, :name=>'00', :parent_id=>6, :x_root_x=>6}, {:id=>3, :name=>'00', :parent_id=>6, :x_root_x=>2},
       {:id=>4, :name=>'?', :parent_id=>7, :x_root_x=>7}, {:id=>5, :name=>'?', :parent_id=>4, :x_root_x=>7}]]
    os = @ds.eager(:descendants).all
    sqls = MODEL_DB.sqls
    sqls.first.should == "SELECT * FROM nodes"
    sqls.last.should =~ /WITH t AS \(SELECT parent_id AS x_root_x, nodes\.\* FROM nodes WHERE \(\(parent_id IN \([267], [267], [267]\)\) AND \(i = 1\)\) UNION ALL SELECT t\.x_root_x, nodes\.\* FROM nodes INNER JOIN t ON \(t\.id = nodes\.parent_id\) WHERE \(i = 1\)\) SELECT \* FROM t AS nodes WHERE \(i = 1\)/
  end
end
