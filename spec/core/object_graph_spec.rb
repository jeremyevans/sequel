require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')

describe Sequel::Dataset, " graphing" do
  before do
    dbc = Class.new
    @db = dbc.new
    @ds1 = Sequel::Dataset.new(@db).from(:points)
    @ds2 = Sequel::Dataset.new(@db).from(:lines)
    @ds3 = Sequel::Dataset.new(@db).from(:graphs)
    dss = {:points=>@ds1, :lines=>@ds2, :graphs=>@ds3}
    dbc.send(:define_method, :[]){|ds| dss[ds]} 
    def @ds1.columns; [:id, :x, :y] end
    def @ds2.columns; [:id, :x, :y, :graph_id] end
    def @ds3.columns; [:id, :name, :x, :y, :lines_x] end
  end

  it "#graph should not modify the current dataset's opts" do
    o1 = @ds1.opts
    o2 = o1.dup
    ds1 = @ds1.graph(@ds2, :x=>:id)
    @ds1.opts.should == o1
    @ds1.opts.should == o2
    ds1.opts.should_not == o1
  end

  it "#graph should accept a simple dataset and pass the table to join" do
    ds = @ds1.graph(@ds2, :x=>:id)
    ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'
  end

  it "#graph should accept a complex dataset and pass it directly to join" do
    ds = @ds1.graph(@ds2.filter(:x=>1), {:x=>:id})
    ds.sql.should == 'SELECT points.id, points.x, points.y, t1.id AS t1_id, t1.x AS t1_x, t1.y AS t1_y, t1.graph_id FROM points LEFT OUTER JOIN (SELECT * FROM lines WHERE (x = 1)) AS t1 ON (t1.x = points.id)'
  end

  it "#graph should work on from_self datasets" do
    ds = @ds1.from_self.graph(@ds2, :x=>:id)
    ds.sql.should == 'SELECT t1.id, t1.x, t1.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM (SELECT * FROM points) AS t1 LEFT OUTER JOIN lines ON (lines.x = t1.id)'
    ds = @ds1.graph(@ds2.from_self, :x=>:id)
    ds.sql.should == 'SELECT points.id, points.x, points.y, t1.id AS t1_id, t1.x AS t1_x, t1.y AS t1_y, t1.graph_id FROM points LEFT OUTER JOIN (SELECT * FROM (SELECT * FROM lines) AS t1) AS t1 ON (t1.x = points.id)'
    ds = @ds1.from_self.from_self.graph(@ds2.from_self.from_self, :x=>:id)
    ds.sql.should == 'SELECT t1.id, t1.x, t1.y, t2.id AS t2_id, t2.x AS t2_x, t2.y AS t2_y, t2.graph_id FROM (SELECT * FROM (SELECT * FROM points) AS t1) AS t1 LEFT OUTER JOIN (SELECT * FROM (SELECT * FROM (SELECT * FROM lines) AS t1) AS t1) AS t2 ON (t2.x = t1.id)'
    ds = @ds1.from(@ds1, @ds3).graph(@ds2.from_self, :x=>:id)
    ds.sql.should == 'SELECT t1.id, t1.x, t1.y, t3.id AS t3_id, t3.x AS t3_x, t3.y AS t3_y, t3.graph_id FROM (SELECT * FROM (SELECT * FROM points) AS t1, (SELECT * FROM graphs) AS t2) AS t1 LEFT OUTER JOIN (SELECT * FROM (SELECT * FROM lines) AS t1) AS t3 ON (t3.x = t1.id)'
  end

  it "#graph should accept a symbol table name as the dataset" do
    ds = @ds1.graph(:lines, :x=>:id)
    ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'
  end

  it "#graph should accept an object that responds to dataset as the dataset" do
    oc = Class.new
    o = oc.new
    ds = @ds2
    oc.send(:define_method, :dataset){ds} 
    ds = @ds1.graph(o, :x=>:id)
    ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'
    ds = :lines
    oc.send(:define_method, :dataset){ds} 
    ds = @ds1.graph(o, :x=>:id)
    ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'
  end

  it "#graph should raise an error if a symbol, dataset, or model is not used" do
    proc{@ds1.graph(Object.new, :x=>:id)}.should raise_error(Sequel::Error)
  end

  it "#graph should accept a :table_alias option" do
    ds = @ds1.graph(:lines, {:x=>:id}, :table_alias=>:planes)
    ds.sql.should == 'SELECT points.id, points.x, points.y, planes.id AS planes_id, planes.x AS planes_x, planes.y AS planes_y, planes.graph_id FROM points LEFT OUTER JOIN lines AS planes ON (planes.x = points.id)'
  end

  it "#graph should accept a :implicit_qualifier option" do
    ds = @ds1.graph(:lines, {:x=>:id}, :implicit_qualifier=>:planes)
    ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN lines ON (lines.x = planes.id)'
  end

  it "#graph should accept a :join_type option" do
    ds = @ds1.graph(:lines, {:x=>:id}, :join_type=>:inner)
    ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points INNER JOIN lines ON (lines.x = points.id)'
  end

  it "#graph should not select any columns from the graphed table if :select option is false" do
    ds = @ds1.graph(:lines, {:x=>:id}, :select=>false).graph(:graphs, :id=>:graph_id)
    ds.sql.should == 'SELECT points.id, points.x, points.y, graphs.id AS graphs_id, graphs.name, graphs.x AS graphs_x, graphs.y AS graphs_y, graphs.lines_x FROM points LEFT OUTER JOIN lines ON (lines.x = points.id) LEFT OUTER JOIN graphs ON (graphs.id = lines.graph_id)'
  end

  it "#graph should use the given columns if :select option is used" do
    ds = @ds1.graph(:lines, {:x=>:id}, :select=>[:x, :graph_id]).graph(:graphs, :id=>:graph_id)
    ds.sql.should == 'SELECT points.id, points.x, points.y, lines.x AS lines_x, lines.graph_id, graphs.id AS graphs_id, graphs.name, graphs.x AS graphs_x, graphs.y AS graphs_y, graphs.lines_x AS graphs_lines_x FROM points LEFT OUTER JOIN lines ON (lines.x = points.id) LEFT OUTER JOIN graphs ON (graphs.id = lines.graph_id)'
  end

  it "#graph should pass all join_conditions to join_table" do
    ds = @ds1.graph(@ds2, [[:x, :id], [:y, :id]])
    ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN lines ON ((lines.x = points.id) AND (lines.y = points.id))'
  end

  it "#graph should accept a block instead of conditions and pass it to join_table" do
    ds = @ds1.graph(@ds2){|ja, lja, js| [[:x.qualify(ja), :id.qualify(lja)], [:y.qualify(ja), :id.qualify(lja)]]}
    ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN lines ON ((lines.x = points.id) AND (lines.y = points.id))'
  end

  it "#graph should not add columns if graph is called after set_graph_aliases" do
    ds = @ds1.set_graph_aliases([[:x,[:points, :x]], [:y,[:lines, :y]]])
    ds.sql.should == 'SELECT points.x, lines.y FROM points'
    ds = ds.graph(:lines, :x=>:id)
    ds.sql.should == 'SELECT points.x, lines.y FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'
  end

  it "#graph should allow graphing of multiple datasets" do
    ds = @ds1.graph(@ds2, :x=>:id).graph(@ds3, :id=>:graph_id)
    ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id, graphs.id AS graphs_id, graphs.name, graphs.x AS graphs_x, graphs.y AS graphs_y, graphs.lines_x AS graphs_lines_x FROM points LEFT OUTER JOIN lines ON (lines.x = points.id) LEFT OUTER JOIN graphs ON (graphs.id = lines.graph_id)'
  end

  it "#graph should allow graphing of the same dataset multiple times" do
    ds = @ds1.graph(@ds2, :x=>:id).graph(@ds2, {:y=>:points__id}, :table_alias=>:graph)
    ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id, graph.id AS graph_id_0, graph.x AS graph_x, graph.y AS graph_y, graph.graph_id AS graph_graph_id FROM points LEFT OUTER JOIN lines ON (lines.x = points.id) LEFT OUTER JOIN lines AS graph ON (graph.y = points.id)'
  end

  it "#graph should raise an error if the table/table alias has already been used" do
    proc{@ds1.graph(@ds1, :x=>:id)}.should raise_error(Sequel::Error)
    proc{@ds1.graph(@ds2, :x=>:id)}.should_not raise_error
    proc{@ds1.graph(@ds2, :x=>:id).graph(@ds2, :x=>:id)}.should raise_error(Sequel::Error)
    proc{@ds1.graph(@ds2, :x=>:id).graph(@ds2, {:x=>:id}, :table_alias=>:blah)}.should_not raise_error
  end

  it "#set_graph_aliases and #add_graph_aliases should not modify the current dataset's opts" do
    o1 = @ds1.opts
    o2 = o1.dup
    ds1 = @ds1.set_graph_aliases(:x=>[:graphs,:id])
    @ds1.opts.should == o1
    @ds1.opts.should == o2
    ds1.opts.should_not == o1
    o3 = ds1.opts
    o4 = o3.dup
    ds2 = ds1.add_graph_aliases(:y=>[:blah,:id])
    ds1.opts.should == o3
    ds1.opts.should == o3
    ds2.opts.should_not == o2
  end

  it "#set_graph_aliases should specify the graph mapping" do
    ds = @ds1.graph(:lines, :x=>:id)
    ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'
    ds = ds.set_graph_aliases(:x=>[:points, :x], :y=>[:lines, :y])
    ['SELECT points.x, lines.y FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)',
    'SELECT lines.y, points.x FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'
    ].should(include(ds.sql))
  end

  it "#add_graph_aliases should add columns to the graph mapping" do
    @ds1.graph(:lines, :x=>:id).set_graph_aliases(:x=>[:points, :q]).add_graph_aliases(:y=>[:lines, :r]).sql.should == 'SELECT points.q AS x, lines.r AS y FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'
  end

  it "#set_graph_aliases should allow a third entry to specify an expression to use other than the default" do
    ds = @ds1.graph(:lines, :x=>:id).set_graph_aliases(:x=>[:points, :x, 1], :y=>[:lines, :y, :random.sql_function])
    ['SELECT 1 AS x, random() AS y FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)',
    'SELECT random() AS y, 1 AS x FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'
    ].should(include(ds.sql))
  end

  it "#set_graph_aliases should only alias columns if necessary" do
    ds = @ds1.set_graph_aliases(:x=>[:points, :x], :y=>[:lines, :y])
    ['SELECT points.x, lines.y FROM points',
    'SELECT lines.y, points.x FROM points'
    ].should(include(ds.sql))

    ds = @ds1.set_graph_aliases(:x1=>[:points, :x], :y=>[:lines, :y])
    ['SELECT points.x AS x1, lines.y FROM points',
    'SELECT lines.y, points.x AS x1 FROM points'
    ].should(include(ds.sql))
  end

  it "#graph_each should split the result set into component tables" do
    ds = @ds1.graph(@ds2, :x=>:id)
    def ds.fetch_rows(sql, &block)
      yield({:id=>1,:x=>2,:y=>3,:lines_id=>4,:lines_x=>5,:lines_y=>6,:graph_id=>7})
    end
    results = ds.all
    results.length.should == 1
    results.first.should == {:points=>{:id=>1, :x=>2, :y=>3}, :lines=>{:id=>4, :x=>5, :y=>6, :graph_id=>7}}

    ds = @ds1.graph(@ds2, :x=>:id).graph(@ds3, :id=>:graph_id)
    def ds.fetch_rows(sql, &block)
      yield({:id=>1,:x=>2,:y=>3,:lines_id=>4,:lines_x=>5,:lines_y=>6,:graph_id=>7, :graphs_id=>8, :name=>9, :graphs_x=>10, :graphs_y=>11, :graphs_lines_x=>12})
    end
    results = ds.all
    results.length.should == 1
    results.first.should == {:points=>{:id=>1, :x=>2, :y=>3}, :lines=>{:id=>4, :x=>5, :y=>6, :graph_id=>7}, :graphs=>{:id=>8, :name=>9, :x=>10, :y=>11, :lines_x=>12}}

    ds = @ds1.graph(@ds2, :x=>:id).graph(@ds2, {:y=>:points__id}, :table_alias=>:graph)
    def ds.fetch_rows(sql, &block)
      yield({:id=>1,:x=>2,:y=>3,:lines_id=>4,:lines_x=>5,:lines_y=>6,:graph_id=>7, :graph_id_0=>8, :graph_x=>9, :graph_y=>10, :graph_graph_id=>11})
    end
    results = ds.all
    results.length.should == 1
    results.first.should == {:points=>{:id=>1, :x=>2, :y=>3}, :lines=>{:id=>4, :x=>5, :y=>6, :graph_id=>7}, :graph=>{:id=>8, :x=>9, :y=>10, :graph_id=>11}}
  end

  it "#ungraphed should remove the splitting of result sets into component tables" do
    ds = @ds1.graph(@ds2, :x=>:id).ungraphed
    def ds.fetch_rows(sql, &block)
      yield({:id=>1,:x=>2,:y=>3,:lines_id=>4,:lines_x=>5,:lines_y=>6,:graph_id=>7})
    end
    ds.all.should == [{:id=>1,:x=>2,:y=>3,:lines_id=>4,:lines_x=>5,:lines_y=>6,:graph_id=>7}]
  end

  it "#graph_each should give a nil value instead of a hash when all values for a table are nil" do
    ds = @ds1.graph(@ds2, :x=>:id)
    def ds.fetch_rows(sql, &block)
      yield({:id=>1,:x=>2,:y=>3,:lines_id=>nil,:lines_x=>nil,:lines_y=>nil,:graph_id=>nil})
    end
    results = ds.all
    results.length.should == 1
    results.first.should == {:points=>{:id=>1, :x=>2, :y=>3}, :lines=>nil}

    ds = @ds1.graph(@ds2, :x=>:id).graph(@ds3, :id=>:graph_id)
    def ds.fetch_rows(sql, &block)
      yield({:id=>1,:x=>2,:y=>3,:lines_id=>4,:lines_x=>5,:lines_y=>6,:graph_id=>7, :graphs_id=>nil, :name=>nil, :graphs_x=>nil, :graphs_y=>nil, :graphs_lines_x=>nil})
      yield({:id=>2,:x=>4,:y=>5,:lines_id=>nil,:lines_x=>nil,:lines_y=>nil,:graph_id=>nil, :graphs_id=>nil, :name=>nil, :graphs_x=>nil, :graphs_y=>nil, :graphs_lines_x=>nil})
      yield({:id=>3,:x=>5,:y=>6,:lines_id=>4,:lines_x=>5,:lines_y=>6,:graph_id=>7, :graphs_id=>7, :name=>8, :graphs_x=>9, :graphs_y=>10, :graphs_lines_x=>11})
      yield({:id=>3,:x=>5,:y=>6,:lines_id=>7,:lines_x=>5,:lines_y=>8,:graph_id=>9, :graphs_id=>9, :name=>10, :graphs_x=>10, :graphs_y=>11, :graphs_lines_x=>12})
    end
    results = ds.all
    results.length.should == 4
    results[0].should == {:points=>{:id=>1, :x=>2, :y=>3}, :lines=>{:id=>4, :x=>5, :y=>6, :graph_id=>7}, :graphs=>nil}
    results[1].should == {:points=>{:id=>2, :x=>4, :y=>5}, :lines=>nil, :graphs=>nil}
    results[2].should == {:points=>{:id=>3, :x=>5, :y=>6}, :lines=>{:id=>4, :x=>5, :y=>6, :graph_id=>7}, :graphs=>{:id=>7, :name=>8, :x=>9, :y=>10, :lines_x=>11}}
    results[3].should == {:points=>{:id=>3, :x=>5, :y=>6}, :lines=>{:id=>7, :x=>5, :y=>8, :graph_id=>9}, :graphs=>{:id=>9, :name=>10, :x=>10, :y=>11, :lines_x=>12}}
  end

  it "#graph_each should not give a nil value instead of a hash when any value for a table is false" do
    ds = @ds1.graph(@ds2, :x=>:id)
    def ds.fetch_rows(sql, &block)
      block.call(:id=>1,:x=>2,:y=>3,:lines_id=>nil,:lines_x=>false,:lines_y=>nil,:graph_id=>nil)
    end
    ds.all.should == [{:points=>{:id=>1, :x=>2, :y=>3}, :lines=>{:id=>nil, :x=>false, :y=>nil, :graph_id=>nil}}]
  end

  it "#graph_each should not included tables graphed with the :select => false option in the result set" do
    ds = @ds1.graph(:lines, {:x=>:id}, :select=>false).graph(:graphs, :id=>:graph_id)
    def ds.fetch_rows(sql, &block)
      yield({:id=>1,:x=>2,:y=>3,:graphs_id=>8, :name=>9, :graphs_x=>10, :graphs_y=>11, :lines_x=>12})
    end
    results = ds.all
    results.length.should == 1
    results.first.should == {:points=>{:id=>1, :x=>2, :y=>3}, :graphs=>{:id=>8, :name=>9, :x=>10, :y=>11, :lines_x=>12}}
  end

  it "#graph_each should only include the columns selected with #set_graph_aliases and #add_graph_aliases, if called" do
    ds = @ds1.graph(:lines, :x=>:id).set_graph_aliases(:x=>[:points, :x], :y=>[:lines, :y])
    def ds.fetch_rows(sql, &block)
      yield({:x=>2,:y=>3})
    end
    results = ds.all
    results.length.should == 1
    results.first.should == {:points=>{:x=>2}, :lines=>{:y=>3}}

    ds = @ds1.graph(:lines, :x=>:id).set_graph_aliases(:x=>[:points, :x])
    def ds.fetch_rows(sql, &block)
      yield({:x=>2})
    end
    results = ds.all
    results.length.should == 1
    results.first.should == {:points=>{:x=>2}, :lines=>nil}

    ds = ds.add_graph_aliases(:q=>[:points, :r, 18])
    def ds.fetch_rows(sql, &block)
      yield({:x=>2, :q=>18})
    end
    ds.all.should == [{:points=>{:x=>2, :r=>18}, :lines=>nil}]
  end

  it "#graph_each should correctly map values when #set_graph_aliases is used with a third argument for each entry" do
    ds = @ds1.graph(:lines, :x=>:id).set_graph_aliases(:x=>[:points, :z1, 2], :y=>[:lines, :z2, :random.sql_function])
    def ds.fetch_rows(sql, &block)
      yield({:x=>2,:y=>3})
    end
    results = ds.all
    results.length.should == 1
    results.first.should == {:points=>{:z1=>2}, :lines=>{:z2=>3}}
  end

  it "#graph_each should run the row_proc for graphed datasets" do
    @ds1.row_proc = proc{|h| h.keys.each{|k| h[k] *= 2}; h}
    @ds2.row_proc = proc{|h| h.keys.each{|k| h[k] *= 3}; h}
    ds = @ds1.graph(@ds2, :x=>:id)
    def ds.fetch_rows(sql, &block)
      yield({:id=>1,:x=>2,:y=>3,:lines_id=>4,:lines_x=>5,:lines_y=>6,:graph_id=>7})
    end
    results = ds.all
    results.length.should == 1
    results.first.should == {:points=>{:id=>2, :x=>4, :y=>6}, :lines=>{:id=>12, :x=>15, :y=>18, :graph_id=>21}}
  end
end
