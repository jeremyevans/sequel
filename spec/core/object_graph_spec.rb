require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')

describe Sequel::Dataset, "graphing" do
  before do
    @db = Sequel.mock(:columns=>proc do |sql|
      case sql
      when /points/
        [:id, :x, :y]
      when /lines|foo/
        [:id, :x, :y, :graph_id]
      else
        [:id, :name, :x, :y, :lines_x]
      end
    end)
    @ds1 = @db.from(:points)
    @ds2 = @db.from(:lines)
    @ds3 = @db.from(:graphs)
    [@ds1, @ds2, @ds3].each{|ds| ds.columns}
    @db.sqls
  end

  describe "#graph" do
    it "should not modify the current dataset's opts" do
      o1 = @ds1.opts
      o2 = o1.dup
      ds1 = @ds1.graph(@ds2, :x=>:id)
      @ds1.opts.should == o1
      @ds1.opts.should == o2
      ds1.opts.should_not == o1
    end

    it "should not modify the current dataset's opts if current dataset is already graphed" do
      ds2 = @ds1.graph(@ds2)
      proc{@ds1.graph(@ds2)}.should_not raise_error
      proc{ds2.graph(@ds3)}.should_not raise_error
      proc{ds2.graph(@ds3)}.should_not raise_error
    end

    it "should accept a simple dataset and pass the table to join" do
      ds = @ds1.graph(@ds2, :x=>:id)
      ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'
    end

    it "should use currently selected columns as the basis for the selected columns in a new graph" do
      ds = @ds1.select(:id).graph(@ds2, :x=>:id)
      ds.sql.should == 'SELECT points.id, lines.id AS lines_id, lines.x, lines.y, lines.graph_id FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'

      ds = @ds1.select(:id, :x).graph(@ds2, :x=>:id)
      ds.sql.should == 'SELECT points.id, points.x, lines.id AS lines_id, lines.x AS lines_x, lines.y, lines.graph_id FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'

      ds = @ds1.select(Sequel.identifier(:id), Sequel.qualify(:points, :x)).graph(@ds2, :x=>:id)
      ds.sql.should == 'SELECT points.id, points.x, lines.id AS lines_id, lines.x AS lines_x, lines.y, lines.graph_id FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'

      ds = @ds1.select(Sequel.identifier(:id).qualify(:points), Sequel.identifier(:x).as(:y)).graph(@ds2, :x=>:id)
      ds.sql.should == 'SELECT points.id, points.x AS y, lines.id AS lines_id, lines.x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'

      ds = @ds1.select(:id, Sequel.identifier(:x).qualify(Sequel.identifier(:points)).as(Sequel.identifier(:y))).graph(@ds2, :x=>:id)
      ds.sql.should == 'SELECT points.id, points.x AS y, lines.id AS lines_id, lines.x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'
    end

    it "should requalify currently selected columns in new graph if current dataset joins tables" do
      ds = @ds1.cross_join(:lines).select(:points__id, :lines__id___lid, :lines__x, :lines__y).graph(@ds3, :x=>:id)
      ds.sql.should == 'SELECT points.id, points.lid, points.x, points.y, graphs.id AS graphs_id, graphs.name, graphs.x AS graphs_x, graphs.y AS graphs_y, graphs.lines_x FROM (SELECT points.id, lines.id AS lid, lines.x, lines.y FROM points CROSS JOIN lines) AS points LEFT OUTER JOIN graphs ON (graphs.x = points.id)'
    end

    it "should raise error if currently selected expressions cannot be handled" do
      proc{@ds1.select(1).graph(@ds2, :x=>:id)}.should raise_error(Sequel::Error)
    end

    it "should accept a complex dataset and pass it directly to join" do
      ds = @ds1.graph(@ds2.select_all(:lines), {:x=>:id})
      ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'
    end

    it "should accept a complex dataset and pass it directly to join" do
      ds = @ds1.graph(@ds2.filter(:x=>1), {:x=>:id})
      ds.sql.should == 'SELECT points.id, points.x, points.y, t1.id AS t1_id, t1.x AS t1_x, t1.y AS t1_y, t1.graph_id FROM points LEFT OUTER JOIN (SELECT * FROM lines WHERE (x = 1)) AS t1 ON (t1.x = points.id)'
      ds = @ds1.graph(@ds2.select_all(:lines).filter(:x=>1), {:x=>:id})
      ds.sql.should == 'SELECT points.id, points.x, points.y, t1.id AS t1_id, t1.x AS t1_x, t1.y AS t1_y, t1.graph_id FROM points LEFT OUTER JOIN (SELECT lines.* FROM lines WHERE (x = 1)) AS t1 ON (t1.x = points.id)'
    end

    it "should work on from_self datasets" do
      ds = @ds1.from_self.graph(@ds2, :x=>:id)
      ds.sql.should == 'SELECT t1.id, t1.x, t1.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM (SELECT * FROM points) AS t1 LEFT OUTER JOIN lines ON (lines.x = t1.id)'
      ds = @ds1.graph(@ds2.from_self, :x=>:id)
      ds.sql.should == 'SELECT points.id, points.x, points.y, t1.id AS t1_id, t1.x AS t1_x, t1.y AS t1_y, t1.graph_id FROM points LEFT OUTER JOIN (SELECT * FROM (SELECT * FROM lines) AS t1) AS t1 ON (t1.x = points.id)'
      ds = @ds1.from_self.from_self.graph(@ds2.from_self.from_self, :x=>:id)
      ds.sql.should == 'SELECT t1.id, t1.x, t1.y, t2.id AS t2_id, t2.x AS t2_x, t2.y AS t2_y, t2.graph_id FROM (SELECT * FROM (SELECT * FROM points) AS t1) AS t1 LEFT OUTER JOIN (SELECT * FROM (SELECT * FROM (SELECT * FROM lines) AS t1) AS t1) AS t2 ON (t2.x = t1.id)'
      ds = @ds1.from(@ds1, @ds3).graph(@ds2.from_self, :x=>:id)
      ds.sql.should == 'SELECT t1.id, t1.x, t1.y, t3.id AS t3_id, t3.x AS t3_x, t3.y AS t3_y, t3.graph_id FROM (SELECT * FROM (SELECT * FROM points) AS t1, (SELECT * FROM graphs) AS t2) AS t1 LEFT OUTER JOIN (SELECT * FROM (SELECT * FROM lines) AS t1) AS t3 ON (t3.x = t1.id)'
    end

    it "should accept a symbol table name as the dataset" do
      ds = @ds1.graph(:lines, :x=>:id)
      ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'
    end

    it "should accept a schema qualified symbolic table name as the dataset" do
      ds = @ds1.graph(:schema__lines, :x=>:id)
      ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN schema.lines AS lines ON (lines.x = points.id)'
    end

    it "allows giving table alias in symbolic argument" do
      ds = @ds1.graph(:lines___sketch, :x=>:id)
      ds.sql.should == 'SELECT points.id, points.x, points.y, sketch.id AS sketch_id, sketch.x AS sketch_x, sketch.y AS sketch_y, sketch.graph_id FROM points LEFT OUTER JOIN lines AS sketch ON (sketch.x = points.id)'
      ds = @ds1.graph(:schema__lines___sketch, :x=>:id)
      ds.sql.should == 'SELECT points.id, points.x, points.y, sketch.id AS sketch_id, sketch.x AS sketch_x, sketch.y AS sketch_y, sketch.graph_id FROM points LEFT OUTER JOIN schema.lines AS sketch ON (sketch.x = points.id)'
    end

    it "should accept a SQL::Identifier as the dataset" do
      ds = @ds1.graph(Sequel.identifier(:lines), :x=>:id)
      ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'
      ds = @ds1.graph(Sequel.identifier('lines'), :x=>:id)
      ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN lines AS lines ON (lines.x = points.id)'
    end

    it "should handle a SQL::Identifier with double underscores correctly" do
      ds = @ds1.graph(Sequel.identifier(:lin__es), :x=>:id)
      ds.sql.should == 'SELECT points.id, points.x, points.y, lin__es.id AS lin__es_id, lin__es.name, lin__es.x AS lin__es_x, lin__es.y AS lin__es_y, lin__es.lines_x FROM points LEFT OUTER JOIN lin__es ON (lin__es.x = points.id)'
      ds = @ds1.from(Sequel.identifier(:poi__nts)).graph(Sequel.identifier(:lin__es), :x=>:id)
      ds.sql.should == 'SELECT poi__nts.id, poi__nts.name, poi__nts.x, poi__nts.y, poi__nts.lines_x, lin__es.id AS lin__es_id, lin__es.name AS lin__es_name, lin__es.x AS lin__es_x, lin__es.y AS lin__es_y, lin__es.lines_x AS lin__es_lines_x FROM poi__nts LEFT OUTER JOIN lin__es ON (lin__es.x = poi__nts.id)'
      ds = @ds1.from(Sequel.identifier(:poi__nts).qualify(:foo)).graph(Sequel.identifier(:lin__es).qualify(:bar), :x=>:id)
      ds.sql.should == 'SELECT foo.poi__nts.id, foo.poi__nts.x, foo.poi__nts.y, foo.poi__nts.graph_id, lin__es.id AS lin__es_id, lin__es.name, lin__es.x AS lin__es_x, lin__es.y AS lin__es_y, lin__es.lines_x FROM foo.poi__nts LEFT OUTER JOIN bar.lin__es AS lin__es ON (lin__es.x = foo.poi__nts.id)'
    end

    it "should accept a SQL::QualifiedIdentifier as the dataset" do
      ds = @ds1.graph(Sequel.qualify(:schema, :lines), :x=>:id)
      ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN schema.lines AS lines ON (lines.x = points.id)'
      ds = @ds1.graph(Sequel.qualify('schema', 'lines'), :x=>:id)
      ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN schema.lines AS lines ON (lines.x = points.id)'
      ds = @ds1.graph(Sequel.qualify(Sequel.identifier(:schema), Sequel.identifier(:lines)), :x=>:id)
      ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN schema.lines AS lines ON (lines.x = points.id)'
      ds = @ds1.graph(Sequel.qualify(Sequel.identifier('schema'), Sequel.identifier('lines')), :x=>:id)
      ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN schema.lines AS lines ON (lines.x = points.id)'
    end

    it "should handle a qualified identifier as the source" do
      ds = @ds1.from(:schema__points).graph(:lines, :x=>:id)
      ds.sql.should == 'SELECT schema.points.id, schema.points.x, schema.points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM schema.points LEFT OUTER JOIN lines ON (lines.x = schema.points.id)'
      ds = @ds1.from(Sequel.qualify(:schema, :points)).graph(:lines, :x=>:id)
      ds.sql.should == 'SELECT schema.points.id, schema.points.x, schema.points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM schema.points LEFT OUTER JOIN lines ON (lines.x = schema.points.id)'
    end

    it "should accept a SQL::AliasedExpression as the dataset" do
      ds = @ds1.graph(Sequel.as(:lines, :foo), :x=>:id)
      ds.sql.should == 'SELECT points.id, points.x, points.y, foo.id AS foo_id, foo.x AS foo_x, foo.y AS foo_y, foo.graph_id FROM points LEFT OUTER JOIN lines AS foo ON (foo.x = points.id)'
      ds = @ds1.graph(Sequel.as(:schema__lines, :foo), :x=>:id)
      ds.sql.should == 'SELECT points.id, points.x, points.y, foo.id AS foo_id, foo.x AS foo_x, foo.y AS foo_y, foo.graph_id FROM points LEFT OUTER JOIN schema.lines AS foo ON (foo.x = points.id)'
      ds = @ds1.graph(Sequel.as(Sequel.identifier(:lines), :foo), :x=>:id)
      ds.sql.should == 'SELECT points.id, points.x, points.y, foo.id AS foo_id, foo.x AS foo_x, foo.y AS foo_y, foo.graph_id FROM points LEFT OUTER JOIN lines AS foo ON (foo.x = points.id)'
      ds = @ds1.graph(Sequel.as(Sequel.qualify(:schema, :lines), :foo), :x=>:id)
      ds.sql.should == 'SELECT points.id, points.x, points.y, foo.id AS foo_id, foo.x AS foo_x, foo.y AS foo_y, foo.graph_id FROM points LEFT OUTER JOIN schema.lines AS foo ON (foo.x = points.id)'
    end

    it "should raise an error if a symbol, dataset, or model is not used" do
      proc{@ds1.graph(Object.new, :x=>:id)}.should raise_error(Sequel::Error)
    end

    it "should accept a :table_alias option" do
      ds = @ds1.graph(:lines, {:x=>:id}, :table_alias=>:planes)
      ds.sql.should == 'SELECT points.id, points.x, points.y, planes.id AS planes_id, planes.x AS planes_x, planes.y AS planes_y, planes.graph_id FROM points LEFT OUTER JOIN lines AS planes ON (planes.x = points.id)'
    end

    it "should accept a :implicit_qualifier option" do
      ds = @ds1.graph(:lines, {:x=>:id}, :implicit_qualifier=>:planes)
      ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN lines ON (lines.x = planes.id)'
    end

    it "should accept a :join_type option" do
      ds = @ds1.graph(:lines, {:x=>:id}, :join_type=>:inner)
      ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points INNER JOIN lines ON (lines.x = points.id)'
    end

    it "should accept a :join_only option" do
      ds = @ds1.graph(:lines, {:x=>:id}, :join_only=>true)
      ds.sql.should == 'SELECT * FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'
    end

    it "should not select any columns from the graphed table if :select option is false" do
      ds = @ds1.graph(:lines, {:x=>:id}, :select=>false).graph(:graphs, :id=>:graph_id)
      ds.sql.should == 'SELECT points.id, points.x, points.y, graphs.id AS graphs_id, graphs.name, graphs.x AS graphs_x, graphs.y AS graphs_y, graphs.lines_x FROM points LEFT OUTER JOIN lines ON (lines.x = points.id) LEFT OUTER JOIN graphs ON (graphs.id = lines.graph_id)'
    end

    it "should use the given columns if :select option is used" do
      ds = @ds1.graph(:lines, {:x=>:id}, :select=>[:x, :graph_id]).graph(:graphs, :id=>:graph_id)
      ds.sql.should == 'SELECT points.id, points.x, points.y, lines.x AS lines_x, lines.graph_id, graphs.id AS graphs_id, graphs.name, graphs.x AS graphs_x, graphs.y AS graphs_y, graphs.lines_x AS graphs_lines_x FROM points LEFT OUTER JOIN lines ON (lines.x = points.id) LEFT OUTER JOIN graphs ON (graphs.id = lines.graph_id)'
    end

    it "should pass all join_conditions to join_table" do
      ds = @ds1.graph(@ds2, [[:x, :id], [:y, :id]])
      ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN lines ON ((lines.x = points.id) AND (lines.y = points.id))'
    end

    it "should accept a block instead of conditions and pass it to join_table" do
      ds = @ds1.graph(@ds2){|ja, lja, js| [[Sequel.qualify(ja, :x), Sequel.qualify(lja, :id)], [Sequel.qualify(ja, :y), Sequel.qualify(lja, :id)]]}
      ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN lines ON ((lines.x = points.id) AND (lines.y = points.id))'
    end

    it "should not add columns if graph is called after set_graph_aliases" do
      ds = @ds1.set_graph_aliases([[:x,[:points, :x]], [:y,[:lines, :y]]])
      ds.sql.should == 'SELECT points.x, lines.y FROM points'
      ds = ds.graph(:lines, :x=>:id)
      ds.sql.should == 'SELECT points.x, lines.y FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'
    end

    it "should allow graphing of multiple datasets" do
      ds = @ds1.graph(@ds2, :x=>:id).graph(@ds3, :id=>:graph_id)
      ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id, graphs.id AS graphs_id, graphs.name, graphs.x AS graphs_x, graphs.y AS graphs_y, graphs.lines_x AS graphs_lines_x FROM points LEFT OUTER JOIN lines ON (lines.x = points.id) LEFT OUTER JOIN graphs ON (graphs.id = lines.graph_id)'
    end

    it "should allow graphing of the same dataset multiple times" do
      ds = @ds1.graph(@ds2, :x=>:id).graph(@ds2, {:y=>:points__id}, :table_alias=>:graph)
      ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id, graph.id AS graph_id_0, graph.x AS graph_x, graph.y AS graph_y, graph.graph_id AS graph_graph_id FROM points LEFT OUTER JOIN lines ON (lines.x = points.id) LEFT OUTER JOIN lines AS graph ON (graph.y = points.id)'
    end

    it "should raise an error if the table/table alias has already been used" do
      proc{@ds1.graph(@ds1, :x=>:id)}.should raise_error(Sequel::Error)
      proc{@ds1.graph(@ds2, :x=>:id)}.should_not raise_error
      proc{@ds1.graph(@ds2, :x=>:id).graph(@ds2, :x=>:id)}.should raise_error(Sequel::Error)
      proc{@ds1.graph(@ds2, :x=>:id).graph(@ds2, {:x=>:id}, :table_alias=>:blah)}.should_not raise_error
    end
  end

  describe "#set_graph_aliases" do
    it "should not modify the current dataset's opts" do
      o1 = @ds1.opts
      o2 = o1.dup
      ds1 = @ds1.set_graph_aliases(:x=>[:graphs,:id])
      @ds1.opts.should == o1
      @ds1.opts.should == o2
      ds1.opts.should_not == o1
    end

    it "should specify the graph mapping" do
      ds = @ds1.graph(:lines, :x=>:id)
      ds.sql.should == 'SELECT points.id, points.x, points.y, lines.id AS lines_id, lines.x AS lines_x, lines.y AS lines_y, lines.graph_id FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'
      ds = ds.set_graph_aliases(:x=>[:points, :x], :y=>[:lines, :y])
      ['SELECT points.x, lines.y FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)',
      'SELECT lines.y, points.x FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'
      ].should(include(ds.sql))
    end

    it "should allow a third entry to specify an expression to use other than the default" do
      ds = @ds1.graph(:lines, :x=>:id).set_graph_aliases(:x=>[:points, :x, 1], :y=>[:lines, :y, Sequel.function(:random)])
      ['SELECT 1 AS x, random() AS y FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)',
      'SELECT random() AS y, 1 AS x FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'
      ].should(include(ds.sql))
    end

    it "should allow a single array entry to specify a table, assuming the same column as the key" do
      ds = @ds1.graph(:lines, :x=>:id).set_graph_aliases(:x=>[:points], :y=>[:lines])
      ['SELECT points.x, lines.y FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)',
      'SELECT lines.y, points.x FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'
      ].should(include(ds.sql))
    end

    it "should allow hash values to be symbols specifying table, assuming the same column as the key" do
      ds = @ds1.graph(:lines, :x=>:id).set_graph_aliases(:x=>:points, :y=>:lines)
      ['SELECT points.x, lines.y FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)',
      'SELECT lines.y, points.x FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'
      ].should(include(ds.sql))
    end

    it "should only alias columns if necessary" do
      ds = @ds1.set_graph_aliases(:x=>[:points, :x], :y=>[:lines, :y])
      ['SELECT points.x, lines.y FROM points',
      'SELECT lines.y, points.x FROM points'
      ].should(include(ds.sql))

      ds = @ds1.set_graph_aliases(:x1=>[:points, :x], :y=>[:lines, :y])
      ['SELECT points.x AS x1, lines.y FROM points',
      'SELECT lines.y, points.x AS x1 FROM points'
      ].should(include(ds.sql))
    end
  end

  describe "#add_graph_aliases" do
    it "should not modify the current dataset's opts" do
      ds1 = @ds1.set_graph_aliases(:x=>[:graphs,:id])
      o1 = ds1.opts
      o2 = o1.dup
      ds2 = ds1.add_graph_aliases(:y=>[:blah,:id])
      ds1.opts.should == o1
      ds1.opts.should == o2
      ds2.opts.should_not == o1
    end

    it "should add columns to the graph mapping" do
      @ds1.graph(:lines, :x=>:id).set_graph_aliases(:x=>[:points, :q]).add_graph_aliases(:y=>[:lines, :r]).sql.should == 'SELECT points.q AS x, lines.r AS y FROM points LEFT OUTER JOIN lines ON (lines.x = points.id)'
    end

    it "should raise an error if called without existing graph aliases" do
      proc{@ds1.add_graph_aliases(:y=>[:lines, :r])}.should raise_error(Sequel::Error)
    end
  end

  describe "#ungraphed" do
    it "should remove the splitting of result sets into component tables" do
      @db.fetch = {:id=>1,:x=>2,:y=>3,:lines_id=>4,:lines_x=>5,:lines_y=>6,:graph_id=>7}
      @ds1.graph(@ds2, :x=>:id).ungraphed.all.should == [{:id=>1,:x=>2,:y=>3,:lines_id=>4,:lines_x=>5,:lines_y=>6,:graph_id=>7}]
    end
  end
end
