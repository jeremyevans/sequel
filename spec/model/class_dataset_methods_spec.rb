require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model, "class dataset methods"  do
  before do
    @db = Sequel.mock
    @c = Class.new(Sequel::Model(@db[:items]))
    @d = @c.dataset
    @d._fetch = {:id=>1}
    @d.autoid = 1
    @d.numrows = 0
    @db.sqls
  end

  it "should call the dataset method of the same name with the same args" do
    @c.<<({}).should == @d
    @db.sqls.should == ["INSERT INTO items DEFAULT VALUES"]
    @c.all.should == [@c.load(:id=>1)]
    @db.sqls.should == ["SELECT * FROM items"]
    @c.avg(:id).should == 1
    @db.sqls.should == ["SELECT avg(id) AS avg FROM items LIMIT 1"]
    @c.count.should == 1
    @db.sqls.should == ["SELECT count(*) AS count FROM items LIMIT 1"]
    @c.cross_join(@c).sql.should == "SELECT * FROM items CROSS JOIN items"
    @c.distinct.sql.should == "SELECT DISTINCT * FROM items"
    @c.each{|r| r.should == @c.load(:id=>1)}.should == @d
    @db.sqls.should == ["SELECT * FROM items"]
    @c.each_server{|r| r.opts[:server].should == :default}
    @c.empty?.should be_false
    @db.sqls.should == ["SELECT 1 AS one FROM items LIMIT 1"]
    @c.except(@d, :from_self=>false).sql.should == "SELECT * FROM items EXCEPT SELECT * FROM items"
    @c.exclude(:a).sql.should == "SELECT * FROM items WHERE NOT a"
    @c.exclude_having(:a).sql.should == "SELECT * FROM items HAVING NOT a"
    @c.exclude_where(:a).sql.should == "SELECT * FROM items WHERE NOT a"
    @c.fetch_rows("S"){|r| r.should == {:id=>1}}
    @db.sqls.should == ["S"]
    @c.filter(:a).sql.should == "SELECT * FROM items WHERE a"
    @c.first.should == @c.load(:id=>1)
    @db.sqls.should == ["SELECT * FROM items LIMIT 1"]
    @c.first!.should == @c.load(:id=>1)
    @db.sqls.should == ["SELECT * FROM items LIMIT 1"]
    @c.for_update.sql.should == "SELECT * FROM items FOR UPDATE"
    @c.from.sql.should == "SELECT *"
    @c.from_self.sql.should == "SELECT * FROM (SELECT * FROM items) AS t1"
    @c.full_join(@c).sql.should == "SELECT * FROM items FULL JOIN items"
    @c.full_outer_join(@c).sql.should == "SELECT * FROM items FULL OUTER JOIN items"
    @c.get(:a).should == 1
    @db.sqls.should == ["SELECT a FROM items LIMIT 1"]
    @c.graph(@c, nil, :table_alias=>:a).sql.should == "SELECT * FROM items LEFT OUTER JOIN items AS a"
    @db.sqls
    @c.grep(:id, 'a%').sql.should == "SELECT * FROM items WHERE ((id LIKE 'a%' ESCAPE '\\'))"
    @c.group(:a).sql.should == "SELECT * FROM items GROUP BY a"
    @c.group_and_count(:a).sql.should == "SELECT a, count(*) AS count FROM items GROUP BY a"
    @c.group_by(:a).sql.should == "SELECT * FROM items GROUP BY a"
    @c.having(:a).sql.should == "SELECT * FROM items HAVING a"
    @c.import([:id], [[1]])
    @db.sqls.should == ["BEGIN", "INSERT INTO items (id) VALUES (1)", "COMMIT"]
    @c.inner_join(@c).sql.should == "SELECT * FROM items INNER JOIN items"
    @c.insert.should == 2
    @db.sqls.should == ["INSERT INTO items DEFAULT VALUES"]
    @c.intersect(@d, :from_self=>false).sql.should == "SELECT * FROM items INTERSECT SELECT * FROM items"
    @c.interval(:id).should == 1
    @db.sqls.should == ["SELECT (max(id) - min(id)) AS interval FROM items LIMIT 1"]
    @c.join(@c).sql.should == "SELECT * FROM items INNER JOIN items"
    @c.join_table(:inner, @c).sql.should == "SELECT * FROM items INNER JOIN items"
    @c.last.should == @c.load(:id=>1)
    @db.sqls.should == ["SELECT * FROM items ORDER BY id DESC LIMIT 1"]
    @c.left_join(@c).sql.should == "SELECT * FROM items LEFT JOIN items"
    @c.left_outer_join(@c).sql.should == "SELECT * FROM items LEFT OUTER JOIN items"
    @c.limit(2).sql.should == "SELECT * FROM items LIMIT 2"
    @c.lock_style(:update).sql.should == "SELECT * FROM items FOR UPDATE"
    @c.map(:id).should == [1]
    @db.sqls.should == ["SELECT * FROM items"]
    @c.max(:id).should == 1
    @db.sqls.should == ["SELECT max(id) AS max FROM items LIMIT 1"]
    @c.min(:id).should == 1
    @db.sqls.should == ["SELECT min(id) AS min FROM items LIMIT 1"]
    @c.multi_insert([{:id=>1}])
    @db.sqls.should == ["BEGIN", "INSERT INTO items (id) VALUES (1)", "COMMIT"]
    @c.naked.row_proc.should == nil
    @c.natural_full_join(@c).sql.should == "SELECT * FROM items NATURAL FULL JOIN items"
    @c.natural_join(@c).sql.should == "SELECT * FROM items NATURAL JOIN items"
    @c.natural_left_join(@c).sql.should == "SELECT * FROM items NATURAL LEFT JOIN items"
    @c.natural_right_join(@c).sql.should == "SELECT * FROM items NATURAL RIGHT JOIN items"
    @c.order(:a).sql.should == "SELECT * FROM items ORDER BY a"
    @c.order_append(:a).sql.should == "SELECT * FROM items ORDER BY a"
    @c.order_by(:a).sql.should == "SELECT * FROM items ORDER BY a"
    @c.order_more(:a).sql.should == "SELECT * FROM items ORDER BY a"
    @c.order_prepend(:a).sql.should == "SELECT * FROM items ORDER BY a"
    @c.paged_each{|r| r.should == @c.load(:id=>1)}
    @db.sqls.should == ["BEGIN", "SELECT * FROM items ORDER BY id LIMIT 1000 OFFSET 0", "COMMIT"]
    @c.qualify.sql.should == 'SELECT items.* FROM items'
    @c.right_join(@c).sql.should == "SELECT * FROM items RIGHT JOIN items"
    @c.right_outer_join(@c).sql.should == "SELECT * FROM items RIGHT OUTER JOIN items"
    @c.select(:a).sql.should == "SELECT a FROM items"
    @c.select_all(:items).sql.should == "SELECT items.* FROM items"
    @c.select_append(:a).sql.should == "SELECT *, a FROM items"
    @c.select_group(:a).sql.should == "SELECT a FROM items GROUP BY a"
    @c.select_hash(:id, :id).should == {1=>1}
    @db.sqls.should == ["SELECT id, id FROM items"]
    @c.select_hash_groups(:id, :id).should == {1=>[1]}
    @db.sqls.should == ["SELECT id, id FROM items"]
    @c.select_map(:id).should == [1]
    @db.sqls.should == ["SELECT id FROM items"]
    @c.select_order_map(:id).should == [1]
    @db.sqls.should == ["SELECT id FROM items ORDER BY id"]
    @c.server(:a).opts[:server].should == :a
    @c.set_graph_aliases(:a=>:b).opts[:graph_aliases].should == {:a=>[:b, :a]}
    @c.single_record.should == @c.load(:id=>1)
    @db.sqls.should == ["SELECT * FROM items LIMIT 1"]
    @c.single_value.should == 1
    @db.sqls.should == ["SELECT * FROM items LIMIT 1"]
    @c.sum(:id).should == 1
    @db.sqls.should == ["SELECT sum(id) AS sum FROM items LIMIT 1"]
    @c.to_hash(:id, :id).should == {1=>1}
    @db.sqls.should == ["SELECT * FROM items"]
    @c.to_hash_groups(:id, :id).should == {1=>[1]}
    @db.sqls.should == ["SELECT * FROM items"]
    @c.truncate
    @db.sqls.should == ["TRUNCATE TABLE items"]
    @c.union(@d, :from_self=>false).sql.should == "SELECT * FROM items UNION SELECT * FROM items"
    @c.where(:a).sql.should == "SELECT * FROM items WHERE a"
    @c.with(:a, @d).sql.should == "WITH a AS (SELECT * FROM items) SELECT * FROM items"
    @c.with_recursive(:a, @d, @d).sql.should == "WITH a AS (SELECT * FROM items UNION ALL SELECT * FROM items) SELECT * FROM items"
    @c.with_sql('S').sql.should == "S"

    sc = Class.new(@c)
    sc.set_dataset(@d.where(:a).order(:a).select(:a).group(:a).limit(2))
    @db.sqls
    sc.invert.sql.should == 'SELECT a FROM items WHERE NOT a GROUP BY a ORDER BY a LIMIT 2'
    sc.dataset._fetch = {:v1=>1, :v2=>2}
    sc.range(:a).should == (1..2)
    @db.sqls.should == ["SELECT min(a) AS v1, max(a) AS v2 FROM (SELECT a FROM items WHERE a GROUP BY a ORDER BY a LIMIT 2) AS t1 LIMIT 1"]
    sc.reverse.sql.should == 'SELECT a FROM items WHERE a GROUP BY a ORDER BY a DESC LIMIT 2'
    sc.reverse_order.sql.should == 'SELECT a FROM items WHERE a GROUP BY a ORDER BY a DESC LIMIT 2'
    sc.select_more(:a).sql.should == 'SELECT a, a FROM items WHERE a GROUP BY a ORDER BY a LIMIT 2'
    sc.unfiltered.sql.should == 'SELECT a FROM items GROUP BY a ORDER BY a LIMIT 2'
    sc.ungrouped.sql.should == 'SELECT a FROM items WHERE a ORDER BY a LIMIT 2'
    sc.unordered.sql.should == 'SELECT a FROM items WHERE a GROUP BY a LIMIT 2'
    sc.unlimited.sql.should == 'SELECT a FROM items WHERE a GROUP BY a ORDER BY a'
    sc.dataset.graph!(:a)
    sc.dataset.ungraphed.opts[:graph].should == nil
  end
end
