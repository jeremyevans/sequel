require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model::DatasetMethods, "#destroy"  do
  before do
    @c = Class.new(Sequel::Model(:items)) do
      self::Destroyed = []
      def destroy
        model::Destroyed << self
      end
    end
    @d = @c.dataset
    @d._fetch = [{:id=>1}, {:id=>2}]
    DB.reset
  end

  it "should instantiate objects in the dataset and call destroy on each" do
    @d.destroy
    @c::Destroyed.collect{|x| x.values}.must_equal [{:id=>1}, {:id=>2}]
  end

  it "should return the number of records destroyed" do
    @d.destroy.must_equal 2
    @d._fetch = [[{:i=>1}], []]
    @d.destroy.must_equal 1
    @d.destroy.must_equal 0
  end

  it "should use a transaction if use_transactions is true for the model" do
    @c.use_transactions = true
    @d.destroy
    DB.sqls.must_equal ["BEGIN", "SELECT * FROM items", "COMMIT"]
  end

  it "should not use a transaction if use_transactions is false for the model" do
    @c.use_transactions = false
    @d.destroy
    DB.sqls.must_equal ["SELECT * FROM items"]
  end
end

describe Sequel::Model::DatasetMethods, "#to_hash"  do
  before do
    @c = Class.new(Sequel::Model(:items)) do
      set_primary_key :name
    end
    @d = @c.dataset
  end

  it "should result in a hash with primary key value keys and model object values" do
    @d._fetch = [{:name=>1}, {:name=>2}]
    h = @d.to_hash
    h.must_be_kind_of(Hash)
    a = h.to_a
    a.collect{|x| x[1].class}.must_equal [@c, @c]
    a.sort_by{|x| x[0]}.collect{|x| [x[0], x[1].values]}.must_equal [[1, {:name=>1}], [2, {:name=>2}]]
  end

  it "should result in a hash with given value keys and model object values" do
    @d._fetch = [{:name=>1, :number=>3}, {:name=>2, :number=>4}]
    h = @d.to_hash(:number)
    h.must_be_kind_of(Hash)
    a = h.to_a
    a.collect{|x| x[1].class}.must_equal [@c, @c]
    a.sort_by{|x| x[0]}.collect{|x| [x[0], x[1].values]}.must_equal [[3, {:name=>1, :number=>3}], [4, {:name=>2, :number=>4}]]
  end

  it "should raise an error if the class doesn't have a primary key" do
    @c.no_primary_key
    proc{@d.to_hash}.must_raise(Sequel::Error)
  end
end

describe Sequel::Model::DatasetMethods  do
  before do
    @c = Class.new(Sequel::Model(:items))
    @c.columns :id
    @c.db.reset
  end

  it "#join_table should allow use to use a model class when joining" do
    @c.join(Class.new(Sequel::Model(:categories)), :item_id => :id).sql.must_equal 'SELECT * FROM items INNER JOIN categories ON (categories.item_id = items.id)'
  end

  it "#join_table should handle model classes that aren't simple selects using a subselect" do
    @c.join(Class.new(Sequel::Model(DB[:categories].where(:foo=>1))), :item_id => :id).sql.must_equal 'SELECT * FROM items INNER JOIN (SELECT * FROM categories WHERE (foo = 1)) AS t1 ON (t1.item_id = items.id)'
  end

  it "#graph should allow use to use a model class when joining" do
    c = Class.new(Sequel::Model(:categories))
    c.columns :id
    @c.graph(c, :item_id => :id).sql.must_equal 'SELECT items.id, categories.id AS categories_id FROM items LEFT OUTER JOIN categories ON (categories.item_id = items.id)'
  end

  it "#insert_sql should handle a single model instance as an argument" do
    @c.dataset.insert_sql(@c.load(:id=>1)).must_equal 'INSERT INTO items (id) VALUES (1)'
  end

  it "#first should handle no primary key" do
    @c.no_primary_key
    @c.first.must_be_kind_of(@c)
    @c.db.sqls.must_equal ['SELECT * FROM items LIMIT 1']
  end

  it "#last should reverse order by primary key if not already ordered" do
    @c.last.must_be_kind_of(@c)
    @c.db.sqls.must_equal ['SELECT * FROM items ORDER BY id DESC LIMIT 1']
    @c.where(:id=>2).last(:foo=>2){{bar=>3}}.must_be_kind_of(@c)
    @c.db.sqls.must_equal ['SELECT * FROM items WHERE ((id = 2) AND (bar = 3) AND (foo = 2)) ORDER BY id DESC LIMIT 1']
  end

  it "#last should use existing order if there is one" do
    @c.order(:foo).last.must_be_kind_of(@c)
    @c.db.sqls.must_equal ['SELECT * FROM items ORDER BY foo DESC LIMIT 1']
  end

  it "#last should handle a composite primary key" do
    @c.set_primary_key [:id1, :id2]
    @c.last.must_be_kind_of(@c)
    @c.db.sqls.must_equal ['SELECT * FROM items ORDER BY id1 DESC, id2 DESC LIMIT 1']
  end

  it "#last should raise an error if no primary key" do
    @c.no_primary_key
    proc{@c.last}.must_raise(Sequel::Error)
  end

  it "#paged_each should order by primary key if not already ordered" do
    @c.paged_each{|r| r.must_be_kind_of(@c)}
    @c.db.sqls.must_equal ['BEGIN', 'SELECT * FROM items ORDER BY id LIMIT 1000 OFFSET 0', 'COMMIT']
    @c.paged_each(:rows_per_fetch=>5){|r|}
    @c.db.sqls.must_equal ['BEGIN', 'SELECT * FROM items ORDER BY id LIMIT 5 OFFSET 0', 'COMMIT']
  end

  it "#paged_each should use existing order if there is one" do
    @c.order(:foo).paged_each{|r| r.must_be_kind_of(@c)}
    @c.db.sqls.must_equal ['BEGIN', 'SELECT * FROM items ORDER BY foo LIMIT 1000 OFFSET 0', 'COMMIT']
  end

  it "#paged_each should handle a composite primary key" do
    @c.set_primary_key [:id1, :id2]
    @c.paged_each{|r| r.must_be_kind_of(@c)}
    @c.db.sqls.must_equal ['BEGIN', 'SELECT * FROM items ORDER BY id1, id2 LIMIT 1000 OFFSET 0', 'COMMIT']
  end

  it "#paged_each should raise an error if no primary key" do
    @c.no_primary_key
    proc{@c.paged_each{|r| }}.must_raise(Sequel::Error)
  end
end 
