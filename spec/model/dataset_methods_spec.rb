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
    MODEL_DB.reset
  end

  it "should instantiate objects in the dataset and call destroy on each" do
    @d.destroy
    @c::Destroyed.collect{|x| x.values}.should == [{:id=>1}, {:id=>2}]
  end

  it "should return the number of records destroyed" do
    @d.destroy.should == 2
    @d._fetch = [[{:i=>1}], []]
    @d.destroy.should == 1
    @d.destroy.should == 0
  end

  it "should use a transaction if use_transactions is true for the model" do
    @c.use_transactions = true
    @d.destroy
    MODEL_DB.sqls.should == ["BEGIN", "SELECT * FROM items", "COMMIT"]
  end

  it "should not use a transaction if use_transactions is false for the model" do
    @c.use_transactions = false
    @d.destroy
    MODEL_DB.sqls.should == ["SELECT * FROM items"]
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
    h.should be_a_kind_of(Hash)
    a = h.to_a
    a.collect{|x| x[1].class}.should == [@c, @c]
    a.sort_by{|x| x[0]}.collect{|x| [x[0], x[1].values]}.should == [[1, {:name=>1}], [2, {:name=>2}]]
  end

  it "should result in a hash with given value keys and model object values" do
    @d._fetch = [{:name=>1, :number=>3}, {:name=>2, :number=>4}]
    h = @d.to_hash(:number)
    h.should be_a_kind_of(Hash)
    a = h.to_a
    a.collect{|x| x[1].class}.should == [@c, @c]
    a.sort_by{|x| x[0]}.collect{|x| [x[0], x[1].values]}.should == [[3, {:name=>1, :number=>3}], [4, {:name=>2, :number=>4}]]
  end

  it "should raise an error if the class doesn't have a primary key" do
    @c.no_primary_key
    proc{@d.to_hash}.should raise_error(Sequel::Error)
  end
end

describe Sequel::Model::DatasetMethods, "#join_table"  do
  before do
    @c = Class.new(Sequel::Model(:items))
  end

  specify "should allow use to use a model class when joining" do
    @c.join(Class.new(Sequel::Model(:categories)), :item_id => :id).sql.should == 'SELECT * FROM items INNER JOIN categories ON (categories.item_id = items.id)'
  end

  specify "should handle model classes that aren't simple selects using a subselect" do
    @c.join(Class.new(Sequel::Model(MODEL_DB[:categories].where(:foo=>1))), :item_id => :id).sql.should == 'SELECT * FROM items INNER JOIN (SELECT * FROM categories WHERE (foo = 1)) AS t1 ON (t1.item_id = items.id)'
  end
end 

describe Sequel::Model::DatasetMethods, "#graph"  do
  before do
    @c = Class.new(Sequel::Model(:items))
    @c.columns :id
  end

  specify "should allow use to use a model class when joining" do
    c = Class.new(Sequel::Model(:categories))
    c.columns :id
    @c.graph(c, :item_id => :id).sql.should == 'SELECT items.id, categories.id AS categories_id FROM items LEFT OUTER JOIN categories ON (categories.item_id = items.id)'
  end
end 
