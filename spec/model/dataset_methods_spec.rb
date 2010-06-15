require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model::DatasetMethods, "#destroy"  do
  before do
    @c = Class.new(Sequel::Model(:items)) do
      class_variable_set(:@@destroyed, [])
      def destroy
        self.class.send(:class_variable_get, :@@destroyed) << self
      end
      def self.destroyed
        class_variable_get(:@@destroyed)
      end
    end
    @d = @c.dataset
  end

  it "should instantiate objects in the dataset and call destroy on each" do
    def @d.fetch_rows(sql)
      yield({:id=>1})
      yield({:id=>2})
    end
    @d.destroy
    @c.destroyed.collect{|x| x.values}.should == [{:id=>1}, {:id=>2}]
  end

  it "should return the number of records destroyed" do
    def @d.fetch_rows(sql)
      yield({:id=>1})
      yield({:id=>2})
    end
    @d.destroy.should == 2
    def @d.fetch_rows(sql)
      yield({:id=>1})
    end
    @d.destroy.should == 1
    def @d.fetch_rows(sql)
    end
    @d.destroy.should == 0
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
    def @d.fetch_rows(sql)
      yield({:name=>1})
      yield({:name=>2})
    end
    h = @d.to_hash
    h.should be_a_kind_of(Hash)
    a = h.to_a
    a.collect{|x| x[1].class}.should == [@c, @c]
    a.sort_by{|x| x[0]}.collect{|x| [x[0], x[1].values]}.should == [[1, {:name=>1}], [2, {:name=>2}]]
  end

  it "should result in a hash with given value keys and model object values" do
    def @d.fetch_rows(sql)
      yield({:name=>1, :number=>3})
      yield({:name=>2, :number=>4})
    end
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
