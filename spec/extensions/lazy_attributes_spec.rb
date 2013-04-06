require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")
require 'yaml'

describe "Sequel::Plugins::LazyAttributes" do
  before do
    @db = Sequel.mock
    @db.meta_def(:schema){|*a| [[:id, {:type=>:integer}], [:name,{:type=>:string}]]}
    class ::LazyAttributesModel < Sequel::Model(@db[:la])
      plugin :lazy_attributes
      set_columns([:id, :name])
      meta_def(:columns){[:id, :name]}
      lazy_attributes :name
      meta_def(:columns){[:id]}
      instance_dataset._fetch = dataset._fetch = proc do |sql|
        if sql !~ /WHERE/
          if sql =~ /name/
            [{:id=>1, :name=>'1'}, {:id=>2, :name=>'2'}]
          else
            [{:id=>1}, {:id=>2}]
          end
        else
          if sql =~ /id IN \(([\d, ]+)\)/
            $1.split(', ')
          elsif sql =~ /id = (\d)/
            [$1]
          end.map do |x|
            if sql =~ /SELECT name FROM/
              {:name=>x.to_s}
            else
              {:id=>x.to_i, :name=>x.to_s}
            end
          end
        end
      end
    end
    @c = ::LazyAttributesModel
    @ds = LazyAttributesModel.dataset
    @db.sqls
  end
  after do
    Object.send(:remove_const, :LazyAttributesModel)
  end
  
  it "should allowing adding additional lazy attributes via plugin :lazy_attributes" do
    @c.set_dataset(@ds.select(:id, :blah))
    @c.dataset.sql.should == 'SELECT id, blah FROM la'
    @c.plugin :lazy_attributes, :blah
    @c.dataset.opts[:select].should == [:id]
    @c.dataset.sql.should == 'SELECT id FROM la'
  end
  
  it "should allowing adding additional lazy attributes via lazy_attributes" do
    @c.set_dataset(@ds.select(:id, :blah))
    @c.dataset.sql.should == 'SELECT id, blah FROM la'
    @c.lazy_attributes :blah
    @c.dataset.opts[:select].should == [:id]
    @c.dataset.sql.should == 'SELECT id FROM la'
  end

  it "should remove the attributes given from the SELECT columns of the model's dataset" do
    @ds.opts[:select].should == [:id]
    @ds.sql.should == 'SELECT id FROM la'
  end

  it "should still typecast correctly in lazy loaded column setters" do
    m = @c.new
    m.name = 1
    m.name.should == '1'
  end

  it "should raise error if the model has no primary key" do
    m = @c.first
    @c.no_primary_key
    proc{m.name}.should raise_error(Sequel::Error)
  end

  it "should lazily load the attribute for a single model object" do
    m = @c.first
    m.values.should == {:id=>1}
    m.name.should == '1'
    m.values.should == {:id=>1, :name=>'1'}
    @db.sqls.should == ['SELECT id FROM la LIMIT 1', 'SELECT name FROM la WHERE (id = 1) LIMIT 1']
  end

  it "should not lazily load the attribute for a single model object if the value already exists" do
    m = @c.first
    m.values.should == {:id=>1}
    m[:name] = '1'
    m.name.should == '1'
    m.values.should == {:id=>1, :name=>'1'}
    @db.sqls.should == ['SELECT id FROM la LIMIT 1']
  end

  it "should not lazily load the attribute for a single model object if it is a new record" do
    m = @c.new
    m.values.should == {}
    m.name.should == nil
    @db.sqls.should == []
  end

  it "should eagerly load the attribute for all model objects reteived with it" do
    ms = @c.all
    ms.map{|m| m.values}.should == [{:id=>1}, {:id=>2}]
    ms.map{|m| m.name}.should == %w'1 2'
    ms.map{|m| m.values}.should == [{:id=>1, :name=>'1'}, {:id=>2, :name=>'2'}]
    @db.sqls.should == ['SELECT id FROM la', 'SELECT id, name FROM la WHERE (id IN (1, 2))']
  end

  it "should add the accessors to a module included in the class, so they can be easily overridden" do
    @c.class_eval do
      def name
        "#{super}-blah"
      end
    end
    ms = @c.all
    ms.map{|m| m.values}.should == [{:id=>1}, {:id=>2}]
    ms.map{|m| m.name}.should == %w'1-blah 2-blah'
    ms.map{|m| m.values}.should == [{:id=>1, :name=>'1'}, {:id=>2, :name=>'2'}]
    @db.sqls.should == ['SELECT id FROM la', 'SELECT id, name FROM la WHERE (id IN (1, 2))']
  end

  it "should work with the serialization plugin" do
    @c.plugin :serialization, :yaml, :name
    @c.instance_dataset._fetch = @ds._fetch = [[{:id=>1}, {:id=>2}], [{:id=>1, :name=>"--- 3\n"}, {:id=>2, :name=>"--- 6\n"}], [{:id=>1}], [{:name=>"--- 3\n"}]]
    ms = @ds.all
    ms.map{|m| m.values}.should == [{:id=>1}, {:id=>2}]
    ms.map{|m| m.name}.should == [3,6]
    ms.map{|m| m.values}.should == [{:id=>1, :name=>"--- 3\n"}, {:id=>2, :name=>"--- 6\n"}]
    ms.map{|m| m.deserialized_values}.should == [{:name=>3}, {:name=>6}]
    ms.map{|m| m.name}.should == [3,6]
    @db.sqls.should == ['SELECT id FROM la', 'SELECT id, name FROM la WHERE (id IN (1, 2))']
    m = @ds.first
    m.values.should == {:id=>1}
    m.name.should == 3
    m.values.should == {:id=>1, :name=>"--- 3\n"}
    m.deserialized_values.should == {:name=>3}
    m.name.should == 3
    @db.sqls.should == ["SELECT id FROM la LIMIT 1", "SELECT name FROM la WHERE (id = 1) LIMIT 1"]
  end
end
