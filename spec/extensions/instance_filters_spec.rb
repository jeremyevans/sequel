require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "instance_filters plugin" do
  before do
    @c = Class.new(Sequel::Model(:people))
    @c.dataset.quote_identifiers = false
    @c.columns :id, :name, :num
    @c.plugin :instance_filters
    @p = @c.load(:id=>1, :name=>'John', :num=>1)
    MODEL_DB.sqls
  end

  specify "should raise an error when updating a stale record" do
    @p.update(:name=>'Bob')
    MODEL_DB.sqls.should == ["UPDATE people SET name = 'Bob' WHERE (id = 1)"]
    @p.instance_filter(:name=>'Jim')
    @p.this.numrows = 0
    proc{@p.update(:name=>'Joe')}.should raise_error(Sequel::Plugins::InstanceFilters::Error)
    MODEL_DB.sqls.should == ["UPDATE people SET name = 'Joe' WHERE ((id = 1) AND (name = 'Jim'))"]
  end 

  specify "should raise an error when destroying a stale record" do
    @p.destroy
    MODEL_DB.sqls.should == ["DELETE FROM people WHERE id = 1"]
    @p.instance_filter(:name=>'Jim')
    @p.this.numrows = 0
    proc{@p.destroy}.should raise_error(Sequel::Plugins::InstanceFilters::Error)
    MODEL_DB.sqls.should == ["DELETE FROM people WHERE ((id = 1) AND (name = 'Jim'))"]
  end 
  
  specify "should apply all instance filters" do
    @p.instance_filter(:name=>'Jim')
    @p.instance_filter{num > 2}
    @p.update(:name=>'Bob')
    MODEL_DB.sqls.should == ["UPDATE people SET name = 'Bob' WHERE ((id = 1) AND (name = 'Jim') AND (num > 2))"]
  end 

  specify "should drop instance filters after updating" do
    @p.instance_filter(:name=>'Joe')
    @p.update(:name=>'Joe')
    MODEL_DB.sqls.should == ["UPDATE people SET name = 'Joe' WHERE ((id = 1) AND (name = 'Joe'))"]
    @p.update(:name=>'Bob')
    MODEL_DB.sqls.should == ["UPDATE people SET name = 'Bob' WHERE (id = 1)"]
  end
end
