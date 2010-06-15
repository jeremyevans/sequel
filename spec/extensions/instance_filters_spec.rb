require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "instance_filters plugin" do
  before do
    @c = Class.new(Sequel::Model(:people)) do
    end
    @sql = sql = ''
    @v = v = [1]
    @c.dataset.quote_identifiers = false
    @c.dataset.meta_def(:update) do |opts|
      sql.replace(update_sql(opts))
      return v.first
    end
    @c.dataset.meta_def(:delete) do
      sql.replace(delete_sql)
      return v.first
    end
    @c.columns :id, :name, :num
    @c.plugin :instance_filters
    @p = @c.load(:id=>1, :name=>'John', :num=>1)
  end

  specify "should raise an error when updating a stale record" do
    @p.update(:name=>'Bob')
    @sql.should == "UPDATE people SET name = 'Bob' WHERE (id = 1)"
    @p.instance_filter(:name=>'Jim')
    @v.replace([0])
    proc{@p.update(:name=>'Joe')}.should raise_error(Sequel::Plugins::InstanceFilters::Error)
    @sql.should == "UPDATE people SET name = 'Joe' WHERE ((id = 1) AND (name = 'Jim'))"
  end 

  specify "should raise an error when destroying a stale record" do
    @p.destroy
    @sql.should == "DELETE FROM people WHERE (id = 1)"
    @p.instance_filter(:name=>'Jim')
    @v.replace([0])
    proc{@p.destroy}.should raise_error(Sequel::Plugins::InstanceFilters::Error)
    @sql.should == "DELETE FROM people WHERE ((id = 1) AND (name = 'Jim'))"
  end 
  
  specify "should apply all instance filters" do
    @p.instance_filter(:name=>'Jim')
    @p.instance_filter{num > 2}
    @p.update(:name=>'Bob')
    @sql.should == "UPDATE people SET name = 'Bob' WHERE ((id = 1) AND (name = 'Jim') AND (num > 2))"
  end 

  specify "should drop instance filters after updating" do
    @p.instance_filter(:name=>'Joe')
    @p.update(:name=>'Joe')
    @sql.should == "UPDATE people SET name = 'Joe' WHERE ((id = 1) AND (name = 'Joe'))"
    @p.update(:name=>'Bob')
    @sql.should == "UPDATE people SET name = 'Bob' WHERE (id = 1)"
  end
end
