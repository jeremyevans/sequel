require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model, "PgTypecastOnLoad plugin" do
  before do
    @db = Sequel.mock(:host=>'postgres', :fetch=>{:id=>1, :b=>"t", :y=>"0"}, :columns=>[:id, :b, :y], :numrows=>1)
    def @db.schema(*args)
      [[:id, {}], [:b, {:type=>:boolean, :oid=>16}], [:y, {:type=>:integer, :oid=>20}]]
    end
    @c = Class.new(Sequel::Model(@db[:items]))
    @c.plugin :pg_typecast_on_load, :b, :y
  end 

  specify "should call the database conversion proc for all given columns" do
    @c.first.values.should == {:id=>1, :b=>true, :y=>0}
  end

  specify "should call the database conversion proc with value when reloading the object, for all given columns" do
    @c.first.refresh.values.should == {:id=>1, :b=>true, :y=>0}
  end

  specify "should call the database conversion proc with value when automatically reloading the object on creation via insert_select" do
    @c.dataset.meta_def(:insert_select){|h| insert(h); first}
    @c.create.values.should == {:id=>1, :b=>true, :y=>0}
  end

  specify "should allowing setting columns separately via add_pg_typecast_on_load_columns" do
    @c = Class.new(Sequel::Model(@db[:items]))
    @c.plugin :pg_typecast_on_load
    @c.first.values.should == {:id=>1, :b=>"t", :y=>"0"}
    @c.add_pg_typecast_on_load_columns :b
    @c.first.values.should == {:id=>1, :b=>true, :y=>"0"}
    @c.add_pg_typecast_on_load_columns :y
    @c.first.values.should == {:id=>1, :b=>true, :y=>0}
  end

  specify "should work with subclasses" do
    @c = Class.new(Sequel::Model(@db[:items]))
    @c.plugin :pg_typecast_on_load
    @c.first.values.should == {:id=>1, :b=>"t", :y=>"0"}

    c1 = Class.new(@c)
    @c.add_pg_typecast_on_load_columns :b
    @c.first.values.should == {:id=>1, :b=>true, :y=>"0"}
    c1.first.values.should == {:id=>1, :b=>"t", :y=>"0"}

    c2 = Class.new(@c)
    @c.add_pg_typecast_on_load_columns :y
    @c.first.values.should == {:id=>1, :b=>true, :y=>0}
    c2.first.values.should == {:id=>1, :b=>true, :y=>"0"}
    
    c1.add_pg_typecast_on_load_columns :y
    c1.first.values.should == {:id=>1, :b=>"t", :y=>0}
  end

  specify "should not mark the object as modified" do
    @c.first.modified?.should == false
  end
end
