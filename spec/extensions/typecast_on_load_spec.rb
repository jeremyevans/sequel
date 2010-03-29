require File.join(File.dirname(__FILE__), "spec_helper")

describe Sequel::Model, "TypecastOnLoad plugin" do
  before do
    @db = Sequel::Database.new({})
    def @db.schema(*args)
      [[:id, {}], [:y, {:type=>:boolean, :db_type=>'tinyint(1)'}], [:b, {:type=>:integer, :db_type=>'integer'}]]
    end
    @c = Class.new(Sequel::Model(@db[:items])) do
      include(Module.new{def _refresh(ds); values[:b] = b.to_s; self; end})
      attr_accessor :bset
      def b=(x)
        self.bset = true
        super
      end
    end
    @c.instance_eval do
      @columns = [:id, :b, :y]
      def columns; @columns; end
    end
  end 

  specify "should call setter method with value when loading the object, for all given columns" do
    @c.plugin :typecast_on_load, :b
    o = @c.load(:id=>1, :b=>"1", :y=>"0")
    o.values.should == {:id=>1, :b=>1, :y=>"0"}
    o.bset.should == true
  end

  specify "should call setter method with value when reloading the object, for all given columns" do
    @c.plugin :typecast_on_load, :b
    o = @c.new({:id=>1, :b=>"1", :y=>"0"}, true)
    o.refresh
    o.values.should == {:id=>1, :b=>1, :y=>"0"}
    o.bset.should == true
  end

  specify "should allowing setting columns separately via add_typecast_on_load_columns" do
    @c.plugin :typecast_on_load
    @c.load(:id=>1, :b=>"1", :y=>"0").values.should == {:id=>1, :b=>"1", :y=>"0"}
    @c.add_typecast_on_load_columns :b
    @c.load(:id=>1, :b=>"1", :y=>"0").values.should == {:id=>1, :b=>1, :y=>"0"}
    @c.add_typecast_on_load_columns :y
    @c.load(:id=>1, :b=>"1", :y=>"0").values.should == {:id=>1, :b=>1, :y=>false}
  end

  specify "should work with subclasses" do
    @c.plugin :typecast_on_load
    @c.load(:id=>1, :b=>"1", :y=>"0").values.should == {:id=>1, :b=>"1", :y=>"0"}

    c1 = Class.new(@c)
    @c.add_typecast_on_load_columns :b
    @c.load(:id=>1, :b=>"1", :y=>"0").values.should == {:id=>1, :b=>1, :y=>"0"}
    c1.load(:id=>1, :b=>"1", :y=>"0").values.should == {:id=>1, :b=>"1", :y=>"0"}

    c2 = Class.new(@c)
    @c.add_typecast_on_load_columns :y
    @c.load(:id=>1, :b=>"1", :y=>"0").values.should == {:id=>1, :b=>1, :y=>false}
    c2.load(:id=>1, :b=>"1", :y=>"0").values.should == {:id=>1, :b=>1, :y=>"0"}
    
    c1.add_typecast_on_load_columns :y
    c1.load(:id=>1, :b=>"1", :y=>"0").values.should == {:id=>1, :b=>"1", :y=>false}
  end

  specify "should not mark the object as modified" do
    @c.plugin :typecast_on_load, :b
    @c.load(:id=>1, :b=>"1", :y=>"0").modified?.should == false
  end
end
