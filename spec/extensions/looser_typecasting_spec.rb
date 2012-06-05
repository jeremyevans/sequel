require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "LooserTypecasting Extension" do
  before do
    @db = Sequel::Database.new({})
    def @db.schema(*args)
      [[:id, {}], [:z, {:type=>:float}], [:b, {:type=>:integer}]]
    end 
    @c = Class.new(Sequel::Model(@db[:items]))
    @c.instance_eval do
      @columns = [:id, :b, :z] 
      def columns; @columns; end 
    end
  end

  specify "Should use to_i instead of Integer() for typecasting integers" do
    proc{@c.new(:b=>'a')}.should raise_error(Sequel::InvalidValue)
    @db.extend(Sequel::LooserTypecasting)
    @c.new(:b=>'a').b.should == 0

    o = Object.new
    def o.to_i
      1
    end
    @c.new(:b=>o).b.should == 1
  end

  specify "Should use to_f instead of Float() for typecasting floats" do
    proc{@c.new(:z=>'a')}.should raise_error(Sequel::InvalidValue)
    @db.extend(Sequel::LooserTypecasting)
    @c.new(:z=>'a').z.should == 0.0

    o = Object.new
    def o.to_f
      1.0
    end
    @c.new(:z=>o).z.should == 1.0
  end
end
