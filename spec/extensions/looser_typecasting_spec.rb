require File.join(File.dirname(__FILE__), "spec_helper")

context "LooserTypecasting Extension" do
  before do
    @db = Sequel::Database.new({})
    def @db.schema(*args)
      [[:id, {}], [:y, {:type=>:float}], [:b, {:type=>:integer}]]
    end 
    @c = Class.new(Sequel::Model(@db[:items]))
    @c.instance_eval do
      @columns = [:id, :b, :y] 
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
    proc{@c.new(:y=>'a')}.should raise_error(Sequel::InvalidValue)
    @db.extend(Sequel::LooserTypecasting)
    @c.new(:y=>'a').y.should == 0.0

    o = Object.new
    def o.to_f
      1.0
    end
    @c.new(:y=>o).y.should == 1.0
  end
end
