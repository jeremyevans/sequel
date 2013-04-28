require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "LooserTypecasting Extension" do
  before do
    @db = Sequel::Database.new({})
    def @db.schema(*args)
      [[:id, {}], [:z, {:type=>:float}], [:b, {:type=>:integer}], [:d, {:type=>:decimal}]]
    end 
    @c = Class.new(Sequel::Model(@db[:items]))
    @db.extension(:looser_typecasting)
    @c.instance_eval do
      @columns = [:id, :b, :z, :d] 
      def columns; @columns; end 
    end
  end

  specify "should not raise errors for invalid strings in integer columns" do
    @c.new(:b=>'a').b.should == 0
    @c.new(:b=>'a').b.should be_a_kind_of(Integer)
  end

  specify "should not raise errors for invalid strings in float columns" do
    @c.new(:z=>'a').z.should == 0.0
    @c.new(:z=>'a').z.should be_a_kind_of(Float)
  end

  specify "should not raise errors for invalid strings in decimal columns" do
    @c.new(:d=>'a').d.should == 0.0
    @c.new(:d=>'a').d.should be_a_kind_of(BigDecimal)
  end

  specify "should not affect conversions of other types in decimal columns" do
    @c.new(:d=>1).d.should == 1
    @c.new(:d=>1).d.should be_a_kind_of(BigDecimal)
  end
end
