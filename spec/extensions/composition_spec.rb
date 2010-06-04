require File.join(File.dirname(__FILE__), "spec_helper")

describe "Composition plugin" do
  before do
    @c = Class.new(Sequel::Model(:items))
    @c.plugin :composition
    @c.columns :id, :year, :month, :day
    @o = @c.load(:id=>1, :year=>1, :month=>2, :day=>3)
    MODEL_DB.reset
  end
  
  it ".composition should add compositions" do
    @o.should_not respond_to(:date)
    @c.composition :date, :mapping=>[:year, :month, :day]
    @o.date.should == Date.new(1, 2, 3)
  end

  it "loading the plugin twice should not remove existing compositions" do
    @c.composition :date, :mapping=>[:year, :month, :day]
    @c.plugin :composition
    @c.compositions.keys.should == [:date]
  end

  it ".composition should raise an error if :composer and :decomposer options are not present and :mapping option is not provided" do
    proc{@c.composition :date}.should raise_error(Sequel::Error)
    proc{@c.composition :date, :composer=>proc{}, :decomposer=>proc{}}.should_not raise_error
    proc{@c.composition :date, :mapping=>[]}.should_not raise_error
  end

  it ".compositions should return the reflection hash of compositions" do
    @c.compositions.should == {}
    @c.composition :date, :mapping=>[:year, :month, :day]
    @c.compositions.keys.should == [:date]
    r = @c.compositions.values.first
    r[:mapping].should == [:year, :month, :day]
    r[:composer].should be_a_kind_of(Proc)
    r[:decomposer].should be_a_kind_of(Proc)
  end

  it "#compositions should be a hash of cached values of compositions" do
    @o.compositions.should == {}
    @c.composition :date, :mapping=>[:year, :month, :day]
    @o.date
    @o.compositions.should == {:date=>Date.new(1, 2, 3)}
  end

  it "should work with custom :composer and :decomposer options" do
    @c.composition :date, :composer=>proc{Date.new(year+1, month+2, day+3)}, :decomposer=>proc{[:year, :month, :day].each{|s| self.send("#{s}=", date.send(s) * 2)}}
    @o.date.should == Date.new(2, 4, 6)
    @o.save
    MODEL_DB.sqls.last.should include("year = 4")
    MODEL_DB.sqls.last.should include("month = 8")
    MODEL_DB.sqls.last.should include("day = 12")
  end

  it "should allow call super in composition getter and setter method definition in class" do
    @c.composition :date, :mapping=>[:year, :month, :day]
    @c.class_eval do
      def date
        super + 1
      end
      def date=(v)
        super(v - 3)
      end
    end
    @o.date.should == Date.new(1, 2, 4)
    @o.compositions[:date].should == Date.new(1, 2, 3)
    @o.date = Date.new(1, 3, 5)
    @o.compositions[:date].should == Date.new(1, 3, 2)
    @o.date.should == Date.new(1, 3, 3)
  end

  it "should mark the object as modified whenever the composition is set" do
    @c.composition :date, :mapping=>[:year, :month, :day]
    @o.modified?.should == false
    @o.date = Date.new(3, 4, 5)
    @o.modified?.should == true
  end

  it "should only decompose existing compositions" do
    called = false
    @c.composition :date, :composer=>proc{}, :decomposer=>proc{called = true}
    called.should == false
    @o.save
    called.should == false
    @o.date = Date.new(1,2,3)
    called.should == false
    @o.save_changes
    called.should == true
  end

  it "should clear compositions cache when reloading" do
    @c.composition :date, :composer=>proc{}, :decomposer=>proc{called = true}
    @o.date = Date.new(3, 4, 5)
    @o.reload
    @o.compositions.should == {}
  end

  it "should instantiate compositions lazily" do
    @c.composition :date, :mapping=>[:year, :month, :day]
    @o.compositions.should == {}
    @o.date
    @o.compositions.should == {:date=>Date.new(1,2,3)}
  end

  it "should cache value of composition" do
    times = 0
    @c.composition :date, :composer=>proc{times+=1}, :decomposer=>proc{called = true}
    times.should == 0
    @o.date
    times.should == 1
    @o.date
    times.should == 1
  end

  it ":class option should take an string, symbol, or class" do
    @c.composition :date1, :class=>'Date', :mapping=>[:year, :month, :day]
    @c.composition :date2, :class=>:Date, :mapping=>[:year, :month, :day]
    @c.composition :date3, :class=>Date, :mapping=>[:year, :month, :day]
    @o.date1.should == Date.new(1, 2, 3)
    @o.date2.should == Date.new(1, 2, 3)
    @o.date3.should == Date.new(1, 2, 3)
  end

  it ":mapping option should work with a single array of symbols" do
    c = Class.new do
      def initialize(y, m)
        @y, @m = y, m
      end
      def year
        @y * 2
      end
      def month
        @m * 3
      end
    end
    @c.composition :date, :class=>c, :mapping=>[:year, :month]
    @o.date.year.should == 2
    @o.date.month.should == 6
    @o.date = c.new(3, 4)
    @o.save
    MODEL_DB.sqls.last.should include("year = 6")
    MODEL_DB.sqls.last.should include("month = 12")
  end

  it ":mapping option should work with an array of two pairs of symbols" do
    c = Class.new do
      def initialize(y, m)
        @y, @m = y, m
      end
      def y
        @y * 2
      end
      def m
        @m * 3
      end
    end
    @c.composition :date, :class=>c, :mapping=>[[:year, :y], [:month, :m]]
    @o.date.y.should == 2
    @o.date.m.should == 6
    @o.date = c.new(3, 4)
    @o.save
    MODEL_DB.sqls.last.should include("year = 6")
    MODEL_DB.sqls.last.should include("month = 12")
  end

  it ":mapping option :composer should return nil if all values are nil" do
    @c.composition :date, :mapping=>[:year, :month, :day]
    @c.new.date.should == nil
  end

  it ":mapping option :decomposer should set all related fields to nil if nil" do
    @c.composition :date, :mapping=>[:year, :month, :day]
    @o.date = nil
    @o.save
    MODEL_DB.sqls.last.should include("year = NULL")
    MODEL_DB.sqls.last.should include("month = NULL")
    MODEL_DB.sqls.last.should include("day = NULL")
  end

  it "should work correctly with subclasses" do
    @c.composition :date, :mapping=>[:year, :month, :day]
    c = Class.new(@c)
    o = c.load(:id=>1, :year=>1, :month=>2, :day=>3)
    o.date.should == Date.new(1, 2, 3)
    o.save
    MODEL_DB.sqls.last.should include("year = 1")
    MODEL_DB.sqls.last.should include("month = 2")
    MODEL_DB.sqls.last.should include("day = 3")
  end
end
