require File.join(File.dirname(__FILE__), "spec_helper")

describe Sequel::Model, "BooleanReaders plugin" do
  before do
    @db = Sequel::Database.new({})
    def @db.schema(*args)
      [[:id, {}], [:y, {:type=>:integer, :db_type=>'tinyint(1)'}], [:b, {:type=>:boolean, :db_type=>'boolean'}]]
    end

    @c = Class.new(Sequel::Model(@db[:items]))
    @p =lambda do
      @columns = [:id, :b, :y]
      def columns; @columns; end
    end
    @c.instance_eval(&@p)
  end 

  specify "should create attribute? readers for all boolean attributes" do
    @c.plugin(:boolean_readers)
    o = @c.new
    o.b?.should == nil
    o.b = '1'
    o.b?.should == true
    o.b = '0'
    o.b?.should == false
    o.b = ''
    o.b?.should == nil
  end

  specify "should not create attribute? readers for non-boolean attributes" do
    @c.plugin(:boolean_readers)
    proc{@c.new.y?}.should raise_error(NoMethodError)
    proc{@c.new.id?}.should raise_error(NoMethodError)
  end

  specify "should accept a block to determine if an attribute is boolean" do
    @c.plugin(:boolean_readers){|c| db_schema[c][:db_type] == 'tinyint(1)'}
    proc{@c.new.b?}.should raise_error(NoMethodError)
    o = @c.new
    o.y.should == nil
    o.y?.should == nil
    o.y = '1'
    o.y.should == 1
    o.y?.should == true
    o.y = '0'
    o.y.should == 0
    o.y?.should == false
    o.y = ''
    o.y.should == nil
    o.y?.should == nil
  end

  specify "should create boolean readers when set_dataset is defined" do
    c = Class.new(Sequel::Model(@db))
    c.instance_eval(&@p)
    c.plugin(:boolean_readers)
    c.set_dataset(@db[:a])
    o = c.new
    o.b?.should == nil
    o.b = '1'
    o.b?.should == true
    o.b = '0'
    o.b?.should == false
    o.b = ''
    o.b?.should == nil
    proc{o.i?}.should raise_error(NoMethodError)

    c = Class.new(Sequel::Model(@db))
    c.instance_eval(&@p)
    c.plugin(:boolean_readers){|x| db_schema[x][:db_type] == 'tinyint(1)'}
    c.set_dataset(@db[:a])
    o = c.new
    o.y.should == nil
    o.y?.should == nil
    o.y = '1'
    o.y.should == 1
    o.y?.should == true
    o.y = '0'
    o.y.should == 0
    o.y?.should == false
    o.y = ''
    o.y.should == nil
    o.y?.should == nil
    proc{o.b?}.should raise_error(NoMethodError)
  end

  specify "should handle cases where getting the columns raises an error" do
    @c.meta_def(:columns){raise Sequel::Error}
    proc{@c.plugin(:boolean_readers)}.should_not raise_error
    proc{@c.new.b?}.should raise_error(NoMethodError)
  end
end
