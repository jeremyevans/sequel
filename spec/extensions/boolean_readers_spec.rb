require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model, "BooleanReaders plugin" do
  before do
    @db = Sequel::Database.new
    def @db.supports_schema_parsing?() true end
    def @db.schema(*args)
      [[:id, {}], [:z, {:type=>:integer, :db_type=>'tinyint(1)'}], [:b, {:type=>:boolean, :db_type=>'boolean'}]]
    end

    @c = Class.new(Sequel::Model(@db[:items]))
    @p = proc do
      @columns = [:id, :b, :z]
      def columns; @columns; end
    end
    @c.instance_eval(&@p)
  end 

  it "should create attribute? readers for all boolean attributes" do
    @c.plugin(:boolean_readers)
    o = @c.new
    o.b?.must_equal nil
    o.b = '1'
    o.b?.must_equal true
    o.b = '0'
    o.b?.must_equal false
    o.b = ''
    o.b?.must_equal nil
  end

  it "should not create attribute? readers for non-boolean attributes" do
    @c.plugin(:boolean_readers)
    proc{@c.new.z?}.must_raise(NoMethodError)
    proc{@c.new.id?}.must_raise(NoMethodError)
  end

  it "should accept a block to determine if an attribute is boolean" do
    @c.plugin(:boolean_readers){|c| db_schema[c][:db_type] == 'tinyint(1)'}
    proc{@c.new.b?}.must_raise(NoMethodError)
    o = @c.new
    o.z.must_equal nil
    o.z?.must_equal nil
    o.z = '1'
    o.z.must_equal 1
    o.z?.must_equal true
    o.z = '0'
    o.z.must_equal 0
    o.z?.must_equal false
    o.z = ''
    o.z.must_equal nil
    o.z?.must_equal nil
  end

  it "should create boolean readers when set_dataset is defined" do
    c = Class.new(Sequel::Model(@db))
    c.instance_eval(&@p)
    c.plugin(:boolean_readers)
    c.set_dataset(@db[:a])
    o = c.new
    o.b?.must_equal nil
    o.b = '1'
    o.b?.must_equal true
    o.b = '0'
    o.b?.must_equal false
    o.b = ''
    o.b?.must_equal nil
    proc{o.i?}.must_raise(NoMethodError)

    c = Class.new(Sequel::Model(@db))
    c.instance_eval(&@p)
    c.plugin(:boolean_readers){|x| db_schema[x][:db_type] == 'tinyint(1)'}
    c.set_dataset(@db[:a])
    o = c.new
    o.z.must_equal nil
    o.z?.must_equal nil
    o.z = '1'
    o.z.must_equal 1
    o.z?.must_equal true
    o.z = '0'
    o.z.must_equal 0
    o.z?.must_equal false
    o.z = ''
    o.z.must_equal nil
    o.z?.must_equal nil
    proc{o.b?}.must_raise(NoMethodError)
  end

  it "should handle cases where getting the columns raises an error" do
    @c.meta_def(:columns){raise Sequel::Error}
    @c.plugin(:boolean_readers)
    proc{@c.new.b?}.must_raise(NoMethodError)
  end
end
