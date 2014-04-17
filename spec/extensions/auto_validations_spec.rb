require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::AutoValidations" do
  before do
    db = Sequel.mock(:fetch=>{:v=>1})
    def db.schema_parse_table(*) true; end
    def db.schema(t, *)
      t = t.first_source if t.is_a?(Sequel::Dataset)
      return [] if t != :test
      [[:id, {:primary_key=>true, :type=>:integer, :allow_null=>false}],
       [:name, {:primary_key=>false, :type=>:string, :allow_null=>false}],
       [:num, {:primary_key=>false, :type=>:integer, :allow_null=>true}],
       [:d, {:primary_key=>false, :type=>:date, :allow_null=>false}],
       [:nnd, {:primary_key=>false, :type=>:string, :allow_null=>false, :ruby_default=>'nnd'}]]
    end
    def db.supports_index_parsing?() true end
    def db.indexes(t, *)
      raise if t.is_a?(Sequel::Dataset)
      return [] if t != :test
      {:a=>{:columns=>[:name, :num], :unique=>true}, :b=>{:columns=>[:num], :unique=>false}}
    end
    @c = Class.new(Sequel::Model(db[:test]))
    @c.send(:def_column_accessor, :id, :name, :num, :d, :nnd)
    @c.raise_on_typecast_failure = false
    @c.plugin :auto_validations
    @m = @c.new
    db.sqls
  end

  it "should have automatically created validations" do
    @m.valid?.should == false
    @m.errors.should == {:d=>["is not present"], :name=>["is not present"]}

    @m.name = ''
    @m.valid?.should == false
    @m.errors.should == {:d=>["is not present"]}

    @m.set(:d=>'/', :num=>'a', :name=>'1')
    @m.valid?.should == false
    @m.errors.should == {:d=>["is not a valid date"], :num=>["is not a valid integer"]}

    @m.set(:d=>Date.today, :num=>1)
    @m.valid?.should == false
    @m.errors.should == {[:name, :num]=>["is already taken"]}
  end

  it "should handle databases that don't support index parsing" do
    def (@m.db).supports_index_parsing?() false end
    @m.model.send(:setup_auto_validations)
    @m.set(:d=>Date.today, :num=>1, :name=>'1')
    @m.valid?.should == true
  end

  it "should handle models that select from subqueries" do
    @c.set_dataset @c.dataset.from_self
    proc{@c.send(:setup_auto_validations)}.should_not raise_error
  end

  it "should support :not_null=>:presence option" do
    @c.plugin :auto_validations, :not_null=>:presence
    @m.set(:d=>Date.today, :num=>'')
    @m.valid?.should == false
    @m.errors.should == {:name=>["is not present"]}
  end

  it "should automatically validate explicit nil values for columns with not nil defaults" do
    @m.set(:d=>Date.today, :name=>1, :nnd=>nil)
    @m.id = nil
    @m.valid?.should == false
    @m.errors.should == {:id=>["is not present"], :nnd=>["is not present"]}
  end

  it "should allow skipping validations by type" do
    @c = Class.new(@c)
    @m = @c.new
    @c.skip_auto_validations(:not_null)
    @m.valid?.should == true

    @m.set(:d=>'/', :num=>'a', :name=>'1')
    @m.valid?.should == false
    @m.errors.should == {:d=>["is not a valid date"], :num=>["is not a valid integer"]}

    @c.skip_auto_validations(:types)
    @m.valid?.should == false
    @m.errors.should == {[:name, :num]=>["is already taken"]}

    @c.skip_auto_validations(:unique)
    @m.valid?.should == true
  end

  it "should allow skipping all auto validations" do
    @c = Class.new(@c)
    @m = @c.new
    @c.skip_auto_validations(:all)
    @m.valid?.should == true
    @m.set(:d=>'/', :num=>'a', :name=>'1')
    @m.valid?.should == true
  end

  it "should work correctly in subclasses" do
    @c = Class.new(@c)
    @m = @c.new
    @m.valid?.should == false
    @m.errors.should == {:d=>["is not present"], :name=>["is not present"]}

    @m.set(:d=>'/', :num=>'a', :name=>'1')
    @m.valid?.should == false
    @m.errors.should == {:d=>["is not a valid date"], :num=>["is not a valid integer"]}

    @m.set(:d=>Date.today, :num=>1)
    @m.valid?.should == false
    @m.errors.should == {[:name, :num]=>["is already taken"]}
  end

  it "should work correctly in STI subclasses" do
    @c.plugin(:single_table_inheritance, :num, :model_map=>{1=>@c}, :key_map=>proc{[1, 2]})
    sc = Class.new(@c)
    @m = sc.new
    @m.valid?.should == false
    @m.errors.should == {:d=>["is not present"], :name=>["is not present"]}

    @m.set(:d=>'/', :num=>'a', :name=>'1')
    @m.valid?.should == false
    @m.errors.should == {:d=>["is not a valid date"], :num=>["is not a valid integer"]}

    @m.db.sqls
    @m.set(:d=>Date.today, :num=>1)
    @m.valid?.should == false
    @m.errors.should == {[:name, :num]=>["is already taken"]}
    @m.db.sqls.should == ["SELECT count(*) AS count FROM test WHERE ((name = '1') AND (num = 1)) LIMIT 1"]
  end

  it "should work correctly when changing the dataset" do
    @c.set_dataset(@c.db[:foo])
    @c.new.valid?.should == true
  end
end
