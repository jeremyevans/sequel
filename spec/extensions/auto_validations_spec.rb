require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::AutoValidations" do
  before do
    db = Sequel.mock(:fetch=>{:v=>1})
    def db.schema_parse_table(*) true; end
    def db.schema(t, *)
      t = t.first_source if t.is_a?(Sequel::Dataset)
      return [] if t != :test
      [[:id, {:primary_key=>true, :type=>:integer}],
       [:name, {:primary_key=>false, :type=>:string, :allow_null=>false}],
       [:num, {:primary_key=>false, :type=>:integer, :allow_null=>true}],
       [:d, {:primary_key=>false, :type=>:date, :allow_null=>false}]]
    end
    def db.supports_index_parsing?() true end
    def db.indexes(t, *)
      return [] if t != :test
      {:a=>{:columns=>[:name, :num], :unique=>true}, :b=>{:columns=>[:num], :unique=>false}}
    end
    @c = Class.new(Sequel::Model(db[:test]))
    @c.send(:def_column_accessor, :id, :name, :num, :d)
    @c.raise_on_typecast_failure = false
    @c.plugin :auto_validations
    @m = @c.new
    db.sqls
  end

  it "should have automatically created validations" do
    @m.valid?.should be_false
    @m.errors.should == {:d=>["is not present"], :name=>["is not present"]}

    @m.name = ''
    @m.valid?.should be_false
    @m.errors.should == {:d=>["is not present"]}

    @m.set(:d=>'/', :num=>'a', :name=>'1')
    @m.valid?.should be_false
    @m.errors.should == {:d=>["is not a valid date"], :num=>["is not a valid integer"]}

    @m.set(:d=>Date.today, :num=>1)
    @m.valid?.should be_false
    @m.errors.should == {[:name, :num]=>["is already taken"]}
  end

  it "should support :not_null=>:presence option" do
    @c.plugin :auto_validations, :not_null=>:presence
    @m.set(:d=>Date.today, :num=>'')
    @m.valid?.should be_false
    @m.errors.should == {:name=>["is not present"]}
  end

  it "should allow skipping validations by type" do
    @c = Class.new(@c)
    @m = @c.new
    @c.skip_auto_validations(:not_null)
    @m.valid?.should be_true

    @m.set(:d=>'/', :num=>'a', :name=>'1')
    @m.valid?.should be_false
    @m.errors.should == {:d=>["is not a valid date"], :num=>["is not a valid integer"]}

    @c.skip_auto_validations(:types)
    @m.valid?.should be_false
    @m.errors.should == {[:name, :num]=>["is already taken"]}

    @c.skip_auto_validations(:unique)
    @m.valid?.should be_true
  end

  it "should allow skipping all auto validations" do
    @c = Class.new(@c)
    @m = @c.new
    @c.skip_auto_validations(:all)
    @m.valid?.should be_true
    @m.set(:d=>'/', :num=>'a', :name=>'1')
    @m.valid?.should be_true
  end

  it "should work correctly in subclasses" do
    @c = Class.new(@c)
    @m = @c.new
    @m.valid?.should be_false
    @m.errors.should == {:d=>["is not present"], :name=>["is not present"]}

    @m.set(:d=>'/', :num=>'a', :name=>'1')
    @m.valid?.should be_false
    @m.errors.should == {:d=>["is not a valid date"], :num=>["is not a valid integer"]}

    @m.set(:d=>Date.today, :num=>1)
    @m.valid?.should be_false
    @m.errors.should == {[:name, :num]=>["is already taken"]}
  end

  it "should work correctly when changing the dataset" do
    @c.set_dataset(@c.db[:foo])
    @c.new.valid?.should be_true
  end
end
