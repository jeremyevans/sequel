require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")
require 'yaml'

describe "serialization_modification_detection plugin" do
  before do
    @ds = Sequel.mock(:fetch=>{:id=>1, :a=>'a', :b=>1, :c=>['a'], :d=>{'b'=>'c'}}, :numrows=>1, :autoid=>1)[:items]
    @c = Class.new(Sequel::Model(@ds))
    @c.plugin :modification_detection
    @c.columns :a, :b, :c, :d
    @o = @c.first
    @ds.db.sqls
  end
  
  it "should only detect columns that have been changed" do
    @o.changed_columns.should == []
    @o.a << 'b'
    @o.changed_columns.should == [:a]
    @o.a.replace('a') 
    @o.changed_columns.should == []

    @o.values[:b] = 2
    @o.changed_columns.should == [:b]
    @o.values[:b] = 1
    @o.changed_columns.should == []

    @o.c[0] << 'b'
    @o.d['b'] << 'b'
    @o.changed_columns.sort_by{|c| c.to_s}.should == [:c, :d]
    @o.c[0] = 'a'
    @o.changed_columns.should == [:d]
    @o.d['b'] = 'c'
    @o.changed_columns.should == []
  end
  
  it "should not list a column twice" do
    @o.a = 'b'
    @o.a << 'a'
    @o.changed_columns.should == [:a]
  end
  
  it "should report correct changed_columns after updating" do
    @o.a << 'a'
    @o.save_changes
    @o.changed_columns.should == []

    @o.values[:b] = 2
    @o.save_changes
    @o.changed_columns.should == []

    @o.c[0] << 'b'
    @o.save_changes
    @o.changed_columns.should == []

    @o.d['b'] << 'a'
    @o.save_changes
    @o.changed_columns.should == []

    @ds.db.sqls.should == ["UPDATE items SET a = 'aa' WHERE (id = 1)",
                       "UPDATE items SET b = 2 WHERE (id = 1)",
                       "UPDATE items SET c = ('ab') WHERE (id = 1)",
                       "UPDATE items SET d = ('b' = 'ca') WHERE (id = 1)"]
  end

  it "should report correct changed_columns after creating new object" do
    o = @c.create
    o.changed_columns.should == []
    o.a << 'a'
    o.changed_columns.should == [:a]
    @ds.db.sqls.should == ["INSERT INTO items DEFAULT VALUES", "SELECT * FROM items WHERE (id = 1) LIMIT 1"]
  end

  it "should report correct changed_columns after refreshing existing object" do
    @o.a << 'a'
    @o.changed_columns.should == [:a]
    @o.refresh
    @o.changed_columns.should == []
    @o.a << 'a'
    @o.changed_columns.should == [:a]
  end
end
