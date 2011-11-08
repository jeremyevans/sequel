require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::Timestamps" do
  before do
    dc = Object.new
    dc.instance_eval do
      def now
        '2009-08-01'
      end
    end
    Sequel.datetime_class = dc
    @c = Class.new(Sequel::Model(:t))
    @c.class_eval do
      columns :id, :created_at, :updated_at
      plugin :timestamps
      def _save_refresh(*) end
      db.reset
    end
    @c.dataset.autoid = nil
  end 
  after do
    Sequel.datetime_class = Time
  end
  
  it "should set the create timestamp field on creation" do
    o = @c.create
    @c.db.sqls.should == ["INSERT INTO t (created_at) VALUES ('2009-08-01')"]
    o.created_at.should == '2009-08-01'
  end

  it "should set the update timestamp field on update" do
    o = @c.load(:id=>1).save
    @c.db.sqls.should == ["UPDATE t SET updated_at = '2009-08-01' WHERE (id = 1)"]
    o.updated_at.should == '2009-08-01'
  end

  it "should not update the update timestamp on creation" do
    @c.create.updated_at.should == nil
  end

  it "should use the same value for the creation and update timestamps when creating if the :update_on_create option is given" do
    @c.plugin :timestamps, :update_on_create=>true
    o = @c.create
    sqls = @c.db.sqls
    sqls.shift.should =~ /INSERT INTO t \((creat|updat)ed_at, (creat|updat)ed_at\) VALUES \('2009-08-01', '2009-08-01'\)/
    sqls.should == []
    o.created_at.should === o.updated_at
  end

  it "should allow specifying the create timestamp field via the :create option" do
    c = Class.new(Sequel::Model(:t))
    c.class_eval do
      columns :id, :c
      plugin :timestamps, :create=>:c
      def _save_refresh(*) end
    end
    o = c.create
    c.db.sqls.should == ["INSERT INTO t (c) VALUES ('2009-08-01')"]
    o.c.should == '2009-08-01'
  end

  it "should allow specifying the update timestamp field via the :update option" do
    c = Class.new(Sequel::Model(:t))
    c.class_eval do
      columns :id, :u
      plugin :timestamps, :update=>:u
      db.reset
      def _save_refresh(*) end
    end
    o = c.load(:id=>1).save
    c.db.sqls.should == ["UPDATE t SET u = '2009-08-01' WHERE (id = 1)"]
    o.u.should == '2009-08-01'
  end

  it "should not raise an error if the model doesn't have the timestamp columns" do
    c = Class.new(Sequel::Model(:t))
    c.class_eval do
      columns :id, :x
      plugin :timestamps
      db.reset
      def _refresh(ds); self end
    end
    c.create(:x=>2)
    c.load(:id=>1, :x=>2).save
    c.db.sqls.should == ["INSERT INTO t (x) VALUES (2)", "UPDATE t SET x = 2 WHERE (id = 1)"] 
  end

  it "should not overwrite an existing create timestamp" do
    o = @c.create(:created_at=>'2009-08-03')
    @c.db.sqls.should == ["INSERT INTO t (created_at) VALUES ('2009-08-03')"]
    o.created_at.should == '2009-08-03'
  end

  it "should overwrite an existing create timestamp if the :force option is used" do
    @c.plugin :timestamps, :force=>true
    o = @c.create(:created_at=>'2009-08-03')
    @c.db.sqls.should == ["INSERT INTO t (created_at) VALUES ('2009-08-01')"]
    o.created_at.should == '2009-08-01'
  end

  it "should have create_timestamp_field give the create timestamp field" do
    @c.create_timestamp_field.should == :created_at
    @c.plugin :timestamps, :create=>:c
    @c.create_timestamp_field.should == :c
  end

  it "should have update_timestamp_field give the update timestamp field" do
    @c.update_timestamp_field.should == :updated_at
    @c.plugin :timestamps, :update=>:u
    @c.update_timestamp_field.should == :u
  end

  it "should have create_timestamp_overwrite? give the whether to overwrite an existing create timestamp" do
    @c.create_timestamp_overwrite?.should == false
    @c.plugin :timestamps, :force=>true
    @c.create_timestamp_overwrite?.should == true
  end

  it "should have set_update_timestamp_on_create? give whether to set the update timestamp on create" do
    @c.set_update_timestamp_on_create?.should == false
    @c.plugin :timestamps, :update_on_create=>true
    @c.set_update_timestamp_on_create?.should == true
  end

  it "should work with subclasses" do
    c = Class.new(@c)
    o = c.create
    o.created_at.should == '2009-08-01'
    o.updated_at.should == nil
    o = c.load(:id=>1).save
    o.updated_at.should == '2009-08-01'
    c.db.sqls.should == ["INSERT INTO t (created_at) VALUES ('2009-08-01')", "UPDATE t SET updated_at = '2009-08-01' WHERE (id = 1)"]
    c.create(:created_at=>'2009-08-03').created_at.should == '2009-08-03'

    c.class_eval do
      columns :id, :c, :u
      plugin :timestamps, :create=>:c, :update=>:u, :force=>true, :update_on_create=>true
    end
    c2 = Class.new(c)
    c2.db.reset
    o = c2.create
    o.c.should == '2009-08-01'
    o.u.should === o.c 
    c2.db.sqls.first.should =~ /INSERT INTO t \([cu], [cu]\) VALUES \('2009-08-01', '2009-08-01'\)/
    c2.db.reset
    o = c2.load(:id=>1).save
    o.u.should == '2009-08-01'
    c2.db.sqls.should == ["UPDATE t SET u = '2009-08-01' WHERE (id = 1)"]
    c2.create(:c=>'2009-08-03').c.should == '2009-08-01'
  end
end
