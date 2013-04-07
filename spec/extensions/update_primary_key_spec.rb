require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::UpdatePrimaryKey" do
  before do
    @c = Class.new(Sequel::Model(:a))
    @c.plugin :update_primary_key
    @c.columns :a, :b
    @c.set_primary_key :a
    @c.unrestrict_primary_key
    @o = @c.new
    @ds = @c.dataset
    MODEL_DB.reset
  end

  specify "should handle regular updates" do
    @ds._fetch = [[{:a=>1, :b=>3}], [{:a=>1, :b=>4}], [{:a=>1, :b=>4}], [{:a=>1, :b=>5}], [{:a=>1, :b=>5}], [{:a=>1, :b=>6}], [{:a=>1, :b=>6}]]
    @c.first.update(:b=>4)
    @c.all.should == [@c.load(:a=>1, :b=>4)]
    MODEL_DB.sqls.should == ["SELECT * FROM a LIMIT 1", "UPDATE a SET b = 4 WHERE (a = 1)", "SELECT * FROM a"]
    @c.first.set(:b=>5).save
    @c.all.should == [@c.load(:a=>1, :b=>5)]
    MODEL_DB.sqls.should == ["SELECT * FROM a LIMIT 1", "UPDATE a SET b = 5 WHERE (a = 1)", "SELECT * FROM a"]
    @c.first.set(:b=>6).save(:b)
    @c.all.should == [@c.load(:a=>1, :b=>6)]
    MODEL_DB.sqls.should == ["SELECT * FROM a LIMIT 1", "UPDATE a SET b = 6 WHERE (a = 1)", "SELECT * FROM a"]
  end

  specify "should handle updating the primary key field with another field" do
    @ds._fetch = [[{:a=>1, :b=>3}], [{:a=>2, :b=>4}]]
    @c.first.update(:a=>2, :b=>4)
    @c.all.should == [@c.load(:a=>2, :b=>4)]
    sqls = MODEL_DB.sqls
    ["UPDATE a SET a = 2, b = 4 WHERE (a = 1)", "UPDATE a SET b = 4, a = 2 WHERE (a = 1)"].should include(sqls.slice!(1))
    sqls.should == ["SELECT * FROM a LIMIT 1", "SELECT * FROM a"]
  end

  specify "should handle updating just the primary key field when saving changes" do
    @ds._fetch = [[{:a=>1, :b=>3}], [{:a=>2, :b=>3}], [{:a=>2, :b=>3}], [{:a=>3, :b=>3}]]
    @c.first.update(:a=>2)
    @c.all.should == [@c.load(:a=>2, :b=>3)]
    MODEL_DB.sqls.should == ["SELECT * FROM a LIMIT 1", "UPDATE a SET a = 2 WHERE (a = 1)", "SELECT * FROM a"]
    @c.first.set(:a=>3).save(:a)
    @c.all.should == [@c.load(:a=>3, :b=>3)]
    MODEL_DB.sqls.should == ["SELECT * FROM a LIMIT 1", "UPDATE a SET a = 3 WHERE (a = 2)", "SELECT * FROM a"]
  end

  specify "should handle saving after modifying the primary key field with another field" do
    @ds._fetch = [[{:a=>1, :b=>3}], [{:a=>2, :b=>4}]]
    @c.first.set(:a=>2, :b=>4).save
    @c.all.should == [@c.load(:a=>2, :b=>4)]
    sqls = MODEL_DB.sqls
    ["UPDATE a SET a = 2, b = 4 WHERE (a = 1)", "UPDATE a SET b = 4, a = 2 WHERE (a = 1)"].should include(sqls.slice!(1))
    sqls.should == ["SELECT * FROM a LIMIT 1", "SELECT * FROM a"]
  end

  specify "should handle saving after modifying just the primary key field" do
    @ds._fetch = [[{:a=>1, :b=>3}], [{:a=>2, :b=>3}]]
    @c.first.set(:a=>2).save
    @c.all.should == [@c.load(:a=>2, :b=>3)]
    sqls = MODEL_DB.sqls
    ["UPDATE a SET a = 2, b = 3 WHERE (a = 1)", "UPDATE a SET b = 3, a = 2 WHERE (a = 1)"].should include(sqls.slice!(1))
    sqls.should == ["SELECT * FROM a LIMIT 1", "SELECT * FROM a"]
  end

  specify "should handle saving after updating the primary key" do
    @ds._fetch = [[{:a=>1, :b=>3}], [{:a=>2, :b=>5}]]
    @c.first.update(:a=>2).update(:b=>4).set(:b=>5).save
    @c.all.should == [@c.load(:a=>2, :b=>5)]
    MODEL_DB.sqls.should == ["SELECT * FROM a LIMIT 1", "UPDATE a SET a = 2 WHERE (a = 1)", "UPDATE a SET b = 4 WHERE (a = 2)", "UPDATE a SET b = 5 WHERE (a = 2)", "SELECT * FROM a"]
  end

  specify "should handle frozen instances" do
    o = @c.new
    o.a = 1
    o.freeze
    o.pk_hash.should == {:a=>1}
  end
end
