require File.join(File.dirname(__FILE__), "spec_helper")

describe "Sequel::Plugins::UpdatePrimaryKey" do
  before do
    @c = Class.new(Sequel::Model(:a))
    @c.plugin :update_primary_key
    @c.columns :a, :b
    @c.set_primary_key :a
    @c.unrestrict_primary_key
    @o = @c.new
    @ds = @c.dataset
    @ds.extend(Module.new do
      attr_accessor :a
      def fetch_rows(sql)
        yield(a.dup)
      end

      def update(h)
        if literal(opts[:where]) =~ /a = (\d)/ and $1.to_i == a[:a]
          a.update(h)
        end
        super
      end
    end)
    @ds.a = {:a=>1, :b=>3}
    @ds2 = @ds.naked 
    MODEL_DB.reset
  end

  specify "should handle regular updates" do
    @c.first.update(:b=>4)
    @ds2.all.should == [{:a=>1, :b=>4}]
    @c.first.set(:b=>5).save
    @ds2.all.should == [{:a=>1, :b=>5}]
    @c.first.set(:b=>6).save(:b)
    @ds2.all.should == [{:a=>1, :b=>6}]
  end

  specify "should handle updating the primary key field with another field" do
    @c.first.update(:a=>2, :b=>4)
    @ds2.all.should == [{:a=>2, :b=>4}]
  end

  specify "should handle updating just the primary key field when saving changes" do
    @c.first.update(:a=>2)
    @ds2.all.should == [{:a=>2, :b=>3}]
    @c.first.set(:a=>3).save(:a)
    @ds2.all.should == [{:a=>3, :b=>3}]
  end

  specify "should handle saving after modifying the primary key field with another field" do
    @c.first.set(:a=>2, :b=>4).save
    @ds2.all.should == [{:a=>2, :b=>4}]
  end

  specify "should handle saving after modifying just the primary key field" do
    @c.first.set(:a=>2).save
    @ds2.all.should == [{:a=>2, :b=>3}]
  end

  specify "should handle saving after updating the primary key" do
    @c.first.update(:a=>2).update(:b=>4).set(:b=>5).save
    @ds2.all.should == [{:a=>2, :b=>5}]
  end
end
