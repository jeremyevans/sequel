require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::StaticAlternateKeyCache" do
  before do
    @db = Sequel.mock
    @v1 = {:id=>1, :code_en=>'ONE', :code_de=>'EINS'}
    @v2 = {:id=>2, :code_en=>'TWO', :code_de=>'ZWEI'}
    @db.fetch = [@v1, @v2]
    @c = Class.new(Sequel::Model(@db[:t]))
    @c.columns :id, :code_en, :code_de
    @c.plugin :static_alternate_key_cache, :code_en, :code_de
    @c1 = @c.alternate_key_cache[:code_en]['ONE']
    @c2 = @c.alternate_key_cache[:code_en]['TWO']
    @db.sqls
  end

  it "should use a ruby hash as a cache of all model instances" do
    c1 = @c.load(@v1)
    c2 = @c.load(@v2)
    @c.alternate_key_cache.should == {
        :code_en=>{'ONE'=>c1, 'TWO'=>c2},
        :code_de=>{'EINS'=>c1, 'ZWEI'=>c2},
    }
  end

  it "should support by_* class methods" do
    @c.by_code_en('ONE').should equal(@c1)
    @c.by_code_en('TWO').should equal(@c2)
    @c.by_code_en('THREE').should be_nil
    @c.by_code_de('EINS').should equal(@c1)
    @c.by_code_de('ZWEI').should equal(@c2)
    @c.by_code_de('DREI').should be_nil
    @db.sqls.should == []
  end

  it "set_dataset should work correctly" do
    ds = @c.dataset.from(:t2)
    ds.instance_variable_set(:@columns, [:id, :code_en, :code_de])
    v3 = {:id=>3, :code_en=>'THREE', :code_de=>'DREI'}
    ds._fetch = v3
    @c.dataset = ds
    c3 = @c.load(v3)
    @c.alternate_key_cache.should == {
        :code_en=>{'THREE'=>c3},
        :code_de=>{'DREI'=>c3},
    }
    @db.sqls.should == ['SELECT * FROM t2']
  end

  it "not specifying any key attributes should be OK" do
    db = Sequel.mock
    db.fetch = [@v1, @v2]
    c = Class.new(Sequel::Model(db[:t]))
    c.columns :id, :code_en, :code_de
    c.plugin :static_alternate_key_cache
    c.alternate_key_cache.should == {}
    db.sqls
  end
end
