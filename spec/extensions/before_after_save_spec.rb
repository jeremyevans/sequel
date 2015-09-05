require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::BeforeAfterSave" do
  before do
    @db = Sequel.mock(:numrows=>1, :fetch=>{:id=>1, :name=>'b'})
    @c = Class.new(Sequel::Model(@db[:test]))
    @ds = @c.dataset
    @c.columns :id, :name
    @c.plugin :before_after_save
    @c.plugin :instance_hooks
    @o = @c.new
    @db.sqls
  end

  it "should reset modified flag before calling after hooks" do
    a = []
    @o.after_create_hook{@o.modified?.must_equal false; a << 1}
    @o.after_save_hook{@o.modified?.must_equal false; a << 2}

    @o.modified!
    @o.save
    a.must_equal [1, 2]

    @o.after_save_hook{@o.modified?.must_equal false; a << 2}
    @o.after_update_hook{@o.modified?.must_equal false; a << 3}
    a = []
    @o.modified!
    @o.save
    a.must_equal [3, 2]
  end

  it "should refresh the instance before calling after hooks" do
    a = []
    @o.after_create_hook{@o.values.must_equal(:id=>1, :name=>'b'); a << 1}
    @o.after_save_hook{@o.values.must_equal(:id=>1, :name=>'b'); a << 2}

    @o.save
    a.must_equal [1, 2]
  end
end
