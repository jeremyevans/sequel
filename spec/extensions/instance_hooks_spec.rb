require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "InstanceHooks plugin" do
  def r(x)
    @r << x
    x
  end

  before do
    @c = Class.new(Sequel::Model(:items))
    @c.plugin :instance_hooks
    @c.raise_on_save_failure = false
    @o = @c.new
    @x = @c.load({:id=>1})
    @r = []
  end

  it "should support before_create_hook and after_create_hook" do
    @o.after_create_hook{r 1}
    @o.before_create_hook{r 2}
    @o.after_create_hook{r 3}
    @o.before_create_hook{r 4}
    @o.save.should_not == nil
    @r.should == [4, 2, 1, 3]
  end

  it "should cancel the save if before_create_hook block returns false" do
    @o.after_create_hook{r 1}
    @o.before_create_hook{r false}
    @o.before_create_hook{r 4}
    @o.save.should == nil
    @r.should == [4, false]
    @r.clear
    @o.save.should == nil
    @r.should == [4, false]
  end

  it "should support before_update_hook and after_update_hook" do
    @x.after_update_hook{r 1}
    @x.before_update_hook{r 2}
    @x.after_update_hook{r 3}
    @x.before_update_hook{r 4}
    @x.save.should_not == nil
    @r.should == [4, 2, 1, 3]
    @x.save.should_not == nil
    @r.should == [4, 2, 1, 3]
  end

  it "should cancel the save if before_update_hook block returns false" do
    @x.after_update_hook{r 1}
    @x.before_update_hook{r false}
    @x.before_update_hook{r 4}
    @x.save.should == nil
    @r.should == [4, false]
    @r.clear
    @x.save.should == nil
    @r.should == [4, false]
  end

  it "should support before_save_hook and after_save_hook" do
    @o.after_save_hook{r 1}
    @o.before_save_hook{r 2}
    @o.after_save_hook{r 3}
    @o.before_save_hook{r 4}
    @o.save.should_not == nil
    @r.should == [4, 2, 1, 3]
    @r.clear

    @x.after_save_hook{r 1}
    @x.before_save_hook{r 2}
    @x.after_save_hook{r 3}
    @x.before_save_hook{r 4}
    @x.save.should_not == nil
    @r.should == [4, 2, 1, 3]
    @x.save.should_not == nil
    @r.should == [4, 2, 1, 3]
  end

  it "should cancel the save if before_save_hook block returns false" do
    @x.after_save_hook{r 1}
    @x.before_save_hook{r false}
    @x.before_save_hook{r 4}
    @x.save.should == nil
    @r.should == [4, false]
    @r.clear

    @x.after_save_hook{r 1}
    @x.before_save_hook{r false}
    @x.before_save_hook{r 4}
    @x.save.should == nil
    @r.should == [4, false]
    @r.clear
    @x.save.should == nil
    @r.should == [4, false]
  end

  it "should support before_destroy_hook and after_destroy_hook" do
    @x.after_destroy_hook{r 1}
    @x.before_destroy_hook{r 2}
    @x.after_destroy_hook{r 3}
    @x.before_destroy_hook{r 4}
    @x.destroy.should_not == nil
    @r.should == [4, 2, 1, 3]
  end

  it "should cancel the destroy if before_destroy_hook block returns false" do
    @x.after_destroy_hook{r 1}
    @x.before_destroy_hook{r false}
    @x.before_destroy_hook{r 4}
    @x.destroy.should == nil
    @r.should == [4, false]
  end

  it "should support before_validation_hook and after_validation_hook" do
    @o.after_validation_hook{r 1}
    @o.before_validation_hook{r 2}
    @o.after_validation_hook{r 3}
    @o.before_validation_hook{r 4}
    @o.valid?.should == true
    @r.should == [4, 2, 1, 3]
  end

  it "should cancel the save if before_validation_hook block returns false" do
    @o.after_validation_hook{r 1}
    @o.before_validation_hook{r false}
    @o.before_validation_hook{r 4}
    @o.valid?.should == false
    @r.should == [4, false]
    @r.clear
    @o.valid?.should == false
    @r.should == [4, false]
  end

  it "should clear only related hooks on successful create" do
    @o.after_destroy_hook{r 1}
    @o.before_destroy_hook{r 2}
    @o.after_update_hook{r 3}
    @o.before_update_hook{r 4}
    @o.before_save_hook{r 5}
    @o.after_save_hook{r 6}
    @o.before_create_hook{r 7}
    @o.after_create_hook{r 8}
    @o.save.should_not == nil
    @r.should == [5, 7, 8, 6]
    @o.instance_variable_set(:@new, false)
    @o.save.should_not == nil
    @r.should == [5, 7, 8, 6, 4, 3]
    @o.save.should_not == nil
    @r.should == [5, 7, 8, 6, 4, 3]
    @o.destroy
    @r.should == [5, 7, 8, 6, 4, 3, 2, 1]
  end

  it "should clear only related hooks on successful update" do
    @x.after_destroy_hook{r 1}
    @x.before_destroy_hook{r 2}
    @x.before_update_hook{r 3}
    @x.after_update_hook{r 4}
    @x.before_save_hook{r 5}
    @x.after_save_hook{r 6}
    @x.save.should_not == nil
    @r.should == [5, 3, 4, 6]
    @x.save.should_not == nil
    @r.should == [5, 3, 4, 6]
    @x.destroy
    @r.should == [5, 3, 4, 6, 2, 1]
  end

  it "should clear only related hooks on successful destroy" do
    @x.after_destroy_hook{r 1}
    @x.before_destroy_hook{r 2}
    @x.before_update_hook{r 3}
    @x.before_save_hook{r 4}
    @x.destroy
    @r.should == [2, 1]
    @x.save.should_not == nil
    @r.should == [2, 1, 4, 3]
  end
end

describe "InstanceHooks plugin with transactions" do
  before do
    @db = Sequel.mock(:numrows=>1)
    pr = proc{|x| r(x)}
    @c = Class.new(Sequel::Model(@db[:items])) do
      attr_accessor :rb
      def after_save
        db.execute('as')
        raise Sequel::Rollback if rb
      end
      def after_destroy
        db.execute('ad')
        raise Sequel::Rollback if rb
      end
    end
    @c.use_transactions = true
    @c.plugin :instance_hooks
    @o = @c.load({:id=>1})
    @or = @c.load({:id=>1})
    @or.rb = true
    @r = []
    @db.sqls
  end

  it "should support after_commit_hook" do
    @o.after_commit_hook{@db.execute('ac1')}
    @o.after_commit_hook{@db.execute('ac2')}
    @o.save.should_not be_nil
    @db.sqls.should == ['BEGIN', 'as', 'COMMIT', 'ac1', 'ac2']
  end

  it "should support after_rollback_hook" do
    @or.after_rollback_hook{@db.execute('ar1')}
    @or.after_rollback_hook{@db.execute('ar2')}
    @or.save.should be_nil
    @db.sqls.should == ['BEGIN', 'as', 'ROLLBACK', 'ar1', 'ar2']
  end

  it "should support after_commit_hook" do
    @o.after_destroy_commit_hook{@db.execute('adc1')}
    @o.after_destroy_commit_hook{@db.execute('adc2')}
    @o.destroy.should_not be_nil
    @db.sqls.should == ['BEGIN', "DELETE FROM items WHERE (id = 1)", 'ad', 'COMMIT', 'adc1', 'adc2']
  end

  it "should support after_rollback_hook" do
    @or.after_destroy_rollback_hook{@db.execute('adr1')}
    @or.after_destroy_rollback_hook{@db.execute('adr2')}
    @or.destroy.should be_nil
    @db.sqls.should == ['BEGIN', "DELETE FROM items WHERE (id = 1)", 'ad', 'ROLLBACK', 'adr1', 'adr2']
  end

  it "should have *_hook methods return self "do
    @o.before_destroy_hook{r 1}.should equal(@o)
    @o.before_validation_hook{r 1}.should equal(@o)
    @o.before_save_hook{r 1}.should equal(@o)
    @o.before_update_hook{r 1}.should equal(@o)
    @o.before_create_hook{r 1}.should equal(@o)

    @o.after_destroy_hook{r 1}.should equal(@o)
    @o.after_validation_hook{r 1}.should equal(@o)
    @o.after_save_hook{r 1}.should equal(@o)
    @o.after_update_hook{r 1}.should equal(@o)
    @o.after_create_hook{r 1}.should equal(@o)
    @o.after_commit_hook{r 1}.should equal(@o)
    @o.after_rollback_hook{r 1}.should equal(@o)
    @o.after_destroy_commit_hook{r 1}.should equal(@o)
    @o.after_destroy_rollback_hook{r 1}.should equal(@o)
  end

end
