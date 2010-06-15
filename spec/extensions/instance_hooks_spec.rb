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
end
