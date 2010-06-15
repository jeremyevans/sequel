require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Model#after_initialize" do
  specify "should be called after initialization" do
    $values1 = nil
    $reached_after_initialized = false
    
    a = Class.new(Sequel::Model)
    a.class_eval do
      columns :x, :y
      def after_initialize
        $values1 = @values.clone
        $reached_after_initialized = true
      end
    end
    
    a.new(:x => 1, :y => 2)
    $values1.should == {:x => 1, :y => 2}
    $reached_after_initialized.should == true
  end
end

describe "Model#before_create && Model#after_create" do
  before do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items))
    @c.class_eval do
      columns :x
      no_primary_key
      
      def after_create
        MODEL_DB << "BLAH after"
      end
    end
  end
  
  specify "should be called around new record creation" do
    @c.send(:define_method, :before_create){MODEL_DB << "BLAH before"}
    @c.create(:x => 2)
    MODEL_DB.sqls.should == [
      'BLAH before',
      'INSERT INTO items (x) VALUES (2)',
      'BLAH after'
    ]
  end

  specify ".create should cancel the save and raise an error if before_create returns false and raise_on_save_failure is true" do
    @c.send(:define_method, :before_create){false}
    proc{@c.load(:id => 2233).save}.should_not raise_error(Sequel::ValidationFailed)
    proc{@c.create(:x => 2)}.should raise_error(Sequel::BeforeHookFailed)
    MODEL_DB.sqls.should == []
  end

  specify ".create should cancel the save and return nil if before_create returns false and raise_on_save_failure is false" do
    @c.send(:define_method, :before_create){false}
    @c.raise_on_save_failure = false
    @c.create(:x => 2).should == nil
    MODEL_DB.sqls.should == []
  end
end

describe "Model#before_update && Model#after_update" do
  before do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items))
    @c.class_eval do
      columns :id, :x
      def after_update; MODEL_DB << "BLAH after" end
    end
  end
  
  specify "should be called around record update" do
    @c.send(:define_method, :before_update){MODEL_DB << "BLAH before"}
    m = @c.load(:id => 2233, :x=>123)
    m.save
    MODEL_DB.sqls.should == [
      'BLAH before',
      'UPDATE items SET x = 123 WHERE (id = 2233)',
      'BLAH after'
    ]
  end

  specify "#save should cancel the save and raise an error if before_update returns false and raise_on_save_failure is true" do
    @c.send(:define_method, :before_update){false}
    proc{@c.load(:id => 2233).save}.should_not raise_error(Sequel::ValidationFailed)
    proc{@c.load(:id => 2233).save}.should raise_error(Sequel::BeforeHookFailed)
    MODEL_DB.sqls.should == []
  end

  specify "#save should cancel the save and return nil if before_update returns false and raise_on_save_failure is false" do
    @c.send(:define_method, :before_update){false}
    @c.raise_on_save_failure = false
    @c.load(:id => 2233).save.should == nil
    MODEL_DB.sqls.should == []
  end
end

describe "Model#before_save && Model#after_save" do
  before do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items))
    @c.class_eval do
      columns :x
      def after_save; MODEL_DB << "BLAH after" end
    end
  end
  
  specify "should be called around record update" do
    @c.send(:define_method, :before_save){MODEL_DB << "BLAH before"}
    m = @c.load(:id => 2233, :x=>123)
    m.save
    MODEL_DB.sqls.should == [
      'BLAH before',
      'UPDATE items SET x = 123 WHERE (id = 2233)',
      'BLAH after'
    ]
  end
  
  specify "should be called around record creation" do
    @c.send(:define_method, :before_save){MODEL_DB << "BLAH before"}
    @c.no_primary_key
    @c.create(:x => 2)
    MODEL_DB.sqls.should == [
      'BLAH before',
      'INSERT INTO items (x) VALUES (2)',
      'BLAH after'
    ]
  end

  specify "#save should cancel the save and raise an error if before_save returns false and raise_on_save_failure is true" do
    @c.send(:define_method, :before_save){false}
    proc{@c.load(:id => 2233).save}.should_not raise_error(Sequel::ValidationFailed)
    proc{@c.load(:id => 2233).save}.should raise_error(Sequel::BeforeHookFailed)
    MODEL_DB.sqls.should == []
  end

  specify "#save should cancel the save and return nil if before_save returns false and raise_on_save_failure is false" do
    @c.send(:define_method, :before_save){false}
    @c.raise_on_save_failure = false
    @c.load(:id => 2233).save.should == nil
    MODEL_DB.sqls.should == []
  end
end

describe "Model#before_destroy && Model#after_destroy" do
  before do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items))
    @c.class_eval do
      def after_destroy; MODEL_DB << "BLAH after"; end
      
      def delete
        MODEL_DB << "DELETE BLAH"
      end
    end
  end
  
  specify "should be called around record destruction" do
    @c.send(:define_method, :before_destroy){MODEL_DB << "BLAH before"}
    m = @c.load(:id => 2233)
    m.destroy
    MODEL_DB.sqls.should == [
      'BLAH before',
      'DELETE BLAH',
      'BLAH after'
    ]
  end

  specify "#destroy should cancel the destroy and raise an error if before_destroy returns false and raise_on_save_failure is true" do
    @c.send(:define_method, :before_destroy){false}
    proc{@c.load(:id => 2233).destroy}.should raise_error(Sequel::BeforeHookFailed)
    MODEL_DB.sqls.should == []
  end

  specify "#destroy should cancel the destroy and return nil if before_destroy returns false and raise_on_save_failure is false" do
    @c.send(:define_method, :before_destroy){false}
    @c.raise_on_save_failure = false
    @c.load(:id => 2233).destroy.should == nil
    MODEL_DB.sqls.should == []
  end
end

describe "Model#before_validation && Model#after_validation" do
  before do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items))
    @c.class_eval do
      def after_validation; MODEL_DB << "BLAH after" end

      def validate
        errors.add(:id, 'not valid') unless id == 2233
      end
      columns :id
    end
  end
  
  specify "should be called around validation" do
    @c.send(:define_method, :before_validation){MODEL_DB << "BLAH before"}
    m = @c.load(:id => 2233)
    m.should be_valid
    MODEL_DB.sqls.should == ['BLAH before', 'BLAH after']

    MODEL_DB.sqls.clear
    m = @c.load(:id => 22)
    m.should_not be_valid
    MODEL_DB.sqls.should == ['BLAH before', 'BLAH after']
  end

  specify "should be called when calling save" do
    @c.send(:define_method, :before_validation){MODEL_DB << "BLAH before"}
    m = @c.load(:id => 2233, :x=>123)
    m.save.should == m
    MODEL_DB.sqls.should == ['BLAH before', 'BLAH after', 'UPDATE items SET x = 123 WHERE (id = 2233)']

    MODEL_DB.sqls.clear
    m = @c.load(:id => 22)
    m.raise_on_save_failure = false
    m.save.should == nil
    MODEL_DB.sqls.should == ['BLAH before', 'BLAH after']
  end

  specify "#save should cancel the save and raise an error if before_validation returns false and raise_on_save_failure is true" do
    @c.send(:define_method, :before_validation){false}
    proc{@c.load(:id => 2233).save}.should_not raise_error(Sequel::ValidationFailed)
    proc{@c.load(:id => 2233).save}.should raise_error(Sequel::BeforeHookFailed)
    MODEL_DB.sqls.should == []
  end

  specify "#save should cancel the save and return nil if before_validation returns false and raise_on_save_failure is false" do
    @c.send(:define_method, :before_validation){false}
    @c.raise_on_save_failure = false
    @c.load(:id => 2233).save.should == nil
    MODEL_DB.sqls.should == []
  end
end
