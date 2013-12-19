require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

model_class = proc do |klass, &block|
  c = Class.new(klass)
  c.plugin :hook_class_methods
  c.class_eval(&block) if block
  c
end

describe Sequel::Model, "hook_class_methods plugin" do
  before do
    DB.reset
  end
  
  specify "should be definable using a block" do
    adds = []
    c = model_class.call Sequel::Model do
      before_save{adds << 'hi'}
    end
    
    c.new.before_save
    adds.should == ['hi']
  end
  
  specify "should be definable using a method name" do
    adds = []
    c = model_class.call Sequel::Model do
      define_method(:bye){adds << 'bye'}
      before_save :bye
    end
    
    c.new.before_save
    adds.should == ['bye']
  end

  specify "should be additive" do
    adds = []
    c = model_class.call Sequel::Model do
      after_save{adds << 'hyiyie'}
      after_save{adds << 'byiyie'}
    end

    c.new.after_save
    adds.should == ['hyiyie', 'byiyie']
  end
  
  specify "before hooks should run in reverse order" do
    adds = []
    c = model_class.call Sequel::Model do
      before_save{adds << 'hyiyie'}
      before_save{adds << 'byiyie'}
    end
    
    c.new.before_save
    adds.should == ['byiyie', 'hyiyie']
  end

  specify "should not be additive if the method or tag already exists" do
    adds = []
    c = model_class.call Sequel::Model do
      define_method(:bye){adds << 'bye'}
      before_save :bye
      before_save :bye
    end
    
    c.new.before_save
    adds.should == ['bye']

    adds = []
    d = model_class.call Sequel::Model do
      before_save(:bye){adds << 'hyiyie'}
      before_save(:bye){adds << 'byiyie'}
    end
    
    d.new.before_save
    adds.should == ['byiyie']

    adds = []
    e = model_class.call Sequel::Model do
      define_method(:bye){adds << 'bye'}
      before_save :bye
      before_save(:bye){adds << 'byiyie'}
    end
    
    e.new.before_save
    adds.should == ['byiyie']

    adds = []
    e = model_class.call Sequel::Model do
      define_method(:bye){adds << 'bye'}
      before_save(:bye){adds << 'byiyie'}
      before_save :bye
    end
    
    e.new.before_save
    adds.should == ['bye']
  end
  
  specify "should be inheritable" do
    adds = []
    a = model_class.call Sequel::Model do
      after_save{adds << '123'}
    end
    
    b = Class.new(a)
    b.class_eval do
      after_save{adds << '456'}
      after_save{adds << '789'}
    end
    
    b.new.after_save
    adds.should == ['123', '456', '789']
  end
  
  specify "should be overridable in descendant classes" do
    adds = []
    a = model_class.call Sequel::Model do
      before_save{adds << '123'}
    end
    
    b = Class.new(a)
    b.class_eval do
      define_method(:before_save){adds << '456'}
    end
    
    a.new.before_save
    adds.should == ['123']
    adds = []
    b.new.before_save
    adds.should == ['456']
  end
  
  specify "should stop processing if a before hook returns false" do
    flag = true
    adds = []
    
    a = model_class.call Sequel::Model do
      before_save{adds << 'cruel'; flag}
      before_save{adds << 'blah'; flag}
    end
    
    a.new.before_save
    adds.should == ['blah', 'cruel']

    # chain should not break on nil
    adds = []
    flag = nil
    a.new.before_save
    adds.should == ['blah', 'cruel']
    
    adds = []
    flag = false
    a.new.before_save
    adds.should == ['blah']
    
    b = Class.new(a)
    b.class_eval do
      before_save{adds << 'mau'}
    end
    
    adds = []
    b.new.before_save
    adds.should == ['mau', 'blah']
  end
end

describe "Model#before_create && Model#after_create" do
  before do
    DB.reset

    @c = model_class.call Sequel::Model(:items)  do
      columns :x
      no_primary_key
      
      after_create {DB << "BLAH after"}
    end
  end
  
  specify "should be called around new record creation" do
    @c.before_create {DB << "BLAH before"}
    @c.create(:x => 2)
    DB.sqls.should == ['BLAH before', 'INSERT INTO items (x) VALUES (2)', 'BLAH after']
  end

  specify ".create should cancel the save and raise an error if before_create returns false and raise_on_save_failure is true" do
    @c.before_create{false}
    proc{@c.create(:x => 2)}.should raise_error(Sequel::BeforeHookFailed)
    DB.sqls.should == []
  end

  specify ".create should cancel the save and return nil if before_create returns false and raise_on_save_failure is false" do
    @c.before_create{false}
    @c.raise_on_save_failure = false
    @c.create(:x => 2).should == nil
    DB.sqls.should == []
  end
end

describe "Model#before_update && Model#after_update" do
  before do
    DB.reset

    @c = model_class.call(Sequel::Model(:items)) do
      after_update {DB << "BLAH after"}
    end
  end
  
  specify "should be called around record update" do
    @c.before_update {DB << "BLAH before"}
    m = @c.load(:id => 2233, :x=>123)
    m.save
    DB.sqls.should == ['BLAH before', 'UPDATE items SET x = 123 WHERE (id = 2233)', 'BLAH after']
  end

  specify "#save should cancel the save and raise an error if before_update returns false and raise_on_save_failure is true" do
    @c.before_update{false}
    proc{@c.load(:id => 2233).save}.should raise_error(Sequel::BeforeHookFailed)
    DB.sqls.should == []
  end

  specify "#save should cancel the save and return nil if before_update returns false and raise_on_save_failure is false" do
    @c.before_update{false}
    @c.raise_on_save_failure = false
    @c.load(:id => 2233).save.should == nil
    DB.sqls.should == []
  end
end

describe "Model#before_save && Model#after_save" do
  before do
    DB.reset

    @c = model_class.call(Sequel::Model(:items)) do
      columns :x
      after_save {DB << "BLAH after"}
    end
  end
  
  specify "should be called around record update" do
    @c.before_save {DB << "BLAH before"}
    m = @c.load(:id => 2233, :x=>123)
    m.save
    DB.sqls.should == ['BLAH before', 'UPDATE items SET x = 123 WHERE (id = 2233)', 'BLAH after']
  end
  
  specify "should be called around record creation" do
    @c.before_save {DB << "BLAH before"}
    @c.no_primary_key
    @c.create(:x => 2)
    DB.sqls.should == ['BLAH before', 'INSERT INTO items (x) VALUES (2)', 'BLAH after']
  end

  specify "#save should cancel the save and raise an error if before_save returns false and raise_on_save_failure is true" do
    @c.before_save{false}
    proc{@c.load(:id => 2233).save}.should raise_error(Sequel::BeforeHookFailed)
    DB.sqls.should == []
  end

  specify "#save should cancel the save and return nil if before_save returns false and raise_on_save_failure is false" do
    @c.before_save{false}
    @c.raise_on_save_failure = false
    @c.load(:id => 2233).save.should == nil
    DB.sqls.should == []
  end
end

describe "Model#before_destroy && Model#after_destroy" do
  before do
    DB.reset

    @c = model_class.call(Sequel::Model(:items)) do
      after_destroy {DB << "BLAH after"}
    end
  end
  
  specify "should be called around record destruction" do
    @c.before_destroy {DB << "BLAH before"}
    m = @c.load(:id => 2233)
    m.destroy
    DB.sqls.should == ['BLAH before', "DELETE FROM items WHERE id = 2233", 'BLAH after']
  end

  specify "#destroy should cancel the destroy and raise an error if before_destroy returns false and raise_on_save_failure is true" do
    @c.before_destroy{false}
    proc{@c.load(:id => 2233).destroy}.should raise_error(Sequel::BeforeHookFailed)
    DB.sqls.should == []
  end

  specify "#destroy should cancel the destroy and return nil if before_destroy returns false and raise_on_save_failure is false" do
    @c.before_destroy{false}
    @c.raise_on_save_failure = false
    @c.load(:id => 2233).destroy.should == nil
    DB.sqls.should == []
  end
end

describe "Model#before_validation && Model#after_validation" do
  before do
    DB.reset

    @c = model_class.call(Sequel::Model(:items)) do
      plugin :validation_class_methods
      after_validation{DB << "BLAH after"}

      def self.validate(o)
        o.errors.add(:id, 'not valid') unless o[:id] == 2233
      end
      columns :id
    end
  end
  
  specify "should be called around validation" do
    @c.before_validation{DB << "BLAH before"}
    m = @c.load(:id => 2233)
    m.should be_valid
    DB.sqls.should == ['BLAH before', 'BLAH after']

    DB.sqls.clear
    m = @c.load(:id => 22)
    m.should_not be_valid
    DB.sqls.should == ['BLAH before', 'BLAH after']
  end

  specify "should be called when calling save" do
    @c.before_validation{DB << "BLAH before"}
    m = @c.load(:id => 2233, :x=>123)
    m.save.should == m
    DB.sqls.should == ['BLAH before', 'BLAH after', 'UPDATE items SET x = 123 WHERE (id = 2233)']

    DB.sqls.clear
    m = @c.load(:id => 22)
    m.raise_on_save_failure = false
    m.save.should == nil
    DB.sqls.should == ['BLAH before', 'BLAH after']
  end

  specify "#save should cancel the save and raise an error if before_validation returns false and raise_on_save_failure is true" do
    @c.before_validation{false}
    proc{@c.load(:id => 2233).save}.should raise_error(Sequel::BeforeHookFailed)
    DB.sqls.should == []
  end

  specify "#save should cancel the save and return nil if before_validation returns false and raise_on_save_failure is false" do
    @c.before_validation{false}
    @c.raise_on_save_failure = false
    @c.load(:id => 2233).save.should == nil
    DB.sqls.should == []
  end
end

describe "Model.has_hooks?" do
  before do
    @c = model_class.call(Sequel::Model(:items))
  end
  
  specify "should return false if no hooks are defined" do
    @c.has_hooks?(:before_save).should == false
  end
  
  specify "should return true if hooks are defined" do
    @c.before_save {'blah'}
    @c.has_hooks?(:before_save).should == true
  end
  
  specify "should return true if hooks are inherited" do
    @d = Class.new(@c)
    @d.has_hooks?(:before_save).should == false
  end
end

describe "Model#add_hook_type" do
  before do
    class ::Foo < Sequel::Model(:items)
      plugin :hook_class_methods
      add_hook_type :before_bar, :after_bar

      def bar
        return :b if before_bar == false
        return :a if after_bar == false
        true
      end
    end
    @f = Class.new(Foo)
  end
  after do
    Object.send(:remove_const, :Foo)
  end

  specify "should have before_bar and after_bar class methods" do
    @f.should respond_to(:before_bar)
    @f.should respond_to(:before_bar)
  end

  specify "should have before_bar and after_bar instance methods" do
    @f.new.should respond_to(:before_bar)
    @f.new.should respond_to(:before_bar)
  end

  specify "it should return true for bar when before_bar and after_bar hooks are returing true" do
    a = 1
    @f.before_bar { a += 1}
    @f.new.bar.should == true
    a.should == 2
    @f.after_bar { a *= 2}
    @f.new.bar.should == true
    a.should == 6
  end

  specify "it should return nil for bar when before_bar and after_bar hooks are returing false" do
    @f.new.bar.should == true
    @f.after_bar { false }
    @f.new.bar.should == :a
    @f.before_bar { false }
    @f.new.bar.should == :b
  end
end
