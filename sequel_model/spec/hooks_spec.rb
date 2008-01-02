require File.join(File.dirname(__FILE__), "spec_helper")

describe "Model hooks" do
  before do
    MODEL_DB.reset

    @hooks = [
      :after_initialize,
      :before_create,
      :after_create,
      :before_update,
      :after_update,
      :before_save,
      :after_save,
      :before_destroy,
      :after_destroy
    ]
    
    # @hooks.each {|h| Sequel::Model.class_def(h) {}}
  end
  
  specify "should be definable using def <hook name>" do
    c = Class.new(Sequel::Model) do
      def before_save
        "hi there"
      end
    end
    
    c.new.before_save.should == 'hi there'
  end
  
  specify "should be definable using a block" do
    $adds = []
    c = Class.new(Sequel::Model) do
      before_save {$adds << 'hi'}
    end
    
    c.new.before_save
    $adds.should == ['hi']
  end
  
  specify "should be definable using a method name" do
    $adds = []
    c = Class.new(Sequel::Model) do
      def bye; $adds << 'bye'; end
      before_save :bye
    end
    
    c.new.before_save
    $adds.should == ['bye']
  end
  
  specify "should be additive" do
    $adds = []
    c = Class.new(Sequel::Model) do
      before_save {$adds << 'hyiyie'}
      before_save {$adds << 'byiyie'}
    end
    
    c.new.before_save
    $adds.should == ['hyiyie', 'byiyie']
  end
  
  specify "should be inheritable" do
    # pending
    
    $adds = []
    a = Class.new(Sequel::Model) do
      before_save {$adds << '123'}
    end
    
    b = Class.new(a) do
      before_save {$adds << '456'}
      before_save {$adds << '789'}
    end
    
    b.new.before_save
    $adds.should == ['123', '456', '789']
  end
  
  specify "should be overridable in descendant classes" do
    $adds = []
    a = Class.new(Sequel::Model) do
      before_save {$adds << '123'}
    end
    
    b = Class.new(a) do
      def before_save; $adds << '456'; end
    end
    
    a.new.before_save
    $adds.should == ['123']
    $adds = []
    b.new.before_save
    $adds.should == ['456']
  end
  
  specify "should stop processing if a hook returns false" do
    $flag = true
    $adds = []
    
    a = Class.new(Sequel::Model) do
      before_save {$adds << 'blah'; $flag}
      before_save {$adds << 'cruel'}
    end
    
    a.new.before_save
    $adds.should == ['blah', 'cruel']

    # chain should not break on nil
    $adds = []
    $flag = nil
    a.new.before_save
    $adds.should == ['blah', 'cruel']
    
    $adds = []
    $flag = false
    a.new.before_save
    $adds.should == ['blah']
    
    b = Class.new(a) do
      before_save {$adds << 'mau'}
    end
    
    $adds = []
    b.new.before_save
    $adds.should == ['blah']
  end
end

describe "Model#after_initialize" do
  specify "should be called after initialization" do
    $values1 = nil
    
    a = Class.new(Sequel::Model) do
      after_initialize do
        $values1 = @values.clone
        raise Sequel::Error if @values[:blow]
      end
    end
    
    a.new(:x => 1, :y => 2)
    $values1.should == {:x => 1, :y => 2}
    
    proc {a.new(:blow => true)}.should raise_error(Sequel::Error)
  end
end

describe "Model#before_create && Model#after_create" do
  setup do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items)) do
      no_primary_key
      
      before_create {MODEL_DB << "BLAH before"}
      after_create {MODEL_DB << "BLAH after"}
    end
  end
  
  specify "should be called around new record creation" do
    @c.create(:x => 2)
    MODEL_DB.sqls.should == [
      'BLAH before',
      'INSERT INTO items (x) VALUES (2)',
      'BLAH after'
    ]
  end
end

describe "Model#before_update && Model#after_update" do
  setup do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items)) do
      before_update {MODEL_DB << "BLAH before"}
      after_update {MODEL_DB << "BLAH after"}
    end
  end
  
  specify "should be called around record update" do
    m = @c.new(:id => 2233)
    m.save
    MODEL_DB.sqls.should == [
      'BLAH before',
      'UPDATE items SET id = 2233 WHERE (id = 2233)',
      'BLAH after'
    ]
  end
end

describe "Model#before_save && Model#after_save" do
  setup do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items)) do
      before_save {MODEL_DB << "BLAH before"}
      after_save {MODEL_DB << "BLAH after"}
    end
  end
  
  specify "should be called around record update" do
    m = @c.new(:id => 2233)
    m.save
    MODEL_DB.sqls.should == [
      'BLAH before',
      'UPDATE items SET id = 2233 WHERE (id = 2233)',
      'BLAH after'
    ]
  end
  
  specify "should be called around record creation" do
    @c.no_primary_key
    @c.create(:x => 2)
    MODEL_DB.sqls.should == [
      'BLAH before',
      'INSERT INTO items (x) VALUES (2)',
      'BLAH after'
    ]
  end
end

describe "Model#before_destroy && Model#after_destroy" do
  setup do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items)) do
      before_destroy {MODEL_DB << "BLAH before"}
      after_destroy {MODEL_DB << "BLAH after"}
      
      def delete
        MODEL_DB << "DELETE BLAH"
      end
    end
  end
  
  specify "should be called around record update" do
    m = @c.new(:id => 2233)
    m.destroy
    MODEL_DB.sqls.should == [
      'BLAH before',
      'DELETE BLAH',
      'BLAH after'
    ]
  end
end

describe "Model#has_hooks?" do
  setup do
    @c = Class.new(Sequel::Model)
  end
  
  specify "should return false if no hooks are defined" do
    @c.has_hooks?(:before_save).should be_false
  end
  
  specify "should return true if hooks are defined" do
    @c.before_save {'blah'}
    @c.has_hooks?(:before_save).should be_true
  end
  
  specify "should return true if hooks are inherited" do
    @d = Class.new(@c)
    @d.has_hooks?(:before_save).should be_false
    
    @c.before_save :blah
    @d.has_hooks?(:before_save).should be_true
  end
end