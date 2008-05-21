require File.join(File.dirname(__FILE__), "spec_helper")

describe "Model hooks" do
  before do
    MODEL_DB.reset
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

  specify "should not be additive if the method or tag already exists" do
    $adds = []
    c = Class.new(Sequel::Model) do
      def bye; $adds << 'bye'; end
      before_save :bye
      before_save :bye
    end
    
    c.new.before_save
    $adds.should == ['bye']

    $adds = []
    d = Class.new(Sequel::Model) do
      before_save(:bye){$adds << 'hyiyie'}
      before_save(:bye){$adds << 'byiyie'}
    end
    
    d.new.before_save
    $adds.should == ['byiyie']

    $adds = []
    e = Class.new(Sequel::Model) do
      def bye; $adds << 'bye'; end
      before_save :bye
      before_save(:bye){$adds << 'byiyie'}
    end
    
    e.new.before_save
    $adds.should == ['byiyie']

    $adds = []
    e = Class.new(Sequel::Model) do
      def bye; $adds << 'bye'; end
      before_save(:bye){$adds << 'byiyie'}
      before_save :bye
    end
    
    e.new.before_save
    $adds.should == ['bye']
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
    $reached_after_initialized = false
    
    a = Class.new(Sequel::Model) do
      columns :x, :y
      after_initialize do
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
  setup do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items)) do
      columns :x
      no_primary_key
      
      after_create {MODEL_DB << "BLAH after"}
    end
  end
  
  specify "should be called around new record creation" do
    @c.before_create {MODEL_DB << "BLAH before"}
    @c.create(:x => 2)
    MODEL_DB.sqls.should == [
      'BLAH before',
      'INSERT INTO items (x) VALUES (2)',
      'BLAH after'
    ]
  end

  specify "should cancel the save if before_create returns false" do
    @c.before_create{false}
    @c.create(:x => 2).should == false
    MODEL_DB.sqls.should == []
  end
end

describe "Model#before_update && Model#after_update" do
  setup do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items)) do
      after_update {MODEL_DB << "BLAH after"}
    end
  end
  
  specify "should be called around record update" do
    @c.before_update {MODEL_DB << "BLAH before"}
    m = @c.load(:id => 2233)
    m.save
    MODEL_DB.sqls.should == [
      'BLAH before',
      'UPDATE items SET id = 2233 WHERE (id = 2233)',
      'BLAH after'
    ]
  end

  specify "should cancel the save if before_update returns false" do
    @c.before_update{false}
    @c.load(:id => 2233).save.should == false
    MODEL_DB.sqls.should == []
  end
end

describe "Model#before_save && Model#after_save" do
  setup do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items)) do
      columns :x
      before_save {MODEL_DB << "BLAH before"}
      after_save {MODEL_DB << "BLAH after"}
    end
  end
  
  specify "should be called around record update" do
    m = @c.load(:id => 2233)
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

  specify "should cancel the save if before_save returns false" do
    @c.before_save{false}
    @c.load(:id => 2233).save.should == false
    MODEL_DB.sqls.should == ["BLAH before"]
  end
end

describe "Model#before_destroy && Model#after_destroy" do
  setup do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items)) do
      after_destroy {MODEL_DB << "BLAH after"}
      
      def delete
        MODEL_DB << "DELETE BLAH"
      end
    end
  end
  
  specify "should be called around record destruction" do
    @c.before_destroy {MODEL_DB << "BLAH before"}
    m = @c.new(:id => 2233)
    m.destroy
    MODEL_DB.sqls.should == [
      'BLAH before',
      'DELETE BLAH',
      'BLAH after'
    ]
  end

  specify "should cancel the destroy if before_destroy returns false" do
    @c.before_destroy{false}
    @c.load(:id => 2233).destroy.should == false
    MODEL_DB.sqls.should == []
  end
end

describe "Model#before_validation && Model#after_validation" do
  setup do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items)) do
      before_validation{MODEL_DB << "BLAH before"}
      after_validation{MODEL_DB << "BLAH after"}

      def self.validate(o)
        o.errors[:id] << 'not valid' unless  o[:id] == 2233
      end
      
      def save!(*columns)
        MODEL_DB << "CREATE BLAH"
        self
      end
    end
  end
  
  specify "should be called around validation" do
    m = @c.new(:id => 2233)
    m.should be_valid
    MODEL_DB.sqls.should == ['BLAH before', 'BLAH after']

    MODEL_DB.sqls.clear
    m = @c.new(:id => 22)
    m.should_not be_valid
    MODEL_DB.sqls.should == ['BLAH before', 'BLAH after']
  end

  specify "should be called when calling save" do
    m = @c.new(:id => 2233)
    m.save.should == m
    MODEL_DB.sqls.should == ['BLAH before', 'BLAH after', 'CREATE BLAH']

    MODEL_DB.sqls.clear
    m = @c.new(:id => 22)
    m.save.should == false
    MODEL_DB.sqls.should == ['BLAH before', 'BLAH after']
  end

  specify "should cancel the save if before_validation returns false" do
    @c.before_validation{false}
    @c.load(:id => 2233).save.should == false
    MODEL_DB.sqls.should == ["BLAH before"]
  end
end

describe "Model.has_hooks?" do
  setup do
    @c = Class.new(Sequel::Model(:items))
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
