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
      set_primary_key :x
      unrestrict_primary_key
       
      def _save_refresh
      end
      
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
    proc{@c.create(:x => 2)}.should raise_error(Sequel::BeforeHookFailed)
    MODEL_DB.sqls.should == []
    proc{@c.load(:id => 2233).save}.should_not raise_error(Sequel::ValidationFailed)
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

  specify "#save should cancel the save and raise an error if before_update returns false and raise_on_failure option is true" do
    @c.send(:define_method, :before_update){false}
    @c.raise_on_save_failure = false
    proc{@c.load(:id => 2233).save(:raise_on_failure => true)}.should_not raise_error(Sequel::ValidationFailed)
    proc{@c.load(:id => 2233).save(:raise_on_failure => true)}.should raise_error(Sequel::BeforeHookFailed)
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
    @c.send(:define_method, :_save_refresh){}
    @c.set_primary_key :x
    @c.unrestrict_primary_key
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

  specify "#save should cancel the save and raise an error if before_save returns false and raise_on_failure option is true" do
    @c.send(:define_method, :before_save){false}
    @c.raise_on_save_failure = false
    proc{@c.load(:id => 2233).save(:raise_on_failure => true)}.should_not raise_error(Sequel::ValidationFailed)
    proc{@c.load(:id => 2233).save(:raise_on_failure => true)}.should raise_error(Sequel::BeforeHookFailed)
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

  specify "#destroy should cancel the destroy and raise an error if before_destroy returns false and raise_on_failure option is true" do
    @c.send(:define_method, :before_destroy){false}
    @c.raise_on_save_failure = false
    proc{@c.load(:id => 2233).destroy(:raise_on_failure => true)}.should raise_error(Sequel::BeforeHookFailed)
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

  specify "#save should cancel the save and raise an error if before_validation returns false and raise_on_failure option is true" do
    @c.send(:define_method, :before_validation){false}
    @c.raise_on_save_failure = false
    proc{@c.load(:id => 2233).save(:raise_on_failure => true)}.should_not raise_error(Sequel::ValidationFailed)
    proc{@c.load(:id => 2233).save(:raise_on_failure => true)}.should raise_error(Sequel::BeforeHookFailed)
    MODEL_DB.sqls.should == []
  end
  
  specify "#save should cancel the save and return nil if before_validation returns false and raise_on_save_failure is false" do
    @c.send(:define_method, :before_validation){false}
    @c.raise_on_save_failure = false
    @c.load(:id => 2233).save.should == nil
    MODEL_DB.sqls.should == []
  end
  
  specify "#valid? should return false if before_validation returns false" do
    @c.send(:define_method, :before_validation){false}
    @c.load(:id => 2233).valid?.should == false
  end
end

describe "Model around filters" do
  before do
    MODEL_DB.reset

    @c = Class.new(Sequel::Model(:items))
    @c.class_eval do
      columns :id, :x
      def _save_refresh(*a) end
    end
  end
  
  specify "around_create should be called around new record creation" do
    @c.class_eval do
      def around_create
        MODEL_DB << 'ac_before'
        super
        MODEL_DB << 'ac_after'
      end
    end
    @c.create(:x => 2)
    MODEL_DB.sqls.should == [ 'ac_before', 'INSERT INTO items (x) VALUES (2)', 'ac_after' ]
  end

  specify "around_delete should be called around record destruction" do
    @c.class_eval do
      def around_destroy
        MODEL_DB << 'ad_before'
        super
        MODEL_DB << 'ad_after'
      end
    end
    @c.load(:id=>1, :x => 2).destroy
    MODEL_DB.sqls.should == [ 'ad_before', 'DELETE FROM items WHERE (id = 1)', 'ad_after' ]
  end
  
  specify "around_update should be called around updating existing records" do
    @c.class_eval do
      def around_update
        MODEL_DB << 'au_before'
        super
        MODEL_DB << 'au_after'
      end
    end
    @c.load(:id=>1, :x => 2).save
    MODEL_DB.sqls.should == [ 'au_before', 'UPDATE items SET x = 2 WHERE (id = 1)', 'au_after' ]
  end

  specify "around_update should be called around saving both new and existing records, around either after_create and after_update" do
    @c.class_eval do
      def around_update
        MODEL_DB << 'au_before'
        super
        MODEL_DB << 'au_after'
      end
      def around_create
        MODEL_DB << 'ac_before'
        super
        MODEL_DB << 'ac_after'
      end
      def around_save
        MODEL_DB << 'as_before'
        super
        MODEL_DB << 'as_after'
      end
    end
    @c.create(:x => 2)
    MODEL_DB.sqls.should == [ 'as_before', 'ac_before', 'INSERT INTO items (x) VALUES (2)', 'ac_after', 'as_after' ]
    MODEL_DB.sqls.clear
    @c.load(:id=>1, :x => 2).save
    MODEL_DB.sqls.should == [ 'as_before', 'au_before', 'UPDATE items SET x = 2 WHERE (id = 1)', 'au_after', 'as_after' ]
  end

  specify "around_validation should be called around validating records" do
    @c.class_eval do
      def around_validation
        MODEL_DB << 'av_before'
        super
        MODEL_DB << 'av_after'
      end
      def validate
        MODEL_DB << 'validate'
      end
    end
    @c.new(:x => 2).valid?.should == true
    MODEL_DB.sqls.should == [ 'av_before', 'validate', 'av_after' ]
  end

  specify "around_validation should be able to catch validation errors and modify them" do
    @c.class_eval do
      def validate
        errors.add(:x, 'foo')
      end
    end
    @c.new(:x => 2).valid?.should == false
    @c.class_eval do
      def around_validation
        super
        errors.clear
      end
    end
    @c.new(:x => 2).valid?.should == true
  end

  specify "around_create that doesn't call super should raise a HookFailed" do
    @c.send(:define_method, :around_create){}
    proc{@c.create(:x => 2)}.should raise_error(Sequel::HookFailed)
  end
  
  specify "around_update that doesn't call super should raise a HookFailed" do
    @c.send(:define_method, :around_update){}
    proc{@c.load(:x => 2).save}.should raise_error(Sequel::HookFailed)
  end
  
  specify "around_save that doesn't call super should raise a HookFailed" do
    @c.send(:define_method, :around_save){}
    proc{@c.create(:x => 2)}.should raise_error(Sequel::HookFailed)
    proc{@c.load(:x => 2).save}.should raise_error(Sequel::HookFailed)
  end
  
  specify "around_destroy that doesn't call super should raise a HookFailed" do
    @c.send(:define_method, :around_destroy){}
    proc{@c.load(:x => 2).destroy}.should raise_error(Sequel::HookFailed)
  end
  
  specify "around_validation that doesn't call super should raise a HookFailed" do
    @c.send(:define_method, :around_validation){}
    proc{@c.new.save}.should raise_error(Sequel::HookFailed)
  end

  specify "around_validation that doesn't call super should have valid? return false" do
    @c.send(:define_method, :around_validation){}
    @c.new.valid?.should == false
  end

  specify "around_* that doesn't call super should return nil if raise_on_save_failure is false" do
    @c.raise_on_save_failure = false

    o = @c.load(:id => 1)
    o.meta_def(:around_save){}
    o.save.should == nil

    o = @c.load(:id => 1)
    o.meta_def(:around_update){}
    o.save.should == nil

    o = @c.new
    o.meta_def(:around_create){}
    o.save.should == nil

    o = @c.new
    o.meta_def(:around_validation){}
    o.save.should == nil
  end
end

describe "Model#after_commit and #after_rollback" do
  before do
    @logger = Object.new
    def @logger.method_missing(meth, sql, args=nil)
      (@sqls ||= []) << sql
    end
    def @logger.sqls
      @sqls
    end
    @db = Class.new(Sequel::Database) do
      def connect(*)
        Object.new
      end
      def log_connection_execute(conn, sql)
        execute(sql)
      end
      def execute(sql, opts={})
        @loggers.each{|l| l.info(sql)}
      end
    end.new(:loggers=>[@logger], :servers=>{:test=>{}})
    @m = Class.new(Sequel::Model(@db[:items])) do
      attr_accessor :rb
      def _delete
      end
      def after_save
        db.execute('as')
        raise Sequel::Rollback if rb
      end
      def after_commit
        db.execute('ac')
      end
      def after_rollback
        db.execute('ar')
      end
      def after_destroy
        db.execute('ad')
        raise Sequel::Rollback if rb
      end
      def after_destroy_commit
        db.execute('adc')
      end
      def after_destroy_rollback
        db.execute('adr')
      end
    end
    @m.use_transactions = true
    @m.set_primary_key :id
    @o = @m.load({})
  end

  specify "should call after_commit for save after the transaction commits if it commits" do
    @o.save
    @logger.sqls.should == ['BEGIN', 'as', 'COMMIT', 'ac']
  end

  specify "should call after_rollback for save after the transaction rolls back if it rolls back" do
    @o.rb = true
    @o.save
    @logger.sqls.should == ['BEGIN', 'as', 'ROLLBACK', 'ar']
  end

  specify "should have after_commit respect any surrounding transactions" do
    @db.transaction do
      @o.save
    end
    @logger.sqls.should == ['BEGIN', 'as', 'COMMIT', 'ac']
    @logger.sqls.clear
  end

  specify "should have after_rollback respect any surrounding transactions" do
    @db.transaction do
      @o.rb = true
      @o.save
    end
    @logger.sqls.should == ['BEGIN', 'as', 'ROLLBACK', 'ar']
    @logger.sqls.clear
  end

  specify "should have after_commit work with surrounding transactions and sharding" do
    @db.transaction(:server=>:test) do
      @o.save
    end
    @logger.sqls.should == ['BEGIN', 'BEGIN', 'as', 'COMMIT', 'ac', 'COMMIT']
    @logger.sqls.clear
  end

  specify "should have after_rollback work with surrounding transactions and sharding" do
    @db.transaction(:server=>:test) do
      @o.rb = true
      @o.save
    end
    @logger.sqls.should == ['BEGIN', 'BEGIN', 'as', 'ROLLBACK', 'ar', 'COMMIT']
    @logger.sqls.clear
  end

  specify "should call after_destroy_commit for destroy after the transaction commits if it commits" do
    @o.destroy
    @logger.sqls.should == ['BEGIN', 'ad', 'COMMIT', 'adc']
  end

  specify "should call after_destroy_rollback for destroy after the transaction rolls back if it rolls back" do
    @o.rb = true
    @o.destroy
    @logger.sqls.should == ['BEGIN', 'ad', 'ROLLBACK', 'adr']
  end

  specify "should have after_destroy_commit respect any surrounding transactions" do
    @db.transaction do
      @o.destroy
    end
    @logger.sqls.should == ['BEGIN', 'ad', 'COMMIT', 'adc']
    @logger.sqls.clear
  end

  specify "should have after_destroy_rollback respect any surrounding transactions" do
    @db.transaction do
      @o.rb = true
      @o.destroy
    end
    @logger.sqls.should == ['BEGIN', 'ad', 'ROLLBACK', 'adr']
    @logger.sqls.clear
  end

  specify "should have after_destroy commit work with surrounding transactions and sharding" do
    @db.transaction(:server=>:test) do
      @o.destroy
    end
    @logger.sqls.should == ['BEGIN', 'BEGIN', 'ad', 'COMMIT', 'adc', 'COMMIT']
    @logger.sqls.clear
  end

  specify "should have after_destroy_rollback work with surrounding transactions and sharding" do
    @db.transaction(:server=>:test) do
      @o.rb = true
      @o.destroy
    end
    @logger.sqls.should == ['BEGIN', 'BEGIN', 'ad', 'ROLLBACK', 'adr', 'COMMIT']
    @logger.sqls.clear
  end

end
