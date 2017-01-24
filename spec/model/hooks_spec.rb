require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Model#before_create && Model#after_create" do
  before do
    @c = Class.new(Sequel::Model(:items)) do
      columns :x
      set_primary_key :x
      unrestrict_primary_key
      
      def after_create
        DB << "BLAH after"
      end
    end
    DB.reset
  end
  
  it "should be called around new record creation" do
    @c.send(:define_method, :before_create){DB << "BLAH before"}
    @c.create(:x => 2)
    DB.sqls.must_equal ['BLAH before', 'INSERT INTO items (x) VALUES (2)', 'BLAH after', 'SELECT * FROM items WHERE x = 2']
  end

  it ".create should cancel the save and raise an error if before_create returns false and raise_on_save_failure is true" do
    @c.send(:define_method, :before_create){false}
    proc{@c.create(:x => 2)}.must_raise(Sequel::HookFailed)
    DB.sqls.must_equal []
    @c.load(:id => 2233).save
  end

  it ".create should cancel the save and raise an error if before_create calls cancel_action and raise_on_save_failure is true" do
    @c.send(:define_method, :before_create){cancel_action 'not good'}
    proc{@c.create(:x => 2)}.must_raise(Sequel::HookFailed, 'not good')
    DB.sqls.must_equal []
    @c.load(:id => 2233).save
  end

  it ".create should cancel the save and return nil if before_create returns false and raise_on_save_failure is false" do
    @c.send(:define_method, :before_create){false}
    @c.raise_on_save_failure = false
    @c.create(:x => 2).must_be_nil
    DB.sqls.must_equal []
  end

  it ".create should cancel the save and return nil if before_create calls cancel_action and raise_on_save_failure is false" do
    @c.send(:define_method, :before_create){cancel_action}
    @c.raise_on_save_failure = false
    @c.create(:x => 2).must_be_nil
    DB.sqls.must_equal []
  end
end

describe "Model#before_update && Model#after_update" do
  before do
    @c = Class.new(Sequel::Model(:items)) do
      columns :id, :x
      def after_update
        DB << "BLAH after"
      end
    end
    DB.reset
  end
  
  it "should be called around record update" do
    @c.send(:define_method, :before_update){DB << "BLAH before"}
    m = @c.load(:id => 2233, :x=>123)
    m.save
    DB.sqls.must_equal ['BLAH before', 'UPDATE items SET x = 123 WHERE (id = 2233)', 'BLAH after']
  end

  it "#save should cancel the save and raise an error if before_update returns false and raise_on_save_failure is true" do
    @c.send(:define_method, :before_update){false}
    proc{@c.load(:id => 2233).save}.must_raise(Sequel::HookFailed)
    DB.sqls.must_equal []
  end

  it "#save should cancel the save and raise an error if before_update calls cancel_action and raise_on_save_failure is true" do
    @c.send(:define_method, :before_update){cancel_action}
    proc{@c.load(:id => 2233).save}.must_raise(Sequel::HookFailed)
    DB.sqls.must_equal []
  end

  it "#save should cancel the save and raise an error if before_update returns false and raise_on_failure option is true" do
    @c.send(:define_method, :before_update){false}
    @c.raise_on_save_failure = false
    proc{@c.load(:id => 2233).save(:raise_on_failure => true)}.must_raise(Sequel::HookFailed)
    DB.sqls.must_equal []
  end

  it "#save should cancel the save and return nil if before_update returns false and raise_on_save_failure is false" do
    @c.send(:define_method, :before_update){false}
    @c.raise_on_save_failure = false
    @c.load(:id => 2233).save.must_be_nil
    DB.sqls.must_equal []
  end

  it "#save should cancel the save and return nil if before_update calls cancel_action and raise_on_save_failure is false" do
    @c.send(:define_method, :before_update){cancel_action}
    @c.raise_on_save_failure = false
    @c.load(:id => 2233).save.must_be_nil
    DB.sqls.must_equal []
  end
end

describe "Model#before_save && Model#after_save" do
  before do
    @c = Class.new(Sequel::Model(:items)) do
      columns :x
      def after_save
        DB << "BLAH after"
      end
    end
    DB.reset
  end
  
  it "should be called around record update" do
    @c.send(:define_method, :before_save){DB << "BLAH before"}
    m = @c.load(:id => 2233, :x=>123)
    m.save
    DB.sqls.must_equal ['BLAH before', 'UPDATE items SET x = 123 WHERE (id = 2233)', 'BLAH after']
  end
  
  it "should be called around record creation" do
    @c.send(:define_method, :before_save){DB << "BLAH before"}
    @c.set_primary_key :x
    @c.unrestrict_primary_key
    @c.create(:x => 2)
    DB.sqls.must_equal ['BLAH before', 'INSERT INTO items (x) VALUES (2)', 'BLAH after', 'SELECT * FROM items WHERE x = 2']
  end

  it "#save should cancel the save and raise an error if before_save returns false and raise_on_save_failure is true" do
    @c.send(:define_method, :before_save){false}
    proc{@c.load(:id => 2233).save}.must_raise(Sequel::HookFailed)
    DB.sqls.must_equal []
  end

  it "#save should cancel the save and raise an error if before_save returns false and raise_on_failure option is true" do
    @c.send(:define_method, :before_save){false}
    @c.raise_on_save_failure = false
    proc{@c.load(:id => 2233).save(:raise_on_failure => true)}.must_raise(Sequel::HookFailed)
    DB.sqls.must_equal []
  end

  it "#save should cancel the save and raise an error if before_save calls cancel_action and raise_on_failure option is true" do
    @c.send(:define_method, :before_save){cancel_action}
    @c.raise_on_save_failure = false
    proc{@c.load(:id => 2233).save(:raise_on_failure => true)}.must_raise(Sequel::HookFailed)
    DB.sqls.must_equal []
  end

  it "#save should cancel the save and return nil if before_save returns false and raise_on_save_failure is false" do
    @c.send(:define_method, :before_save){false}
    @c.raise_on_save_failure = false
    @c.load(:id => 2233).save.must_be_nil
    DB.sqls.must_equal []
  end

  it "#save should cancel the save and return nil if before_save calls cancel_action and raise_on_save_failure is false" do
    @c.send(:define_method, :before_save){cancel_action}
    @c.raise_on_save_failure = false
    @c.load(:id => 2233).save.must_be_nil
    DB.sqls.must_equal []
  end

  it "#save should have a raised exception reference the model instance" do
    @c.send(:define_method, :before_save){false}
    proc{@c.create(:x => 2233)}.must_raise(Sequel::HookFailed){|e| e.model.must_equal @c.load(:x=>2233)}
    DB.sqls.must_equal []
  end
end

describe "Model#before_destroy && Model#after_destroy" do
  before do
    @c = Class.new(Sequel::Model(:items)) do
      def after_destroy
        DB << "BLAH after"
      end
    end
    DB.reset
  end
  
  it "should be called around record destruction" do
    @c.send(:define_method, :before_destroy){DB << "BLAH before"}
    m = @c.load(:id => 2233)
    m.destroy
    DB.sqls.must_equal ['BLAH before', 'DELETE FROM items WHERE id = 2233', 'BLAH after']
  end

  it "#destroy should cancel the destroy and raise an error if before_destroy returns false and raise_on_save_failure is true" do
    @c.send(:define_method, :before_destroy){false}
    proc{@c.load(:id => 2233).destroy}.must_raise(Sequel::HookFailed)
    DB.sqls.must_equal []
  end

  it "#destroy should cancel the destroy and raise an error if before_destroy calls cancel_action and raise_on_save_failure is true" do
    @c.send(:define_method, :before_destroy){cancel_action; true}
    proc{@c.load(:id => 2233).destroy}.must_raise(Sequel::HookFailed)
    DB.sqls.must_equal []
  end

  it "#destroy should cancel the destroy and raise an error if before_destroy returns false and raise_on_failure option is true" do
    @c.send(:define_method, :before_destroy){false}
    @c.raise_on_save_failure = false
    proc{@c.load(:id => 2233).destroy(:raise_on_failure => true)}.must_raise(Sequel::HookFailed)
    DB.sqls.must_equal []
  end

  it "#destroy should cancel the destroy and return nil if before_destroy returns false and raise_on_save_failure is false" do
    @c.send(:define_method, :before_destroy){false}
    @c.raise_on_save_failure = false
    @c.load(:id => 2233).destroy.must_be_nil
    DB.sqls.must_equal []
  end

  it "#destroy should cancel the destroy and return nil if before_destroy calls cancel_action and raise_on_save_failure is false" do
    @c.send(:define_method, :before_destroy){cancel_action; true}
    @c.raise_on_save_failure = false
    @c.load(:id => 2233).destroy.must_be_nil
    DB.sqls.must_equal []
  end
end

describe "Model#before_validation && Model#after_validation" do
  before do
    @c = Class.new(Sequel::Model(:items)) do
      columns :id
      def after_validation
        DB << "BLAH after"
      end

      def validate
        errors.add(:id, 'not valid') unless id == 2233
      end
    end
    DB.reset
  end
  
  it "should be called around validation" do
    @c.send(:define_method, :before_validation){DB << "BLAH before"}
    m = @c.load(:id => 2233)
    m.must_be :valid?
    DB.sqls.must_equal ['BLAH before', 'BLAH after']

    m = @c.load(:id => 22)
    m.wont_be :valid?
    DB.sqls.must_equal ['BLAH before', 'BLAH after']
  end

  it "should be called when calling save" do
    @c.send(:define_method, :before_validation){DB << "BLAH before"}
    m = @c.load(:id => 2233, :x=>123)
    m.save.must_equal m
    DB.sqls.must_equal ['BLAH before', 'BLAH after', 'UPDATE items SET x = 123 WHERE (id = 2233)']

    m = @c.load(:id => 22)
    m.raise_on_save_failure = false
    m.save.must_be_nil
    DB.sqls.must_equal ['BLAH before', 'BLAH after']
  end

  it "#save should cancel the save and raise an error if before_validation returns false and raise_on_save_failure is true" do
    @c.send(:define_method, :before_validation){false}
    proc{@c.load(:id => 2233).save}.must_raise(Sequel::HookFailed)
    DB.sqls.must_equal []
  end

  it "#save should cancel the save and raise an error if before_validation returns false and raise_on_failure option is true" do
    @c.send(:define_method, :before_validation){false}
    @c.raise_on_save_failure = false
    proc{@c.load(:id => 2233).save(:raise_on_failure => true)}.must_raise(Sequel::HookFailed)
    DB.sqls.must_equal []
  end
  
  it "#save should cancel the save and raise an error if before_validation calls cancel_action and raise_on_save_failure is true" do
    @c.send(:define_method, :before_validation){cancel_action}
    proc{@c.load(:id => 2233).save}.must_raise(Sequel::HookFailed)
    DB.sqls.must_equal []
  end

  it "#save should cancel the save and return nil if before_validation returns false and raise_on_save_failure is false" do
    @c.send(:define_method, :before_validation){false}
    @c.raise_on_save_failure = false
    @c.load(:id => 2233).save.must_be_nil
    DB.sqls.must_equal []
  end

  it "#save should cancel the save and return nil if before_validation calls cancel_action and raise_on_save_failure is false" do
    @c.send(:define_method, :before_validation){cancel_action}
    @c.raise_on_save_failure = false
    @c.load(:id => 2233).save.must_be_nil
    DB.sqls.must_equal []
  end
  
  it "#valid? should return false if before_validation returns false" do
    @c.send(:define_method, :before_validation){false}
    @c.load(:id => 2233).valid?.must_equal false
  end
end

describe "Model around filters" do
  before do
    @c = Class.new(Sequel::Model(:items)) do
      columns :id, :x
    end
    DB.reset
  end
  
  it "around_create should be called around new record creation" do
    @c.class_eval do
      def around_create
        DB << 'ac_before'
        super
        DB << 'ac_after'
      end
    end
    @c.create(:x => 2)
    DB.sqls.must_equal ['ac_before', 'INSERT INTO items (x) VALUES (2)', 'ac_after', "SELECT * FROM items WHERE id = 10"]
  end

  it "around_delete should be called around record destruction" do
    @c.class_eval do
      def around_destroy
        DB << 'ad_before'
        super
        DB << 'ad_after'
      end
    end
    @c.load(:id=>1, :x => 2).destroy
    DB.sqls.must_equal ['ad_before', 'DELETE FROM items WHERE id = 1', 'ad_after']
  end
  
  it "around_update should be called around updating existing records" do
    @c.class_eval do
      def around_update
        DB << 'au_before'
        super
        DB << 'au_after'
      end
    end
    @c.load(:id=>1, :x => 2).save
    DB.sqls.must_equal ['au_before', 'UPDATE items SET x = 2 WHERE (id = 1)', 'au_after']
  end

  it "around_save should be called around saving both new and existing records, around either after_create and after_update" do
    @c.class_eval do
      def around_update
        DB << 'au_before'
        super
        DB << 'au_after'
      end
      def around_create
        DB << 'ac_before'
        super
        DB << 'ac_after'
      end
      def around_save
        DB << 'as_before'
        super
        DB << 'as_after'
      end
    end
    @c.create(:x => 2)
    DB.sqls.must_equal ['as_before', 'ac_before', 'INSERT INTO items (x) VALUES (2)', 'ac_after', 'as_after', "SELECT * FROM items WHERE id = 10"]
    @c.load(:id=>1, :x => 2).save
    DB.sqls.must_equal ['as_before', 'au_before', 'UPDATE items SET x = 2 WHERE (id = 1)', 'au_after', 'as_after']
  end

  it "around_validation should be called around validating records" do
    @c.class_eval do
      def around_validation
        DB << 'av_before'
        super
        DB << 'av_after'
      end
      def validate
        DB << 'validate'
      end
    end
    @c.new(:x => 2).valid?.must_equal true
    DB.sqls.must_equal [ 'av_before', 'validate', 'av_after' ]
  end

  it "around_validation should be able to catch validation errors and modify them" do
    @c.class_eval do
      def validate
        errors.add(:x, 'foo')
      end
    end
    @c.new(:x => 2).valid?.must_equal false
    @c.class_eval do
      def around_validation
        super
        errors.clear
      end
    end
    @c.new(:x => 2).valid?.must_equal true
  end

  it "around_create that doesn't call super should raise a HookFailed" do
    @c.send(:define_method, :around_create){}
    proc{@c.create(:x => 2)}.must_raise(Sequel::HookFailed)
  end
  
  it "around_update that doesn't call super should raise a HookFailed" do
    @c.send(:define_method, :around_update){}
    proc{@c.load(:x => 2).save}.must_raise(Sequel::HookFailed)
  end
  
  it "around_save that doesn't call super should raise a HookFailed" do
    @c.send(:define_method, :around_save){}
    proc{@c.create(:x => 2)}.must_raise(Sequel::HookFailed)
    proc{@c.load(:x => 2).save}.must_raise(Sequel::HookFailed)
  end
  
  it "around_destroy that doesn't call super should raise a HookFailed" do
    @c.send(:define_method, :around_destroy){}
    proc{@c.load(:x => 2).destroy}.must_raise(Sequel::HookFailed)
  end
  
  it "around_validation that doesn't call super should raise a HookFailed" do
    @c.send(:define_method, :around_validation){}
    proc{@c.new.save}.must_raise(Sequel::HookFailed)
  end

  it "around_validation that doesn't call super should have valid? return false" do
    @c.send(:define_method, :around_validation){}
    @c.new.valid?.must_equal false
  end

  it "around_* that doesn't call super should return nil if raise_on_save_failure is false" do
    @c.raise_on_save_failure = false

    o = @c.load(:id => 1)
    def o.around_save() end
    o.save.must_be_nil

    o = @c.load(:id => 1)
    def o.around_update() end
    o.save.must_be_nil

    o = @c.new
    def o.around_create() end
    o.save.must_be_nil

    o = @c.new
    def o.around_validation() end
    o.save.must_be_nil
  end
end

describe "Model#after_commit and #after_rollback" do
  before do
    @db = Sequel.mock(:servers=>{:test=>{}})
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
    @o = @m.load({})
    @db.sqls
  end

  it "should call after_commit for save after the transaction commits if it commits" do
    @o.save
    @db.sqls.must_equal ['BEGIN', 'as', 'COMMIT', 'ac']
  end

  it "should call after_rollback for save after the transaction rolls back if it rolls back" do
    @o.rb = true
    @o.save
    @db.sqls.must_equal ['BEGIN', 'as', 'ROLLBACK', 'ar']
  end

  it "should have after_commit respect any surrounding transactions" do
    @db.transaction do
      @o.save
    end
    @db.sqls.must_equal ['BEGIN', 'as', 'COMMIT', 'ac']
  end

  it "should have after_rollback respect any surrounding transactions" do
    @db.transaction do
      @o.rb = true
      @o.save
    end
    @db.sqls.must_equal ['BEGIN', 'as', 'ROLLBACK', 'ar']
  end

  it "should have after_commit work with surrounding transactions and sharding" do
    @db.transaction(:server=>:test) do
      @o.save
    end
    @db.sqls.must_equal ['BEGIN -- test', 'BEGIN', 'as', 'COMMIT', 'ac', 'COMMIT -- test']
  end

  it "should have after_rollback work with surrounding transactions and sharding" do
    @db.transaction(:server=>:test) do
      @o.rb = true
      @o.save
    end
    @db.sqls.must_equal ['BEGIN -- test', 'BEGIN', 'as', 'ROLLBACK', 'ar', 'COMMIT -- test']
  end

  it "should call after_destroy_commit for destroy after the transaction commits if it commits" do
    @o.destroy
    @db.sqls.must_equal ['BEGIN', 'ad', 'COMMIT', 'adc']
  end

  it "should call after_destroy_rollback for destroy after the transaction rolls back if it rolls back" do
    @o.rb = true
    @o.destroy
    @db.sqls.must_equal ['BEGIN', 'ad', 'ROLLBACK', 'adr']
  end

  it "should have after_destroy_commit respect any surrounding transactions" do
    @db.transaction do
      @o.destroy
    end
    @db.sqls.must_equal ['BEGIN', 'ad', 'COMMIT', 'adc']
  end

  it "should have after_destroy_rollback respect any surrounding transactions" do
    @db.transaction do
      @o.rb = true
      @o.destroy
    end
    @db.sqls.must_equal ['BEGIN', 'ad', 'ROLLBACK', 'adr']
  end

  it "should have after_destroy commit work with surrounding transactions and sharding" do
    @db.transaction(:server=>:test) do
      @o.destroy
    end
    @db.sqls.must_equal ['BEGIN -- test', 'BEGIN', 'ad', 'COMMIT', 'adc', 'COMMIT -- test']
  end

  it "should have after_destroy_rollback work with surrounding transactions and sharding" do
    @db.transaction(:server=>:test) do
      @o.rb = true
      @o.destroy
    end
    @db.sqls.must_equal ['BEGIN -- test', 'BEGIN', 'ad', 'ROLLBACK', 'adr', 'COMMIT -- test']
  end

  it "should not call after_commit if use_after_commit_rollback is false" do
    @o.use_after_commit_rollback = false
    @o.save
    @db.sqls.must_equal ['BEGIN', 'as', 'COMMIT']
  end

  it "should not call after_rollback if use_after_commit_rollback is false" do
    @o.use_after_commit_rollback = false
    @o.rb = true
    @o.save
    @db.sqls.must_equal ['BEGIN', 'as', 'ROLLBACK']
  end

  it "should not call after_destroy_commit if use_after_commit_rollback is false" do
    @o.use_after_commit_rollback = false
    @o.destroy
    @db.sqls.must_equal ['BEGIN', 'ad', 'COMMIT']
  end

  it "should not call after_destroy_rollback for save if use_after_commit_rollback is false" do
    @o.use_after_commit_rollback = false
    @o.rb = true
    @o.destroy
    @db.sqls.must_equal ['BEGIN', 'ad', 'ROLLBACK']
  end

  it "should handle use_after_commit_rollback at the class level" do
    @m.use_after_commit_rollback = false
    @o.save
    @db.sqls.must_equal ['BEGIN', 'as', 'COMMIT']
  end

  it "should handle use_after_commit_rollback when subclassing" do
    @m.use_after_commit_rollback = false
    o = Class.new(@m).load({})
    @db.sqls
    o.save
    @db.sqls.must_equal ['BEGIN', 'as', 'COMMIT']
  end

  it "should handle use_after_commit_rollback when subclassing after loading" do
    @m = Class.new(Sequel::Model(@db[:items]))
    @m.use_transactions = true
    o = @m.load({})
    @db.sqls
    o.save
    @db.sqls.must_equal ['BEGIN', 'COMMIT']

    c = Class.new(@m)
    o = c.load({})
    def o.after_commit; db.execute 'ac' end
    @db.sqls
    o.save
    @db.sqls.must_equal ['BEGIN', 'COMMIT', 'ac']

    o = c.load({})
    @db.sqls
    o.save
    @db.sqls.must_equal ['BEGIN', 'COMMIT']

    c.send(:define_method, :after_commit){db.execute 'ac'}
    o = c.load({})
    @db.sqls
    o.save
    @db.sqls.must_equal ['BEGIN', 'COMMIT', 'ac']

    c = Class.new(@m)
    c.send(:include, Module.new{def after_commit; db.execute 'ac'; end})
    o = c.load({})
    @db.sqls
    o.save
    @db.sqls.must_equal ['BEGIN', 'COMMIT', 'ac']

    c.use_after_commit_rollback = false
    o = c.load({})
    @db.sqls
    o.save
    @db.sqls.must_equal ['BEGIN', 'COMMIT']
  end
end
