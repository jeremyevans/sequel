require_relative "spec_helper"

describe "Skip Saving Generated Columns" do
  before do
    @db = Sequel.mock
    @db.numrows = 1
    @db.autoid = 1
    def @db.schema(*)
      {
        :id=>{:type=>:integer},
        :user_id=>{:type=>:integer},
        :name=>{:type=>:string},
        :search=>{:type=>:string, :generated=>true}
      }
    end
    @db.singleton_class.send(:alias_method, :schema, :schema)
    @c = Class.new(Sequel::Model(@db[:t]))
    def @c.db_schema; @db.schema; end
    @c.columns :id, :user_id, :name, :search
    @c.plugin :skip_saving_columns
    @o = @c.load(id: 2, user_id: 1, name: 'a', search: 's')
    @db.sqls
  end

  it "should not include generated columns by default when saving" do
    @o.save
    @db.sqls.must_equal ["UPDATE t SET user_id = 1, name = 'a' WHERE (id = 2)"]
  end

  it "should not include generated columns by default when saving if loaded into class without dataset" do
    @db = Sequel.mock
    @db.numrows = 1
    @db.autoid = 1
    def @db.schema(*)
      {
        :id=>{:type=>:integer},
        :user_id=>{:type=>:integer},
        :name=>{:type=>:string},
        :search=>{:type=>:string, :generated=>true}
      }
    end
    @c = Class.new(Sequel::Model)
    @c.plugin :skip_saving_columns
    def @c.db_schema; @db.schema; end
    @c.dataset = @db[:t]
    @c.columns :id, :user_id, :name, :search
    @o = @c.load(id: 2, user_id: 1, name: 'a', search: 's')
    @db.sqls
    @o.save
    @db.sqls.must_equal ["UPDATE t SET user_id = 1, name = 'a' WHERE (id = 2)"]
  end

  it "should allow overriding which columns to skip" do
    @c.skip_saving_columns = @c.skip_saving_columns + [:name]
    @o.save
    @db.sqls.must_equal ["UPDATE t SET user_id = 1 WHERE (id = 2)"]

    @c.skip_saving_columns = [:name]
    @o.save
    @db.sqls.must_equal ["UPDATE t SET user_id = 1, search = 's' WHERE (id = 2)"]
  end

  it "should reset columns to skip from generated columns when resetting dataset if not previously overridden" do
    def @db.schema(*)
      {
        :id=>{:type=>:integer},
        :user_id=>{:type=>:integer},
        :name=>{:type=>:string, :generated=>true},
        :search=>{:type=>:string}
      }
    end
    @c.dataset = @db[:x]
    @db.sqls
    @o.save
    @db.sqls.must_equal ["UPDATE x SET user_id = 1, search = 's' WHERE (id = 2)"]

    @c.skip_saving_columns = [:search]
    @c.dataset = @db[:y]
    @db.sqls
    @o.save
    @db.sqls.must_equal ["UPDATE y SET user_id = 1, name = 'a' WHERE (id = 2)"]
  end

  it "should freeze generated columns when freezing class" do
    @c.freeze
    proc{@c.skip_saving_columns << :name}.must_raise(RuntimeError)
  end

  it "should not include skipped columns when updating, even if they have been modified" do
    @o.update(user_id: 3, search: 'sd')
    @db.sqls.must_equal ["UPDATE t SET user_id = 3 WHERE (id = 2)"]
  end

  it "should include skipped columns when specified explicitly as columns to save" do
    @o.save(:columns=>[:user_id, :search])
    @db.sqls.must_equal ["UPDATE t SET user_id = 1, search = 's' WHERE (id = 2)"]
  end

  it "should not include skipped columns when inserting, even if they are present" do
    @db.fetch = {id: 1, user_id: 2, name: 'a', search: 's2'}
    o = @c.new
    o.values.merge!(id: 1, user_id: 2, name: 'a', search: 's')
    o.save
    @db.sqls.must_equal ["INSERT INTO t (id, user_id, name) VALUES (1, 2, 'a')", "SELECT * FROM t WHERE (id = 1) LIMIT 1"]
    o.values.must_equal(id: 1, user_id: 2, name: 'a', search: 's2')
  end

  it "should work correctly in subclasses" do
    @c.skip_saving_columns = @c.skip_saving_columns + [:name]
    @sc = Class.new(@c)
    @c.skip_saving_columns = [:search]

    @so = @sc.load(id: 2, user_id: 1, name: 'a', search: 's')
    @o.save
    @db.sqls.must_equal ["UPDATE t SET user_id = 1, name = 'a' WHERE (id = 2)"]
    @so.save
    @db.sqls.must_equal ["UPDATE t SET user_id = 1 WHERE (id = 2)"]

    @sc.skip_saving_columns = [:name]
    @o.save
    @db.sqls.must_equal ["UPDATE t SET user_id = 1, name = 'a' WHERE (id = 2)"]
    @so.save
    @db.sqls.must_equal ["UPDATE t SET user_id = 1, search = 's' WHERE (id = 2)"]
  end
end
