require_relative "spec_helper"

describe "sql_comments plugin " do
  before do
    @db = Sequel.mock(:fetch=>{:id=>1, :name=>'a'})
    @c = Class.new(Sequel::Model(@db[:t]))
    @c.columns :id, :name
    def @c.to_s; 'C' end
    @c.many_to_one :c, :class=>@c, :key=>:id
    @c.one_to_many :cs, :class=>@c, :key=>:id
    @o = @c.new(:name=>'a'){|o| o.id = 1}
    @c.plugin :sql_comments
    @ds = @c.dataset
    @db.sqls
  end
  
  it "should give temporary name to name model-specific dataset module" do
    def @c.name; "Foo" end
    @c.dataset_module{where :a, :a}
    @c.sql_comments_dataset_methods :a
    @c.dataset.class.ancestors[1].name.must_equal "Foo::@_sql_comments_dataset_module"
    @c.a.all
    @db.sqls.must_equal ["SELECT * FROM t WHERE a -- model:C,method_type:dataset,method:all\n"]
  end if RUBY_VERSION >= '3.3'

  it "should include SQL comments for default class methods that issue queries" do
    @c.with_pk!(1)
    @db.sqls.must_equal ["SELECT * FROM t WHERE (id = 1) LIMIT 1 -- model:C,method_type:class,method:with_pk\n"]
  end
  
  it "should include SQL comments for default instance methods that issue queries" do
    @o.update(:name=>'b')
    @db.sqls.must_equal ["INSERT INTO t (name, id) VALUES ('b', 1) -- model:C,method_type:instance,method:update\n",
      "SELECT * FROM t WHERE (id = 1) LIMIT 1 -- model:C,method_type:instance,method:update\n"]
  end
  
  it "should include SQL comments for default dataset methods that issue queries" do
    @c.all
    @db.sqls.must_equal ["SELECT * FROM t -- model:C,method_type:dataset,method:all\n"]
  end
  
  it "should add comments for instance methods if :model is not already one of the comments" do
    @db.with_comments(:foo=>'bar'){@o.update(:name=>'b')}
    @db.sqls.must_equal ["INSERT INTO t (name, id) VALUES ('b', 1) -- foo:bar,model:C,method_type:instance,method:update\n",
      "SELECT * FROM t WHERE (id = 1) LIMIT 1 -- foo:bar,model:C,method_type:instance,method:update\n"]
  end
  
  it "should add comments for dataset methods if :model is not already one of the comments" do
    @db.with_comments(:foo=>'bar'){@c.all}
    @db.sqls.must_equal ["SELECT * FROM t -- foo:bar,model:C,method_type:dataset,method:all\n"]
  end
  
  it "should include SQL comments for association load queries" do
    @o.c
    @db.sqls.must_equal ["SELECT * FROM t WHERE (id = 1) LIMIT 1 -- model:C,method_type:association_load,association:c\n"]
  end
  
  it "should include SQL comments for association load queries even after finalizing associations " do
    @c.finalize_associations
    @c.freeze
    @o.cs
    @db.sqls.must_equal ["SELECT * FROM t WHERE (t.id = 1) -- model:C,method_type:association_load,association:cs\n"]
  end
  
  it "should include SQL comments for eager association loads issue queries" do
    @c.eager(:c).all
    @db.sqls.must_equal ["SELECT * FROM t -- model:C,method_type:dataset,method:all\n",
      "SELECT * FROM t WHERE (t.id IN (1)) -- model:C,method_type:association_eager_load,association:c\n"]
  end
  
  it "should support adding comments for custom class methods" do
    @c.extend(Module.new{def c; all; end; def d; all; end})
    @c.sql_comments_class_methods :c, :d
    @c.c
    @db.sqls.must_equal ["SELECT * FROM t -- model:C,method_type:class,method:c\n"]
    @c.d
    @db.sqls.must_equal ["SELECT * FROM t -- model:C,method_type:class,method:d\n"]
  end

  it "should support adding comments for custom instance methods" do
    @c.send(:include, Module.new{def c; model.all; end; def d; model.all; end})
    @c.sql_comments_instance_methods :c, :d
    @o.c
    @db.sqls.must_equal ["SELECT * FROM t -- model:C,method_type:instance,method:c\n"]
    @o.d
    @db.sqls.must_equal ["SELECT * FROM t -- model:C,method_type:instance,method:d\n"]
  end

  it "should support adding comments for custom dataset methods" do
    @c.dataset_module(Module.new{def c; all; end; def d; all; end})
    @c.sql_comments_dataset_methods :c
    @c.dataset.c
    @db.sqls.must_equal ["SELECT * FROM t -- model:C,method_type:dataset,method:c\n"]
    @c.sql_comments_dataset_methods :d
    @c.dataset.d
    @db.sqls.must_equal ["SELECT * FROM t -- model:C,method_type:dataset,method:d\n"]
  end
end
