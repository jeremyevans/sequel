require_relative "spec_helper"

shared_specs = Module.new do
  extend Minitest::Spec::DSL

  it "should not add a comment if one is not set for the dataset" do
    @ds.select_sql.must_equal 'SELECT * FROM t'
    @ds.insert_sql(:a=>1).must_equal 'INSERT INTO t (a) VALUES (1)'
    @ds.delete_sql.must_equal 'DELETE FROM t'
    @ds.update_sql(:a=>1).must_equal 'UPDATE t SET a = 1'
  end

  it "should add a comment if one is set for the dataset" do
    ds = @ds.comment("Some\nComment\r\n Here")
    ds.select_sql.must_equal "SELECT * FROM t -- Some Comment Here\n"
    ds.insert_sql(:a=>1).must_equal "INSERT INTO t (a) VALUES (1) -- Some Comment Here\n"
    ds.delete_sql.must_equal "DELETE FROM t -- Some Comment Here\n"
    ds.update_sql(:a=>1).must_equal "UPDATE t SET a = 1 -- Some Comment Here\n"
  end

  it "should not add a comment multiple times" do
    ds = @ds.comment("Some\nComment\r\n Here")
    ds.sql.must_equal "SELECT * FROM t -- Some Comment Here\n"
    ds.sql.must_equal "SELECT * FROM t -- Some Comment Here\n"
  end

  it "should not add a comment multiple times" do
    ds = @ds.comment("Some\nComment\r\n Here")
    5.times do
      ds.first(:x=>1)
      ds.db.sqls.must_equal ["SELECT * FROM t WHERE (x = 1) LIMIT 1 -- Some Comment Here\n"]
    end
  end

  it "should handle comments used in nested datasets" do
    ds = @ds.comment("Some\nComment\r\n Here")
    ds.where(:id=>ds).select_sql.must_equal "SELECT * FROM t WHERE (id IN (SELECT * FROM t -- Some Comment Here\n)) -- Some Comment Here\n"
  end

  it "should allow overriding comments" do
    @ds.comment("Foo").comment("Some\nComment\r\n Here").select_sql.must_equal "SELECT * FROM t -- Some Comment Here\n"
  end

  it "should allow disabling comments by overridding with nil" do
    @ds.comment("Foo").comment(nil).select_sql.must_equal "SELECT * FROM t"
  end

  it "should handle frozen SQL strings" do
    @ds = Sequel.mock[:t].with_extend{def select_sql; super.freeze; end}.extension(:sql_comments)
    ds = @ds.comment("Some\nComment\r\n Here")
    ds.select_sql.must_equal "SELECT * FROM t -- Some Comment Here\n"
  end
end

describe "sql_comments dataset extension" do
  before do
    @ds = Sequel.mock[:t].extension(:sql_comments)
  end
  
  include shared_specs
end

describe "sql_comments database extension" do
  before do
    @db = Sequel.mock.extension(:sql_comments)
    @ds = @db[:t]
  end
  
  include shared_specs

  it "should support setting comments for all queries executed inside a with_comments block" do
    @db.with_comments(:foo=>'bar', :baz=>'quux') do
      @ds.select_sql.must_equal "SELECT * FROM t -- foo:bar,baz:quux\n"
    end
  end

  it "should work if loading the extension multiple times" do
    @db.with_comments(:foo=>'bar', :baz=>'quux') do
      @db.extension :sql_comments
      @ds.select_sql.must_equal "SELECT * FROM t -- foo:bar,baz:quux\n"
    end
  end

  it "should support nesting with_comments blocks" do
    @db.with_comments(:foo=>'bar') do
      @db.with_comments(:baz=>'quux') do
        @ds.select_sql.must_equal "SELECT * FROM t -- foo:bar,baz:quux\n"
      end
    end
  end

  it "should support nesting with_comments blocks multiple times" do
    @db.with_comments(:foo=>'bar') do
      @db.with_comments(:baz=>'quux') do
        @ds.select_sql.must_equal "SELECT * FROM t -- foo:bar,baz:quux\n"
      end
      @db.with_comments(:x=>'y') do
        @ds.select_sql.must_equal "SELECT * FROM t -- foo:bar,x:y\n"
      end
    end
  end

  it "should support overridding values in nested blocks" do
    @db.with_comments(:foo=>'bar', :baz=>'q') do
      @db.with_comments(:baz=>'quux') do
        @ds.select_sql.must_equal "SELECT * FROM t -- foo:bar,baz:quux\n"
      end
    end
  end

  it "should support removing values in nested using nil" do
    @db.with_comments(:foo=>'bar', :bat=>'q') do
      @db.with_comments(:baz=>'quux', :bat=>nil) do
        @ds.select_sql.must_equal "SELECT * FROM t -- foo:bar,baz:quux\n"
      end
    end
  end

  it "should support combining with dataset-specific comments" do
    @db.with_comments(:foo=>'bar', :baz=>'quux') do
      @ds.comment('specific').select_sql.must_equal "SELECT * FROM t -- foo:bar,baz:quux -- specific \n"
    end
  end

  it "should only use block level comments for main dataset, not for nested datasets" do
    @db.with_comments(:foo=>'bar', :baz=>'quux') do
      ds = @ds.comment("Some\nComment\r\n Here")
      ds.where(:id=>ds).select_sql.must_equal "SELECT * FROM t WHERE (id IN (SELECT * FROM t -- Some Comment Here\n)) -- foo:bar,baz:quux -- Some Comment Here \n"
    end
  end
end
