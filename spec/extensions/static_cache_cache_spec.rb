require_relative "spec_helper"

describe "static_cache_cache plugin" do
  before do
    @db = Sequel.mock
    @db.fetch = [{:id=>1, :name=>'A'}, {:id=>2, :name=>'B'}]
    @c = Class.new(Sequel::Model(@db[:t]))
    def @c.name; 'Foo' end
    @c.columns :id, :name
    @file = "spec/files/static_cache_cache-spec-#{$$}.cache"
  end
  after do
    File.delete(@file) if File.file?(@file)
  end

  it "should allow dumping and loading when using static_cache plugin" do
    @c.plugin :static_cache_cache, @file
    @db.sqls
    @c.plugin :static_cache
    @db.sqls.must_equal ['SELECT * FROM t']
    @c.all.must_equal [@c.load(:id=>1, :name=>'A'), @c.load(:id=>2, :name=>'B')]

    @c.dump_static_cache_cache

    @db.fetch = []
    @c = Class.new(Sequel::Model(@db[:t]))
    def @c.name; 'Foo' end
    @c.columns :id, :name
    @c.plugin :static_cache_cache, @file
    @db.sqls
    @c.plugin :static_cache
    @db.sqls.must_be_empty
    @c.all.must_equal [@c.load(:id=>1, :name=>'A'), @c.load(:id=>2, :name=>'B')]
  end

  it "should allow dumping and loading when using subset_static_cache plugin" do
    file = @file
    @db.sqls
    setup_block = proc do
      plugin :static_cache_cache, file
      dataset_module do
        where :a, :b
        where :c, :d
      end
      plugin :subset_static_cache
      cache_subset :c
      cache_subset :a
    end

    @c.class_eval(&setup_block)
    @db.sqls.must_equal ['SELECT * FROM t WHERE d', 'SELECT * FROM t WHERE b']
    @c.c.all.must_equal [@c.load(:id=>1, :name=>'A'), @c.load(:id=>2, :name=>'B')]
    @db.sqls.must_equal []

    @c.dump_static_cache_cache

    @db.fetch = []
    @c = Class.new(Sequel::Model(@db[:t]))
    def @c.name; 'Foo' end
    @c.columns :id, :name
    @db.sqls
    @c.class_eval(&setup_block)
    @db.sqls.must_be_empty
    @c.c.all.must_equal [@c.load(:id=>1, :name=>'A'), @c.load(:id=>2, :name=>'B')]
  end

  it "should sort cache file by model name, and optionally method name" do
    @c.plugin :static_cache_cache, @file
    c1 = Class.new(@c)
    def c1.name; 'Foo' end
    c1.plugin :static_cache
    Class.new(@c) do
      def self.name; 'Bar' end
      plugin :static_cache
      dataset_module do
        where :a, :b
        where :c, :d
      end
      plugin :subset_static_cache
      cache_subset :c
      cache_subset :a
    end

    @c.instance_variable_get(:@static_cache_cache).keys.must_equal ['Foo', 'Bar', ['Bar', :c], ['Bar', :a]]
    @c.dump_static_cache_cache
    @c.instance_variable_get(:@static_cache_cache).keys.must_equal ['Foo', 'Bar', ['Bar', :c], ['Bar', :a]]

    c = Class.new(Sequel::Model)
    c.plugin :static_cache_cache, @file
    c.instance_variable_get(:@static_cache_cache).keys.must_equal ['Bar', 'Foo', ['Bar', :a], ['Bar', :c]]

    c.send(:sort_static_cache_hash, {"Foo"=>[], ["Bar", "baz"]=>[]}).keys.must_equal ["Foo", ["Bar", "baz"]]
    c.send(:sort_static_cache_hash, {["Bar", "baz"]=>[], "Foo"=>[]}).keys.must_equal ["Foo", ["Bar", "baz"]]
    c.send(:sort_static_cache_hash, {["Foo", "baz"]=>[], ["Bar", "bar"]=>[]}).keys.must_equal [["Bar", "bar"], ["Foo", "baz"]]
  end
end
