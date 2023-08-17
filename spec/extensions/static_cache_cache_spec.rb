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

  it "should allow dumping and loading static cache rows from a cache file" do
    @c.plugin :static_cache_cache, @file
    @db.sqls
    @c.plugin :static_cache
    @db.sqls.must_equal ['SELECT * FROM t']
    @c.all.must_equal [@c.load(:id=>1, :name=>'A'), @c.load(:id=>2, :name=>'B')]

    @c.dump_static_cache_cache

    @db.fetch = []
    c = Class.new(Sequel::Model(@db[:t]))
    def c.name; 'Foo' end
    c.columns :id, :name
    @c.plugin :static_cache_cache, @file
    @db.sqls
    @c.plugin :static_cache
    @db.sqls.must_be_empty
    @c.all.must_equal [@c.load(:id=>1, :name=>'A'), @c.load(:id=>2, :name=>'B')]
  end

  it "should sort cache file by model name" do
    @c.plugin :static_cache_cache, @file
    c1 = Class.new(@c)
    def c1.name; 'Foo' end
    c1.plugin :static_cache
    c2 = Class.new(@c)
    def c2.name; 'Bar' end
    c2.plugin :static_cache

    @c.instance_variable_get(:@static_cache_cache).keys.must_equal %w'Foo Bar'
    @c.dump_static_cache_cache
    @c.instance_variable_get(:@static_cache_cache).keys.must_equal %w'Foo Bar'

    c = Class.new(Sequel::Model)
    c.plugin :static_cache_cache, @file
    c.instance_variable_get(:@static_cache_cache).keys.must_equal %w'Bar Foo'
  end
end
