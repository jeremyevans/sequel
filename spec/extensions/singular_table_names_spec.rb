require_relative "spec_helper"

describe "singular_table_names plugin" do
  before do
    @c = Class.new(Sequel::Model)
    @c.plugin :singular_table_names
  end
  after do
    Object.send(:remove_const, :Foo)
  end

  it "should use the singular form of model name for table name" do
    class ::Foo < @c; end
    Foo.table_name.must_equal :foo
  end

  it "should handle namespaced models using single form of last component of model name" do
    module ::Foo; end
    class Foo::Bar < @c; end
    Foo::Bar.table_name.must_equal :bar
  end
end
