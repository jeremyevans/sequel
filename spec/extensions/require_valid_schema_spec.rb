require_relative "spec_helper"

describe "Sequel::Model.require_valid_schema" do
  before do
    @db = Sequel.mock
    @db.columns = proc{|sql| [:id]}
    def @db.supports_schema_parsing?; true end
    def @db.schema(t, *) t.first_source == :foos ? (raise Sequel::Error) : [[:id, {}]] end
    @c = Class.new(Sequel::Model)
    @c.db = @db
    @c.plugin :require_valid_schema
  end
  after do
    if Object.const_defined?(:Bar)
      Object.send(:remove_const, :Bar)
    end
    if Object.const_defined?(:Foo)
      Object.send(:remove_const, :Foo)
    end
  end

  it "should raise an exception when creating a model with invalid schema" do
    proc{class ::Foo < @c; end}.must_raise Sequel::Error
  end

  it "should raise an exception when setting the dataset to a table with invalid schema" do
    proc{@c.set_dataset(:foos)}.must_raise Sequel::Error
  end

  it "should raise an exception when setting the dataset to a dataset with invalid schema" do
    proc{@c.set_dataset(@db[:foos])}.must_raise Sequel::Error
  end

  it "should not raise an exception when setting the dataset to a dataset with invalid schema" do
    @c.set_dataset(@db.from(:foos, :bars))
    @c.columns.must_equal [:id]
  end

  it "should not raise an exception when creating a model with a valid implicit table" do
    class ::Bar < @c; end
    Bar.columns.must_equal [:id]
  end

  it "should not raise an exception when setting the dataset with invalid schema" do
    @c.set_dataset(:bars)
    @c.columns.must_equal [:id]
  end

  it "should warn when setting the dataset with invalid schema, when using :warn" do
    @c.plugin :require_valid_schema, :warn
    message = nil
    @c.define_singleton_method(:warn){|msg| message = msg}
    class ::Foo < @c; end
    message.must_equal "Not able to parse schema for model: Foo, table: foos"
  end

  it "should not raise an exception when creating a model with invalid schema if require_valid_schema is false" do
    @c.plugin :require_valid_schema, false
    @c.set_dataset(:foos)
    @c.columns.must_equal [:id]
  end
end

