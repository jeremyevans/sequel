require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::InputTransformer" do
  before do
    @c = Class.new(Sequel::Model)
    @c.columns :name, :b
    @c.plugin(:input_transformer, :reverser){|v| v.is_a?(String) ? v.reverse : v}
    @o = @c.new
  end

  it "should apply transformation to input" do
    @o.name = ' name '
    @o.name.must_equal ' eman '
    @o.name = [1, 2, 3]
    @o.name.must_equal [1, 2, 3]
  end

  it "should not apply any transformers by default" do
    c = Class.new(Sequel::Model)
    c.columns :name, :b
    c.plugin :input_transformer
    c.new(:name => ' name ').name.must_equal ' name '
  end

  it "should allow skipping of columns using .skip_input_transformer" do
    @c.skip_input_transformer :reverser, :name
    v = ' name '
    @o.name = v
    @o.name.must_be_same_as(v)
  end

  it "should work correctly in subclasses" do
    o = Class.new(@c).new
    o.name = ' name '
    o.name.must_equal ' eman '
  end

  it "should raise an error if adding input filter without name" do
    proc{@c.add_input_transformer(nil){}}.must_raise(Sequel::Error)
    proc{@c.plugin(:input_transformer){}}.must_raise(Sequel::Error)
  end

  it "should raise an error if adding input filter without block" do
    proc{@c.add_input_transformer(:foo)}.must_raise(Sequel::Error)
    proc{@c.plugin(:input_transformer, :foo)}.must_raise(Sequel::Error)
  end

  it "should apply multiple input transformers in reverse order of their call" do
    @c.add_input_transformer(:add_bar){|v| v << 'bar'}
    @c.add_input_transformer(:add_foo){|v| v << 'foo'}
    @o.name = ' name '.dup
    @o.name.must_equal 'raboof eman '
  end
end
