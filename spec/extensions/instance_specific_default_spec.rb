require_relative "spec_helper"

describe "instance_specific_default plugin" do
  before do
    @db = Sequel.mock
    @c = Class.new(Sequel::Model(@db[:test]))
    def @c.name; 'C' end
    @c.columns :id, :name
    @db.sqls
  end

  it "should support setting a true value" do
    @c.plugin :instance_specific_default, true
    @c.many_to_one :c, :class=>@c do |ds| ds end
    @c.association_reflection(:c)[:instance_specific].must_equal true
  end

  it "should support setting a false value" do
    @c.plugin :instance_specific_default, false
    @c.many_to_one :c, :class=>@c do |ds| ds end
    @c.association_reflection(:c)[:instance_specific].must_equal false
  end

  it "should support setting a :default value" do
    @c.plugin :instance_specific_default, :default
    @c.many_to_one :c, :class=>@c do |ds| ds end
    @c.association_reflection(:c)[:instance_specific].must_equal true
  end

  it "should support setting a :warn value" do
    warn_args = nil
    @c.define_singleton_method(:warn){|*args| warn_args = args}
    @c.plugin :instance_specific_default, :warn
    @c.many_to_one :c, :class=>@c do |ds| ds end
    @c.association_reflection(:c)[:instance_specific].must_equal true
    warn_args[0].must_match(/possibly instance-specific association without :instance_specific option/)
    warn_args[1].must_equal(:uplevel=>3)
  end

  it "should support setting a :raise value" do
    @c.plugin :instance_specific_default, :raise
    proc{@c.many_to_one :c, :class=>@c do |ds| ds end}.must_raise Sequel::Error
  end

  it "should raise in invalid option is given" do
    @c.plugin :instance_specific_default, Object.new
    proc{@c.many_to_one :c, :class=>@c do |ds| ds end}.must_raise Sequel::Error
  end

  it "should work correctly in subclasses" do
    @c.plugin :instance_specific_default, false
    c = Class.new(@c)
    c.many_to_one :c, :class=>@c do |ds| ds end
    c.association_reflection(:c)[:instance_specific].must_equal false
  end

  it "should be ignored for associations with a :dataset option" do
    @c.plugin :instance_specific_default, false
    @c.many_to_one :c, :class=>@c, :dataset=>proc{|r| r.associated_class.where(:id=>id)}
    @c.association_reflection(:c)[:instance_specific].must_equal true
  end

  it "should be considered for when cloning association with block" do
    @c.plugin :instance_specific_default, false
    @c.many_to_one :c, :class=>@c do |ds| ds end
    @c.many_to_one :c, :clone=>:c
    @c.association_reflection(:c)[:instance_specific].must_equal false
  end
end
