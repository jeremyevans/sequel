require_relative "spec_helper"

describe Sequel::MassAssignmentRestriction, "#create"  do
  it "should set model attr" do
    model_cls = Class.new

    model = model_cls.new
    err = Sequel::MassAssignmentRestriction.create("method foo doesn't exist", model, "foo")
    err.message.must_include("method foo doesn't exist for class ")
    err.model.must_equal(model)
    err.column.must_equal("foo")

    def model_cls.inspect; 'TestModel' end
    model = model_cls.new
    err = Sequel::MassAssignmentRestriction.create("method foo doesn't exist", model, "foo")
    err.message.must_equal("method foo doesn't exist for class TestModel")
    err.model.must_equal(model)
    err.column.must_equal("foo")
  end
end
