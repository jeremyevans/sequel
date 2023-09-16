require_relative "spec_helper"

describe Sequel::MassAssignmentRestriction, "#create"  do
  it "should set model attr" do
    model_cls = Class.new
    model_cls.class_eval do
      def self.name; 'TestModel' end
    end

    model = model_cls.new
    err = Sequel::MassAssignmentRestriction.create("method foo doesn't exist", model)
    err.message.must_equal("method foo doesn't exist for class TestModel")
    err.model.must_equal(model)
  end
end
