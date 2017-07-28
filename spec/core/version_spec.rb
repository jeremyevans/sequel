require_relative "spec_helper"

describe "Sequel.version" do
  it "should be in the form X.Y.Z with all being numbers" do
    Sequel.version.must_match(/\A\d+\.\d+\.\d+\z/)
  end
end
