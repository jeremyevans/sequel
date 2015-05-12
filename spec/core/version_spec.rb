require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')

describe "Sequel.version" do
  it "should be in the form X.Y.Z with all being numbers" do
    Sequel.version.must_match(/\A\d+\.\d+\.\d+\z/)
  end
end
