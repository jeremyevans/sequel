require File.join(File.dirname(__FILE__), 'spec_helper')

context "Sequel.version" do
  specify "should be in the form X.Y.Z with all being numbers" do
    Sequel.version.should =~ /\A\d+\.\d+\.\d+\z/
  end
end
