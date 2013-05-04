require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "filter_having extension" do
  before do
    @ds = Sequel.mock.dataset.extension(:hash_aliases)
  end

  it "should make from treat hash arguments as alias specifiers" do
    @ds.from(:a=>:b).sql.should == "SELECT * FROM a AS b"
  end

  it "should make select treat hash arguments as alias specifiers" do
    @ds.select(:a=>:b).sql.should == "SELECT a AS b"
    @ds.select{{:a=>:b}}.sql.should == "SELECT a AS b"
  end
end
