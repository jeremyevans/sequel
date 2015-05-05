require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "hash_aliases extension" do
  before do
    @ds = Sequel.mock.dataset.extension(:hash_aliases)
  end

  it "should make from treat hash arguments as alias specifiers" do
    @ds.from(:a=>:b).sql.must_equal "SELECT * FROM a AS b"
  end

  it "should not affect other arguments to from" do
    @ds.from(:a, :b).sql.must_equal "SELECT * FROM a, b"
  end

  it "should make select treat hash arguments as alias specifiers" do
    @ds.select(:a=>:b).sql.must_equal "SELECT a AS b"
    @ds.select{{:a=>:b}}.sql.must_equal "SELECT a AS b"
  end

  it "should not affect other arguments to select" do
    @ds.select(:a, :b).sql.must_equal "SELECT a, b"
  end
end
