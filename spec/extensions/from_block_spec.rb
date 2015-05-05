require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "from_block extension" do
  before do
    @db = Sequel.mock
    @db.extension(:from_block)
  end

  it "should make Database#from blocks apply to FROM" do
    @db.from{f}.sql.must_equal 'SELECT * FROM f'
    @db.from{[f, g(f)]}.sql.must_equal 'SELECT * FROM f, g(f)'
  end

  it "should handle from blocks with method arguments" do
    @db.from(:f){g(f)}.sql.must_equal 'SELECT * FROM f, g(f)'
  end

  it "should handle from without block" do
    @db.from(:f).sql.must_equal 'SELECT * FROM f'
  end
end
