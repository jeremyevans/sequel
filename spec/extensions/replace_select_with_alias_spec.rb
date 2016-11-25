require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Dataset#replace_select_with_alias" do
  before do
    @d = Sequel.mock.from(:test).extension(:replace_select_with_alias)
    @d.columns :a, :b
  end

  specify "should do nothing if aliased columns are not present" do
    @d.select(:a).replace_select_with_alias(Sequel.as("5", :b)).sql.should == "SELECT a FROM test"
  end

  specify "should select all if no select are present" do
    @d.replace_select_with_alias(Sequel.as("1", :a)).sql.should == 'SELECT * FROM test'
  end

  specify "should replace the currently selected columns with matching alias" do
    @d.select(:a, :b).replace_select_with_alias(Sequel.as("1", :a), Sequel.as("2", :b)).sql.should == "SELECT '1' AS a, '2' AS b FROM test"
  end

  specify "should leave unaliased columns untouched" do
    @d.select(:a, :b).replace_select_with_alias(Sequel.as("1", :a)).sql.should == "SELECT '1' AS a, b FROM test"
  end

  specify "should accept a block that yields a virtual row" do
    @d.select(:a, :b).replace_select_with_alias { |o| Sequel.as("1", o.a) }.sql.should == "SELECT '1' AS a, b FROM test"
  end
end
