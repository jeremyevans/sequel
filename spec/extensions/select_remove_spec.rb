require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Dataset#select_remove" do
  before do
    @d = Sequel.mock.from(:test).extension(:select_remove)
    @d.columns :a, :b, :c
  end

  specify "should remove columns from the selected columns" do
    @d.sql.should == 'SELECT * FROM test'
    @d.select_remove(:a).sql.should == 'SELECT b, c FROM test'
    @d.select_remove(:b).sql.should == 'SELECT a, c FROM test'
    @d.select_remove(:c).sql.should == 'SELECT a, b FROM test'
  end

  specify "should work correctly if there are already columns selected" do
    d = @d.select(:a, :b, :c)
    d.columns :a, :b, :c
    d.select_remove(:c).sql.should == 'SELECT a, b FROM test'
  end

  specify "should have no effect if the columns given are not currently selected" do
    @d.select_remove(:d).sql.should == 'SELECT a, b, c FROM test'
  end

  specify "should handle expressions where Sequel can't determine the alias by itself" do
    d = @d.select(:a, Sequel.function(:b), Sequel.as(:c, :b))
    d.columns :a, :"b()", :b
    d.select_remove(:"b()").sql.should == 'SELECT a, c AS b FROM test'
  end

  specify "should remove expressions if given exact expressions" do
    d = @d.select(:a, Sequel.function(:b), Sequel.as(:c, :b))
    d.columns :a, :"b()", :b
    d.select_remove(Sequel.function(:b)).sql.should == 'SELECT a, c AS b FROM test'
    d.select_remove(Sequel.as(:c, :b)).sql.should == 'SELECT a, b() FROM test'
  end
end
