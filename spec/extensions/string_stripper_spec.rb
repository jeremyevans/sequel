#encoding: utf-8

require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::StringStripper" do
  before do
    @db = Sequel::Database.new
    @c = Class.new(Sequel::Model(@db))
    @c.plugin :string_stripper
    @c.columns :name
    @o = @c.new
  end

  it "should strip all input strings" do
    @o.name = ' name '
    @o.name.should == 'name'
  end
  
  it "should gracefully ignore non UTF-8 strings" do
    @o.name = " \xEF"
    @o.name.should == " \xEF"
  end

  it "should not affect other types" do
    @o.name = 1
    @o.name.should == 1
    @o.name = Date.today
    @o.name.should == Date.today
  end
end
