require_relative "spec_helper"
require "pp"

describe "pretty_print plugin" do
  before do
    @db = Sequel.mock(:fetch=>{:name=>'a', :b=>'c'}, :numrows=>1)
    @c = Class.new(Sequel::Model(@db[:test]))
    @c.columns :name, :b
    @c.plugin :pretty_print
  end

  it "should print the model prettily" do
    expect(@c.new.pretty_inspect).to match(/^#<\S+ name: nil, b: nil>$/)
    expect(@c.new(b: 1).pretty_inspect).to match(/^#<\S+ name: nil, b: 1>$/)
  end
end
