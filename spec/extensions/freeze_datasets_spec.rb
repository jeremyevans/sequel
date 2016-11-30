require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "freeze_datasets extension" do
  before do
    @db = Sequel.mock.extension(:freeze_datasets)
  end

  it "should freeze datasets by default" do
    @db.dataset.frozen?.must_equal true
    @db.fetch('SQL').frozen?.must_equal true
    @db.from(:table).frozen?.must_equal true
    @db[:table].frozen?.must_equal true
  end

  it "should have dataset#dup return frozen dataset" do
    @db.dataset.dup.frozen?.must_equal true
  end
end
