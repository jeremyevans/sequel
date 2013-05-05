require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "filter_having extension" do
  before do
    @dataset = Sequel.mock[:test].extension(:empty_array_ignore_nulls)
  end

  specify "should handle all types of IN/NOT IN queries with empty arrays" do
    @dataset.filter(:id => []).sql.should == "SELECT * FROM test WHERE (1 = 0)"
    @dataset.filter([:id1, :id2] => []).sql.should == "SELECT * FROM test WHERE (1 = 0)"
    @dataset.exclude(:id => []).sql.should == "SELECT * FROM test WHERE (1 = 1)"
    @dataset.exclude([:id1, :id2] => []).sql.should == "SELECT * FROM test WHERE (1 = 1)"
  end

  specify "should handle IN/NOT IN queries with multiple columns and an empty dataset where the database doesn't support it" do
    @dataset.meta_def(:supports_multiple_column_in?){false}
    db = Sequel.mock
    d1 = db[:test].select(:id1, :id2).filter(:region=>'Asia').columns(:id1, :id2)
    @dataset.filter([:id1, :id2] => d1).sql.should == "SELECT * FROM test WHERE (1 = 0)"
    db.sqls.should == ["SELECT id1, id2 FROM test WHERE (region = 'Asia')"]
    @dataset.exclude([:id1, :id2] => d1).sql.should == "SELECT * FROM test WHERE (1 = 1)"
    db.sqls.should == ["SELECT id1, id2 FROM test WHERE (region = 'Asia')"]
  end
end
