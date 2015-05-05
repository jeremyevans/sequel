require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "scissors plugin" do
  before do
    @m = Class.new(Sequel::Model(:items))
    @m.use_transactions = true
    @m.plugin :scissors
    @m.db.sqls
  end

  it "Model.delete should delete from the dataset" do
    @m.delete
    @m.db.sqls.must_equal ['DELETE FROM items']
  end

  it "Model.update should update the dataset" do
    @m.update(:a=>1)
    @m.db.sqls.must_equal ['UPDATE items SET a = 1']
  end

  it "Model.destory each instance in the dataset" do
    @m.dataset._fetch = {:id=>1}
    @m.destroy
    @m.db.sqls.must_equal ['BEGIN', 'SELECT * FROM items', 'DELETE FROM items WHERE id = 1', 'COMMIT']
  end
end
