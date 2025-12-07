require_relative "spec_helper"

describe "single_statement_dataset_destroy plugin" do
  before do
    @db = Sequel.mock(:fetch=>[{:id=>1}, {:id=>3}])
    @a = a = []
    @class = Class.new(Sequel::Model(@db[:t])) do
      plugin :single_statement_dataset_destroy
      define_method(:before_destroy){a << :"b#{pk}"; super()}
      define_method(:after_destroy){super(); a << :"a#{pk}"}
    end
    @db.sqls
  end

  it "should delete expected rows, running all before hooks first and all after hooks last" do
    @db.numrows = 2
    @class.dataset.destroy.must_equal 2
    @a.must_equal [:b1, :b3, :a1, :a3]
    @db.sqls.must_equal ["BEGIN", "SELECT * FROM t", "DELETE FROM t", "COMMIT"]
  end

  it "should use default behavior of multiple queries if a custom around_destroy hook is used" do
    @class.send(:define_method, :around_destroy){|&b|b.call}
    @db.numrows = 1
    @class.dataset.destroy.must_equal 2
    @a.must_equal [:b1, :a1, :b3, :a3]
    @db.sqls.must_equal [
      "SELECT * FROM t",
      "DELETE FROM t WHERE (id = 1)",
      "DELETE FROM t WHERE (id = 3)",
    ]
  end

  it "should raise and rollback if the dataset is modified during the destroy" do
    @db.numrows = 4
    proc{@class.dataset.destroy}.must_raise(Sequel::Error).
      message.must_equal "dataset changed during destroy, expected rows: 2, actual rows: 4"
    @db.sqls.must_equal ["BEGIN", "SELECT * FROM t", "DELETE FROM t", "ROLLBACK"]
  end
end
