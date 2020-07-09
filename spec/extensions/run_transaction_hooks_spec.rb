require_relative "spec_helper"

describe "run_transaction_hooks extension" do
  before do
    @db = Sequel.mock.extension(:run_transaction_hooks)
  end

  it "should support #run_after_{commit,rollback} hooks to run the hooks early" do
    @db.transaction do
      @db.sqls.must_equal ["BEGIN"]
      @db.run_after_commit_hooks
      @db.run_after_rollback_hooks
      @db.after_commit{@db.run "C"}
      @db.after_commit{@db.run "C2"}
      @db.after_rollback{@db.run "R"}
      @db.after_rollback{@db.run "R2"}
      @db.sqls.must_equal []
      @db.run_after_commit_hooks
      @db.sqls.must_equal ["C", "C2"]
      @db.run_after_rollback_hooks
      @db.sqls.must_equal ["R", "R2"]
    end
    @db.sqls.must_equal ["COMMIT"]

    @db.transaction(:rollback=>:always) do
      @db.after_commit{@db.run "C"}
      @db.after_commit{@db.run "C2"}
      @db.after_rollback{@db.run "R"}
      @db.after_rollback{@db.run "R2"}
      @db.sqls.must_equal ["BEGIN"]
      @db.run_after_commit_hooks
      @db.sqls.must_equal ["C", "C2"]
      @db.run_after_rollback_hooks
      @db.sqls.must_equal ["R", "R2"]
    end
    @db.sqls.must_equal ["ROLLBACK"]
  end

  it "should support #run_after_{commit,rollback} hooks to run the hooks early when savepoints are not supported" do
    def @db.supports_savepoints?; false end
    @db.transaction do
      @db.sqls.must_equal ["BEGIN"]
      @db.run_after_commit_hooks
      @db.run_after_rollback_hooks
      @db.after_commit{@db.run "C"}
      @db.after_commit{@db.run "C2"}
      @db.after_rollback{@db.run "R"}
      @db.after_rollback{@db.run "R2"}
      @db.sqls.must_equal []
      @db.run_after_commit_hooks
      @db.sqls.must_equal ["C", "C2"]
      @db.run_after_rollback_hooks
      @db.sqls.must_equal ["R", "R2"]
    end
    @db.sqls.must_equal ["COMMIT"]

    @db.transaction(:rollback=>:always) do
      @db.after_commit{@db.run "C"}
      @db.after_commit{@db.run "C2"}
      @db.after_rollback{@db.run "R"}
      @db.after_rollback{@db.run "R2"}
      @db.sqls.must_equal ["BEGIN"]
      @db.run_after_commit_hooks
      @db.sqls.must_equal ["C", "C2"]
      @db.run_after_rollback_hooks
      @db.sqls.must_equal ["R", "R2"]
    end
    @db.sqls.must_equal ["ROLLBACK"]
  end

  it "should not same hook on transaction completion when using #run_after_{commit,rollback} hooks" do
    @db.transaction do
      @db.after_commit{@db.run "C"}
      @db.after_commit{@db.run "C2"}
      @db.after_rollback{@db.run "R"}
      @db.after_rollback{@db.run "R2"}
      @db.sqls.must_equal ["BEGIN"]
      @db.run_after_commit_hooks
      @db.run_after_rollback_hooks
      @db.sqls
    end
    @db.sqls.must_equal ["COMMIT"]

    @db.transaction(:rollback=>:always) do
      @db.after_commit{@db.run "C"}
      @db.after_commit{@db.run "C2"}
      @db.after_rollback{@db.run "R"}
      @db.after_rollback{@db.run "R2"}
      @db.sqls.must_equal ["BEGIN"]
      @db.run_after_commit_hooks
      @db.run_after_rollback_hooks
      @db.sqls
    end
    @db.sqls.must_equal ["ROLLBACK"]
  end

  it "should handle savepoint hooks in #run_after_{commit,rollback} hooks" do
    @db.transaction do
      @db.after_commit{@db.run "C"}
      @db.after_rollback{@db.run "R"}
      @db.transaction(:savepoint=>:true) do
        @db.after_commit(:savepoint=>true){@db.run "SC"}
        @db.after_rollback(:savepoint=>true){@db.run "SR"}
        @db.sqls.must_equal ["BEGIN", "SAVEPOINT autopoint_1"]
        @db.run_after_commit_hooks
        @db.sqls.must_equal ["C", "SC"]
        @db.run_after_rollback_hooks
        @db.sqls.must_equal ["R", "SR"]
      end
    end
    @db.sqls.must_equal ["RELEASE SAVEPOINT autopoint_1", "COMMIT"]

    @db.transaction(:rollback=>:always) do
      @db.after_commit{@db.run "C"}
      @db.after_rollback{@db.run "R"}
      @db.transaction(:savepoint=>:true) do
        @db.after_commit(:savepoint=>true){@db.run "SC"}
        @db.after_rollback(:savepoint=>true){@db.run "SR"}
      end
      @db.sqls.must_equal ["BEGIN", "SAVEPOINT autopoint_1", "RELEASE SAVEPOINT autopoint_1"]
      @db.run_after_commit_hooks
      @db.sqls.must_equal ["C", "SC"]
      @db.run_after_rollback_hooks
      @db.sqls.must_equal ["R", "SR"]
    end
    @db.sqls.must_equal ["ROLLBACK"]

    @db.transaction(:rollback=>:always) do
      @db.after_commit{@db.run "C"}
      @db.after_rollback{@db.run "R"}
      @db.transaction(:savepoint=>:true, :rollback=>:always) do
        @db.after_commit(:savepoint=>true){@db.run "SC"}
        @db.after_rollback(:savepoint=>true){@db.run "SR"}
      end
      @db.sqls.must_equal ["BEGIN", "SAVEPOINT autopoint_1", "ROLLBACK TO SAVEPOINT autopoint_1", "SR"]
      @db.run_after_commit_hooks
      @db.sqls.must_equal ["C"]
      @db.run_after_rollback_hooks
      @db.sqls.must_equal ["R"]
    end
    @db.sqls.must_equal ["ROLLBACK"]
  end

  it "should raise Error if trying to run transaction hooks outside of a transaction" do
    proc{@db.run_after_commit_hooks}.must_raise Sequel::Error
    proc{@db.run_after_rollback_hooks}.must_raise Sequel::Error
  end
end
