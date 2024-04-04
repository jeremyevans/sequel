require_relative "spec_helper"

describe "provenance extension" do
  before do
    @ds = Sequel.mock.dataset.extension(:provenance)
  end

  line = __LINE__ + 2
  def ds
    @ds.
      from(:t).
      select(:a).
      where(:c)
  end

  if RUBY_ENGINE == 'jruby'
    line1 = line2 = line3 = line + 3
  elsif RUBY_VERSION >= '2'
    line1 = line+1
    line2 = line+2
    line3 = line+3
  else
    line1 = line2 = line3 = line
  end

  it "should not include provenance comment if there is no comment" do
    @ds.sql.must_equal 'SELECT *'
  end

  it "should include provenance comment in select SQL" do
    ds.sql.must_match %r/
      \ASELECT\ a\ FROM\ t\ WHERE\ c\ --\ 
      \n\ --\ Dataset\ Provenance
      \n\ --\ Keys:\[:from\]\ Source:#{__FILE__}:#{line1}:in\ .+
      \n\ --\ Keys:\[:select\]\ Source:#{__FILE__}:#{line2}:in\ .+
      \n\ --\ Keys:\[:where\]\ Source:#{__FILE__}:#{line3}:in\ .+
      \n\z
    /x
  end

  it "should include provenance comment in insert SQL" do
    ds.insert_sql.must_match %r/
      \AINSERT\ INTO\ t\ DEFAULT\ VALUES\ --\ 
      \n\ --\ Dataset\ Provenance
      \n\ --\ Keys:\[:from\]\ Source:#{__FILE__}:#{line1}:in\ .+
      \n\ --\ Keys:\[:select\]\ Source:#{__FILE__}:#{line2}:in\ .+
      \n\ --\ Keys:\[:where\]\ Source:#{__FILE__}:#{line3}:in\ .+
      \n\z
    /x
  end

  it "should include provenance comment in delete SQL" do
    ds.delete_sql.must_match %r/
      \ADELETE\ FROM\ t\ WHERE\ c\ --\ 
      \n\ --\ Dataset\ Provenance
      \n\ --\ Keys:\[:from\]\ Source:#{__FILE__}:#{line1}:in\ .+
      \n\ --\ Keys:\[:select\]\ Source:#{__FILE__}:#{line2}:in\ .+
      \n\ --\ Keys:\[:where\]\ Source:#{__FILE__}:#{line3}:in\ .+
      \n\z
    /x
  end

  it "should include provenance comment in insert SQL" do
    ds.update_sql(:d=>1).must_match %r/
      \AUPDATE\ t\ SET\ d\ =\ 1\ WHERE\ c\ --\ 
      \n\ --\ Dataset\ Provenance
      \n\ --\ Keys:\[:from\]\ Source:#{__FILE__}:#{line1}:in\ .+
      \n\ --\ Keys:\[:select\]\ Source:#{__FILE__}:#{line2}:in\ .+
      \n\ --\ Keys:\[:where\]\ Source:#{__FILE__}:#{line3}:in\ .+
      \n\z
    /x
  end

  it "should include provenance comment for subqueries" do
    line4 = __LINE__+1
    ds.db.from(ds).sql.must_match %r/
      \ASELECT\ \*\ FROM\ \(SELECT\ a\ FROM\ t\ WHERE\ c\ --\ 
      \n\ --\ Dataset\ Provenance
      \n\ --\ Keys:\[:from\]\ Source:#{__FILE__}:#{line1}:in\ .+
      \n\ --\ Keys:\[:select\]\ Source:#{__FILE__}:#{line2}:in\ .+
      \n\ --\ Keys:\[:where\]\ Source:#{__FILE__}:#{line3}:in\ .+
      \n\ --\ Keys:\[:append_sql\]\ Source:#{__FILE__}:#{line4}:in\ .+
      \n\)\ AS\ t1\z
    /x
  end

  it "should handle frozen SQL strings" do
    @ds = @ds.db.dataset.with_extend{def select_sql; super.freeze; end}.extension(:provenance)
    ds.sql.must_match %r/
      \ASELECT\ a\ FROM\ t\ WHERE\ c\ --\ 
      \n\ --\ Dataset\ Provenance
      \n\ --\ Keys:\[:from\]\ Source:#{__FILE__}:#{line1}:in\ .+
      \n\ --\ Keys:\[:select\]\ Source:#{__FILE__}:#{line2}:in\ .+
      \n\ --\ Keys:\[:where\]\ Source:#{__FILE__}:#{line3}:in\ .+
      \n\z
    /x
  end

  it "should handle use with placeholder literalizers" do
    ds = self.ds

    2.times do
      line4 = __LINE__+1
      ds.first(1)
      sql = ds.db.sqls.last
      sql.must_match %r/
        \ASELECT\ a\ FROM\ t\ WHERE\ c\ LIMIT\ 1\ --\ 
        \n\ --\ Dataset\ Provenance
        \n\ --\ Keys:\[:from\]\ Source:#{__FILE__}:#{line1}:in\ .+
        \n\ --\ Keys:\[:select\]\ Source:#{__FILE__}:#{line2}:in\ .+
        \n\ --\ Keys:\[:where\]\ Source:#{__FILE__}:#{line3}:in\ .+
        \n\ --\ Keys:\[:limit\]\ Source:#{__FILE__}:#{line4}:in\ .+
        \n\z
      /x
    end

    2.times do
      line4 = __LINE__+1
      ds.first(1)
      sql = ds.db.sqls.last
      sql.must_match %r/
        \ASELECT\ a\ FROM\ t\ WHERE\ c\ LIMIT\ 1\ --\ 
        \n\ --\ Dataset\ Provenance
        \n\ --\ Keys:\[:from\]\ Source:#{__FILE__}:#{line1}:in\ .+
        \n\ --\ Keys:\[:select\]\ Source:#{__FILE__}:#{line2}:in\ .+
        \n\ --\ Keys:\[:where\]\ Source:#{__FILE__}:#{line3}:in\ .+
        \n\ --\ Keys:\[:limit\]\ Source:#{__FILE__}:#{line4}:in\ .+
        \n\ --\ Keys:\[:placeholder_literalizer\]\ Source:#{__FILE__}:#{line4}:in\ .+
        \n\z
      /x
    end
  end

  it "should respect :provenance_caller_ignore Database option" do
    ds.db.opts[:provenance_caller_ignore] = /:(#{line1}|#{line2}|#{line3}):/
    line4 = __LINE__+1
    ds.sql.must_match %r/
      \ASELECT\ a\ FROM\ t\ WHERE\ c\ --\ 
      \n\ --\ Dataset\ Provenance
      \n\ --\ Keys:\[:from\]\ Source:#{__FILE__}:#{line4}:in\ .+
      \n\ --\ Keys:\[:select\]\ Source:#{__FILE__}:#{line4}:in\ .+
      \n\ --\ Keys:\[:where\]\ Source:#{__FILE__}:#{line4}:in\ .+
      \n\z
    /x
  end
end
