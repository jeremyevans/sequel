require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "pg_auto_parameterize_in_array extension" do
  before do
    @db = Sequel.connect('mock://postgres')
    @db.extension :pg_auto_parameterize_duplicate_query_detection
  end

  it "should still work when queries are not recorded" do
    3.times do
      @db["SELECT 1"].all
    end
    @db.sqls.must_equal(["SELECT 1"] * 3)
  end

  it "should raise for multiple identical queries at same location" do
    e = proc do
      @db.detect_duplicate_queries do
        3.times do
          @db["SELECT 1"].all
        end
      end
    end.must_raise Sequel::Postgres::AutoParameterizeDuplicateQueryDetection::DuplicateQueries
    e.queries.keys.map(&:first).must_equal ["SELECT 1"]
    e.queries.values.must_equal [3]
  end

  it "should work when executing prepared statements" do
    e = proc do
      @db.detect_duplicate_queries do
        3.times do
          @db.execute(:sym)
        end
      end
    end.must_raise Sequel::Postgres::AutoParameterizeDuplicateQueryDetection::DuplicateQueries
    e.queries.keys.map(&:first).must_equal [:sym]
    e.queries.values.must_equal [3]
  end

  it "should call :handler option with duplicate queries if provided" do
    queries = nil
    @db.detect_duplicate_queries(:handler => proc{|qs| queries = qs}) do
      3.times do
        @db["SELECT 1"].all
      end
    end
    queries.keys.map(&:first).must_equal ["SELECT 1"]
    queries.values.must_equal [3]
  end

  it "should warn if detect_duplicate_queries given :warn and :backtrace_filter options" do
    message = nil
    @db.define_singleton_method(:warn) do |m|
      message = m
    end
    l = nil
    @db.detect_duplicate_queries(:warn => true, :backtrace_filter => /\A#{Regexp.escape(__FILE__)}/) do
      3.times do
        l = __LINE__ + 1
        @db["SELECT 1"].all
      end
    end
    message.must_include(<<MESSAGE.chomp)
duplicate queries detected:

times:3
sql:SELECT 1
backtrace (filtered):
#{__FILE__}:#{l}:in
MESSAGE
  end

  it "should warn if detect_duplicate_queries block raises" do
    message = nil
    @db.define_singleton_method(:warn) do |m|
      message = m
    end
    l = nil
    proc do
      @db.detect_duplicate_queries do
        3.times do
          l = __LINE__ + 1
          @db["SELECT 1"].all
        end
        raise RuntimeError
      end
    end.must_raise RuntimeError
    message.must_include("#{__FILE__}:#{l}:")
    message.must_include(<<MESSAGE.chomp)
duplicate queries detected:

times:3
sql:SELECT 1
backtrace:
MESSAGE
  end

  it "should raise for queries inside nested detect_duplicate_queries blocks" do
    e = proc do
      @db.detect_duplicate_queries do
        @db.detect_duplicate_queries do
          3.times do
            @db["SELECT 1"].all
          end
        end
      end
    end.must_raise Sequel::Postgres::AutoParameterizeDuplicateQueryDetection::DuplicateQueries
    e.queries.keys.map(&:first).must_equal ["SELECT 1"]
    e.queries.values.must_equal [3]

    e = proc do
      @db.detect_duplicate_queries do
        @db.ignore_duplicate_queries do
          @db.detect_duplicate_queries do
            3.times do
              @db["SELECT 1"].all
            end
          end
        end
      end
    end.must_raise Sequel::Postgres::AutoParameterizeDuplicateQueryDetection::DuplicateQueries
    e.queries.keys.map(&:first).must_equal ["SELECT 1"]
    e.queries.values.must_equal [3]

    e = proc do
      @db.detect_duplicate_queries do
        @db.ignore_duplicate_queries{}
        3.times do
          @db["SELECT 1"].all
        end
      end
    end.must_raise Sequel::Postgres::AutoParameterizeDuplicateQueryDetection::DuplicateQueries
    e.queries.keys.map(&:first).must_equal ["SELECT 1"]
    e.queries.values.must_equal [3]
  end

  it "should not raise for duplicate queries inside ignore_duplicate_queries" do
    @db.detect_duplicate_queries do
      @db.ignore_duplicate_queries do
        3.times do
          @db["SELECT 1"].all
        end
      end
    end

    @db.detect_duplicate_queries do
      @db.ignore_duplicate_queries do
        @db.ignore_duplicate_queries do
          3.times do
            @db["SELECT 1"].all
          end
        end
      end
    end

    @db.detect_duplicate_queries do
      @db.ignore_duplicate_queries do
        @db.detect_duplicate_queries{}
        3.times do
          @db["SELECT 1"].all
        end
      end
    end

    @db.detect_duplicate_queries do
      @db.ignore_duplicate_queries do
        @db.ignore_duplicate_queries{}
        3.times do
          @db["SELECT 1"].all
        end
      end
    end
  end

  it "should support ignore_duplicate_queries outside of a detect_duplicate_queries block" do
    @db.ignore_duplicate_queries do
      3.times do
        @db["SELECT 1"].all
      end
    end
    @db.sqls.must_equal(["SELECT 1"] * 3)
  end

  it "should not raise for multiple identical queries at different locations" do
    @db.detect_duplicate_queries do
      @db["SELECT 1"].all
      @db["SELECT 1"].all
      @db["SELECT 1"].all
    end
  end

  it "should not raise for multiple different queries at same location" do
    @db.detect_duplicate_queries do
      3.times do |i|
        @db["SELECT #{i}"].all
      end
    end
  end
end
