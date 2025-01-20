require_relative "spec_helper"

describe "query_blocker extension" do
  fiber_is_thread = RUBY_ENGINE == 'jruby' && Fiber.new{Thread.current}.resume != Thread.current

  before do
    @db = Sequel.mock(:extensions=>[:query_blocker])
    @ds = @db[:items]
  end

  it "#block_queries should block queries globally inside the block when called without options" do
    @ds.all.must_equal []
    proc{@db.block_queries{@ds.all}}.must_raise Sequel::QueryBlocker::BlockedQuery
    @db.block_queries{Thread.new{assert_raises(Sequel::QueryBlocker::BlockedQuery){@ds.all}}.join}
    @ds.all.must_equal []
  end

  it "#block_queries should block queries globally inside the block when called with scope: :global" do
    @ds.all.must_equal []
    proc{@db.block_queries(:scope=>:global){@ds.all}}.must_raise Sequel::QueryBlocker::BlockedQuery
    @db.block_queries(:scope=>:global){Thread.new{assert_raises(Sequel::QueryBlocker::BlockedQuery){@ds.all}}.join}
    @ds.all.must_equal []
  end

  it "#block_queries should block queries inside the current thread when called with scope: :thread" do
    @ds.all.must_equal []
    proc{@db.block_queries(:scope=>:thread){@ds.all}}.must_raise Sequel::QueryBlocker::BlockedQuery
    @db.block_queries(:scope=>:thread){Thread.new{@ds.all}.value}.must_equal []
    @db.block_queries(:scope=>:thread){Fiber.new{assert_raises(Sequel::QueryBlocker::BlockedQuery){@ds.all}}.resume} unless fiber_is_thread
    @ds.all.must_equal []
  end

  it "#block_queries should block queries inside the current fiber when called with scope: :fiber" do
    @ds.all.must_equal []
    proc{@db.block_queries(:scope=>:fiber){@ds.all}}.must_raise Sequel::QueryBlocker::BlockedQuery
    @db.block_queries(:scope=>:fiber){Thread.new{@ds.all}.value}.must_equal []
    @db.block_queries(:scope=>:fiber){Fiber.new{@ds.all}.resume}.must_equal []
    @ds.all.must_equal []
  end

  it "#block_queries should block queries inside the given thread when called with scope: Thread" do
    @ds.all.must_equal []
    proc{@db.block_queries(:scope=>Thread.current){@ds.all}}.must_raise Sequel::QueryBlocker::BlockedQuery
    @db.block_queries(:scope=>Thread.current){Thread.new{@ds.all}.value}.must_equal []
    @db.block_queries(:scope=>Thread.current){Fiber.new{assert_raises(Sequel::QueryBlocker::BlockedQuery){@ds.all}}.resume} unless fiber_is_thread
    @ds.all.must_equal []
  end

  it "#block_queries should block queries inside the given fiber when called with scope: Fiber" do
    @ds.all.must_equal []
    proc{@db.block_queries(:scope=>Fiber.current){@ds.all}}.must_raise Sequel::QueryBlocker::BlockedQuery
    @db.block_queries(:scope=>Fiber.current){Thread.new{@ds.all}.value}.must_equal []
    @db.block_queries(:scope=>Fiber.current){Fiber.new{@ds.all}.resume}.must_equal []
    @ds.all.must_equal []
  end

  it "#block_queries should raise Error if called with unsupported :scope option" do
    proc{@db.block_queries(:scope=>Object.new){}}.must_raise Sequel::Error
  end

  it "#block_queries should handle nested usage" do
    @ds.all.must_equal []
    Thread.new{@ds.all}.value.must_equal []
    Fiber.new{@ds.all}.resume.must_equal []

    @db.block_queries(scope: :fiber) do
      proc{@db.block_queries(:scope=>:fiber){@ds.all}}.must_raise Sequel::QueryBlocker::BlockedQuery
      @db.block_queries(:scope=>:fiber){Thread.new{@ds.all}.value}.must_equal []
      @db.block_queries(:scope=>:fiber){Fiber.new{@ds.all}.resume}.must_equal []

      @db.block_queries(scope: :fiber) do
        proc{@db.block_queries(:scope=>:fiber){@ds.all}}.must_raise Sequel::QueryBlocker::BlockedQuery
        @db.block_queries(:scope=>:fiber){Thread.new{@ds.all}.value}.must_equal []
        @db.block_queries(:scope=>:fiber){Fiber.new{@ds.all}.resume}.must_equal []
      end

      @db.block_queries(scope: :thread) do
        proc{@db.block_queries(:scope=>:thread){@ds.all}}.must_raise Sequel::QueryBlocker::BlockedQuery
        @db.block_queries(:scope=>:thread){Thread.new{@ds.all}.value}.must_equal []
        @db.block_queries(:scope=>:thread){Fiber.new{assert_raises(Sequel::QueryBlocker::BlockedQuery){@ds.all}}.resume}  unless fiber_is_thread

        @db.block_queries(scope: :thread) do
          proc{@db.block_queries(:scope=>:thread){@ds.all}}.must_raise Sequel::QueryBlocker::BlockedQuery
          @db.block_queries(:scope=>:thread){Thread.new{@ds.all}.value}.must_equal []
          @db.block_queries(:scope=>:thread){Fiber.new{assert_raises(Sequel::QueryBlocker::BlockedQuery){@ds.all}}.resume} unless fiber_is_thread
        end

        @db.block_queries do
          proc{@ds.all}.must_raise Sequel::QueryBlocker::BlockedQuery
          proc{@db.block_queries{@ds.all}}.must_raise Sequel::QueryBlocker::BlockedQuery
          @db.block_queries{Thread.new{assert_raises(Sequel::QueryBlocker::BlockedQuery){@ds.all}}.join}

          @db.block_queries do
            proc{@ds.all}.must_raise Sequel::QueryBlocker::BlockedQuery
            proc{@db.block_queries{@ds.all}}.must_raise Sequel::QueryBlocker::BlockedQuery
            @db.block_queries{Thread.new{assert_raises(Sequel::QueryBlocker::BlockedQuery){@ds.all}}.join}
          end

          proc{@ds.all}.must_raise Sequel::QueryBlocker::BlockedQuery
          proc{@db.block_queries{@ds.all}}.must_raise Sequel::QueryBlocker::BlockedQuery
          @db.block_queries{Thread.new{assert_raises(Sequel::QueryBlocker::BlockedQuery){@ds.all}}.join}
        end

        proc{@db.block_queries(:scope=>:thread){@ds.all}}.must_raise Sequel::QueryBlocker::BlockedQuery
        @db.block_queries(:scope=>:thread){Thread.new{@ds.all}.value}.must_equal []
        @db.block_queries(:scope=>:thread){Fiber.new{assert_raises(Sequel::QueryBlocker::BlockedQuery){@ds.all}}.resume} unless fiber_is_thread
      end

      proc{@db.block_queries(:scope=>:fiber){@ds.all}}.must_raise Sequel::QueryBlocker::BlockedQuery
      @db.block_queries(:scope=>:fiber){Thread.new{@ds.all}.value}.must_equal []
      @db.block_queries(:scope=>:fiber){Fiber.new{@ds.all}.resume}.must_equal []
    end

    @ds.all.must_equal []
    Thread.new{@ds.all}.value.must_equal []
    Fiber.new{@ds.all}.resume.must_equal []
  end

  it "#block_queries? should check whether queries are currently blocked" do
    @db.block_queries?.must_equal false
    @db.block_queries{@db.block_queries?}.must_equal true
    @db.block_queries?.must_equal false
  end
end
