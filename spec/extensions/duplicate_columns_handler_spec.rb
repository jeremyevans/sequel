require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::DuplicateColumnsHandler" do
  describe "database-level configuration" do
    it "should raise error when `on_duplicate_columns` is :raise and 2 or more columns have the same name" do
      @db = Sequel.mock(:on_duplicate_columns => :raise)
      @db.extension(:duplicate_columns_handler)
      @ds = @db[:things]
      cols = [:id, :name, :id]
      proc do
        @ds.send(:columns=, cols)
      end.must_raise(Sequel::DuplicateColumnError)
    end

    it "should warn when `on_duplicate_columns` is :warn and 2 or more columns have the same name" do
      @db = Sequel.mock(:on_duplicate_columns => :warn)
      @db.extension(:duplicate_columns_handler)
      @ds = @db[:things]
      cols = [:id, :name, :id]
      warned = nil
      @ds.send(:define_singleton_method, :warn) do |message|
        warned = message
      end
      @ds.send(:columns=, cols)
      warned.must_equal("One or more duplicate columns present in #{cols.inspect}")
    end

    it "should do nothing when `on_duplicate_columns` is :ignore and 2 or more columns have the same name" do
      @db = Sequel.mock(:on_duplicate_columns => :ignore)
      @db.extension(:duplicate_columns_handler)
      @ds = @db[:things]
      cols = [:id, :name, :id]
      warned = nil
      @ds.send(:define_singleton_method, :warn) do |message|
        warned = message
      end
      @ds.send(:columns=, cols)
      warned.must_be_nil
    end

    it "should warn when `on_duplicate_columns` is not specified and 2 or more columns have the same name" do
      @db = Sequel.mock
      @db.extension(:duplicate_columns_handler)
      @ds = @db[:things]
      cols = [:id, :name, :id]
      warned = nil
      @ds.send(:define_singleton_method, :warn) do |message|
        warned = message
      end
      @ds.send(:columns=, cols)
      warned.must_equal("One or more duplicate columns present in #{cols.inspect}")
    end

    it "should not raise error when `on_duplicate_columns` is :raise and no columns have the same name" do
      @db = Sequel.mock(:on_duplicate_columns => :raise)
      @db.extension(:duplicate_columns_handler)
      @ds = @db[:things]
      cols = [:id, :name]
      @ds.send(:columns=, cols)
    end

    it "should not warn when `on_duplicate_columns` is :warn and no columns have the same name" do
      @db = Sequel.mock
      @db.extension(:duplicate_columns_handler)
      @ds = @db[:things]
      cols = [:id, :name]
      warned = nil
      @ds.send(:define_singleton_method, :warn) do |message|
        warned = message
      end
      @ds.send(:columns=, cols)
      warned.must_be_nil
    end

    it "should not warn when `on_duplicate_columns` is not specified and no columns have the same name" do
      @db = Sequel.mock(:on_duplicate_columns => :warn)
      @db.extension(:duplicate_columns_handler)
      @ds = @db[:things]
      cols = [:id, :name]
      warned = nil
      @ds.send(:define_singleton_method, :warn) do |message|
        warned = message
      end
      @ds.send(:columns=, cols)
      warned.must_be_nil
    end

    it "should raise when `on_duplicate_columns` is callable and returns :raise and 2 or more columns have the same name" do
      received_columns = nil
      handler = proc do |columns|
        received_columns = columns
        :raise
      end
      @db = Sequel.mock(:on_duplicate_columns => handler)
      @db.extension(:duplicate_columns_handler)
      @ds = @db[:things]
      cols = [:id, :name, :id]
      proc do
        @ds.send(:columns=, cols)
      end.must_raise(Sequel::DuplicateColumnError)
      received_columns.must_equal(cols)
    end
  end

  describe "dataset-level configuration" do
    before do
      @db = Sequel.mock
      @db.extension(:duplicate_columns_handler)
    end

    it "should raise error when `on_duplicate_columns` is :raise and 2 or more columns have the same name" do
      @ds = @db[:things].on_duplicate_columns(:raise)
      cols = [:id, :name, :id]
      proc do
        @ds.send(:columns=, cols)
      end.must_raise(Sequel::DuplicateColumnError)
    end

    it "should warn when `on_duplicate_columns` is :warn and 2 or more columns have the same name" do
      @ds = @db[:things].on_duplicate_columns(:warn)
      cols = [:id, :name, :id]
      warned = nil
      @ds.send(:define_singleton_method, :warn) do |message|
        warned = message
      end
      @ds.send(:columns=, cols)
      warned.must_equal("One or more duplicate columns present in #{cols.inspect}")
    end

    it "should do nothing when `on_duplicate_columns` is :ignore and 2 or more columns have the same name" do
      @ds = @db[:things].on_duplicate_columns(:ignore)
      cols = [:id, :name, :id]
      warned = nil
      @ds.send(:define_singleton_method, :warn) do |message|
        warned = message
      end
      @ds.send(:columns=, cols)
      warned.must_be_nil
    end

    it "should warn when `on_duplicate_columns` is not specified and 2 or more columns have the same name" do
      @ds = @db[:things]
      cols = [:id, :name, :id]
      warned = nil
      @ds.send(:define_singleton_method, :warn) do |message|
        warned = message
      end
      @ds.send(:columns=, cols)
      warned.must_equal("One or more duplicate columns present in #{cols.inspect}")
    end

    it "should not raise error when `on_duplicate_columns` is :raise and no columns have the same name" do
      @ds = @db[:things].on_duplicate_columns(:raise)
      cols = [:id, :name]
      @ds.send(:columns=, cols)
    end

    it "should not warn when `on_duplicate_columns` is :warn and no columns have the same name" do
      @ds = @db[:things].on_duplicate_columns(:warn)
      cols = [:id, :name]
      warned = nil
      @ds.send(:define_singleton_method, :warn) do |message|
        warned = message
      end
      @ds.send(:columns=, cols)
      warned.must_be_nil
    end

    it "should not warn when `on_duplicate_columns` is not specified and no columns have the same name" do
      @ds = @db[:things]
      cols = [:id, :name]
      warned = nil
      @ds.send(:define_singleton_method, :warn) do |message|
        warned = message
      end
      @ds.send(:columns=, cols)
      warned.must_be_nil
    end

    it "should be able to accept a block handler which receives a list of columns and returns a symbol" do
      received_columns = nil
      @ds = @db[:things].on_duplicate_columns do |columns|
        received_columns = columns
        :raise
      end
      cols = [:id, :name, :id]
      proc do
        @ds.send(:columns=, cols)
      end.must_raise(Sequel::DuplicateColumnError)
      received_columns.must_equal(cols)
    end

    it "should be able to accept a block handler which performs its own action and does not return anything useful" do
      received_columns = nil
      @ds = @db[:things].on_duplicate_columns do |columns|
        received_columns = columns
      end
      cols = [:id, :name, :id]
      warned = nil
      @ds.send(:define_singleton_method, :warn) do |message|
        warned = message
      end
      @ds.send(:columns=, cols)
      warned.must_be_nil
      received_columns.must_equal(cols)
    end

    it "should not call the block when no columns have the same name" do
      called = false
      @ds = @db[:things].on_duplicate_columns do
        called = true
      end
      cols = [:id, :name]
      warned = nil
      @ds.send(:define_singleton_method, :warn) do |message|
        warned = message
      end
      @ds.send(:columns=, cols)
      warned.must_be_nil
      called.must_equal(false)
    end

    it "should be able to accept a callable argument handler which receives a list of columns and returns a symbol" do
      received_columns = nil
      handler = proc do |columns|
        received_columns = columns
        :raise
      end
      @ds = @db[:things].on_duplicate_columns(handler)
      cols = [:id, :name, :id]
      proc do
        @ds.send(:columns=, cols)
      end.must_raise(Sequel::DuplicateColumnError)
      received_columns.must_equal(cols)
    end
  end

end
