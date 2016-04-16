require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::DuplicateColumnsHandler" do

  def must_warn_for_columns(*cols)
    warned = nil
    @ds.send(:define_singleton_method, :warn) do |message|
      warned = message
    end
    @ds.send(:columns=, cols)
    warned.must_equal("One or more duplicate columns present in #{cols.inspect}")
  end

  def must_raise_for_columns(*cols)
    proc do
      @ds.send(:columns=, cols)
    end.must_raise(Sequel::DuplicateColumnError)
  end

  def must_not_warn_for_columns(*cols)
    warned = nil
    @ds.send(:define_singleton_method, :warn) do |message|
      warned = message
    end
    @ds.send(:columns=, cols)
    warned.must_be_nil
  end

  def must_not_raise_for_columns(*cols)
    @ds.send(:columns=, cols)
  end

  describe "database-level configuration" do
    it "should raise error when `on_duplicate_columns` is :raise and 2 or more columns have the same name" do
      @db = Sequel.mock(:on_duplicate_columns => :raise)
      @db.extension(:duplicate_columns_handler)
      @ds = @db[:things]
      must_raise_for_columns(:id, :name, :id)
    end

    it "should warn when `on_duplicate_columns` is :warn and 2 or more columns have the same name" do
      @db = Sequel.mock(:on_duplicate_columns => :warn)
      @db.extension(:duplicate_columns_handler)
      @ds = @db[:things]
      must_warn_for_columns(:id, :name, :id)
    end

    it "should do nothing when `on_duplicate_columns` is :ignore and 2 or more columns have the same name" do
      @db = Sequel.mock(:on_duplicate_columns => :ignore)
      @db.extension(:duplicate_columns_handler)
      @ds = @db[:things]
      must_not_warn_for_columns(:id, :name, :id)
    end

    it "should warn when `on_duplicate_columns` is not specified and 2 or more columns have the same name" do
      @db = Sequel.mock
      @db.extension(:duplicate_columns_handler)
      @ds = @db[:things]
      must_warn_for_columns(:id, :name, :id)
    end

    it "should not raise error when `on_duplicate_columns` is :raise and no columns have the same name" do
      @db = Sequel.mock(:on_duplicate_columns => :raise)
      @db.extension(:duplicate_columns_handler)
      @ds = @db[:things]
      must_not_raise_for_columns(:id, :name)
    end

    it "should not warn when `on_duplicate_columns` is :warn and no columns have the same name" do
      @db = Sequel.mock
      @db.extension(:duplicate_columns_handler)
      @ds = @db[:things]
      must_not_warn_for_columns(:id, :name)
    end

    it "should not warn when `on_duplicate_columns` is not specified and no columns have the same name" do
      @db = Sequel.mock(:on_duplicate_columns => :warn)
      @db.extension(:duplicate_columns_handler)
      @ds = @db[:things]
      must_not_warn_for_columns(:id, :name)
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
      must_raise_for_columns(:id, :name, :id)
      received_columns.must_equal([:id, :name, :id])
    end
  end

  describe "dataset-level configuration" do
    before do
      @db = Sequel.mock
      @db.extension(:duplicate_columns_handler)
    end

    it "should raise error when `on_duplicate_columns` is :raise and 2 or more columns have the same name" do
      @ds = @db[:things].on_duplicate_columns(:raise)
      must_raise_for_columns(:id, :name, :id)
    end

    it "should warn when `on_duplicate_columns` is :warn and 2 or more columns have the same name" do
      @ds = @db[:things].on_duplicate_columns(:warn)
      must_warn_for_columns(:id, :name, :id)
    end

    it "should do nothing when `on_duplicate_columns` is :ignore and 2 or more columns have the same name" do
      @ds = @db[:things].on_duplicate_columns(:ignore)
      must_not_warn_for_columns(:id, :name, :id)
    end

    it "should warn when `on_duplicate_columns` is not specified and 2 or more columns have the same name" do
      @ds = @db[:things]
      must_warn_for_columns(:id, :name, :id)
    end

    it "should not raise error when `on_duplicate_columns` is :raise and no columns have the same name" do
      @ds = @db[:things].on_duplicate_columns(:raise)
      must_not_raise_for_columns(:id, :name)
    end

    it "should not warn when `on_duplicate_columns` is :warn and no columns have the same name" do
      @ds = @db[:things].on_duplicate_columns(:warn)
      must_not_warn_for_columns(:id, :name)
    end

    it "should not warn when `on_duplicate_columns` is not specified and no columns have the same name" do
      @ds = @db[:things]
      must_not_warn_for_columns(:id, :name)
    end

    it "should be able to accept a block handler which receives a list of columns and returns a symbol" do
      received_columns = nil
      @ds = @db[:things].on_duplicate_columns do |columns|
        received_columns = columns
        :raise
      end
      must_raise_for_columns(:id, :name, :id)
      received_columns.must_equal([:id, :name, :id])
    end

    it "should be able to accept a block handler which performs its own action and does not return anything useful" do
      called = false
      @ds = @db[:things].on_duplicate_columns do
        called = true
      end
      must_not_warn_for_columns(:id, :name, :id)
      must_not_raise_for_columns(:id, :name, :id)
      called.must_equal(true)
    end

    it "should not call the block when no columns have the same name" do
      called = false
      @ds = @db[:things].on_duplicate_columns do
        called = true
      end
      must_not_warn_for_columns(:id, :name)
      called.must_equal(false)
    end

    it "should be able to accept a callable argument handler which receives a list of columns and returns a symbol" do
      received_columns = nil
      handler = proc do |columns|
        received_columns = columns
        :raise
      end
      @ds = @db[:things].on_duplicate_columns(handler)
      must_raise_for_columns(:id, :name, :id)
      received_columns.must_equal([:id, :name, :id])
    end
  end

end
