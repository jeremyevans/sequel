require_relative 'spec_helper'

{''=>false, ' with :preempt_async_thread Database option'=>true}.each do |desc, preempt_async_thread|
  describe "async_thread_pool extension" do
    before do
      @db = Sequel.mock(:extensions=>'async_thread_pool', :fetch=>{:v=>1}, :keep_reference=>false, :num_async_threads=>1, :preempt_async_thread=>preempt_async_thread)
    end

    it 'should allow running queries in async threads' do
      t = Thread.current
      t2 = nil
      q = Queue.new
      q2 = Queue.new

      @db[:test].async.all{|x| t3 = Thread.current; q2.push(x); q.pop; t2 = t3; q2.push(nil)}
      t2.must_be_nil
      q2.pop.must_equal(:v=>1)
      q.push(nil)
      q2.pop
      t2.wont_be_nil
      t.wont_equal t2
    end

    it 'should raise exceptions that occur in async threads when result is accessed' do
      v = @db[:test].with_fetch(RuntimeError).async.first
      proc{v.__value}.must_raise Sequel::DatabaseError
    end

    it 'should have proxy objects delegate all methods other than equal?, __id__, and __send__' do
      v = @db[:test].async.first
      v.class.must_equal Hash
      (!v).must_equal false
      (v == {:v=>1}).must_equal true
      (v != {:v=>1}).must_equal false
      v.instance_eval{__id__}.must_equal v.__value.instance_eval{__id__}
      v.instance_exec{__id__}.must_equal v.__value.instance_exec{__id__}
      v.__send__(:__id__).wont_equal v.__value.__send__(:__id__)
      v.respond_to?(:each).must_equal true
      v.__send__(:respond_to_missing?, :each).must_equal true

      v = @db[:test].async.with_fetch(:v=>false).get(:v)
      v.class.must_equal FalseClass
      (!v).must_equal true
      (v == false).must_equal true
      (v != false).must_equal false
      v.respond_to?(:each).must_equal false
      v.__send__(:respond_to_missing?, :each).must_equal false
    end

    it 'should work when loading async_thread_pool extension after already loaded' do
      @db.extension(:async_thread_pool)
      @db[:test].async.first.must_equal(:v=>1)
    end

    it 'should support sync methods on async datasets to not use an async thread or proxy object' do
      t = Thread.current
      t2 = nil
      v = @db[:test].async.sync.all{|x| t2 = Thread.current}
      (Array === v).must_equal true
      t2.must_equal t
    end

    it 'should support async loading with proxy objects on all dataset action and enumerable methods' do
      ds = @db[:test].async.with_autoid(1)

      ds.<<(:v=>1).__value.must_be_kind_of Sequel::Dataset
      ds.each{}.__value.must_be_kind_of Sequel::Dataset
      ds.fetch_rows('foo'){}.__value.must_be_nil
      ds.import([:v], [[1]]).__value
      ds.multi_insert([{:v=>1}]).__value
      ds.order(:v).paged_each{}.__value.must_be_kind_of Sequel::Dataset
      ds.where_each(:v){}.__value.must_be_kind_of Sequel::Dataset
      ds.truncate.__value.must_be_nil
      @db.sqls.must_equal [
        "INSERT INTO test (v) VALUES (1)",
        "SELECT * FROM test",
        "foo",
        "BEGIN", "INSERT INTO test (v) VALUES (1)", "COMMIT",
        "BEGIN", "INSERT INTO test (v) VALUES (1)", "COMMIT",
        "BEGIN", "SELECT * FROM test ORDER BY v LIMIT 1000 OFFSET 0", "COMMIT",
        "SELECT * FROM test WHERE v",
        "TRUNCATE TABLE test",
      ]

      ds[:v].__value.must_equal(:v=>1)
      ds.all.__value.must_equal [{:v=>1}]
      ds.as_hash(:v, :v).__value.must_equal(1=>1)
      ds.avg(:v).__value.must_equal(1)
      ds.count.__value.must_equal(1)
      ds.columns.__value.must_equal []
      ds.columns!.__value.must_equal []
      ds.delete.__value.must_equal 0
      ds.empty?.__value.must_equal false
      ds.first.__value.must_equal(:v=>1)
      ds.first!.__value.must_equal(:v=>1)
      ds.get(:v).__value.must_equal 1
      ds.insert.__value.must_equal 2
      ds.order(:v).last.__value.must_equal(:v=>1)
      ds.max(:v).__value.must_equal 1
      ds.min(:v).__value.must_equal 1
      ds.select_hash(:v, :v).__value.must_equal(1=>1)
      ds.select_hash_groups(:v, :v).__value.must_equal(1=>[1])
      ds.select_map(:v).__value.must_equal([1])
      ds.select_order_map(:v).__value.must_equal([1])
      ds.single_record.__value.must_equal(:v=>1)
      ds.single_record!.__value.must_equal(:v=>1)
      ds.single_value.__value.must_equal 1
      ds.single_value!.__value.must_equal 1
      ds.sum(:v).__value.must_equal 1
      ds.to_hash(:v).__value.must_equal(1=>{:v=>1})
      ds.to_hash_groups(:v).__value.must_equal(1=>[{:v=>1}])
      ds.update(:v=>1).__value.must_equal 0
      ds.where_all(:v).__value.must_equal [{:v=>1}]
      ds.where_single_value(:v).__value.must_equal 1

      ds.all?.__value.must_equal true
      ds.any?.__value.must_equal true
      ds.drop(0).__value.must_equal [{:v=>1}]
      ds.entries.__value.must_equal [{:v=>1}]
      ds.grep_v(//).__value.must_equal [{:v=>1}] if RUBY_VERSION >= '2.3'
      ds.include?(:v=>1).__value.must_equal true
      ds.inject.__value.must_equal(:v=>1)
      ds.member?(:v=>1).__value.must_equal true
      ds.minmax.__value.must_equal([{:v=>1}, {:v=>1}])
      ds.none?.__value.must_equal false
      ds.one?.__value.must_equal true
      ds.reduce.__value.must_equal(:v=>1)
      ds.sort.__value.must_equal [{:v=>1}]
      ds.take(1).__value.must_equal [{:v=>1}]
      ds.tally.__value.must_equal({:v=>1}=>1) if RUBY_VERSION >= '2.7'
      ds.to_a.__value.must_equal [{:v=>1}]
      ds.to_h{|x| [x[:v], x]}.__value.must_equal(1=>{:v=>1})  if RUBY_VERSION >= '2.6'
      ds.uniq.__value.must_equal [{:v=>1}] if RUBY_VERSION >= '2.4'
      ds.zip.__value.must_equal [[{:v=>1}]]

      ds.collect{|x| x}.__value.must_equal [{:v=>1}]
      ds.collect_concat{|x| x}.__value.must_equal [{:v=>1}]
      ds.detect{true}.__value.must_equal(:v=>1)
      ds.drop_while{false}.__value.must_equal [{:v=>1}]
      ds.each_with_object(0){|x| x}.__value.must_equal 0
      ds.filter_map{|x| x}.__value.must_equal [{:v=>1}] if RUBY_VERSION >= '2.7'
      ds.find{true}.__value.must_equal(:v=>1)
      ds.find_all{true}.__value.must_equal [{:v=>1}]
      ds.find_index{true}.__value.must_equal 0
      ds.flat_map{|x| x}.__value.must_equal [{:v=>1}]
      ds.max_by{}.__value.must_equal(:v=>1)
      ds.min_by{}.__value.must_equal(:v=>1)
      ds.minmax_by{}.__value.must_equal [{:v=>1}, {:v=>1}]
      ds.partition{true}.__value.must_equal [[{:v=>1}], []]
      ds.reject{false}.__value.must_equal [{:v=>1}]
      ds.sort_by{}.__value.must_equal [{:v=>1}]
      ds.take_while{true}.__value.must_equal [{:v=>1}]

      @db.sqls
      ds.each_cons(1){}.__value.must_be_nil
      ds.each_entry{}.__value.must_be_kind_of Sequel::Dataset
      ds.each_slice(1){}.__value.must_be_nil
      ds.each_with_index{}.__value.must_be_kind_of Sequel::Dataset
      ds.reverse_each{}.__value.must_be_kind_of Sequel::Dataset
      @db.sqls.must_equal [
        "SELECT * FROM test",
        "SELECT * FROM test",
        "SELECT * FROM test",
        "SELECT * FROM test",
        "SELECT * FROM test",
      ]

      (Enumerator === ds.collect).must_equal true
      (Enumerator === ds.collect_concat).must_equal true
      (Enumerator === ds.detect).must_equal true
      (Enumerator === ds.drop_while).must_equal true
      (Enumerator === ds.each_cons(1)).must_equal true
      (Enumerator === ds.each_entry).must_equal true
      (Enumerator === ds.each_slice(1)).must_equal true
      (Enumerator === ds.each_with_index).must_equal true
      (Enumerator === ds.each_with_object(1)).must_equal true
      (Enumerator === ds.filter_map).must_equal true if RUBY_VERSION >= '2.7'
      (Enumerator === ds.find).must_equal true
      (Enumerator === ds.find_all).must_equal true
      (Enumerator === ds.find_index).must_equal true
      (Enumerator === ds.flat_map).must_equal true
      (Enumerator === ds.max_by).must_equal true
      (Enumerator === ds.min_by).must_equal true
      (Enumerator === ds.minmax_by).must_equal true
      (Enumerator === ds.partition).must_equal true
      (Enumerator === ds.reject).must_equal true
      (Enumerator === ds.reverse_each).must_equal true
      (Enumerator === ds.sort_by).must_equal true
      (Enumerator === ds.take_while).must_equal true
      (Enumerator === ds.order(:v).paged_each).must_equal true

      ds.map(:v).__value.must_equal [1]
      ds.map{|x| x}.__value.must_equal [{:v=>1}]
      (Enumerator === ds.map).must_equal true
    end
  end
end

describe "async_thread_pool extension" do
  before do
    @db = Sequel.mock(:extensions=>'async_thread_pool', :fetch=>{:v=>1}, :keep_reference=>false, :num_async_threads=>1)
  end

  it 'should perform async work before returning value' do
    t = Thread.current
    t2 = nil

    v = @db[:test].async.all{|x| t2 = Thread.current}
    v.must_equal [{:v=>1}]
    t2.wont_be_nil
    t.wont_equal t2
    v.equal?(v.to_a).must_equal false
    (Array === v).must_equal false
    v.__value.equal?(v.to_a).must_equal true
    (Array === v.__value).must_equal true

    if RUBY_VERSION >= '2.2'
      v.itself.equal?(v.to_a).must_equal true
      (Array === v.itself).must_equal true
    end
  end

  it 'should not allow calling the __run_block multiple times' do
    v = Sequel::Database::AsyncThreadPool::Proxy.new{1}
    v.__send__(:__run_block)
    proc{v.__send__(:__run_block)}.must_raise Sequel::Error
  end

  it 'should not allow creating proxy objects without a block' do
    proc{Sequel::Database::AsyncThreadPool::Proxy.new}.must_raise Sequel::Error
  end
end

describe "async_thread_pool extension with :preempt_async_thread Database option" do
  before do
    @db = Sequel.mock(:extensions=>'async_thread_pool', :fetch=>{:v=>1}, :keep_reference=>false, :num_async_threads=>1, :preempt_async_thread=>true)
  end

  it 'should allow preempting async threads' do
    t = Thread.current
    t2 = nil
    t4 = nil
    q = Queue.new
    q2 = Queue.new

    @db[:test].async.all{|x| t3 = Thread.current; q2.push(x); q.pop; t2 = t3; q2.push(nil)}
    t2.must_be_nil
    q2.pop.must_equal(:v=>1)

    v = @db[:test].async.all{|x| t4 = Thread.current}.__value
    t4.must_equal t

    q.push(nil)
    q2.pop
    t2.wont_be_nil
    t.wont_equal t2
  end
end

describe "async_thread_pool extension" do
  it "should raise an error if trying to load the async_thread_pool extension into a single connection pool" do
    db = Sequel.mock(:keep_reference=>false, :single_threaded=>true)
    proc{db.extension(:async_thread_pool)}.must_raise Sequel::Error
  end

  it "should use :num_async_threads as size of async thread pool" do
    3.times do |i|
      Sequel.mock(:extensions=>'async_thread_pool', :num_async_threads=>i+1, :max_connections=>4).instance_variable_get(:@async_thread_pool).size.must_equal(i+1)
    end
  end

  it "should use :max_connections as size of async thread pool if :num_async_threads is not given" do
    3.times do |i|
      Sequel.mock(:extensions=>'async_thread_pool', :max_connections=>i+1).instance_variable_get(:@async_thread_pool).size.must_equal(i+1)
    end
  end

  it "should use 4 as size of async thread pool if :num_async_threads and :max_connections is not given" do
    Sequel.mock(:extensions=>'async_thread_pool').instance_variable_get(:@async_thread_pool).size.must_equal 4
  end

  it "should raise if the number of async threads is not positive" do
    proc{Sequel.mock(:extensions=>'async_thread_pool', :num_async_threads=>0)}.must_raise Sequel::Error
  end
end
