require_relative 'spec_helper'

describe "concurrent_eager_loading plugin" do
  before do
    @db = Sequel.mock(:extensions=>'async_thread_pool', :fetch=>{:id=>1}, :keep_reference=>false, :num_async_threads=>2, :numrows=>1)
    @C = Class.new(Sequel::Model)
    @C.columns :id
    @C.set_dataset(@db[:cs])
    @C.plugin :concurrent_eager_loading
    @db.sqls
  end

  it 'should eager load concurrently if configured' do
    m2o_thread = nil
    o2m_thread = nil
    m2o_mutex = nil
    o2m_mutex = nil
    q1 = Queue.new
    q2 = Queue.new
    @C.many_to_one :c, :key=>:id, :class=>@C, :eager_loader=>(proc do |eo|
      m2o_thread = Thread.current
      if m2o_mutex = eo[:mutex]
        q2.push(nil)
        q1.pop
      end
    end)
    @C.one_to_many :cs, :key=>:id, :class=>@C, :eager_loader=>(proc do |eo|
      o2m_thread = Thread.current
      if o2m_mutex = eo[:mutex]
        q1.push(nil)
        q2.pop
      end
    end)

    @C.eager(:c, :cs).all
    m2o_thread.must_equal Thread.current
    o2m_thread.must_equal Thread.current
    m2o_mutex.must_be_nil
    o2m_mutex.must_be_nil

    @C.eager_load_serially.eager(:c, :cs).all
    m2o_thread.must_equal Thread.current
    o2m_thread.must_equal Thread.current
    m2o_mutex.must_be_nil
    o2m_mutex.must_be_nil

    @C.eager_load_concurrently.eager(:c, :cs).all
    m2o_thread.wont_equal Thread.current
    o2m_thread.wont_equal Thread.current
    o2m_thread.wont_equal m2o_thread
    m2o_mutex.wont_be_nil
    o2m_mutex.must_equal m2o_mutex

    @C.eager_load_concurrently.eager_load_serially.eager(:c, :cs).all
    m2o_thread.must_equal Thread.current
    o2m_thread.must_equal Thread.current
    m2o_mutex.must_be_nil
    o2m_mutex.must_be_nil

    @C.eager_load_serially.eager_load_concurrently.eager(:c, :cs).all
    m2o_thread.wont_equal Thread.current
    o2m_thread.wont_equal Thread.current
    o2m_thread.wont_equal m2o_thread
    m2o_mutex.wont_be_nil
    o2m_mutex.must_equal m2o_mutex

    @C.plugin :concurrent_eager_loading, :always=>true
    @C.eager(:c, :cs).all
    m2o_thread.wont_equal Thread.current
    o2m_thread.wont_equal Thread.current
    o2m_thread.wont_equal m2o_thread
    m2o_mutex.wont_be_nil
    o2m_mutex.must_equal m2o_mutex

    @C.eager_load_serially.eager(:c, :cs).all
    m2o_thread.must_equal Thread.current
    o2m_thread.must_equal Thread.current
    m2o_mutex.must_be_nil
    o2m_mutex.must_be_nil

    m2o_thread = nil
    @C.eager(:c).all
    m2o_thread.must_equal Thread.current
    m2o_mutex.must_be_nil

    o2m_thread = nil
    @C.eager(:cs).all
    o2m_thread.must_equal Thread.current
    o2m_mutex.must_be_nil

    vs = []
    @C.eager(:c, :cs).
      with_extend{define_method(:perform_eager_load) do |*a| vs << Struct.new(:wrapped).new(super(*a)) end}.all
    vs.map{|v| v.wrapped.__value}
    m2o_thread.wont_equal Thread.current
    o2m_thread.wont_equal Thread.current
    o2m_thread.wont_equal m2o_thread
    m2o_mutex.wont_be_nil
    o2m_mutex.must_equal m2o_mutex

    Class.new(@C).eager(:c, :cs).all
    m2o_thread.wont_equal Thread.current
    o2m_thread.wont_equal Thread.current
    o2m_thread.wont_equal m2o_thread
    m2o_mutex.wont_be_nil
    o2m_mutex.must_equal m2o_mutex
  end
end
