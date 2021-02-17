require_relative 'spec_helper'

describe "async_thread_pool plugin" do
  before do
    @db = Sequel.mock(:extensions=>'async_thread_pool', :fetch=>{:id=>1}, :keep_reference=>false, :num_async_threads=>1, :numrows=>1)
    @Album = Class.new(Sequel::Model)
    @Album.set_dataset(@db[:albums])
    @Album.plugin :async_thread_pool
    @db.sqls
  end

  it 'should support creating async datasets via Model.async' do
    t = Thread.current
    t2 = nil

    v = @Album.all{|x| t2 = Thread.current}
    (Array === v).must_equal true
    v.first.must_be_kind_of @Album
    t2.must_equal t

    v = @Album.async.all{|x| t2 = Thread.current}
    (Array === v).must_equal false
    v.first.must_be_kind_of @Album
    t2.wont_be_nil
    t2.wont_equal t

    @db.sqls.must_equal ["SELECT * FROM albums", "SELECT * FROM albums"]
  end

  it 'should support async versions of destroy' do
    @Album.dataset.async.destroy.__value.must_equal 1
    @db.sqls.must_equal ["SELECT * FROM albums", "DELETE FROM albums WHERE (id = 1)"]
  end

  it 'should support async versions of with_pk' do
    @Album.dataset.async.with_pk(1).__value.pk.must_equal 1
    @db.sqls.must_equal ["SELECT * FROM albums WHERE (albums.id = 1) LIMIT 1"]
  end

  it 'should support async versions of with_pk!' do
    @Album.dataset.async.with_pk!(1).__value.pk.must_equal 1
    @db.sqls.must_equal ["SELECT * FROM albums WHERE (albums.id = 1) LIMIT 1"]
  end
end
