require_relative 'spec_helper'

describe "Sequel::Dataset::SynchronizeSQL" do

    # Simulates an adapter which calls synchronize around literal_string_append,
    # like the MySQL & Postgres ones do
    # The adapter also tracks how many times a connection was removed from the pool
    class SynchronizedMockAdapter < ::Sequel::Mock::Database
        def initialize(*args)
            @times_checkedout = 0
            super
            pool.extend(TrackCallsToAcquireConnection)
        end

        def dataset_class_default
            SynchronizedMockDataset
        end
    end

    class SynchronizedMockDataset < ::Sequel::Mock::Dataset
        def literal_string_append(sql, v)
            db.synchronize { super }
        end
    end

    # Extends a connection pool to track how many times a connection was actually checked out
    # by looking at how many times a `ThreadedConnectionPool#assign_connection` was successful
    module TrackCallsToAcquireConnection
        def assign_connection(*args)
            r = super
            @times_connection_acquired += 1 if r
            return r
        end

        def self.extended(i)
            i.instance_exec { @times_connection_acquired = 0 }
        end

        attr_reader :times_connection_acquired

        def clear_times_connection_acquired
            @times_connection_acquired = 0
        end
    end


    before(:each) do
        @db = Sequel.connect(adapter: SynchronizedMockAdapter)
        @ds = @db[:tab1]
        @db.pool.clear_times_connection_acquired
    end

    it 'checks out an extra connection on insert_sql if there are no strings' do
        @ds.insert_sql(:numeric_foo => 8)
        @db.pool.times_connection_acquired.must_equal(0)
        @db.pool.clear_times_connection_acquired

        extds = @ds.extension(:synchronize_sql)
        extds.insert_sql(:numeric_foo => 8)
        @db.pool.times_connection_acquired.must_equal(1)
    end

    it 'checks out just one connection on insert_sql if there are multiple strings' do
        @ds.insert_sql(:string_foo1 => 'eight', :string_foo2 => 'nine', :string_foo3 => 'ten')
        @db.pool.times_connection_acquired.must_equal(3)
        @db.pool.clear_times_connection_acquired

        extds = @ds.extension(:synchronize_sql)
        extds.insert_sql(:string_foo1 => 'eight', :string_foo2 => 'nine', :string_foo3 => 'ten')
        @db.pool.times_connection_acquired.must_equal(1)
    end

    it 'cheks out an extra connectrion on update_sql if there are no strings' do
        @ds.where(:numeric_foo => [1, 2, 3, 4, 5]).update_sql(:numeric_foo => 99)
        @db.pool.times_connection_acquired.must_equal(0)
        @db.pool.clear_times_connection_acquired

        extds = @ds.extension(:synchronize_sql)
        extds.where(:numeric_foo => [1, 2, 3, 4, 5]).update_sql(:numeric_foo => 99)
        @db.pool.times_connection_acquired.must_equal(1)
    end

    it 'checks out just one connection on update_sql if there are multiple strings' do
        @ds.where(:numeric_foo => [1, 2, 3, 4, 5]).update_sql(:string_foo1 => 'eight', :string_foo2 => 'nine', :string_foo3 => 'ten')
        @db.pool.times_connection_acquired.must_equal(3)
        @db.pool.clear_times_connection_acquired

        extds = @ds.extension(:synchronize_sql)
        extds.where(:numeric_foo => [1, 2, 3, 4, 5]).update_sql(:string_foo1 => 'eight', :string_foo2 => 'nine', :string_foo3 => 'ten')
        @db.pool.times_connection_acquired.must_equal(1)
    end

    it 'checks out an extra connection on delete_sql if there are no strings' do
        @ds.where(:numeric_foo => [1, 2, 3]).delete_sql
        @db.pool.times_connection_acquired.must_equal(0)
        @db.pool.clear_times_connection_acquired

        extds = @ds.extension(:synchronize_sql)
        extds.where(:numeric_foo => [1, 2, 3]).delete_sql
        @db.pool.times_connection_acquired.must_equal(1)
    end

    it 'checks out just one connection on delete_sql if there are multiple strings' do
        @ds.where(:string_foo => ['one', 'two', 'three', 'four']).delete_sql
        @db.pool.times_connection_acquired.must_equal(4)
        @db.pool.clear_times_connection_acquired

        extds = @ds.extension(:synchronize_sql)
        extds.where(:string_foo => ['one', 'two', 'three', 'four']).delete_sql
        @db.pool.times_connection_acquired.must_equal(1)
    end

    it 'checks out an extra connection on select_sql if there are no strings' do
        @ds.where(:numeric_foo => [1, 2, 3]).select_sql
        @db.pool.times_connection_acquired.must_equal(0)
        @db.pool.clear_times_connection_acquired

        extds = @ds.extension(:synchronize_sql)
        extds.where(:numeric_foo => [1, 2, 3]).select_sql
        @db.pool.times_connection_acquired.must_equal(1)
    end

    it 'checks out just one connection on select_sql if there are multiple strings' do
        @ds.where(:string_foo => ['one', 'two', 'three', 'four']).select_sql
        @db.pool.times_connection_acquired.must_equal(4)
        @db.pool.clear_times_connection_acquired

        extds = @ds.extension(:synchronize_sql)
        extds.where(:string_foo => ['one', 'two', 'three', 'four']).select_sql
        @db.pool.times_connection_acquired.must_equal(1)
    end

    it 'checks out an extra connection on fetch if there are no strings' do
        @db.fetch('SELECT * FROM tab1 WHERE numeric_foo IN (?, ?, ?, ?)', 1, 2, 3, 4).select_sql
        @db.pool.times_connection_acquired.must_equal(0)
        @db.pool.clear_times_connection_acquired

        @db.extension(:synchronize_sql)
        @db.fetch('SELECT * FROM tab1 WHERE numeric_foo IN (?, ?, ?, ?)', 1, 2, 3, 4).select_sql
        @db.pool.times_connection_acquired.must_equal(1)
    end

    it 'checks out just one connection on fetch if there are multiple strings' do
        @db.fetch('SELECT * FROM tab1 WHERE string_foo IN (?, ?, ?, ?)', 'one', 'two', 'three', 'four').select_sql
        @db.pool.times_connection_acquired.must_equal(4)
        @db.pool.clear_times_connection_acquired

        @db.extension(:synchronize_sql)
        @db.fetch('SELECT * FROM tab1 WHERE string_foo IN (?, ?, ?, ?)', 'one', 'two', 'three', 'four').select_sql
        @db.pool.times_connection_acquired.must_equal(1)
    end
end
