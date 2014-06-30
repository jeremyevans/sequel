SEQUEL_ADAPTER_TEST = :fdbsql unless defined? SEQUEL_ADAPTER_TEST and SEQUEL_ADAPTER_TEST == :fdbsql

unless defined? SEQUEL_PATH
  require 'sequel'
  SEQUEL_PATH = Gem.loaded_specs['sequel'].full_gem_path
  require File.join("#{SEQUEL_PATH}",'spec','adapters','spec_helper.rb')
end

describe 'Fdbsql Dataset' do
  before(:all) do
    @db = DB
  end

  describe 'provides_accurate_rows_matched' do
    before do
      DB.create_table!(:test) {Integer :a}
      DB[:test].insert(1)
      DB[:test].insert(2)
      DB[:test].insert(3)
      DB[:test].insert(4)
      DB[:test].insert(5)
    end

    after do
      DB.drop_table?(:test)
    end

    specify '#delete' do
      DB[:test].where(a: 8..10).delete.should eq 0
      DB[:test].where(a: 5).delete.should eq 1
      DB[:test].where(a: 1..3).delete.should eq 3
    end

    specify '#update' do
      DB[:test].where(a: 8..10).update(a: Sequel.+(:a, 10)).should eq 0
      DB[:test].where(a: 5).update(a: Sequel.+(:a, 1000)).should eq 1
      DB[:test].where(a: 1..3).update(a: Sequel.+(:a, 100)).should eq 3
    end

  end


  describe 'intersect and except ALL' do
    before do
      DB.create_table!(:test) {Integer :a; Integer :b}
      DB[:test].insert(1, 10)
      DB[:test].insert(2, 10)
      DB[:test].insert(8, 15)
      DB[:test].insert(2, 10)
      DB[:test].insert(2, 10)
      DB[:test].insert(1, 10)

      DB.create_table!(:test2) {Integer :a; Integer :b}
      DB[:test2].insert(1, 10)
      DB[:test2].insert(2, 10)
      DB[:test2].insert(2, 12)
      DB[:test2].insert(3, 10)
      DB[:test2].insert(1, 10)
    end

    after do
      DB.drop_table?(:test)
      DB.drop_table?(:test2)
    end

    specify 'intersect all' do
      @db[:test].intersect(@db[:test2], all: true).map{|r| [r[:a],r[:b]]}.to_a.should match_array [[1, 10], [1,10], [2, 10]]
    end

    specify 'except all' do
      @db[:test].except(@db[:test2], all: true).map{|r| [r[:a],r[:b]]}.to_a.should match_array [[8, 15], [2,10], [2, 10]]
    end
  end

end
