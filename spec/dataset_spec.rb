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

end
