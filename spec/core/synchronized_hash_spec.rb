require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')

describe Sequel::SynchronizedHash do
  describe '#[]' do
    it 'should return the value of a key' do
      hash = Sequel::SynchronizedHash.new

      hash[:number] = 10

      hash[:number].must_equal 10
    end
  end

  describe '#delete' do
    it 'should remove a key' do
      hash = Sequel::SynchronizedHash.new

      hash[:number] = 10

      hash.delete(:number)

      hash[:number].must_equal nil
    end
  end

  describe '#keys' do
    it 'should return an Array of the keys' do
      hash = Sequel::SynchronizedHash.new

      hash[:number] = 10

      hash.keys.must_equal [:number]
    end
  end

  describe '#empty?' do
    it 'should return true for an empty Hash' do
      hash = Sequel::SynchronizedHash.new

      hash.empty?.must_equal true
    end

    it 'should return false for a non-empty Hash' do
      hash = Sequel::SynchronizedHash.new

      hash[:number] = 10

      hash.empty?.must_equal false
    end
  end
end
