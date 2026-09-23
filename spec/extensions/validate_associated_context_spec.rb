# frozen_string_literal: true
require_relative "spec_helper"

describe "validate_associated_context plugin" do
  before do
    @db = Sequel.mock
    @c = Class.new(Sequel::Model(@db))
    @c.plugin :validate_associated_context
    @Artist = Class.new(@c).set_dataset(:artists)
    @Album = Class.new(@c).set_dataset(:albums)
    @Artist.columns :id, :name
    @Album.columns :id, :name, :artist_id
    @Artist.one_to_many :albums, :class=>@Album, :key=>:artist_id
    @Album.class_eval do
      def validate
        super
        errors.add(:name, 'is b') if name == 'b' && validation_context == :foo
      end
    end
    @artist = @Artist.load(:id=>1, :name=>'a')
    @album = @Album.load(:id=>2, :name=>'b', :artist_id=>1)
    @reflection = @Artist.association_reflection(:albums)
  end

  it "should use the validation context of the current object when validating associated objects" do
    @artist.send(:delay_validate_associated_object, @reflection, @album)
    @artist.valid?.must_equal true
    @artist.valid?(:validation_context=>:foo).must_equal false
    @artist.errors[:albums].must_equal ["name is b"]
  end

  it "should use the validation context when validating new associated objects" do
    artist = @Artist.new(:name=>'a')
    album = @Album.new(:name=>'b')
    artist.send(:delay_validate_associated_object, @reflection, album)
    artist.valid?.must_equal true
    artist.valid?(:validation_context=>:foo).must_equal false
    artist.errors[:albums].must_equal ["name is b"]
  end

  it "should use the validation context when validate_associated_object is called in validate" do
    @Album.many_to_one :artist, :class=>@Artist, :key=>:artist_id
    @Album.class_eval do
      def validate
        super
        validate_associated_object(model.association_reflection(:artist), artist) if artist
      end
    end
    @Artist.class_eval do
      def validate
        super
        errors.add(:name, 'is a') if name == 'a' && validation_context == :foo
      end
    end
    @album.associations[:artist] = @artist

    @album.valid?.must_equal true
    @album.valid?(:validation_context=>:foo).must_equal false
    @album.errors[:artist].must_equal ["name is a"]
  end

  it "should work if the associated object does not use validation_contexts plugin" do
    _Album = Class.new(@c) do
      set_dataset(:albums)
      columns :id, :name, :artist_id
      def validate
        super
        errors.add(:name, 'is b') if name == 'b'
      end
    end
    @Artist.one_to_many :albums, :class=>_Album, :key=>:artist_id
    reflection = @Artist.association_reflection(:albums)

    album = _Album.load(:id=>2, :name=>'b', :artist_id=>1)
    @artist.send(:delay_validate_associated_object, @Artist.association_reflection(:albums), album)
    @artist.valid?.must_equal false
    @artist.errors[:albums].must_equal ["name is b"]
    @artist.valid?(:validation_context=>:foo).must_equal false
    @artist.errors[:albums].must_equal ["name is b"]

    album.name = 'c'
    @artist.send(:delay_validate_associated_object, @Artist.association_reflection(:albums), album)
    @artist.valid?.must_equal true
    @artist.valid?(:validation_context=>:foo).must_equal true
  end
end
