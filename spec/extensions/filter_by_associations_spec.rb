require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::FilterByAssociations" do
  before do
    @Album = Class.new(Sequel::Model(:albums))
    artist = @Artist = Class.new(Sequel::Model(:artists))
    tag = @Tag = Class.new(Sequel::Model(:tags))
    track = @Track = Class.new(Sequel::Model(:tracks))
    album_info = @AlbumInfo = Class.new(Sequel::Model(:album_infos))
    @Artist.columns :id, :id1, :id2
    @Tag.columns :id, :tid1, :tid2
    @Track.columns :id, :album_id, :album_id1, :album_id2
    @AlbumInfo.columns :id, :album_id, :album_id1, :album_id2
    @Album.class_eval do
      plugin :filter_by_associations
      columns :id, :id1, :id2, :artist_id, :artist_id1, :artist_id2
      many_to_one :artist, :class=>artist
      one_to_many :tracks, :class=>track, :key=>:album_id
      one_to_one :album_info, :class=>album_info, :key=>:album_id
      many_to_many :tags, :class=>tag, :left_key=>:album_id, :join_table=>:albums_tags

      many_to_one :cartist, :class=>artist, :key=>[:artist_id1, :artist_id2], :primary_key=>[:id1, :id2]
      one_to_many :ctracks, :class=>track, :key=>[:album_id1, :album_id2], :primary_key=>[:id1, :id2]
      one_to_one :calbum_info, :class=>album_info, :key=>[:album_id1, :album_id2], :primary_key=>[:id1, :id2]
      many_to_many :ctags, :class=>tag, :left_key=>[:album_id1, :album_id2], :left_primary_key=>[:id1, :id2], :right_key=>[:tag_id1, :tag_id2], :right_primary_key=>[:tid1, :tid2], :join_table=>:albums_tags
    end
  end

  it "should be able to filter on many_to_one associations" do
    @Album.filter(:artist=>@Artist.load(:id=>3)).sql.should == 'SELECT * FROM albums WHERE (artist_id = 3)'
  end

  it "should be able to filter on one_to_many associations" do
    @Album.filter(:tracks=>@Track.load(:album_id=>3)).sql.should == 'SELECT * FROM albums WHERE (id = 3)'
  end

  it "should be able to filter on one_to_one associations" do
    @Album.filter(:album_info=>@AlbumInfo.load(:album_id=>3)).sql.should == 'SELECT * FROM albums WHERE (id = 3)'
  end

  it "should be able to filter on many_to_many associations" do
    @Album.filter(:tags=>@Tag.load(:id=>3)).sql.should == 'SELECT * FROM albums WHERE (id IN (SELECT album_id FROM albums_tags WHERE (tag_id = 3)))'
  end

  it "should be able to filter on many_to_one associations with composite keys" do
    @Album.filter(:cartist=>@Artist.load(:id1=>3, :id2=>4)).sql.should == 'SELECT * FROM albums WHERE ((artist_id1 = 3) AND (artist_id2 = 4))'
  end

  it "should be able to filter on one_to_many associations with composite keys" do
    @Album.filter(:ctracks=>@Track.load(:album_id1=>3, :album_id2=>4)).sql.should == 'SELECT * FROM albums WHERE ((id1 = 3) AND (id2 = 4))'
  end

  it "should be able to filter on one_to_one associations with composite keys" do
    @Album.filter(:calbum_info=>@AlbumInfo.load(:album_id1=>3, :album_id2=>4)).sql.should == 'SELECT * FROM albums WHERE ((id1 = 3) AND (id2 = 4))' 
  end

  it "should be able to filter on many_to_many associations with composite keys" do
    @Album.filter(:ctags=>@Tag.load(:tid1=>3, :tid2=>4)).sql.should == 'SELECT * FROM albums WHERE ((id1, id2) IN (SELECT album_id1, album_id2 FROM albums_tags WHERE ((tag_id1 = 3) AND (tag_id2 = 4))))'
  end

  it "should work inside a complex filter" do
    artist = @Artist.load(:id=>3)
    @Album.filter{foo & {:artist=>artist}}.sql.should == 'SELECT * FROM albums WHERE (foo AND (artist_id = 3))'
    track = @Track.load(:album_id=>4)
    @Album.filter{foo & [[:artist, artist], [:tracks, track]]}.sql.should == 'SELECT * FROM albums WHERE (foo AND (artist_id = 3) AND (id = 4))'
  end

  it "should raise for an invalid association name" do
    proc{@Album.filter(:foo=>@Artist.load(:id=>3)).sql}.should raise_error(Sequel::Error)
  end

  it "should raise for an invalid association type" do
    @Album.plugin :many_through_many
    @Album.many_through_many :mtmtags, [[:album_id, :album_tags, :tag_id]], :class=>@Tag
    proc{@Album.filter(:mtmtags=>@Tag.load(:id=>3)).sql}.should raise_error(Sequel::Error)
  end

  it "should raise for an invalid associated object class " do
    proc{@Album.filter(:tags=>@Artist.load(:id=>3)).sql}.should raise_error(Sequel::Error)
  end

  it "should work correctly in subclasses" do
    c = Class.new(@Album)
    c.many_to_one :sartist, :class=>@Artist
    c.filter(:sartist=>@Artist.load(:id=>3)).sql.should == 'SELECT * FROM albums WHERE (sartist_id = 3)'
  end
end
