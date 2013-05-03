require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::DatasetAssociations" do
  before do
    @db = Sequel.mock
    @Base = Class.new(Sequel::Model)
    @Base.plugin :dataset_associations

    @Artist = Class.new(@Base)
    @Album = Class.new(@Base)
    @Tag = Class.new(@Base)

    @Artist.meta_def(:name){'Artist'}
    @Album.meta_def(:name){'Album'}
    @Tag.meta_def(:name){'Tag'}

    @Artist.dataset = @db[:artists]
    @Album.dataset = @db[:albums]
    @Tag.dataset = @db[:tags]

    @Artist.columns :id, :name
    @Album.columns :id, :name, :artist_id
    @Tag.columns :id, :name

    @Artist.plugin :many_through_many
    @Artist.one_to_many :albums, :class=>@Album
    @Artist.one_to_one :first_album, :class=>@Album
    @Album.many_to_one :artist, :class=>@Artist
    @Album.many_to_many :tags, :class=>@Tag
    @Tag.many_to_many :albums, :class=>@Album
    @Artist.many_through_many :tags, [[:albums, :artist_id, :id], [:albums_tags, :album_id, :tag_id]], :class=>@Tag
  end

  it "should work for many_to_one associations" do
    ds = @Album.artists
    ds.should be_a_kind_of(Sequel::Dataset)
    ds.model.should == @Artist
    ds.sql.should == "SELECT * FROM artists WHERE (artists.id IN (SELECT albums.artist_id FROM albums))"
  end

  it "should work for one_to_many associations" do
    ds = @Artist.albums
    ds.should be_a_kind_of(Sequel::Dataset)
    ds.model.should == @Album
    ds.sql.should == "SELECT * FROM albums WHERE (albums.artist_id IN (SELECT artists.id FROM artists))"
  end

  it "should work for one_to_one associations" do
    ds = @Artist.first_albums
    ds.should be_a_kind_of(Sequel::Dataset)
    ds.model.should == @Album
    ds.sql.should == "SELECT * FROM albums WHERE (albums.artist_id IN (SELECT artists.id FROM artists))"
  end

  it "should work for many_to_many associations" do
    ds = @Album.tags
    ds.should be_a_kind_of(Sequel::Dataset)
    ds.model.should == @Tag
    ds.sql.should == "SELECT tags.* FROM tags WHERE (tags.id IN (SELECT albums_tags.tag_id FROM albums INNER JOIN albums_tags ON (albums_tags.album_id = albums.id)))"
  end

  it "should work for many_through_many associations" do
    ds = @Artist.tags
    ds.should be_a_kind_of(Sequel::Dataset)
    ds.model.should == @Tag
    ds.sql.should == "SELECT tags.* FROM tags WHERE (tags.id IN (SELECT albums_tags.tag_id FROM artists INNER JOIN albums ON (albums.artist_id = artists.id) INNER JOIN albums_tags ON (albums_tags.album_id = albums.id)))"
  end

  it "should have an associated method that takes an association symbol" do
    ds = @Album.associated(:artist)
    ds.should be_a_kind_of(Sequel::Dataset)
    ds.model.should == @Artist
    ds.sql.should == "SELECT * FROM artists WHERE (artists.id IN (SELECT albums.artist_id FROM albums))"
  end

  it "should raise an Error if an invalid association is given to associated" do
    proc{@Album.associated(:foo)}.should raise_error(Sequel::Error)
  end

  it "should raise an Error if an unrecognized association type is used" do
    @Album.association_reflection(:artist)[:type] = :foo
    proc{@Album.artists}.should raise_error(Sequel::Error)
  end

  it "should work correctly when chaining" do
    ds = @Artist.albums.tags
    ds.should be_a_kind_of(Sequel::Dataset)
    ds.model.should == @Tag
    ds.sql.should == "SELECT tags.* FROM tags WHERE (tags.id IN (SELECT albums_tags.tag_id FROM albums INNER JOIN albums_tags ON (albums_tags.album_id = albums.id) WHERE (albums.artist_id IN (SELECT artists.id FROM artists))))"
  end

  it "should deal correctly with filters before the association method" do
    @Artist.filter(:id=>1).albums.sql.should == "SELECT * FROM albums WHERE (albums.artist_id IN (SELECT artists.id FROM artists WHERE (id = 1)))"
  end

  it "should deal correctly with filters after the association method" do
    @Artist.albums.filter(:id=>1).sql.should == "SELECT * FROM albums WHERE ((albums.artist_id IN (SELECT artists.id FROM artists)) AND (id = 1))"
  end

  it "should deal correctly with block on the association" do
    @Artist.one_to_many :albums, :clone=>:albums do |ds| ds.filter(:id=>1..100) end
    @Artist.albums.sql.should == "SELECT * FROM albums WHERE ((albums.artist_id IN (SELECT artists.id FROM artists)) AND (id >= 1) AND (id <= 100))"
  end

  it "should deal correctly with :conditions option on the association" do
    @Artist.one_to_many :albums, :clone=>:albums, :conditions=>{:id=>1..100}
    @Artist.albums.sql.should == "SELECT * FROM albums WHERE ((albums.artist_id IN (SELECT artists.id FROM artists)) AND (id >= 1) AND (id <= 100))"
  end

  it "should deal correctly with :distinct option on the association" do
    @Artist.one_to_many :albums, :clone=>:albums, :distinct=>true
    @Artist.albums.sql.should == "SELECT DISTINCT * FROM albums WHERE (albums.artist_id IN (SELECT artists.id FROM artists))"
  end

  it "should deal correctly with :eager option on the association" do
    @Artist.one_to_many :albums, :clone=>:albums, :eager=>:tags
    @Artist.albums.opts[:eager].should == {:tags=>nil}
  end

  it "should deal correctly with :eager_block option on the association, ignoring the association block" do
    @Artist.one_to_many :albums, :clone=>:albums, :eager_block=>proc{|ds| ds.filter(:id=>1..100)} do |ds| ds.filter(:id=>2..200) end
    @Artist.albums.sql.should == "SELECT * FROM albums WHERE ((albums.artist_id IN (SELECT artists.id FROM artists)) AND (id >= 1) AND (id <= 100))"
  end

  it "should deal correctly with :extend option on the association" do
    @Artist.one_to_many :albums, :clone=>:albums, :extend=>Module.new{def foo(x) filter(:id=>x) end}
    @Artist.albums.foo(1).sql.should == "SELECT * FROM albums WHERE ((albums.artist_id IN (SELECT artists.id FROM artists)) AND (id = 1))"
  end

  it "should deal correctly with :order option on the association" do
    @Artist.one_to_many :albums, :clone=>:albums, :order=>:name
    @Artist.albums.sql.should == "SELECT * FROM albums WHERE (albums.artist_id IN (SELECT artists.id FROM artists)) ORDER BY name"
  end

  it "should deal correctly with :select option on the association" do
    @Artist.one_to_many :albums, :clone=>:albums, :select=>[:id, :name]
    @Artist.albums.sql.should == "SELECT id, name FROM albums WHERE (albums.artist_id IN (SELECT artists.id FROM artists))"
  end
end

describe "Sequel::Plugins::DatasetAssociations with composite keys" do
  before do
    @db = Sequel.mock
    @Base = Class.new(Sequel::Model)
    @Base.plugin :dataset_associations

    @Artist = Class.new(@Base)
    @Album = Class.new(@Base)
    @Tag = Class.new(@Base)

    @Artist.meta_def(:name){'Artist'}
    @Album.meta_def(:name){'Album'}
    @Tag.meta_def(:name){'Tag'}

    @Artist.dataset = @db[:artists]
    @Album.dataset = @db[:albums]
    @Tag.dataset = @db[:tags]

    @Artist.set_primary_key([:id1, :id2])
    @Album.set_primary_key([:id1, :id2])
    @Tag.set_primary_key([:id1, :id2])

    @Artist.columns :id1, :id2, :name
    @Album.columns :id1, :id2, :name, :artist_id1, :artist_id2
    @Tag.columns :id1, :id2, :name

    @Artist.plugin :many_through_many
    @Artist.one_to_many :albums, :class=>@Album, :key=>[:artist_id1, :artist_id2]
    @Artist.one_to_one :first_album, :class=>@Album, :key=>[:artist_id1, :artist_id2]
    @Album.many_to_one :artist, :class=>@Artist, :key=>[:artist_id1, :artist_id2]
    @Album.many_to_many :tags, :class=>@Tag, :left_key=>[:album_id1, :album_id2], :right_key=>[:tag_id1, :tag_id2]
    @Tag.many_to_many :albums, :class=>@Album, :right_key=>[:album_id1, :album_id2], :left_key=>[:tag_id1, :tag_id2]
    @Artist.many_through_many :tags, [[:albums, [:artist_id1, :artist_id2], [:id1, :id2]], [:albums_tags, [:album_id1, :album_id2], [:tag_id1, :tag_id2]]], :class=>@Tag
  end

  it "should work for many_to_one associations" do
    @Album.artists.sql.should == "SELECT * FROM artists WHERE ((artists.id1, artists.id2) IN (SELECT albums.artist_id1, albums.artist_id2 FROM albums))"
  end

  it "should work for one_to_many associations" do
    @Artist.albums.sql.should == "SELECT * FROM albums WHERE ((albums.artist_id1, albums.artist_id2) IN (SELECT artists.id1, artists.id2 FROM artists))"
  end

  it "should work for one_to_one associations" do
    @Artist.first_albums.sql.should == "SELECT * FROM albums WHERE ((albums.artist_id1, albums.artist_id2) IN (SELECT artists.id1, artists.id2 FROM artists))"
  end

  it "should work for many_to_many associations" do
    @Album.tags.sql.should == "SELECT tags.* FROM tags WHERE ((tags.id1, tags.id2) IN (SELECT albums_tags.tag_id1, albums_tags.tag_id2 FROM albums INNER JOIN albums_tags ON ((albums_tags.album_id1 = albums.id1) AND (albums_tags.album_id2 = albums.id2))))"
  end

  it "should work for many_through_many associations" do
    @Artist.tags.sql.should == "SELECT tags.* FROM tags WHERE ((tags.id1, tags.id2) IN (SELECT albums_tags.tag_id1, albums_tags.tag_id2 FROM artists INNER JOIN albums ON ((albums.artist_id1 = artists.id1) AND (albums.artist_id2 = artists.id2)) INNER JOIN albums_tags ON ((albums_tags.album_id1 = albums.id1) AND (albums_tags.album_id2 = albums.id2))))"
  end

  it "should work correctly when chaining" do
    @Artist.albums.tags.sql.should == "SELECT tags.* FROM tags WHERE ((tags.id1, tags.id2) IN (SELECT albums_tags.tag_id1, albums_tags.tag_id2 FROM albums INNER JOIN albums_tags ON ((albums_tags.album_id1 = albums.id1) AND (albums_tags.album_id2 = albums.id2)) WHERE ((albums.artist_id1, albums.artist_id2) IN (SELECT artists.id1, artists.id2 FROM artists))))"
  end
end
