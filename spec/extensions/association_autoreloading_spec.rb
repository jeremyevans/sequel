require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "AssociationAutoreloading plugin" do
  before do
    @c = Class.new(Sequel::Model)
    @c.plugin :association_autoreloading
    @Artist = Class.new(@c).set_dataset(:artists)
    @Artist.dataset._fetch = {:id=>2, :name=>'Ar'}
    @Album = Class.new(@c).set_dataset(:albums)
    @Artist.columns :id, :name
    @Album.columns :id, :name, :artist_id
    @Album.db_schema[:artist_id][:type] = :integer
    @Album.many_to_one :artist, :class=>@Artist
    MODEL_DB.reset
  end

  specify "should reload many_to_one association when foreign key is modified" do
    album = @Album.load(:id => 1, :name=>'Al', :artist_id=>2)
    album.artist
    MODEL_DB.sqls.should == ['SELECT * FROM artists WHERE (artists.id = 2) LIMIT 1']

    album.artist_id = 1
    album.artist
    MODEL_DB.sqls.should == ['SELECT * FROM artists WHERE (artists.id = 1) LIMIT 1']
  end

  specify "should not reload when value has not changed" do
    album = @Album.load(:id => 1, :name=>'Al', :artist_id=>2)
    album.artist
    MODEL_DB.sqls.should == ['SELECT * FROM artists WHERE (artists.id = 2) LIMIT 1']

    album.artist_id = 2
    album.artist
    MODEL_DB.sqls.should == []

    album.artist_id = "2"
    album.artist
    MODEL_DB.sqls.should == []
  end

  specify "should reload all associations which use the foreign key" do
    @Album.many_to_one :other_artist, :key => :artist_id, :foreign_key => :id, :class => @Artist
    album = @Album.load(:id => 1, :name=>'Al', :artist_id=>2)
    album.artist
    album.other_artist
    MODEL_DB.reset

    album.artist_id = 1
    album.artist
    MODEL_DB.sqls.should == ['SELECT * FROM artists WHERE (artists.id = 1) LIMIT 1']

    album.other_artist
    MODEL_DB.sqls.should == ['SELECT * FROM artists WHERE (artists.id = 1) LIMIT 1']
  end

  specify "should work with composite keys" do
    @Album.many_to_one :composite_artist, :key => [:artist_id, :name], :primary_key => [:id, :name], :class => @Artist
    album = @Album.load(:id => 1, :name=>'Al', :artist_id=>2)
    album.composite_artist
    MODEL_DB.reset

    album.artist_id = 1
    album.composite_artist
    MODEL_DB.sqls.should == ["SELECT * FROM artists WHERE ((artists.id = 1) AND (artists.name = 'Al')) LIMIT 1"]

    album.name = 'Al2'
    album.composite_artist
    MODEL_DB.sqls.should == ["SELECT * FROM artists WHERE ((artists.id = 1) AND (artists.name = 'Al2')) LIMIT 1"]
  end

  specify "should work with subclasses" do
    salbum = Class.new(@Album)
    oartist = Class.new(@c).set_dataset(:oartist)
    oartist.columns :id, :name
    salbum.many_to_one :artist2, :class=>oartist, :key=>:artist_id
    album = salbum.load(:id => 1, :name=>'Al', :artist_id=>2)
    album.artist
    MODEL_DB.sqls.should == ['SELECT * FROM artists WHERE (artists.id = 2) LIMIT 1']

    album.artist_id = 1
    album.artist
    MODEL_DB.sqls.should == ['SELECT * FROM artists WHERE (artists.id = 1) LIMIT 1']
  end

end
