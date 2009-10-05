require File.join(File.dirname(__FILE__), 'spec_helper.rb')

shared_examples_for "regular and composite key associations" do  
  specify "should return no objects if none are associated" do
    @album.artist.should == nil
    @artist.albums.should == []
    @album.tags.should == []
    @tag.albums.should == []
  end

  specify "should have add and set methods work any associated objects" do
    @album.update(:artist => @artist)
    @album.add_tag(@tag)
    
    @album.reload
    @artist.reload
    @tag.reload
    
    @album.artist.should == @artist
    @artist.albums.should == [@album]
    @album.tags.should == [@tag]
    @tag.albums.should == [@album]
  end
  
  specify "should have remove methods work" do
    @album.update(:artist => @artist)
    @album.add_tag(@tag)
    
    @album.update(:artist => nil)
    @album.remove_tag(@tag)
    
    @album.reload
    @artist.reload
    @tag.reload
    
    @album.artist.should == nil
    @artist.albums.should == []
    @album.tags.should == []
    @tag.albums.should == []
  end
  
  specify "should have remove_all methods work" do
    @artist.add_album(@album)
    @album.add_tag(@tag)
    
    @artist.remove_all_albums
    @album.remove_all_tags
    
    @album.reload
    @artist.reload
    @tag.reload
    
    @album.artist.should == nil
    @artist.albums.should == []
    @album.tags.should == []
    @tag.albums.should == []
  end
  
  specify "should eager load via eager correctly" do
    @album.update(:artist => @artist)
    @album.add_tag(@tag)
    
    a = Artist.eager(:albums=>:tags).all
    a.should == [@artist]
    a.first.albums.should == [@album]
    a.first.albums.first.tags.should == [@tag]
    
    a = Tag.eager(:albums=>:artist).all
    a.should == [@tag]
    a.first.albums.should == [@album]
    a.first.albums.first.artist.should == @artist
  end
  
  specify "should eager load via eager_graph correctly" do
    @album.update(:artist => @artist)
    @album.add_tag(@tag)
    
    a = Artist.eager_graph(:albums=>:tags).all
    a.should == [@artist]
    a.first.albums.should == [@album]
    a.first.albums.first.tags.should == [@tag]
    
    a = Tag.eager_graph(:albums=>:artist).all
    a.should == [@tag]
    a.first.albums.should == [@album]
    a.first.albums.first.artist.should == @artist
  end
end

describe "Sequel::Model Simple Associations" do
  before do
    @db = INTEGRATION_DB
    @db.create_table!(:artists) do
      primary_key :id
      String :name
    end
    @db.create_table!(:albums) do
      primary_key :id
      String :name
      foreign_key :artist_id, :artists
    end
    @db.create_table!(:tags) do
      primary_key :id
      String :name
    end
    @db.create_table!(:albums_tags) do
      foreign_key :album_id, :albums
      foreign_key :tag_id, :tags
    end
    class ::Artist < Sequel::Model(@db)
      one_to_many :albums
    end
    class ::Album < Sequel::Model(@db)
      many_to_one :artist
      many_to_many :tags
    end
    class ::Tag < Sequel::Model(@db)
      many_to_many :albums
    end
    @album = Album.create(:name=>'Al')
    @artist = Artist.create(:name=>'Ar')
    @tag = Tag.create(:name=>'T')
  end
  after do
    @db.drop_table(:albums_tags, :tags, :albums, :artists)
    [:Tag, :Album, :Artist].each{|x| Object.send(:remove_const, x)}
  end
  
  it_should_behave_like "regular and composite key associations"
end

describe "Sequel::Model Composite Key Associations" do
  before do
    @db = INTEGRATION_DB
    @db.create_table!(:artists) do
      Integer :id1
      Integer :id2
      String :name
      primary_key [:id1, :id2]
    end
    @db.create_table!(:albums) do
      Integer :id1
      Integer :id2
      String :name
      Integer :artist_id1
      Integer :artist_id2
      foreign_key [:artist_id1, :artist_id2], :artists
      primary_key [:id1, :id2]
    end
    @db.create_table!(:tags) do
      Integer :id1
      Integer :id2
      String :name
      primary_key [:id1, :id2]
    end
    @db.create_table!(:albums_tags) do
      Integer :album_id1
      Integer :album_id2
      Integer :tag_id1
      Integer :tag_id2
      foreign_key [:album_id1, :album_id2], :albums
      foreign_key [:tag_id1, :tag_id2], :tags
    end
    class ::Artist < Sequel::Model(@db)
      set_primary_key :id1, :id2
      unrestrict_primary_key
      one_to_many :albums, :key=>[:artist_id1, :artist_id2], :primary_key=>[:id1, :id2]
    end
    class ::Album < Sequel::Model(@db)
      set_primary_key :id1, :id2
      unrestrict_primary_key
      many_to_one :artist, :key=>[:artist_id1, :artist_id2], :primary_key=>[:id1, :id2]
      many_to_many :tags, :left_key=>[:album_id1, :album_id2], :right_key=>[:tag_id1, :tag_id2], :left_primary_key=>[:id1, :id2], :right_primary_key=>[:id1, :id2]
    end
    class ::Tag < Sequel::Model(@db)
      set_primary_key :id1, :id2
      unrestrict_primary_key
      many_to_many :albums, :right_key=>[:album_id1, :album_id2], :left_key=>[:tag_id1, :tag_id2], :left_primary_key=>[:id1, :id2], :right_primary_key=>[:id1, :id2]
    end
    @album = Album.create(:name=>'Al', :id1=>1, :id2=>2)
    @artist = Artist.create(:name=>'Ar', :id1=>3, :id2=>4)
    @tag = Tag.create(:name=>'T', :id1=>5, :id2=>6)
  end
  after do
    @db.drop_table(:albums_tags, :tags, :albums, :artists)
    [:Tag, :Album, :Artist].each{|x| Object.send(:remove_const, x)}
  end

  it_should_behave_like "regular and composite key associations"
end
