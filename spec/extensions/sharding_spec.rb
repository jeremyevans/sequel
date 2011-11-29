require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "sharding plugin" do
  before do
    @db = Sequel.mock(:numrows=>1, :autoid=>proc{1}, :servers=>{:s1=>{}, :s2=>{}, :s3=>{}, :s4=>{}})
    @Artist = Class.new(Sequel::Model(@db[:artists]))
    @Artist.class_eval do
      dataset._fetch = {:id=>2, :name=>'YJM'}
      columns :id, :name
      plugin :sharding
    end
    @Album = Class.new(Sequel::Model(@db[:albums]))
    @Album.class_eval do
      dataset._fetch = {:id=>1, :name=>'RF', :artist_id=>2}
      columns :id, :artist_id, :name
      plugin :sharding
    end
    @Tag = Class.new(Sequel::Model(@db[:tags]))
    @Tag.class_eval do
      dataset._fetch = {:id=>3, :name=>'M'}
      columns :id, :name
      plugin :sharding
    end
    @Artist.one_to_many :albums, :class=>@Album, :key=>:artist_id
    @Album.many_to_one :artist, :class=>@Artist
    @Album.many_to_many :tags, :class=>@Tag, :left_key=>:album_id, :right_key=>:tag_id, :join_table=>:albums_tags
    @db.sqls
  end 

  specify "should allow you to instantiate a new object for a specified shard" do
    @Album.new_using_server(:s1, :name=>'RF').save
    @db.sqls.should == ["INSERT INTO albums (name) VALUES ('RF') -- s1", "SELECT * FROM albums WHERE (id = 1) LIMIT 1 -- s1"]
    
    @Album.new_using_server(:s2){|o| o.name = 'MO'}.save
    @db.sqls.should == ["INSERT INTO albums (name) VALUES ('MO') -- s2", "SELECT * FROM albums WHERE (id = 1) LIMIT 1 -- s2"]
  end 

  specify "should allow you to create and save a new object for a specified shard" do
    @Album.create_using_server(:s1, :name=>'RF')
    @db.sqls.should == ["INSERT INTO albums (name) VALUES ('RF') -- s1", "SELECT * FROM albums WHERE (id = 1) LIMIT 1 -- s1"]

    @Album.create_using_server(:s2){|o| o.name = 'MO'}
    @db.sqls.should == ["INSERT INTO albums (name) VALUES ('MO') -- s2", "SELECT * FROM albums WHERE (id = 1) LIMIT 1 -- s2"]
  end 

  specify "should have objects retrieved from a specific shard update that shard" do
    @Album.server(:s1).first.update(:name=>'MO')
    @db.sqls.should == ["SELECT * FROM albums LIMIT 1 -- s1", "UPDATE albums SET name = 'MO' WHERE (id = 1) -- s1"]
  end 

  specify "should have objects retrieved from a specific shard delete from that shard" do
    @Album.server(:s1).first.delete
    @db.sqls.should == ["SELECT * FROM albums LIMIT 1 -- s1", "DELETE FROM albums WHERE (id = 1) -- s1"]
  end 

  specify "should have objects retrieved from a specific shard reload from that shard" do
    @Album.server(:s1).first.reload
    @db.sqls.should == ["SELECT * FROM albums LIMIT 1 -- s1", "SELECT * FROM albums WHERE (id = 1) LIMIT 1 -- s1"]
  end 

  specify "should use current dataset's shard when eager loading if eagerly loaded dataset doesn't have its own shard" do
    albums = @Album.server(:s1).eager(:artist).all
    @db.sqls.should == ["SELECT * FROM albums -- s1", "SELECT * FROM artists WHERE (artists.id IN (2)) -- s1"]
    albums.length.should == 1
    albums.first.artist.save
    @db.sqls.should == ["UPDATE artists SET name = 'YJM' WHERE (id = 2) -- s1"]
  end 

  specify "should not use current dataset's shard when eager loading if eagerly loaded dataset has its own shard" do
    @Artist.dataset.opts[:server] = :s2
    albums = @Album.server(:s1).eager(:artist).all
    @db.sqls.should == ["SELECT * FROM albums -- s1", "SELECT * FROM artists WHERE (artists.id IN (2)) -- s2"]
    albums.length.should == 1
    albums.first.artist.save
    @db.sqls.should == ["UPDATE artists SET name = 'YJM' WHERE (id = 2) -- s2"]
  end 

  specify "should use current dataset's shard when eager graphing if eagerly graphed dataset doesn't have its own shard" do
    ds = @Album.server(:s1).eager_graph(:artist)
    ds._fetch = {:id=>1, :artist_id=>2, :name=>'RF', :artist_id_0=>2, :artist_name=>'YJM'}
    albums = ds.all
    @db.sqls.should == ["SELECT albums.id, albums.artist_id, albums.name, artist.id AS artist_id_0, artist.name AS artist_name FROM albums LEFT OUTER JOIN artists AS artist ON (artist.id = albums.artist_id) -- s1"]
    albums.length.should == 1
    albums.first.artist.save
    @db.sqls.should == ["UPDATE artists SET name = 'YJM' WHERE (id = 2) -- s1"]
  end 

  specify "should not use current dataset's shard when eager graphing if eagerly graphed dataset has its own shard" do
    @Artist.dataset.opts[:server] = :s2
    ds = @Album.server(:s1).eager_graph(:artist)
    ds._fetch = {:id=>1, :artist_id=>2, :name=>'RF', :artist_id_0=>2, :artist_name=>'YJM'}
    albums = ds.all
    @db.sqls.should == ["SELECT albums.id, albums.artist_id, albums.name, artist.id AS artist_id_0, artist.name AS artist_name FROM albums LEFT OUTER JOIN artists AS artist ON (artist.id = albums.artist_id) -- s1"]
    albums.length.should == 1
    albums.first.artist.save
    @db.sqls.should == ["UPDATE artists SET name = 'YJM' WHERE (id = 2) -- s2"]
  end 

  specify "should use eagerly graphed dataset shard for eagerly graphed objects even if current dataset does not have a shard" do
    @Artist.dataset.opts[:server] = :s2
    ds = @Album.eager_graph(:artist)
    ds._fetch = {:id=>1, :artist_id=>2, :name=>'RF', :artist_id_0=>2, :artist_name=>'YJM'}
    albums = ds.all
    @db.sqls.should == ["SELECT albums.id, albums.artist_id, albums.name, artist.id AS artist_id_0, artist.name AS artist_name FROM albums LEFT OUTER JOIN artists AS artist ON (artist.id = albums.artist_id)"]
    albums.length.should == 1
    albums.first.artist.save
    @db.sqls.should == ["UPDATE artists SET name = 'YJM' WHERE (id = 2) -- s2"]
  end 

  specify "should have objects retrieved from a specific shard use associated objects from that shard, with modifications to the associated objects using that shard" do
    album = @Album.server(:s1).first
    @db.sqls.should == ["SELECT * FROM albums LIMIT 1 -- s1"]
    album.artist.update(:name=>'AS')
    @db.sqls.should == ["SELECT * FROM artists WHERE (artists.id = 2) LIMIT 1 -- s1", "UPDATE artists SET name = 'AS' WHERE (id = 2) -- s1"]
    album.tags.map{|a| a.update(:name=>'SR')}
    @db.sqls.should == ["SELECT tags.* FROM tags INNER JOIN albums_tags ON ((albums_tags.tag_id = tags.id) AND (albums_tags.album_id = 1)) -- s1", "UPDATE tags SET name = 'SR' WHERE (id = 3) -- s1"]
    @Artist.server(:s2).first.albums.map{|a| a.update(:name=>'MO')}
    @db.sqls.should == ["SELECT * FROM artists LIMIT 1 -- s2", "SELECT * FROM albums WHERE (albums.artist_id = 2) -- s2", "UPDATE albums SET name = 'MO' WHERE (id = 1) -- s2"]
  end 

  specify "should have objects retrieved from a specific shard add associated objects to that shard" do
    album = @Album.server(:s1).first
    artist = @Artist.server(:s2).first
    @db.sqls.should == ["SELECT * FROM albums LIMIT 1 -- s1", "SELECT * FROM artists LIMIT 1 -- s2"]

    artist.add_album(:name=>'MO')
    @db.sqls.should == ["INSERT INTO albums (artist_id, name) VALUES (2, 'MO') -- s2", "SELECT * FROM albums WHERE (id = 1) LIMIT 1 -- s2"]
    
    album.add_tag(:name=>'SR')
    @db.sqls.should == ["INSERT INTO tags (name) VALUES ('SR') -- s1", "SELECT * FROM tags WHERE (id = 1) LIMIT 1 -- s1", "INSERT INTO albums_tags (album_id, tag_id) VALUES (1, 3) -- s1"]
  end 

  specify "should have objects retrieved from a specific shard remove associated objects from that shard" do
    album = @Album.server(:s1).first
    artist = @Artist.server(:s2).first
    @db.sqls.should == ["SELECT * FROM albums LIMIT 1 -- s1", "SELECT * FROM artists LIMIT 1 -- s2"]

    artist.remove_album(1)
    @db.sqls.should == ["SELECT * FROM albums WHERE ((albums.artist_id = 2) AND (albums.id = 1)) LIMIT 1 -- s2", "UPDATE albums SET artist_id = NULL, name = 'RF' WHERE (id = 1) -- s2"]
    
    album.remove_tag(3)
    @db.sqls.should == ["SELECT tags.* FROM tags INNER JOIN albums_tags ON ((albums_tags.tag_id = tags.id) AND (albums_tags.album_id = 1)) WHERE (tags.id = 3) LIMIT 1 -- s1", "DELETE FROM albums_tags WHERE ((album_id = 1) AND (tag_id = 3)) -- s1"]
  end 

  specify "should have objects retrieved from a specific shard remove all associated objects from that shard" do
    album = @Album.server(:s1).first
    artist = @Artist.server(:s2).first
    @db.sqls.should == ["SELECT * FROM albums LIMIT 1 -- s1", "SELECT * FROM artists LIMIT 1 -- s2"]

    artist.remove_all_albums
    @db.sqls.should == ["UPDATE albums SET artist_id = NULL WHERE (artist_id = 2) -- s2"]
    
    album.remove_all_tags
    @db.sqls.should == ["DELETE FROM albums_tags WHERE (album_id = 1) -- s1"]
  end 

  specify "should not override a server already set on an associated object" do
    album = @Album.server(:s1).first
    artist = @Artist.server(:s2).first
    @db.sqls.should == ["SELECT * FROM albums LIMIT 1 -- s1", "SELECT * FROM artists LIMIT 1 -- s2"]

    artist.add_album(@Album.load(:id=>4, :name=>'MO').set_server(:s3))
    @db.sqls.should == ["UPDATE albums SET artist_id = 2, name = 'MO' WHERE (id = 4) -- s3"]

    artist.remove_album(@Album.load(:id=>5, :name=>'T', :artist_id=>2).set_server(:s4))
    # Should select from current object's shard to check existing association, but update associated object's shard
    @db.sqls.should == ["SELECT 1 FROM albums WHERE ((albums.artist_id = 2) AND (id = 5)) LIMIT 1 -- s2", "UPDATE albums SET artist_id = NULL, name = 'T' WHERE (id = 5) -- s4"]
  end 

  specify "should be able to set a shard to use for any object using set_server" do
    @Album.server(:s1).first.set_server(:s2).reload
    @db.sqls.should == ["SELECT * FROM albums LIMIT 1 -- s1", "SELECT * FROM albums WHERE (id = 1) LIMIT 1 -- s2"]
  end 

  specify "should use transactions on the correct shard" do
    @Album.use_transactions = true
    @Album.server(:s2).first.save
    @db.sqls.should == ["SELECT * FROM albums LIMIT 1 -- s2", "BEGIN -- s2", "UPDATE albums SET artist_id = 2, name = 'RF' WHERE (id = 1) -- s2", "COMMIT -- s2"]
  end 

  specify "should use not override shard given when saving" do
    @Album.use_transactions = true
    @Album.server(:s2).first.save(:server=>:s1)
    @db.sqls.should == ["SELECT * FROM albums LIMIT 1 -- s2", "BEGIN -- s1", "UPDATE albums SET artist_id = 2, name = 'RF' WHERE (id = 1) -- s2", "COMMIT -- s1"]
  end 
end
