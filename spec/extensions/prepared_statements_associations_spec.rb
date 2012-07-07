require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::AssociationPks" do
  before do
    @db = Sequel.mock
    @db.extend_datasets do
      def select_sql
        sql = super
        sql << ' -- prepared' if is_a?(Sequel::Dataset::PreparedStatementMethods)
        sql
      end
    end
    @Artist = Class.new(Sequel::Model(@db[:artists]))
    @Artist.columns :id, :id2
    @Album= Class.new(Sequel::Model(@db[:albums]))
    @Album.columns :id, :artist_id, :id2, :artist_id2
    @Tag = Class.new(Sequel::Model(@db[:tags]))
    @Tag.columns :id, :id2
    @Artist.plugin :prepared_statements_associations
    @Album.plugin :prepared_statements_associations
    @Artist.one_to_many :albums, :class=>@Album, :key=>:artist_id
    @Album.many_to_one :artist, :class=>@Artist
    @Album.many_to_many :tags, :class=>@Tag, :join_table=>:albums_tags, :left_key=>:album_id
    @Artist.plugin :many_through_many
    @Artist.many_through_many :tags, [[:albums, :artist_id, :id], [:albums_tags, :album_id, :tag_id]], :class=>@Tag
    @db.sqls
  end

  specify "should run correct SQL for associations" do
    @Artist.load(:id=>1).albums
    @db.sqls.should == ["SELECT * FROM albums WHERE (albums.artist_id = 1) -- prepared"]

    @Album.load(:id=>1, :artist_id=>2).artist
    @db.sqls.should == ["SELECT * FROM artists WHERE (artists.id = 2) LIMIT 1 -- prepared"]

    @Album.load(:id=>1, :artist_id=>2).tags
    @db.sqls.should == ["SELECT tags.* FROM tags INNER JOIN albums_tags ON ((albums_tags.tag_id = tags.id) AND (albums_tags.album_id = 1)) -- prepared"]

    @Artist.load(:id=>1).tags
    @db.sqls.should == ["SELECT tags.* FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON ((albums.id = albums_tags.album_id) AND (albums.artist_id = 1)) -- prepared"]
  end

  specify "should run correct SQL for composite key associations" do
    @Artist.one_to_many :albums, :class=>@Album, :key=>[:artist_id, :artist_id2], :primary_key=>[:id, :id2]
    @Album.many_to_one :artist, :class=>@Artist, :key=>[:artist_id, :artist_id2], :primary_key=>[:id, :id2]
    @Album.many_to_many :tags, :class=>@Tag, :join_table=>:albums_tags, :left_key=>[:album_id, :album_id2], :right_key=>[:tag_id, :tag_id2], :right_primary_key=>[:id, :id2], :left_primary_key=>[:id, :id2]
    @Artist.many_through_many :tags, [[:albums, [:artist_id, :artist_id2], [:id, :id2]], [:albums_tags, [:album_id, :album_id2], [:tag_id, :tag_id2]]], :class=>@Tag, :right_primary_key=>[:id, :id2], :left_primary_key=>[:id, :id2]

    @Artist.load(:id=>1, :id2=>2).albums
    @db.sqls.should == ["SELECT * FROM albums WHERE ((albums.artist_id = 1) AND (albums.artist_id2 = 2)) -- prepared"]

    @Album.load(:id=>1, :artist_id=>2, :artist_id2=>3).artist
    @db.sqls.should == ["SELECT * FROM artists WHERE ((artists.id = 2) AND (artists.id2 = 3)) LIMIT 1 -- prepared"]

    @Album.load(:id=>1, :artist_id=>2, :id2=>3).tags
    @db.sqls.should == ["SELECT tags.* FROM tags INNER JOIN albums_tags ON ((albums_tags.tag_id = tags.id) AND (albums_tags.tag_id2 = tags.id2) AND (albums_tags.album_id = 1) AND (albums_tags.album_id2 = 3)) -- prepared"]

    @Artist.load(:id=>1, :id2=>2).tags
    @db.sqls.should == ["SELECT tags.* FROM tags INNER JOIN albums_tags ON ((albums_tags.tag_id = tags.id) AND (albums_tags.tag_id2 = tags.id2)) INNER JOIN albums ON ((albums.id = albums_tags.album_id) AND (albums.id2 = albums_tags.album_id2) AND (albums.artist_id = 1) AND (albums.artist_id2 = 2)) -- prepared"]
  end

  specify "should not run query if no objects can be associated" do
    @Artist.new.albums.should == []
    @Album.new.artist.should == nil
    @db.sqls.should == []
  end

  specify "should run a regular query if there is a callback" do
    @Artist.load(:id=>1).albums(proc{|ds| ds})
    @db.sqls.should == ["SELECT * FROM albums WHERE (albums.artist_id = 1)"]
  end

  specify "should run a regular query if :prepared_statement=>false option is used for the association" do
    @Artist.one_to_many :albums, :class=>@Album, :key=>:artist_id, :prepared_statement=>false
    @Artist.load(:id=>1).albums
    @db.sqls.should == ["SELECT * FROM albums WHERE (albums.artist_id = 1)"]
  end

  specify "should run a regular query if unrecognized association is used" do
    a = @Artist.one_to_many :albums, :class=>@Album, :key=>:artist_id
    a[:type] = :foo
    @Artist.load(:id=>1).albums
    @db.sqls.should == ["SELECT * FROM albums WHERE (albums.artist_id = 1)"]
  end

  specify "should run a regular query if a block is used when defining the association" do
    @Artist.one_to_many :albums, :class=>@Album, :key=>:artist_id do |ds| ds end
    @Artist.load(:id=>1).albums
    @db.sqls.should == ["SELECT * FROM albums WHERE (albums.artist_id = 1)"]
  end

  specify "should run a regular query if :conditions option is used when defining the association" do
    @Artist.one_to_many :albums, :class=>@Album, :key=>:artist_id, :conditions=>{:a=>1}
    @Artist.load(:id=>1).albums
    @db.sqls.should == ["SELECT * FROM albums WHERE ((albums.artist_id = 1) AND (a = 1))"]
  end

  specify "should run a regular query if :dataset option is used when defining the association" do
    album = @Album
    @Artist.one_to_many :albums, :class=>@Album, :dataset=>proc{album.filter(:artist_id=>id)}
    @Artist.load(:id=>1).albums
    @db.sqls.should == ["SELECT * FROM albums WHERE (artist_id = 1)"]
  end

  specify "should run a regular query if :cloning an association that doesn't used prepared statements" do
    @Artist.one_to_many :albums, :class=>@Album, :key=>:artist_id, :conditions=>{:a=>1}
    @Artist.one_to_many :oalbums, :clone=>:albums
    @Artist.load(:id=>1).oalbums
    @db.sqls.should == ["SELECT * FROM albums WHERE ((albums.artist_id = 1) AND (a = 1))"]
  end
end
