require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::PreparedStatementsAssociations" do
  before do
    @db = Sequel.mock(:servers=>{:foo=>{}})
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
    @Artist.one_to_one :album, :class=>@Album, :key=>:artist_id
    @Album.many_to_one :artist, :class=>@Artist
    @Album.many_to_many :tags, :class=>@Tag, :join_table=>:albums_tags, :left_key=>:album_id
    @Album.one_through_one :tag, :clone=>:tags
    @Artist.plugin :many_through_many
    @Artist.many_through_many :tags, [[:albums, :artist_id, :id], [:albums_tags, :album_id, :tag_id]], :class=>@Tag
    @Artist.one_through_many :tag, :clone=>:tags
    @db.sqls
  end

  it "should run correct SQL for associations" do
    @Artist.load(:id=>1).albums
    @db.sqls.must_equal ["SELECT id, artist_id, id2, artist_id2 FROM albums WHERE (albums.artist_id = 1) -- prepared"]

    @Artist.load(:id=>1).album
    @db.sqls.must_equal ["SELECT id, artist_id, id2, artist_id2 FROM albums WHERE (albums.artist_id = 1) LIMIT 1 -- prepared"]

    @Album.load(:id=>1, :artist_id=>2).artist
    @db.sqls.must_equal ["SELECT id, id2 FROM artists WHERE (artists.id = 2) LIMIT 1 -- prepared"]

    @Album.load(:id=>1, :artist_id=>2).tags
    @db.sqls.must_equal ["SELECT tags.id, tags.id2 FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) WHERE (albums_tags.album_id = 1) -- prepared"]

    @Album.load(:id=>1, :artist_id=>2).tag
    @db.sqls.must_equal ["SELECT tags.id, tags.id2 FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) WHERE (albums_tags.album_id = 1) LIMIT 1 -- prepared"]

    @Artist.load(:id=>1).tags
    @db.sqls.must_equal ["SELECT tags.id, tags.id2 FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) WHERE (albums.artist_id = 1) -- prepared"]

    @Artist.load(:id=>1).tag
    @db.sqls.must_equal ["SELECT tags.id, tags.id2 FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) WHERE (albums.artist_id = 1) LIMIT 1 -- prepared"]
  end

  it "should run correct shard for associations when also using sharding plugin" do
    @Artist.plugin :sharding
    @Album.plugin :sharding

    @Artist.load(:id=>1).set_server(:foo).albums
    @db.sqls.must_equal ["SELECT id, artist_id, id2, artist_id2 FROM albums WHERE (albums.artist_id = 1) -- prepared -- foo"]

    @Artist.load(:id=>1).set_server(:foo).album
    @db.sqls.must_equal ["SELECT id, artist_id, id2, artist_id2 FROM albums WHERE (albums.artist_id = 1) LIMIT 1 -- prepared -- foo"]

    @Album.load(:id=>1, :artist_id=>2).set_server(:foo).artist
    @db.sqls.must_equal ["SELECT id, id2 FROM artists WHERE (artists.id = 2) LIMIT 1 -- prepared -- foo"]

    @Album.load(:id=>1, :artist_id=>2).set_server(:foo).tags
    @db.sqls.must_equal ["SELECT tags.id, tags.id2 FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) WHERE (albums_tags.album_id = 1) -- prepared -- foo"]

    @Album.load(:id=>1, :artist_id=>2).set_server(:foo).tag
    @db.sqls.must_equal ["SELECT tags.id, tags.id2 FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) WHERE (albums_tags.album_id = 1) LIMIT 1 -- prepared -- foo"]

    @Artist.load(:id=>1).set_server(:foo).tags
    @db.sqls.must_equal ["SELECT tags.id, tags.id2 FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) WHERE (albums.artist_id = 1) -- prepared -- foo"]

    @Artist.load(:id=>1).set_server(:foo).tag
    @db.sqls.must_equal ["SELECT tags.id, tags.id2 FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) WHERE (albums.artist_id = 1) LIMIT 1 -- prepared -- foo"]

    @Tag.plugin :sharding
    @Tag.plugin :prepared_statements_associations
    @Tag.many_to_many :albums, :class=>@Album, :join_table=>:albums_tags, :left_key=>:tag_id
    @Tag.load(:id=>1).set_server(:foo).albums
    @db.sqls.must_equal ["SELECT albums.id, albums.artist_id, albums.id2, albums.artist_id2 FROM albums INNER JOIN albums_tags ON (albums_tags.album_id = albums.id) WHERE (albums_tags.tag_id = 1) -- prepared -- foo"]
  end

  it "should not override the shard for associations if not using the sharding plugin" do
    @Artist.load(:id=>1).set_server(:foo).albums
    @db.sqls.must_equal ["SELECT id, artist_id, id2, artist_id2 FROM albums WHERE (albums.artist_id = 1) -- prepared"]
  end

  it "should run correct SQL for composite key associations" do
    @Artist.one_to_many :albums, :class=>@Album, :key=>[:artist_id, :artist_id2], :primary_key=>[:id, :id2]
    @Artist.one_to_one :album, :class=>@Album, :key=>[:artist_id, :artist_id2], :primary_key=>[:id, :id2]
    @Album.many_to_one :artist, :class=>@Artist, :key=>[:artist_id, :artist_id2], :primary_key=>[:id, :id2]
    @Album.many_to_many :tags, :class=>@Tag, :join_table=>:albums_tags, :left_key=>[:album_id, :album_id2], :right_key=>[:tag_id, :tag_id2], :right_primary_key=>[:id, :id2], :left_primary_key=>[:id, :id2]
    @Album.one_through_one :tag, :clone=>:tags

    @Artist.many_through_many :tags, [[:albums, [:artist_id, :artist_id2], [:id, :id2]], [:albums_tags, [:album_id, :album_id2], [:tag_id, :tag_id2]]], :class=>@Tag, :right_primary_key=>[:id, :id2], :left_primary_key=>[:id, :id2]
    @Artist.one_through_many :tag, :clone=>:tags

    @Artist.load(:id=>1, :id2=>2).albums
    @db.sqls.must_equal ["SELECT id, artist_id, id2, artist_id2 FROM albums WHERE ((albums.artist_id = 1) AND (albums.artist_id2 = 2)) -- prepared"]

    @Artist.load(:id=>1, :id2=>2).album
    @db.sqls.must_equal ["SELECT id, artist_id, id2, artist_id2 FROM albums WHERE ((albums.artist_id = 1) AND (albums.artist_id2 = 2)) LIMIT 1 -- prepared"]

    @Album.load(:id=>1, :artist_id=>2, :artist_id2=>3).artist
    @db.sqls.must_equal ["SELECT id, id2 FROM artists WHERE ((artists.id = 2) AND (artists.id2 = 3)) LIMIT 1 -- prepared"]

    @Album.load(:id=>1, :artist_id=>2, :id2=>3).tags
    @db.sqls.must_equal ["SELECT tags.id, tags.id2 FROM tags INNER JOIN albums_tags ON ((albums_tags.tag_id = tags.id) AND (albums_tags.tag_id2 = tags.id2)) WHERE ((albums_tags.album_id = 1) AND (albums_tags.album_id2 = 3)) -- prepared"]

    @Album.load(:id=>1, :artist_id=>2, :id2=>3).tag
    @db.sqls.must_equal ["SELECT tags.id, tags.id2 FROM tags INNER JOIN albums_tags ON ((albums_tags.tag_id = tags.id) AND (albums_tags.tag_id2 = tags.id2)) WHERE ((albums_tags.album_id = 1) AND (albums_tags.album_id2 = 3)) LIMIT 1 -- prepared"]

    @Artist.load(:id=>1, :id2=>2).tags
    @db.sqls.must_equal ["SELECT tags.id, tags.id2 FROM tags INNER JOIN albums_tags ON ((albums_tags.tag_id = tags.id) AND (albums_tags.tag_id2 = tags.id2)) INNER JOIN albums ON ((albums.id = albums_tags.album_id) AND (albums.id2 = albums_tags.album_id2)) WHERE ((albums.artist_id = 1) AND (albums.artist_id2 = 2)) -- prepared"]

    @Artist.load(:id=>1, :id2=>2).tag
    @db.sqls.must_equal ["SELECT tags.id, tags.id2 FROM tags INNER JOIN albums_tags ON ((albums_tags.tag_id = tags.id) AND (albums_tags.tag_id2 = tags.id2)) INNER JOIN albums ON ((albums.id = albums_tags.album_id) AND (albums.id2 = albums_tags.album_id2)) WHERE ((albums.artist_id = 1) AND (albums.artist_id2 = 2)) LIMIT 1 -- prepared"]
  end

  it "should not run query if no objects can be associated" do
    @Artist.new.albums.must_equal []
    @Album.new.artist.must_be_nil
    @db.sqls.must_equal []
  end

  it "should run a regular query if not caching association metadata" do
    @Artist.cache_associations = false
    @Artist.load(:id=>1).albums
    @db.sqls.must_equal ["SELECT * FROM albums WHERE (albums.artist_id = 1)"]
    @Artist.load(:id=>1).album
    @db.sqls.must_equal ["SELECT * FROM albums WHERE (albums.artist_id = 1) LIMIT 1"]
  end

  it "should run a regular query if there is a callback" do
    @Artist.load(:id=>1).albums(proc{|ds| ds})
    @db.sqls.must_equal ["SELECT * FROM albums WHERE (albums.artist_id = 1)"]
    @Artist.load(:id=>1).album(proc{|ds| ds})
    @db.sqls.must_equal ["SELECT * FROM albums WHERE (albums.artist_id = 1) LIMIT 1"]
  end

  it "should run a regular query if :prepared_statement=>false option is used for the association" do
    @Artist.one_to_many :albums, :class=>@Album, :key=>:artist_id, :prepared_statement=>false
    @Artist.load(:id=>1).albums
    @db.sqls.must_equal ["SELECT * FROM albums WHERE (albums.artist_id = 1)"]
  end

  it "should run a regular query if unrecognized association is used" do
    a = @Artist.one_to_many :albums, :class=>@Album, :key=>:artist_id
    a[:type] = :foo
    @Artist.load(:id=>1).albums
    @db.sqls.must_equal ["SELECT * FROM albums WHERE (albums.artist_id = 1)"]
  end

  it "should run a regular query if a block is used when defining the association" do
    @Artist.one_to_many :albums, :class=>@Album, :key=>:artist_id do |ds| ds end
    @Artist.load(:id=>1).albums
    @db.sqls.must_equal ["SELECT * FROM albums WHERE (albums.artist_id = 1)"]
  end

  it "should use a prepared statement if the associated dataset has conditions" do
    @Album.dataset = @Album.dataset.where(:a=>2)
    @Artist.one_to_many :albums, :class=>@Album, :key=>:artist_id
    @Artist.load(:id=>1).albums
    @db.sqls.must_equal ["SELECT id, artist_id, id2, artist_id2 FROM albums WHERE ((a = 2) AND (albums.artist_id = 1)) -- prepared"]
  end

  it "should use a prepared statement if the :conditions association option" do
    @Artist.one_to_many :albums, :class=>@Album, :key=>:artist_id, :conditions=>{:a=>2} 
    @Artist.load(:id=>1).albums
    @db.sqls.must_equal ["SELECT id, artist_id, id2, artist_id2 FROM albums WHERE ((a = 2) AND (albums.artist_id = 1)) -- prepared"]
  end

  it "should not use a prepared statement if :conditions association option uses an identifier" do
    @Artist.one_to_many :albums, :class=>@Album, :key=>:artist_id, :conditions=>{Sequel.identifier('a')=>2}
    @Artist.load(:id=>1).albums
    @db.sqls.must_equal ["SELECT id, artist_id, id2, artist_id2 FROM albums WHERE ((a = 2) AND (albums.artist_id = 1)) -- prepared"]
  end

  it "should run a regular query if :dataset option is used when defining the association" do
    album = @Album
    @Artist.one_to_many :albums, :class=>@Album, :dataset=>proc{album.filter(:artist_id=>id)} 
    @Artist.load(:id=>1).albums
    @db.sqls.must_equal ["SELECT * FROM albums WHERE (artist_id = 1)"]
  end

  it "should run a regular query if :cloning an association that doesn't used prepared statements" do
    @Artist.one_to_many :albums, :class=>@Album, :key=>:artist_id do |ds| ds end
    @Artist.one_to_many :oalbums, :clone=>:albums
    @Artist.load(:id=>1).oalbums
    @db.sqls.must_equal ["SELECT * FROM albums WHERE (albums.artist_id = 1)"]
  end

  it "should work correctly when using an instance specific association" do
    tag = @Tag 
    @Artist.many_to_one :tag, :key=>nil, :read_only=>true, :dataset=>proc{tag.where(:id=>id).limit(1)}, :reciprocal=>nil, :reciprocal_type=>nil
    @Artist.load(:id=>1).tag.must_be_nil
    @db.sqls.must_equal ["SELECT * FROM tags WHERE (id = 1) LIMIT 1"]
  end
end
