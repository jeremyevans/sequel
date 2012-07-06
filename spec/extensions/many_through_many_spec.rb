require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model, "many_through_many" do
  before do
    class ::Artist < Sequel::Model
      attr_accessor :yyy
      columns :id
      plugin :many_through_many
    end
    class ::Tag < Sequel::Model
      columns :id, :h1, :h2
    end
    @c1 = Artist
    @c2 = Tag
    @dataset = @c2.dataset
    @dataset._fetch = {:id=>1}
    MODEL_DB.reset
  end
  after do
    Object.send(:remove_const, :Artist)
    Object.send(:remove_const, :Tag)
  end

  it "should populate :key_hash and :id_map option correctly for custom eager loaders" do
    khs = []
    pr = proc{|h| khs << [h[:key_hash], h[:id_map]]}
    @c1.many_through_many :tags, :through=>[[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :eager_loader=>pr
    @c1.eager(:tags).all
    khs.should == [[{:id=>{1=>[Artist.load(:x=>1, :id=>1)]}}, {1=>[Artist.load(:x=>1, :id=>1)]}]]

    khs.clear
    @c1.many_through_many :tags, :through=>[[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :left_primary_key=>:id, :left_primary_key_column=>:i, :eager_loader=>pr
    @c1.eager(:tags).all
    khs.should == [[{:id=>{1=>[Artist.load(:x=>1, :id=>1)]}}, {1=>[Artist.load(:x=>1, :id=>1)]}]]
  end

  it "should support using a custom :left_primary_key option when eager loading many_to_many associations" do
    @c1.send(:define_method, :id3){id*3}
    @c1.dataset._fetch = {:id=>1}
    @c2.dataset._fetch = {:id=>4, :x_foreign_key_x=>3}
    @c1.many_through_many :tags, :through=>[[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :left_primary_key=>:id3
    a = @c1.eager(:tags).all
    a.should == [@c1.load(:id => 1)]
    MODEL_DB.sqls.should == ['SELECT * FROM artists', "SELECT tags.*, albums_artists.artist_id AS x_foreign_key_x FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id IN (3)))"]
    a.first.tags.should == [@c2.load(:id=>4)]
    MODEL_DB.sqls.should == []
  end

  it "should handle a :eager_loading_predicate_key option to change the SQL used in the lookup" do
    @c1.dataset._fetch = {:id=>1}
    @c2.dataset._fetch = {:id=>4, :x_foreign_key_x=>1}
    @c1.many_through_many :tags, :through=>[[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :eager_loading_predicate_key=>Sequel./(:albums_artists__artist_id, 3)
    a = @c1.eager(:tags).all
    a.should == [@c1.load(:id => 1)]
    MODEL_DB.sqls.should == ['SELECT * FROM artists', "SELECT tags.*, (albums_artists.artist_id / 3) AS x_foreign_key_x FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND ((albums_artists.artist_id / 3) IN (1)))"]
    a.first.tags.should == [@c2.load(:id=>4)]
  end
  
  it "should default to associating to other models in the same scope" do
    begin
      class ::AssociationModuleTest
        class Artist < Sequel::Model
          plugin :many_through_many
          many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]]
        end  
        class Tag < Sequel::Model
        end  
      end  
      
      ::AssociationModuleTest::Artist.association_reflection(:tags).associated_class.should == ::AssociationModuleTest::Tag
    ensure
      Object.send(:remove_const, :AssociationModuleTest)
    end
  end 

  it "should raise an error if in invalid form of through is used" do
    proc{@c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id]]}.should raise_error(Sequel::Error)
    proc{@c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], {:table=>:album_tags, :left=>:album_id}]}.should raise_error(Sequel::Error)
    proc{@c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], :album_tags]}.should raise_error(Sequel::Error)
  end

  it "should allow only two arguments with the :through option" do
    @c1.many_through_many :tags, :through=>[[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]]
    n = @c1.load(:id => 1234)
    n.tags_dataset.sql.should == 'SELECT tags.* FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id = 1234))'
    n.tags.should == [@c2.load(:id=>1)]
  end

  it "should be clonable" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]]
    @c1.many_through_many :other_tags, :clone=>:tags
    n = @c1.load(:id => 1234)
    n.other_tags_dataset.sql.should == 'SELECT tags.* FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id = 1234))'
    n.tags.should == [@c2.load(:id=>1)]
  end

  it "should use join tables given" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]]
    n = @c1.load(:id => 1234)
    n.tags_dataset.sql.should == 'SELECT tags.* FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id = 1234))'
    n.tags.should == [@c2.load(:id=>1)]
  end

  it "should handle multiple aliasing of tables" do
    class ::Album < Sequel::Model
    end
    @c1.many_through_many :albums, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_artists, :album_id, :artist_id], [:artists, :id, :id], [:albums_artists, :artist_id, :album_id]]
    n = @c1.load(:id => 1234)
    n.albums_dataset.sql.should == 'SELECT albums.* FROM albums INNER JOIN albums_artists ON (albums_artists.album_id = albums.id) INNER JOIN artists ON (artists.id = albums_artists.artist_id) INNER JOIN albums_artists AS albums_artists_0 ON (albums_artists_0.artist_id = artists.id) INNER JOIN albums AS albums_0 ON (albums_0.id = albums_artists_0.album_id) INNER JOIN albums_artists AS albums_artists_1 ON ((albums_artists_1.album_id = albums_0.id) AND (albums_artists_1.artist_id = 1234))'
    n.albums.should == [Album.load(:id=>1, :x=>1)]
    Object.send(:remove_const, :Album)
  end

  it "should use explicit class if given" do
    @c1.many_through_many :albums_tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :class=>Tag
    n = @c1.load(:id => 1234)
    n.albums_tags_dataset.sql.should == 'SELECT tags.* FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id = 1234))'
    n.albums_tags.should == [@c2.load(:id=>1)]
  end

  it "should accept :left_primary_key and :right_primary_key option for primary keys to use in current and associated table" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :right_primary_key=>:tag_id, :left_primary_key=>:yyy
    n = @c1.load(:id => 1234)
    n.yyy = 85
    n.tags_dataset.sql.should == 'SELECT tags.* FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.tag_id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id = 85))'
    n.tags.should == [@c2.load(:id=>1)]
  end
  
  it "should handle composite keys" do
    @c1.many_through_many :tags, [[:albums_artists, [:b1, :b2], [:c1, :c2]], [:albums, [:d1, :d2], [:e1, :e2]], [:albums_tags, [:f1, :f2], [:g1, :g2]]], :right_primary_key=>[:h1, :h2], :left_primary_key=>[:id, :yyy]
    n = @c1.load(:id => 1234)
    n.yyy = 85
    n.tags_dataset.sql.should == 'SELECT tags.* FROM tags INNER JOIN albums_tags ON ((albums_tags.g1 = tags.h1) AND (albums_tags.g2 = tags.h2)) INNER JOIN albums ON ((albums.e1 = albums_tags.f1) AND (albums.e2 = albums_tags.f2)) INNER JOIN albums_artists ON ((albums_artists.c1 = albums.d1) AND (albums_artists.c2 = albums.d2) AND (albums_artists.b1 = 1234) AND (albums_artists.b2 = 85))'
    n.tags.should == [@c2.load(:id=>1)]
  end
  
  it "should allowing filtering by many_through_many associations" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]]
    @c1.filter(:tags=>@c2.load(:id=>1234)).sql.should == 'SELECT * FROM artists WHERE (artists.id IN (SELECT albums_artists.artist_id FROM albums_artists INNER JOIN albums ON (albums.id = albums_artists.album_id) INNER JOIN albums_tags ON (albums_tags.album_id = albums.id) WHERE ((albums_tags.tag_id = 1234) AND (albums_artists.artist_id IS NOT NULL))))'
  end

  it "should allowing filtering by many_through_many associations with a single through table" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id]]
    @c1.filter(:tags=>@c2.load(:id=>1234)).sql.should == 'SELECT * FROM artists WHERE (artists.id IN (SELECT albums_artists.artist_id FROM albums_artists WHERE ((albums_artists.album_id = 1234) AND (albums_artists.artist_id IS NOT NULL))))'
  end

  it "should allowing filtering by many_through_many associations with aliased tables" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums_artists, :id, :id], [:albums_artists, :album_id, :tag_id]]
    @c1.filter(:tags=>@c2.load(:id=>1234)).sql.should == 'SELECT * FROM artists WHERE (artists.id IN (SELECT albums_artists.artist_id FROM albums_artists INNER JOIN albums_artists AS albums_artists_0 ON (albums_artists_0.id = albums_artists.album_id) INNER JOIN albums_artists AS albums_artists_1 ON (albums_artists_1.album_id = albums_artists_0.id) WHERE ((albums_artists_1.tag_id = 1234) AND (albums_artists.artist_id IS NOT NULL))))'
  end

  it "should allowing filtering by many_through_many associations with composite keys" do
    @c1.many_through_many :tags, [[:albums_artists, [:b1, :b2], [:c1, :c2]], [:albums, [:d1, :d2], [:e1, :e2]], [:albums_tags, [:f1, :f2], [:g1, :g2]]], :right_primary_key=>[:h1, :h2], :left_primary_key=>[:id, :yyy]
    @c1.filter(:tags=>@c2.load(:h1=>1234, :h2=>85)).sql.should == 'SELECT * FROM artists WHERE ((artists.id, artists.yyy) IN (SELECT albums_artists.b1, albums_artists.b2 FROM albums_artists INNER JOIN albums ON ((albums.d1 = albums_artists.c1) AND (albums.d2 = albums_artists.c2)) INNER JOIN albums_tags ON ((albums_tags.f1 = albums.e1) AND (albums_tags.f2 = albums.e2)) WHERE ((albums_tags.g1 = 1234) AND (albums_tags.g2 = 85) AND (albums_artists.b1 IS NOT NULL) AND (albums_artists.b2 IS NOT NULL))))'
  end

  it "should allowing excluding by many_through_many associations" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]]
    @c1.exclude(:tags=>@c2.load(:id=>1234)).sql.should == 'SELECT * FROM artists WHERE ((artists.id NOT IN (SELECT albums_artists.artist_id FROM albums_artists INNER JOIN albums ON (albums.id = albums_artists.album_id) INNER JOIN albums_tags ON (albums_tags.album_id = albums.id) WHERE ((albums_tags.tag_id = 1234) AND (albums_artists.artist_id IS NOT NULL)))) OR (artists.id IS NULL))'
  end

  it "should allowing excluding by many_through_many associations with composite keys" do
    @c1.many_through_many :tags, [[:albums_artists, [:b1, :b2], [:c1, :c2]], [:albums, [:d1, :d2], [:e1, :e2]], [:albums_tags, [:f1, :f2], [:g1, :g2]]], :right_primary_key=>[:h1, :h2], :left_primary_key=>[:id, :yyy]
    @c1.exclude(:tags=>@c2.load(:h1=>1234, :h2=>85)).sql.should == 'SELECT * FROM artists WHERE (((artists.id, artists.yyy) NOT IN (SELECT albums_artists.b1, albums_artists.b2 FROM albums_artists INNER JOIN albums ON ((albums.d1 = albums_artists.c1) AND (albums.d2 = albums_artists.c2)) INNER JOIN albums_tags ON ((albums_tags.f1 = albums.e1) AND (albums_tags.f2 = albums.e2)) WHERE ((albums_tags.g1 = 1234) AND (albums_tags.g2 = 85) AND (albums_artists.b1 IS NOT NULL) AND (albums_artists.b2 IS NOT NULL)))) OR (artists.id IS NULL) OR (artists.yyy IS NULL))'
  end

  it "should allowing filtering by multiple many_through_many associations" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]]
    @c1.filter(:tags=>[@c2.load(:id=>1234), @c2.load(:id=>2345)]).sql.should == 'SELECT * FROM artists WHERE (artists.id IN (SELECT albums_artists.artist_id FROM albums_artists INNER JOIN albums ON (albums.id = albums_artists.album_id) INNER JOIN albums_tags ON (albums_tags.album_id = albums.id) WHERE ((albums_tags.tag_id IN (1234, 2345)) AND (albums_artists.artist_id IS NOT NULL))))'
  end

  it "should allowing filtering by multiple many_through_many associations with composite keys" do
    @c1.many_through_many :tags, [[:albums_artists, [:b1, :b2], [:c1, :c2]], [:albums, [:d1, :d2], [:e1, :e2]], [:albums_tags, [:f1, :f2], [:g1, :g2]]], :right_primary_key=>[:h1, :h2], :left_primary_key=>[:id, :yyy]
    @c1.filter(:tags=>[@c2.load(:h1=>1234, :h2=>85), @c2.load(:h1=>2345, :h2=>95)]).sql.should == 'SELECT * FROM artists WHERE ((artists.id, artists.yyy) IN (SELECT albums_artists.b1, albums_artists.b2 FROM albums_artists INNER JOIN albums ON ((albums.d1 = albums_artists.c1) AND (albums.d2 = albums_artists.c2)) INNER JOIN albums_tags ON ((albums_tags.f1 = albums.e1) AND (albums_tags.f2 = albums.e2)) WHERE (((albums_tags.g1, albums_tags.g2) IN ((1234, 85), (2345, 95))) AND (albums_artists.b1 IS NOT NULL) AND (albums_artists.b2 IS NOT NULL))))'
  end

  it "should allowing excluding by multiple many_through_many associations" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]]
    @c1.exclude(:tags=>[@c2.load(:id=>1234), @c2.load(:id=>2345)]).sql.should == 'SELECT * FROM artists WHERE ((artists.id NOT IN (SELECT albums_artists.artist_id FROM albums_artists INNER JOIN albums ON (albums.id = albums_artists.album_id) INNER JOIN albums_tags ON (albums_tags.album_id = albums.id) WHERE ((albums_tags.tag_id IN (1234, 2345)) AND (albums_artists.artist_id IS NOT NULL)))) OR (artists.id IS NULL))'
  end

  it "should allowing excluding by multiple many_through_many associations with composite keys" do
    @c1.many_through_many :tags, [[:albums_artists, [:b1, :b2], [:c1, :c2]], [:albums, [:d1, :d2], [:e1, :e2]], [:albums_tags, [:f1, :f2], [:g1, :g2]]], :right_primary_key=>[:h1, :h2], :left_primary_key=>[:id, :yyy]
    @c1.exclude(:tags=>[@c2.load(:h1=>1234, :h2=>85), @c2.load(:h1=>2345, :h2=>95)]).sql.should == 'SELECT * FROM artists WHERE (((artists.id, artists.yyy) NOT IN (SELECT albums_artists.b1, albums_artists.b2 FROM albums_artists INNER JOIN albums ON ((albums.d1 = albums_artists.c1) AND (albums.d2 = albums_artists.c2)) INNER JOIN albums_tags ON ((albums_tags.f1 = albums.e1) AND (albums_tags.f2 = albums.e2)) WHERE (((albums_tags.g1, albums_tags.g2) IN ((1234, 85), (2345, 95))) AND (albums_artists.b1 IS NOT NULL) AND (albums_artists.b2 IS NOT NULL)))) OR (artists.id IS NULL) OR (artists.yyy IS NULL))'
  end

  it "should allowing filtering/excluding many_through_many associations with NULL values" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]]
    @c1.filter(:tags=>@c2.new).sql.should == 'SELECT * FROM artists WHERE \'f\''
    @c1.exclude(:tags=>@c2.new).sql.should == 'SELECT * FROM artists WHERE \'t\''
  end

  it "should allowing filtering by many_through_many association datasets" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]]
    @c1.filter(:tags=>@c2.filter(:x=>1)).sql.should == 'SELECT * FROM artists WHERE (artists.id IN (SELECT albums_artists.artist_id FROM albums_artists INNER JOIN albums ON (albums.id = albums_artists.album_id) INNER JOIN albums_tags ON (albums_tags.album_id = albums.id) WHERE ((albums_tags.tag_id IN (SELECT tags.id FROM tags WHERE ((x = 1) AND (tags.id IS NOT NULL)))) AND (albums_artists.artist_id IS NOT NULL))))'
  end

  it "should allowing filtering by many_through_many association datasets with composite keys" do
    @c1.many_through_many :tags, [[:albums_artists, [:b1, :b2], [:c1, :c2]], [:albums, [:d1, :d2], [:e1, :e2]], [:albums_tags, [:f1, :f2], [:g1, :g2]]], :right_primary_key=>[:h1, :h2], :left_primary_key=>[:id, :yyy]
    @c1.filter(:tags=>@c2.filter(:x=>1)).sql.should == 'SELECT * FROM artists WHERE ((artists.id, artists.yyy) IN (SELECT albums_artists.b1, albums_artists.b2 FROM albums_artists INNER JOIN albums ON ((albums.d1 = albums_artists.c1) AND (albums.d2 = albums_artists.c2)) INNER JOIN albums_tags ON ((albums_tags.f1 = albums.e1) AND (albums_tags.f2 = albums.e2)) WHERE (((albums_tags.g1, albums_tags.g2) IN (SELECT tags.h1, tags.h2 FROM tags WHERE ((x = 1) AND (tags.h1 IS NOT NULL) AND (tags.h2 IS NOT NULL)))) AND (albums_artists.b1 IS NOT NULL) AND (albums_artists.b2 IS NOT NULL))))'
  end

  it "should allowing excluding by many_through_many association datasets" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]]
    @c1.exclude(:tags=>@c2.filter(:x=>1)).sql.should == 'SELECT * FROM artists WHERE ((artists.id NOT IN (SELECT albums_artists.artist_id FROM albums_artists INNER JOIN albums ON (albums.id = albums_artists.album_id) INNER JOIN albums_tags ON (albums_tags.album_id = albums.id) WHERE ((albums_tags.tag_id IN (SELECT tags.id FROM tags WHERE ((x = 1) AND (tags.id IS NOT NULL)))) AND (albums_artists.artist_id IS NOT NULL)))) OR (artists.id IS NULL))'
  end

  it "should allowing excluding by many_through_many association datasets with composite keys" do
    @c1.many_through_many :tags, [[:albums_artists, [:b1, :b2], [:c1, :c2]], [:albums, [:d1, :d2], [:e1, :e2]], [:albums_tags, [:f1, :f2], [:g1, :g2]]], :right_primary_key=>[:h1, :h2], :left_primary_key=>[:id, :yyy]
    @c1.exclude(:tags=>@c2.filter(:x=>1)).sql.should == 'SELECT * FROM artists WHERE (((artists.id, artists.yyy) NOT IN (SELECT albums_artists.b1, albums_artists.b2 FROM albums_artists INNER JOIN albums ON ((albums.d1 = albums_artists.c1) AND (albums.d2 = albums_artists.c2)) INNER JOIN albums_tags ON ((albums_tags.f1 = albums.e1) AND (albums_tags.f2 = albums.e2)) WHERE (((albums_tags.g1, albums_tags.g2) IN (SELECT tags.h1, tags.h2 FROM tags WHERE ((x = 1) AND (tags.h1 IS NOT NULL) AND (tags.h2 IS NOT NULL)))) AND (albums_artists.b1 IS NOT NULL) AND (albums_artists.b2 IS NOT NULL)))) OR (artists.id IS NULL) OR (artists.yyy IS NULL))'
  end

  it "should support a :conditions option" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :conditions=>{:a=>32}
    n = @c1.load(:id => 1234)
    n.tags_dataset.sql.should == 'SELECT tags.* FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id = 1234)) WHERE (a = 32)'
    n.tags.should == [@c2.load(:id=>1)]

    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :conditions=>['a = ?', 42]
    n = @c1.load(:id => 1234)
    n.tags_dataset.sql.should == 'SELECT tags.* FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id = 1234)) WHERE (a = 42)'
    n.tags.should == [@c2.load(:id=>1)]
  end
  
  it "should support an :order option" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :order=>:blah
    n = @c1.load(:id => 1234)
    n.tags_dataset.sql.should == 'SELECT tags.* FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id = 1234)) ORDER BY blah'
    n.tags.should == [@c2.load(:id=>1)]
  end
  
  it "should support an array for the :order option" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :order=>[:blah1, :blah2]
    n = @c1.load(:id => 1234)
    n.tags_dataset.sql.should == 'SELECT tags.* FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id = 1234)) ORDER BY blah1, blah2'
    n.tags.should == [@c2.load(:id=>1)]
  end

  it "should support a select option" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :select=>:blah
    n = @c1.load(:id => 1234)
    n.tags_dataset.sql.should == 'SELECT blah FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id = 1234))'
    n.tags.should == [@c2.load(:id=>1)]
  end
  
  it "should support an array for the select option" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :select=>[Sequel::SQL::ColumnAll.new(:tags), :albums__name]
    n = @c1.load(:id => 1234)
    n.tags_dataset.sql.should == 'SELECT tags.*, albums.name FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id = 1234))'
    n.tags.should == [@c2.load(:id=>1)]
  end
  
  it "should accept a block" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]] do |ds| ds.filter(:yyy=>@yyy) end
    n = @c1.load(:id => 1234)
    n.yyy = 85
    n.tags_dataset.sql.should == 'SELECT tags.* FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id = 1234)) WHERE (yyy = 85)'
    n.tags.should == [@c2.load(:id=>1)]
  end

  it "should allow the :order option while accepting a block" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :order=>:blah do |ds| ds.filter(:yyy=>@yyy) end
    n = @c1.load(:id => 1234)
    n.yyy = 85
    n.tags_dataset.sql.should == 'SELECT tags.* FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id = 1234)) WHERE (yyy = 85) ORDER BY blah'
    n.tags.should == [@c2.load(:id=>1)]
  end

  it "should support a :dataset option that is used instead of the default" do
    @c1.many_through_many :tags, [[:a, :b, :c]], :dataset=>proc{Tag.join(:albums_tags, [:tag_id]).join(:albums, [:album_id]).join(:albums_artists, [:album_id]).filter(:albums_artists__artist_id=>id)}
    n = @c1.load(:id => 1234)
    n.tags_dataset.sql.should == 'SELECT tags.* FROM tags INNER JOIN albums_tags USING (tag_id) INNER JOIN albums USING (album_id) INNER JOIN albums_artists USING (album_id) WHERE (albums_artists.artist_id = 1234)'
    n.tags.should == [@c2.load(:id=>1)]
  end

  it "should support a :limit option" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :limit=>10
    n = @c1.load(:id => 1234)
    n.tags_dataset.sql.should == 'SELECT tags.* FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id = 1234)) LIMIT 10'
    n.tags.should == [@c2.load(:id=>1)]

    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :limit=>[10, 10]
    n = @c1.load(:id => 1234)
    n.tags_dataset.sql.should == 'SELECT tags.* FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id = 1234)) LIMIT 10 OFFSET 10'
    n.tags.should == [@c2.load(:id=>1)]
  end

  it "should have the :eager option affect the _dataset method" do
    @c2.many_to_many :fans
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :eager=>:fans
    @c1.load(:id => 1234).tags_dataset.opts[:eager].should == {:fans=>nil}
  end
  
  it "should provide an array with all members of the association" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]]
    @c1.load(:id => 1234).tags.should == [@c2.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT tags.* FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id = 1234))']
  end

  it "should set cached instance variable when accessed" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]]
    n = @c1.load(:id => 1234)
    n.associations[:tags].should == nil
    MODEL_DB.sqls.should == []
    n.tags.should == [@c2.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT tags.* FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id = 1234))']
    n.associations[:tags].should == n.tags
    MODEL_DB.sqls.length.should == 0
  end

  it "should use cached instance variable if available" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]]
    n = @c1.load(:id => 1234)
    n.associations[:tags] = []
    n.tags.should == []
    MODEL_DB.sqls.should == []
  end

  it "should not use cached instance variable if asked to reload" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]]
    n = @c1.load(:id => 1234)
    n.associations[:tags] = []
    MODEL_DB.sqls.should == []
    n.tags(true).should == [@c2.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT tags.* FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id = 1234))']
    n.associations[:tags].should == n.tags
    MODEL_DB.sqls.length.should == 0
  end

  it "should not add associations methods directly to class" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]]
    im = @c1.instance_methods.collect{|x| x.to_s}
    im.should(include('tags'))
    im.should(include('tags_dataset'))
    im2 = @c1.instance_methods(false).collect{|x| x.to_s}
    im2.should_not(include('tags'))
    im2.should_not(include('tags_dataset'))
  end

  it "should support after_load association callback" do
    h = []
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :after_load=>:al
    @c1.class_eval do
      self::Foo = h
      def al(v)
        v.each{|x| model::Foo << x.pk * 20}
      end
    end
    @c2.dataset._fetch = [{:id=>20}, {:id=>30}]
    p = @c1.load(:id=>10, :parent_id=>20)
    p.tags
    h.should == [400, 600]
    p.tags.collect{|a| a.pk}.should == [20, 30]
  end

  it "should support a :uniq option that removes duplicates from the association" do
    h = []
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :uniq=>true
    @c2.dataset._fetch = [{:id=>20}, {:id=>30}, {:id=>20}, {:id=>30}]
    @c1.load(:id=>10).tags.should == [@c2.load(:id=>20), @c2.load(:id=>30)]
  end
end

describe 'Sequel::Plugins::ManyThroughMany::ManyThroughManyAssociationReflection' do
  before do
    class ::Artist < Sequel::Model
      plugin :many_through_many
      many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]]
    end
    class ::Tag < Sequel::Model
    end
    MODEL_DB.reset
    @ar = Artist.association_reflection(:tags)
  end
  after do
    Object.send(:remove_const, :Artist)
    Object.send(:remove_const, :Tag)
  end
  
  it "#edges should be an array of joins to make when eager graphing" do
    @ar.edges.should == [{:conditions=>[], :left=>:id, :right=>:artist_id, :table=>:albums_artists, :join_type=>:left_outer, :block=>nil}, {:conditions=>[], :left=>:album_id, :right=>:id, :table=>:albums, :join_type=>:left_outer, :block=>nil}, {:conditions=>[], :left=>:id, :right=>:album_id, :table=>:albums_tags, :join_type=>:left_outer, :block=>nil}]
  end
  
  it "#edges should handle composite keys" do
    Artist.many_through_many :tags, [[:albums_artists, [:b1, :b2], [:c1, :c2]], [:albums, [:d1, :d2], [:e1, :e2]], [:albums_tags, [:f1, :f2], [:g1, :g2]]], :right_primary_key=>[:h1, :h2], :left_primary_key=>[:id, :yyy]
    Artist.association_reflection(:tags).edges.should == [{:conditions=>[], :left=>[:id, :yyy], :right=>[:b1, :b2], :table=>:albums_artists, :join_type=>:left_outer, :block=>nil}, {:conditions=>[], :left=>[:c1, :c2], :right=>[:d1, :d2], :table=>:albums, :join_type=>:left_outer, :block=>nil}, {:conditions=>[], :left=>[:e1, :e2], :right=>[:f1, :f2], :table=>:albums_tags, :join_type=>:left_outer, :block=>nil}]
  end
  
  it "#reverse_edges should be an array of joins to make when lazy loading or eager loading" do
    @ar.reverse_edges.should == [{:alias=>:albums_tags, :left=>:tag_id, :right=>:id, :table=>:albums_tags}, {:alias=>:albums, :left=>:id, :right=>:album_id, :table=>:albums}]
  end
  
  it "#reverse_edges should handle composite keys" do
    Artist.many_through_many :tags, [[:albums_artists, [:b1, :b2], [:c1, :c2]], [:albums, [:d1, :d2], [:e1, :e2]], [:albums_tags, [:f1, :f2], [:g1, :g2]]], :right_primary_key=>[:h1, :h2], :left_primary_key=>[:id, :yyy]
    Artist.association_reflection(:tags).reverse_edges.should == [{:alias=>:albums_tags, :left=>[:g1, :g2], :right=>[:h1, :h2], :table=>:albums_tags}, {:alias=>:albums, :left=>[:e1, :e2], :right=>[:f1, :f2], :table=>:albums}]
  end
  
  it "#reciprocal should be nil" do
    @ar.reciprocal.should == nil
  end
end

describe "Sequel::Plugins::ManyThroughMany eager loading methods" do
  before do
    class ::Artist < Sequel::Model
      plugin :many_through_many
      many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]]
      many_through_many :other_tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :class=>:Tag
      many_through_many :albums, [[:albums_artists, :artist_id, :album_id]]
      many_through_many :artists, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_artists, :album_id, :artist_id]]
    end
    class ::Tag < Sequel::Model
      plugin :many_through_many
      many_through_many :tracks, [[:albums_tags, :tag_id, :album_id], [:albums, :id, :id]], :right_primary_key=>:album_id
    end
    class ::Album < Sequel::Model
    end
    class ::Track < Sequel::Model
    end
    Artist.dataset.columns(:id)._fetch = proc do |sql|
      h = {:id => 1}
      if sql =~ /FROM artists LEFT OUTER JOIN albums_artists/
        h[:tags_id] = 2
        h[:albums_0_id] = 3 if sql =~ /LEFT OUTER JOIN albums AS albums_0/
        h[:tracks_id] = 4 if sql =~ /LEFT OUTER JOIN tracks/
        h[:other_tags_id] = 9 if sql =~ /other_tags\.id AS other_tags_id/
        h[:artists_0_id] = 10 if sql =~ /artists_0\.id AS artists_0_id/
      end
      h
    end
    
    Tag.dataset._fetch = proc do |sql|
      h = {:id => 2}
      if sql =~ /albums_artists.artist_id IN \(([18])\)/
        h[:x_foreign_key_x] = $1.to_i 
      elsif sql =~ /\(\(albums_artists.b1, albums_artists.b2\) IN \(\(1, 8\)\)\)/
        h.merge!(:x_foreign_key_0_x=>1, :x_foreign_key_1_x=>8)
      end
      h[:tag_id] = h.delete(:id) if sql =~ /albums_artists.artist_id IN \(8\)/
      h
    end
    
    Album.dataset._fetch = proc do |sql|
      h = {:id => 3}
      h[:x_foreign_key_x] = 1 if sql =~ /albums_artists.artist_id IN \(1\)/
      h
    end
    
    Track.dataset._fetch = proc do |sql|
      h = {:id => 4}
      h[:x_foreign_key_x] = 2 if sql =~ /albums_tags.tag_id IN \(2\)/
      h
    end

    @c1 = Artist
    MODEL_DB.reset
  end
  after do
    [:Artist, :Tag, :Album, :Track].each{|x| Object.send(:remove_const, x)}
  end
  
  it "should eagerly load a single many_through_many association" do
    a = @c1.eager(:tags).all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT * FROM artists', 'SELECT tags.*, albums_artists.artist_id AS x_foreign_key_x FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id IN (1)))']
    a.first.tags.should == [Tag.load(:id=>2)]
    MODEL_DB.sqls.length.should == 0
  end
  
  it "should eagerly load multiple associations in a single call" do
    a = @c1.eager(:tags, :albums).all
    a.should == [@c1.load(:id=>1)]
    sqls = MODEL_DB.sqls
    sqls.length.should == 3
    sqls[0].should == 'SELECT * FROM artists'
    sqls[1..-1].should(include('SELECT tags.*, albums_artists.artist_id AS x_foreign_key_x FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id IN (1)))'))
    sqls[1..-1].should(include('SELECT albums.*, albums_artists.artist_id AS x_foreign_key_x FROM albums INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id IN (1)))'))
    a = a.first
    a.tags.should == [Tag.load(:id=>2)]
    a.albums.should == [Album.load(:id=>3)]
    MODEL_DB.sqls.length.should == 0
  end
  
  it "should eagerly load multiple associations in separate" do
    a = @c1.eager(:tags).eager(:albums).all
    a.should == [@c1.load(:id=>1)]
    sqls = MODEL_DB.sqls
    sqls.length.should == 3
    sqls[0].should == 'SELECT * FROM artists'
    sqls[1..-1].should(include('SELECT tags.*, albums_artists.artist_id AS x_foreign_key_x FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id IN (1)))'))
    sqls[1..-1].should(include('SELECT albums.*, albums_artists.artist_id AS x_foreign_key_x FROM albums INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id IN (1)))'))
    a = a.first
    a.tags.should == [Tag.load(:id=>2)]
    a.albums.should == [Album.load(:id=>3)]
    MODEL_DB.sqls.length.should == 0
  end
  
  it "should allow cascading of eager loading for associations of associated models" do
    a = @c1.eager(:tags=>:tracks).all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT * FROM artists',
      'SELECT tags.*, albums_artists.artist_id AS x_foreign_key_x FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id IN (1)))',
      'SELECT tracks.*, albums_tags.tag_id AS x_foreign_key_x FROM tracks INNER JOIN albums ON (albums.id = tracks.album_id) INNER JOIN albums_tags ON ((albums_tags.album_id = albums.id) AND (albums_tags.tag_id IN (2)))']
    a = a.first
    a.tags.should == [Tag.load(:id=>2)]
    a.tags.first.tracks.should == [Track.load(:id=>4)]
    MODEL_DB.sqls.length.should == 0
  end
  
  it "should cascade eagerly loading when the :eager association option is used" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :eager=>:tracks
    a = @c1.eager(:tags).all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT * FROM artists',
      'SELECT tags.*, albums_artists.artist_id AS x_foreign_key_x FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id IN (1)))',
      'SELECT tracks.*, albums_tags.tag_id AS x_foreign_key_x FROM tracks INNER JOIN albums ON (albums.id = tracks.album_id) INNER JOIN albums_tags ON ((albums_tags.album_id = albums.id) AND (albums_tags.tag_id IN (2)))']
    a = a.first
    a.tags.should == [Tag.load(:id=>2)]
    a.tags.first.tracks.should == [Track.load(:id=>4)]
    MODEL_DB.sqls.length.should == 0
  end
  
  it "should respect :eager when lazily loading an association" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :eager=>:tracks
    a = @c1.load(:id=>1)
    a.tags.should == [Tag.load(:id=>2)]
    MODEL_DB.sqls.should == ['SELECT tags.* FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id = 1))',
      'SELECT tracks.*, albums_tags.tag_id AS x_foreign_key_x FROM tracks INNER JOIN albums ON (albums.id = tracks.album_id) INNER JOIN albums_tags ON ((albums_tags.album_id = albums.id) AND (albums_tags.tag_id IN (2)))']
    a.tags.first.tracks.should == [Track.load(:id=>4)]
    MODEL_DB.sqls.length.should == 0
  end
  
  it "should cascade eagerly loading when the :eager_graph association option is used" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :eager_graph=>:tracks
    proc{@c1.eager(:tags).all}.should raise_error(Sequel::Error)
  end
  
  it "should respect :eager_graph when lazily loading an association" do
    Tag.dataset._fetch = {:id=>2, :tracks_id=>4}
    Tag.dataset.extend(Module.new {
      def columns
        [:id]
      end
    })
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :eager_graph=>:tracks
    a = @c1.load(:id=>1)
    a.tags
    MODEL_DB.sqls.should == [ 'SELECT tags.id, tracks.id AS tracks_id FROM (SELECT tags.* FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id = 1))) AS tags LEFT OUTER JOIN albums_tags AS albums_tags_0 ON (albums_tags_0.tag_id = tags.id) LEFT OUTER JOIN albums ON (albums.id = albums_tags_0.album_id) LEFT OUTER JOIN tracks ON (tracks.album_id = albums.id)']
    a.tags.should == [Tag.load(:id=>2)]
    a.tags.first.tracks.should == [Track.load(:id=>4)]
    MODEL_DB.sqls.length.should == 0
  end
  
  it "should respect :conditions when eagerly loading" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :conditions=>{:a=>32}
    a = @c1.eager(:tags).all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT * FROM artists',
      'SELECT tags.*, albums_artists.artist_id AS x_foreign_key_x FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id IN (1))) WHERE (a = 32)']
    a.first.tags.should == [Tag.load(:id=>2)]
    MODEL_DB.sqls.length.should == 0
  end
  
  it "should respect :order when eagerly loading" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :order=>:blah
    a = @c1.eager(:tags).all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT * FROM artists',
      'SELECT tags.*, albums_artists.artist_id AS x_foreign_key_x FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id IN (1))) ORDER BY blah']
    a.first.tags.should == [Tag.load(:id=>2)]
    MODEL_DB.sqls.length.should == 0
  end
  
  it "should use the association's block when eager loading by default" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]] do |ds| ds.filter(:a) end
    a = @c1.eager(:tags).all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT * FROM artists',
      'SELECT tags.*, albums_artists.artist_id AS x_foreign_key_x FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id IN (1))) WHERE a']
    a.first.tags.should == [Tag.load(:id=>2)]
    MODEL_DB.sqls.length.should == 0
  end

  it "should use the :eager_block option when eager loading if given" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :eager_block=>proc{|ds| ds.filter(:b)} do |ds| ds.filter(:a) end
    a = @c1.eager(:tags).all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT * FROM artists',
      'SELECT tags.*, albums_artists.artist_id AS x_foreign_key_x FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id IN (1))) WHERE b']
    a.first.tags.should == [Tag.load(:id=>2)]
    MODEL_DB.sqls.length.should == 0
  end

  it "should respect the :limit option on a many_through_many association" do
    @c1.many_through_many :first_two_tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :class=>Tag, :limit=>2
    Tag.dataset._fetch = [{:x_foreign_key_x=>1, :id=>5},{:x_foreign_key_x=>1, :id=>6}, {:x_foreign_key_x=>1, :id=>7}]
    a = @c1.eager(:first_two_tags).all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT * FROM artists',
      'SELECT tags.*, albums_artists.artist_id AS x_foreign_key_x FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id IN (1)))']
    a.first.first_two_tags.should == [Tag.load(:id=>5), Tag.load(:id=>6)]
    MODEL_DB.sqls.length.should == 0

    @c1.many_through_many :first_two_tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :class=>Tag, :limit=>[2,1]
    a = @c1.eager(:first_two_tags).all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT * FROM artists',
      'SELECT tags.*, albums_artists.artist_id AS x_foreign_key_x FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id IN (1)))']
    a.first.first_two_tags.should == [Tag.load(:id=>6), Tag.load(:id=>7)]
    MODEL_DB.sqls.length.should == 0
  end

  it "should respect the :limit option on a many_through_many association using a :window_function strategy" do
    Tag.dataset.meta_def(:supports_window_functions?){true}
    @c1.many_through_many :first_two_tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :class=>Tag, :limit=>2, :eager_limit_strategy=>true, :order=>:name
    Tag.dataset._fetch = [{:x_foreign_key_x=>1, :id=>5},{:x_foreign_key_x=>1, :id=>6}]
    a = @c1.eager(:first_two_tags).all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT * FROM artists',
      'SELECT * FROM (SELECT tags.*, albums_artists.artist_id AS x_foreign_key_x, row_number() OVER (PARTITION BY albums_artists.artist_id ORDER BY name) AS x_sequel_row_number_x FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id IN (1)))) AS t1 WHERE (x_sequel_row_number_x <= 2)']
    a.first.first_two_tags.should == [Tag.load(:id=>5), Tag.load(:id=>6)]
    MODEL_DB.sqls.length.should == 0

    @c1.many_through_many :first_two_tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :class=>Tag, :limit=>[2,1], :eager_limit_strategy=>true, :order=>:name
    a = @c1.eager(:first_two_tags).all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT * FROM artists',
      'SELECT * FROM (SELECT tags.*, albums_artists.artist_id AS x_foreign_key_x, row_number() OVER (PARTITION BY albums_artists.artist_id ORDER BY name) AS x_sequel_row_number_x FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id IN (1)))) AS t1 WHERE ((x_sequel_row_number_x >= 2) AND (x_sequel_row_number_x < 4))']
    a.first.first_two_tags.should == [Tag.load(:id=>5), Tag.load(:id=>6)]
    MODEL_DB.sqls.length.should == 0
  end

  it "should respect the :limit option on a many_through_many association with composite primary keys on the main table using a :window_function strategy" do
    Tag.dataset.meta_def(:supports_window_functions?){true}
    @c1.set_primary_key([:id1, :id2])
    @c1.columns :id1, :id2
    @c1.many_through_many :first_two_tags, [[:albums_artists, [:artist_id1, :artist_id2], :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :class=>Tag, :limit=>2, :eager_limit_strategy=>true, :order=>:name
    @c1.dataset._fetch = [{:id1=>1, :id2=>2}]
    Tag.dataset._fetch = [{:x_foreign_key_0_x=>1, :x_foreign_key_1_x=>2, :id=>5}, {:x_foreign_key_0_x=>1, :x_foreign_key_1_x=>2, :id=>6}]
    a = @c1.eager(:first_two_tags).all
    a.should == [@c1.load(:id1=>1, :id2=>2)]
    MODEL_DB.sqls.should == ['SELECT * FROM artists',
      'SELECT * FROM (SELECT tags.*, albums_artists.artist_id1 AS x_foreign_key_0_x, albums_artists.artist_id2 AS x_foreign_key_1_x, row_number() OVER (PARTITION BY albums_artists.artist_id1, albums_artists.artist_id2 ORDER BY name) AS x_sequel_row_number_x FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND ((albums_artists.artist_id1, albums_artists.artist_id2) IN ((1, 2))))) AS t1 WHERE (x_sequel_row_number_x <= 2)']
    a.first.first_two_tags.should == [Tag.load(:id=>5), Tag.load(:id=>6)]
    MODEL_DB.sqls.length.should == 0

    @c1.many_through_many :first_two_tags, [[:albums_artists, [:artist_id1, :artist_id2], :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :class=>Tag, :limit=>[2,1], :eager_limit_strategy=>true, :order=>:name
    a = @c1.eager(:first_two_tags).all
    a.should == [@c1.load(:id1=>1, :id2=>2)]
    MODEL_DB.sqls.should == ['SELECT * FROM artists',
      'SELECT * FROM (SELECT tags.*, albums_artists.artist_id1 AS x_foreign_key_0_x, albums_artists.artist_id2 AS x_foreign_key_1_x, row_number() OVER (PARTITION BY albums_artists.artist_id1, albums_artists.artist_id2 ORDER BY name) AS x_sequel_row_number_x FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND ((albums_artists.artist_id1, albums_artists.artist_id2) IN ((1, 2))))) AS t1 WHERE ((x_sequel_row_number_x >= 2) AND (x_sequel_row_number_x < 4))']
    a.first.first_two_tags.should == [Tag.load(:id=>5), Tag.load(:id=>6)]
    MODEL_DB.sqls.length.should == 0
  end

  it "should respect the :limit option on a many_through_many association using a :correlated_subquery strategy" do
    @c1.many_through_many :first_two_tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :class=>Tag, :limit=>2, :eager_limit_strategy=>:correlated_subquery, :order=>:name
    Tag.dataset._fetch = [{:x_foreign_key_x=>1, :id=>5},{:x_foreign_key_x=>1, :id=>6}]
    a = @c1.eager(:first_two_tags).all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT * FROM artists',
      'SELECT tags.*, albums_artists.artist_id AS x_foreign_key_x FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id IN (1))) WHERE (tags.id IN (SELECT t1.id FROM tags AS t1 INNER JOIN albums_tags ON (albums_tags.tag_id = t1.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists AS t2 ON ((t2.album_id = albums.id) AND (t2.artist_id = albums_artists.artist_id)) ORDER BY name LIMIT 2)) ORDER BY name']
    a.first.first_two_tags.should == [Tag.load(:id=>5), Tag.load(:id=>6)]
    MODEL_DB.sqls.length.should == 0

    @c1.many_through_many :first_two_tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :class=>Tag, :limit=>[2,1], :eager_limit_strategy=>:correlated_subquery, :order=>:name
    a = @c1.eager(:first_two_tags).all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT * FROM artists',
      'SELECT tags.*, albums_artists.artist_id AS x_foreign_key_x FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id IN (1))) WHERE (tags.id IN (SELECT t1.id FROM tags AS t1 INNER JOIN albums_tags ON (albums_tags.tag_id = t1.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists AS t2 ON ((t2.album_id = albums.id) AND (t2.artist_id = albums_artists.artist_id)) ORDER BY name LIMIT 2 OFFSET 1)) ORDER BY name']
    a.first.first_two_tags.should == [Tag.load(:id=>5), Tag.load(:id=>6)]
    MODEL_DB.sqls.length.should == 0
  end

  it "should raise an error when attempting to eagerly load an association with the :allow_eager option set to false" do
    proc{@c1.eager(:tags).all}.should_not raise_error
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :allow_eager=>false
    proc{@c1.eager(:tags).all}.should raise_error(Sequel::Error)
  end

  it "should respect the association's :select option" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :select=>:tags__name
    a = @c1.eager(:tags).all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT * FROM artists',
      'SELECT tags.name, albums_artists.artist_id AS x_foreign_key_x FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id IN (1)))']
    a.first.tags.should == [Tag.load(:id=>2)]
    MODEL_DB.sqls.length.should == 0
  end

  it "should respect many_through_many association's :left_primary_key and :right_primary_key options" do
    @c1.send(:define_method, :yyy){values[:yyy]}
    @c1.dataset._fetch = {:id=>1, :yyy=>8}
    @c1.dataset.meta_def(:columns){[:id, :yyy]}
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :left_primary_key=>:yyy, :right_primary_key=>:tag_id
    a = @c1.eager(:tags).all
    a.should == [@c1.load(:id=>1, :yyy=>8)]
    MODEL_DB.sqls.should == ['SELECT * FROM artists',
      'SELECT tags.*, albums_artists.artist_id AS x_foreign_key_x FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.tag_id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id IN (8)))']
    a.first.tags.should == [Tag.load(:tag_id=>2)]
    MODEL_DB.sqls.length.should == 0
  end
  
  it "should handle composite keys" do
    @c1.send(:define_method, :yyy){values[:yyy]}
    @c1.dataset._fetch = {:id=>1, :yyy=>8}
    @c1.dataset.meta_def(:columns){[:id, :yyy]}
    @c1.many_through_many :tags, [[:albums_artists, [:b1, :b2], [:c1, :c2]], [:albums, [:d1, :d2], [:e1, :e2]], [:albums_tags, [:f1, :f2], [:g1, :g2]]], :right_primary_key=>[:h1, :h2], :left_primary_key=>[:id, :yyy]
    a = @c1.eager(:tags).all
    a.should == [@c1.load(:id=>1, :yyy=>8)]
    MODEL_DB.sqls.should == ['SELECT * FROM artists',
      'SELECT tags.*, albums_artists.b1 AS x_foreign_key_0_x, albums_artists.b2 AS x_foreign_key_1_x FROM tags INNER JOIN albums_tags ON ((albums_tags.g1 = tags.h1) AND (albums_tags.g2 = tags.h2)) INNER JOIN albums ON ((albums.e1 = albums_tags.f1) AND (albums.e2 = albums_tags.f2)) INNER JOIN albums_artists ON ((albums_artists.c1 = albums.d1) AND (albums_artists.c2 = albums.d2) AND ((albums_artists.b1, albums_artists.b2) IN ((1, 8))))']
    a.first.tags.should == [Tag.load(:id=>2)]
    MODEL_DB.sqls.length.should == 0
  end

  it "should respect :after_load callbacks on associations when eager loading" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :after_load=>lambda{|o, as| o[:id] *= 2; as.each{|a| a[:id] *= 3}}
    a = @c1.eager(:tags).all
    a.should == [@c1.load(:id=>2)]
    MODEL_DB.sqls.should == ['SELECT * FROM artists',
      'SELECT tags.*, albums_artists.artist_id AS x_foreign_key_x FROM tags INNER JOIN albums_tags ON (albums_tags.tag_id = tags.id) INNER JOIN albums ON (albums.id = albums_tags.album_id) INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id IN (1)))']
    a.first.tags.should == [Tag.load(:id=>6)]
    MODEL_DB.sqls.length.should == 0
  end
    
  it "should raise an error if called without a symbol or hash" do
    proc{@c1.eager_graph(Object.new)}.should raise_error(Sequel::Error)
  end

  it "should eagerly graph a single many_through_many association" do
    a = @c1.eager_graph(:tags).all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT artists.id, tags.id AS tags_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.id = albums_tags.tag_id)']
    a.first.tags.should == [Tag.load(:id=>2)]
    MODEL_DB.sqls.length.should == 0
  end

  it "should eagerly graph multiple associations in a single call" do 
    a = @c1.eager_graph(:tags, :albums).all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT artists.id, tags.id AS tags_id, albums_0.id AS albums_0_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.id = albums_tags.tag_id) LEFT OUTER JOIN albums_artists AS albums_artists_0 ON (albums_artists_0.artist_id = artists.id) LEFT OUTER JOIN albums AS albums_0 ON (albums_0.id = albums_artists_0.album_id)']
    a = a.first
    a.tags.should == [Tag.load(:id=>2)]
    a.albums.should == [Album.load(:id=>3)]
    MODEL_DB.sqls.length.should == 0
  end

  it "should eagerly graph multiple associations in separate calls" do 
    a = @c1.eager_graph(:tags).eager_graph(:albums).all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT artists.id, tags.id AS tags_id, albums_0.id AS albums_0_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.id = albums_tags.tag_id) LEFT OUTER JOIN albums_artists AS albums_artists_0 ON (albums_artists_0.artist_id = artists.id) LEFT OUTER JOIN albums AS albums_0 ON (albums_0.id = albums_artists_0.album_id)']
    a = a.first
    a.tags.should == [Tag.load(:id=>2)]
    a.albums.should == [Album.load(:id=>3)]
    MODEL_DB.sqls.length.should == 0
  end

  it "should allow cascading of eager graphing for associations of associated models" do
    a = @c1.eager_graph(:tags=>:tracks).all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT artists.id, tags.id AS tags_id, tracks.id AS tracks_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.id = albums_tags.tag_id) LEFT OUTER JOIN albums_tags AS albums_tags_0 ON (albums_tags_0.tag_id = tags.id) LEFT OUTER JOIN albums AS albums_0 ON (albums_0.id = albums_tags_0.album_id) LEFT OUTER JOIN tracks ON (tracks.album_id = albums_0.id)']
    a = a.first
    a.tags.should == [Tag.load(:id=>2)]
    a.tags.first.tracks.should == [Track.load(:id=>4)]
    MODEL_DB.sqls.length.should == 0
  end
  
  it "eager graphing should eliminate duplicates caused by cartesian products" do
    ds = @c1.eager_graph(:tags)
    # Assume artist has 2 albums each with 2 tags
    ds._fetch = [{:id=>1, :tags_id=>2}, {:id=>1, :tags_id=>3}, {:id=>1, :tags_id=>2}, {:id=>1, :tags_id=>3}]
    a = ds.all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT artists.id, tags.id AS tags_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.id = albums_tags.tag_id)']
    a.first.tags.should == [Tag.load(:id=>2), Tag.load(:id=>3)]
    MODEL_DB.sqls.length.should == 0
  end
  
  it "should eager graph multiple associations from the same table" do
    a = @c1.eager_graph(:tags, :other_tags).all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT artists.id, tags.id AS tags_id, other_tags.id AS other_tags_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.id = albums_tags.tag_id) LEFT OUTER JOIN albums_artists AS albums_artists_0 ON (albums_artists_0.artist_id = artists.id) LEFT OUTER JOIN albums AS albums_0 ON (albums_0.id = albums_artists_0.album_id) LEFT OUTER JOIN albums_tags AS albums_tags_0 ON (albums_tags_0.album_id = albums_0.id) LEFT OUTER JOIN tags AS other_tags ON (other_tags.id = albums_tags_0.tag_id)']
    a = a.first
    a.tags.should == [Tag.load(:id=>2)]
    a.other_tags.should == [Tag.load(:id=>9)]
    MODEL_DB.sqls.length.should == 0
  end

  it "should eager graph a self_referential association" do
    a = @c1.eager_graph(:tags, :artists).all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT artists.id, tags.id AS tags_id, artists_0.id AS artists_0_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.id = albums_tags.tag_id) LEFT OUTER JOIN albums_artists AS albums_artists_0 ON (albums_artists_0.artist_id = artists.id) LEFT OUTER JOIN albums AS albums_0 ON (albums_0.id = albums_artists_0.album_id) LEFT OUTER JOIN albums_artists AS albums_artists_1 ON (albums_artists_1.album_id = albums_0.id) LEFT OUTER JOIN artists AS artists_0 ON (artists_0.id = albums_artists_1.artist_id)']
    a = a.first
    a.tags.should == [Tag.load(:id=>2)]
    a.artists.should == [@c1.load(:id=>10)]
    MODEL_DB.sqls.length.should == 0
  end

  it "eager graphing should give you a plain hash when called without .all" do 
    @c1.eager_graph(:tags, :artists).first.should == {:albums_0_id=>3, :artists_0_id=>10, :id=>1, :tags_id=>2}
  end

  it "should be able to use eager and eager_graph together" do
    a = @c1.eager_graph(:tags).eager(:albums).all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT artists.id, tags.id AS tags_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.id = albums_tags.tag_id)',
      'SELECT albums.*, albums_artists.artist_id AS x_foreign_key_x FROM albums INNER JOIN albums_artists ON ((albums_artists.album_id = albums.id) AND (albums_artists.artist_id IN (1)))']
    a = a.first
    a.tags.should == [Tag.load(:id=>2)]
    a.albums.should == [Album.load(:id=>3)]
    MODEL_DB.sqls.length.should == 0
  end

  it "should handle no associated records when eagerly graphing a single many_through_many association" do
    ds = @c1.eager_graph(:tags)
    ds._fetch = {:id=>1, :tags_id=>nil}
    a = ds.all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT artists.id, tags.id AS tags_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.id = albums_tags.tag_id)']
    a.first.tags.should == []
    MODEL_DB.sqls.length.should == 0
  end

  it "should handle no associated records when eagerly graphing multiple many_through_many associations" do
    ds = @c1.eager_graph(:tags, :albums)
    ds._fetch = [{:id=>1, :tags_id=>nil, :albums_0_id=>3}, {:id=>1, :tags_id=>2, :albums_0_id=>nil}, {:id=>1, :tags_id=>5, :albums_0_id=>6}, {:id=>7, :tags_id=>nil, :albums_0_id=>nil}]
    a = ds.all
    a.should == [@c1.load(:id=>1), @c1.load(:id=>7)]
    MODEL_DB.sqls.should == ['SELECT artists.id, tags.id AS tags_id, albums_0.id AS albums_0_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.id = albums_tags.tag_id) LEFT OUTER JOIN albums_artists AS albums_artists_0 ON (albums_artists_0.artist_id = artists.id) LEFT OUTER JOIN albums AS albums_0 ON (albums_0.id = albums_artists_0.album_id)']
    a.first.tags.should == [Tag.load(:id=>2), Tag.load(:id=>5)]
    a.first.albums.should == [Album.load(:id=>3), Album.load(:id=>6)]
    a.last.tags.should == []
    a.last.albums.should == []
    MODEL_DB.sqls.length.should == 0
  end

  it "should handle missing associated records when cascading eager graphing for associations of associated models" do
    ds = @c1.eager_graph(:tags=>:tracks)
    ds._fetch = [{:id=>1, :tags_id=>2, :tracks_id=>4}, {:id=>1, :tags_id=>3, :tracks_id=>nil}, {:id=>2, :tags_id=>nil, :tracks_id=>nil}]
    a = ds.all
    a.should == [@c1.load(:id=>1), @c1.load(:id=>2)]
    MODEL_DB.sqls.should == ['SELECT artists.id, tags.id AS tags_id, tracks.id AS tracks_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.id = albums_tags.tag_id) LEFT OUTER JOIN albums_tags AS albums_tags_0 ON (albums_tags_0.tag_id = tags.id) LEFT OUTER JOIN albums AS albums_0 ON (albums_0.id = albums_tags_0.album_id) LEFT OUTER JOIN tracks ON (tracks.album_id = albums_0.id)']
    a.last.tags.should == []
    a = a.first
    a.tags.should == [Tag.load(:id=>2), Tag.load(:id=>3)]
    a.tags.first.tracks.should == [Track.load(:id=>4)]
    a.tags.last.tracks.should == []
    MODEL_DB.sqls.length.should == 0
  end

  it "eager graphing should respect :left_primary_key and :right_primary_key options" do 
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :left_primary_key=>:yyy, :right_primary_key=>:tag_id
    @c1.dataset.meta_def(:columns){[:id, :yyy]}
    Tag.dataset.meta_def(:columns){[:id, :tag_id]}
    ds = @c1.eager_graph(:tags)
    ds._fetch = {:id=>1, :yyy=>8, :tags_id=>2, :tag_id=>4}
    a = ds.all
    a.should == [@c1.load(:id=>1, :yyy=>8)]
    MODEL_DB.sqls.should == ['SELECT artists.id, artists.yyy, tags.id AS tags_id, tags.tag_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.yyy) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.tag_id = albums_tags.tag_id)']
    a.first.tags.should == [Tag.load(:id=>2, :tag_id=>4)]
    MODEL_DB.sqls.length.should == 0
  end
  
  it "eager graphing should respect composite keys" do 
    @c1.many_through_many :tags, [[:albums_artists, [:b1, :b2], [:c1, :c2]], [:albums, [:d1, :d2], [:e1, :e2]], [:albums_tags, [:f1, :f2], [:g1, :g2]]], :right_primary_key=>[:id, :tag_id], :left_primary_key=>[:id, :yyy]
    @c1.dataset.meta_def(:columns){[:id, :yyy]}
    Tag.dataset.meta_def(:columns){[:id, :tag_id]}
    ds = @c1.eager_graph(:tags)
    ds._fetch = {:id=>1, :yyy=>8, :tags_id=>2, :tag_id=>4}
    a = ds.all
    a.should == [@c1.load(:id=>1, :yyy=>8)]
    MODEL_DB.sqls.should == ['SELECT artists.id, artists.yyy, tags.id AS tags_id, tags.tag_id FROM artists LEFT OUTER JOIN albums_artists ON ((albums_artists.b1 = artists.id) AND (albums_artists.b2 = artists.yyy)) LEFT OUTER JOIN albums ON ((albums.d1 = albums_artists.c1) AND (albums.d2 = albums_artists.c2)) LEFT OUTER JOIN albums_tags ON ((albums_tags.f1 = albums.e1) AND (albums_tags.f2 = albums.e2)) LEFT OUTER JOIN tags ON ((tags.id = albums_tags.g1) AND (tags.tag_id = albums_tags.g2))']
    a.first.tags.should == [Tag.load(:id=>2, :tag_id=>4)]
    MODEL_DB.sqls.length.should == 0
  end

  it "should respect the association's :graph_select option" do 
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :graph_select=>:b
    ds = @c1.eager_graph(:tags)
    ds._fetch = {:id=>1, :b=>2}
    a = ds.all
    a.should == [@c1.load(:id=>1)]
    MODEL_DB.sqls.should == ['SELECT artists.id, tags.b FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.id = albums_tags.tag_id)']
    a.first.tags.should == [Tag.load(:b=>2)]
    MODEL_DB.sqls.length.should == 0
  end

  it "should respect the association's :graph_join_type option" do 
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_tags, :album_id, :tag_id]], :graph_join_type=>:inner
    @c1.eager_graph(:tags).sql.should == 'SELECT artists.id, tags.id AS tags_id FROM artists INNER JOIN albums_artists ON (albums_artists.artist_id = artists.id) INNER JOIN albums ON (albums.id = albums_artists.album_id) INNER JOIN albums_tags ON (albums_tags.album_id = albums.id) INNER JOIN tags ON (tags.id = albums_tags.tag_id)'
  end

  it "should respect the association's :join_type option on through" do 
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], {:table=>:albums, :left=>:id, :right=>:id, :join_type=>:natural}, [:albums_tags, :album_id, :tag_id]], :graph_join_type=>:inner
    @c1.eager_graph(:tags).sql.should == 'SELECT artists.id, tags.id AS tags_id FROM artists INNER JOIN albums_artists ON (albums_artists.artist_id = artists.id) NATURAL JOIN albums ON (albums.id = albums_artists.album_id) INNER JOIN albums_tags ON (albums_tags.album_id = albums.id) INNER JOIN tags ON (tags.id = albums_tags.tag_id)'
  end

  it "should respect the association's :conditions option" do 
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], {:table=>:albums, :left=>:id, :right=>:id}, [:albums_tags, :album_id, :tag_id]], :conditions=>{:a=>32}
    @c1.eager_graph(:tags).sql.should == 'SELECT artists.id, tags.id AS tags_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON ((tags.id = albums_tags.tag_id) AND (tags.a = 32))'
  end

  it "should respect the association's :graph_conditions option" do 
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], {:table=>:albums, :left=>:id, :right=>:id}, [:albums_tags, :album_id, :tag_id]], :graph_conditions=>{:a=>42}
    @c1.eager_graph(:tags).sql.should == 'SELECT artists.id, tags.id AS tags_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON ((tags.id = albums_tags.tag_id) AND (tags.a = 42))'
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], {:table=>:albums, :left=>:id, :right=>:id}, [:albums_tags, :album_id, :tag_id]], :graph_conditions=>{:a=>42}, :conditions=>{:a=>32}
    @c1.eager_graph(:tags).sql.should == 'SELECT artists.id, tags.id AS tags_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON ((tags.id = albums_tags.tag_id) AND (tags.a = 42))'
  end

  it "should respect the association's :conditions option on through" do 
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], {:table=>:albums, :left=>:id, :right=>:id, :conditions=>{:a=>42}}, [:albums_tags, :album_id, :tag_id]]
    @c1.eager_graph(:tags).sql.should == 'SELECT artists.id, tags.id AS tags_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON ((albums.id = albums_artists.album_id) AND (albums.a = 42)) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.id = albums_tags.tag_id)'
  end

  it "should respect the association's :graph_block option" do 
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], {:table=>:albums, :left=>:id, :right=>:id}, [:albums_tags, :album_id, :tag_id]], :graph_block=>proc{|ja,lja,js| {Sequel.qualify(ja, :active)=>true}}
    @c1.eager_graph(:tags).sql.should == 'SELECT artists.id, tags.id AS tags_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON ((tags.id = albums_tags.tag_id) AND (tags.active IS TRUE))'
  end

  it "should respect the association's :block option on through" do 
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], {:table=>:albums, :left=>:id, :right=>:id, :block=>proc{|ja,lja,js| {Sequel.qualify(ja, :active)=>true}}}, [:albums_tags, :album_id, :tag_id]]
    @c1.eager_graph(:tags).sql.should == 'SELECT artists.id, tags.id AS tags_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON ((albums.id = albums_artists.album_id) AND (albums.active IS TRUE)) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.id = albums_tags.tag_id)'
  end

  it "should respect the association's :graph_only_conditions option" do 
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], {:table=>:albums, :left=>:id, :right=>:id}, [:albums_tags, :album_id, :tag_id]], :graph_only_conditions=>{:a=>32}
    @c1.eager_graph(:tags).sql.should == 'SELECT artists.id, tags.id AS tags_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.a = 32)'
  end

  it "should respect the association's :only_conditions option on through" do 
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], {:table=>:albums, :left=>:id, :right=>:id, :only_conditions=>{:a=>42}}, [:albums_tags, :album_id, :tag_id]]
    @c1.eager_graph(:tags).sql.should == 'SELECT artists.id, tags.id AS tags_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.a = 42) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.id = albums_tags.tag_id)'
  end

  it "should create unique table aliases for all associations" do
    @c1.eager_graph(:artists=>{:artists=>:artists}).sql.should == "SELECT artists.id, artists_0.id AS artists_0_id, artists_1.id AS artists_1_id, artists_2.id AS artists_2_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_artists AS albums_artists_0 ON (albums_artists_0.album_id = albums.id) LEFT OUTER JOIN artists AS artists_0 ON (artists_0.id = albums_artists_0.artist_id) LEFT OUTER JOIN albums_artists AS albums_artists_1 ON (albums_artists_1.artist_id = artists_0.id) LEFT OUTER JOIN albums AS albums_0 ON (albums_0.id = albums_artists_1.album_id) LEFT OUTER JOIN albums_artists AS albums_artists_2 ON (albums_artists_2.album_id = albums_0.id) LEFT OUTER JOIN artists AS artists_1 ON (artists_1.id = albums_artists_2.artist_id) LEFT OUTER JOIN albums_artists AS albums_artists_3 ON (albums_artists_3.artist_id = artists_1.id) LEFT OUTER JOIN albums AS albums_1 ON (albums_1.id = albums_artists_3.album_id) LEFT OUTER JOIN albums_artists AS albums_artists_4 ON (albums_artists_4.album_id = albums_1.id) LEFT OUTER JOIN artists AS artists_2 ON (artists_2.id = albums_artists_4.artist_id)"
  end

  it "should respect the association's :order" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], {:table=>:albums, :left=>:id, :right=>:id}, [:albums_tags, :album_id, :tag_id]], :order=>[:blah1, :blah2]
    @c1.order(:artists__blah2, :artists__blah3).eager_graph(:tags).sql.should == 'SELECT artists.id, tags.id AS tags_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.id = albums_tags.tag_id) ORDER BY artists.blah2, artists.blah3, tags.blah1, tags.blah2'
  end

  it "should only qualify unqualified symbols, identifiers, or ordered versions in association's :order" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], {:table=>:albums, :left=>:id, :right=>:id}, [:albums_tags, :album_id, :tag_id]], :order=>[Sequel.identifier(:blah__id), Sequel.identifier(:blah__id).desc, Sequel.desc(:blah__id), :blah__id, :album_id, Sequel.desc(:album_id), 1, Sequel.lit('RANDOM()'), Sequel.qualify(:b, :a)]
    @c1.order(:artists__blah2, :artists__blah3).eager_graph(:tags).sql.should == 'SELECT artists.id, tags.id AS tags_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.id = albums_tags.tag_id) ORDER BY artists.blah2, artists.blah3, tags.blah__id, tags.blah__id DESC, blah.id DESC, blah.id, tags.album_id, tags.album_id DESC, 1, RANDOM(), b.a'
  end

  it "should not respect the association's :order if :order_eager_graph is false" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], {:table=>:albums, :left=>:id, :right=>:id}, [:albums_tags, :album_id, :tag_id]], :order=>[:blah1, :blah2], :order_eager_graph=>false
    @c1.order(:artists__blah2, :artists__blah3).eager_graph(:tags).sql.should == 'SELECT artists.id, tags.id AS tags_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.id = albums_tags.tag_id) ORDER BY artists.blah2, artists.blah3'
  end

  it "should add the associations :order for multiple associations" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], {:table=>:albums, :left=>:id, :right=>:id}, [:albums_tags, :album_id, :tag_id]], :order=>[:blah1, :blah2]
    @c1.many_through_many :albums, [[:albums_artists, :artist_id, :album_id]], :order=>[:blah3, :blah4]
    @c1.eager_graph(:tags, :albums).sql.should == 'SELECT artists.id, tags.id AS tags_id, albums_0.id AS albums_0_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.id = albums_tags.tag_id) LEFT OUTER JOIN albums_artists AS albums_artists_0 ON (albums_artists_0.artist_id = artists.id) LEFT OUTER JOIN albums AS albums_0 ON (albums_0.id = albums_artists_0.album_id) ORDER BY tags.blah1, tags.blah2, albums_0.blah3, albums_0.blah4'
  end

  it "should add the association's :order for cascading associations" do
    @c1.many_through_many :tags, [[:albums_artists, :artist_id, :album_id], {:table=>:albums, :left=>:id, :right=>:id}, [:albums_tags, :album_id, :tag_id]], :order=>[:blah1, :blah2]
    Tag.many_through_many :tracks, [[:albums_tags, :tag_id, :album_id], [:albums, :id, :id]], :right_primary_key=>:album_id, :order=>[:blah3, :blah4]
    @c1.eager_graph(:tags=>:tracks).sql.should == 'SELECT artists.id, tags.id AS tags_id, tracks.id AS tracks_id FROM artists LEFT OUTER JOIN albums_artists ON (albums_artists.artist_id = artists.id) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.id = albums_tags.tag_id) LEFT OUTER JOIN albums_tags AS albums_tags_0 ON (albums_tags_0.tag_id = tags.id) LEFT OUTER JOIN albums AS albums_0 ON (albums_0.id = albums_tags_0.album_id) LEFT OUTER JOIN tracks ON (tracks.album_id = albums_0.id) ORDER BY tags.blah1, tags.blah2, tracks.blah3, tracks.blah4'
  end

  it "should use the correct qualifier when graphing multiple tables with extra conditions" do
    @c1.many_through_many :tags, [{:table=>:albums_artists, :left=>:artist_id, :right=>:album_id, :conditions=>{:a=>:b}}, {:table=>:albums, :left=>:id, :right=>:id}, [:albums_tags, :album_id, :tag_id]]
    @c1.many_through_many :albums, [{:table=>:albums_artists, :left=>:artist_id, :right=>:album_id, :conditions=>{:c=>:d}}]
    @c1.eager_graph(:tags, :albums).sql.should == 'SELECT artists.id, tags.id AS tags_id, albums_0.id AS albums_0_id FROM artists LEFT OUTER JOIN albums_artists ON ((albums_artists.artist_id = artists.id) AND (albums_artists.a = artists.b)) LEFT OUTER JOIN albums ON (albums.id = albums_artists.album_id) LEFT OUTER JOIN albums_tags ON (albums_tags.album_id = albums.id) LEFT OUTER JOIN tags ON (tags.id = albums_tags.tag_id) LEFT OUTER JOIN albums_artists AS albums_artists_0 ON ((albums_artists_0.artist_id = artists.id) AND (albums_artists_0.c = artists.d)) LEFT OUTER JOIN albums AS albums_0 ON (albums_0.id = albums_artists_0.album_id)'
  end
end

describe "many_through_many associations with non-column expression keys" do
  before do
    @db = Sequel.mock(:fetch=>{:id=>1, :object_ids=>[2]})
    @Foo = Class.new(Sequel::Model(@db[:foos]))
    @Foo.columns :id, :object_ids
    @Foo.plugin :many_through_many
    m = Module.new{def obj_id; object_ids[0]; end}
    @Foo.include m

    @Foo.many_through_many :foos, [
      [:f, Sequel.subscript(:l, 0), Sequel.subscript(:r, 0)],
      [:f, Sequel.subscript(:l, 1), Sequel.subscript(:r, 1)]
    ], :class=>@Foo, :left_primary_key=>:obj_id, :left_primary_key_column=>Sequel.subscript(:object_ids, 0), :right_primary_key=>Sequel.subscript(:object_ids, 0), :right_primary_key_method=>:obj_id
    @foo = @Foo.load(:id=>1, :object_ids=>[2])
    @db.sqls
  end

  it "should have working regular association methods" do
    @Foo.first.foos.should == [@foo]
    @db.sqls.should == ["SELECT * FROM foos LIMIT 1", "SELECT foos.* FROM foos INNER JOIN f ON (f.r[1] = foos.object_ids[0]) INNER JOIN f AS f_0 ON ((f_0.r[0] = f.l[1]) AND (f_0.l[0] = 2))"]
  end

  it "should have working eager loading methods" do
    @db.fetch = [[{:id=>1, :object_ids=>[2]}], [{:id=>1, :object_ids=>[2], :x_foreign_key_x=>2}]]
    @Foo.eager(:foos).all.map{|o| [o, o.foos]}.should == [[@foo, [@foo]]]
    @db.sqls.should == ["SELECT * FROM foos", "SELECT foos.*, f_0.l[0] AS x_foreign_key_x FROM foos INNER JOIN f ON (f.r[1] = foos.object_ids[0]) INNER JOIN f AS f_0 ON ((f_0.r[0] = f.l[1]) AND (f_0.l[0] IN (2)))"]
  end

  it "should have working eager graphing methods" do
    @db.fetch = {:id=>1, :object_ids=>[2], :foos_0_id=>1, :foos_0_object_ids=>[2]}
    @Foo.eager_graph(:foos).all.map{|o| [o, o.foos]}.should == [[@foo, [@foo]]]
    @db.sqls.should == ["SELECT foos.id, foos.object_ids, foos_0.id AS foos_0_id, foos_0.object_ids AS foos_0_object_ids FROM foos LEFT OUTER JOIN f ON (f.l[0] = foos.object_ids[0]) LEFT OUTER JOIN f AS f_0 ON (f_0.l[1] = f.r[0]) LEFT OUTER JOIN foos AS foos_0 ON (foos_0.object_ids[0] = f_0.r[1])"]
  end

  it "should have working filter by associations with model instances" do
    @Foo.first(:foos=>@foo).should == @foo
    @db.sqls.should == ["SELECT * FROM foos WHERE (foos.object_ids[0] IN (SELECT f.l[0] FROM f INNER JOIN f AS f_0 ON (f_0.l[1] = f.r[0]) WHERE ((f_0.r[1] = 2) AND (f.l[0] IS NOT NULL)))) LIMIT 1"]
  end

  it "should have working filter by associations with model datasets" do
    @Foo.first(:foos=>@Foo.where(:id=>@foo.id)).should == @foo
    @db.sqls.should == ["SELECT * FROM foos WHERE (foos.object_ids[0] IN (SELECT f.l[0] FROM f INNER JOIN f AS f_0 ON (f_0.l[1] = f.r[0]) WHERE ((f_0.r[1] IN (SELECT foos.object_ids[0] FROM foos WHERE ((id = 1) AND (foos.object_ids[0] IS NOT NULL)))) AND (f.l[0] IS NOT NULL)))) LIMIT 1"]
  end
end
