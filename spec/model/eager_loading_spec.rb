require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model, "#eager" do
  before do
    class ::EagerAlbum < Sequel::Model(:albums)
      columns :id, :band_id
      many_to_one :band, :class=>'EagerBand', :key=>:band_id
      one_to_many :tracks, :class=>'EagerTrack', :key=>:album_id
      many_to_many :genres, :class=>'EagerGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag
      one_to_many :good_tracks, :class=>'EagerTrack', :key=>:album_id do |ds|
        ds.filter(:name=>'Good')
      end
      many_to_one :band_name, :class=>'EagerBand', :key=>:band_id, :select=>[:id, :name]
      one_to_many :track_names, :class=>'EagerTrack', :key=>:album_id, :select=>[:id, :name]
      many_to_many :genre_names, :class=>'EagerGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :select=>[:id]
    end

    class ::EagerBand < Sequel::Model(:bands)
      columns :id, :p_k
      one_to_many :albums, :class=>'EagerAlbum', :key=>:band_id, :eager=>:tracks
      one_to_many :graph_albums, :class=>'EagerAlbum', :key=>:band_id, :eager_graph=>:tracks
      many_to_many :members, :class=>'EagerBandMember', :left_key=>:band_id, :right_key=>:member_id, :join_table=>:bm
      many_to_many :graph_members, :clone=>:members, :eager_graph=>:bands
      one_to_many :good_albums, :class=>'EagerAlbum', :key=>:band_id, :eager_block=>proc{|ds| ds.filter(:name=>'good')} do |ds|
        ds.filter(:name=>'Good')
      end
      one_to_many :self_titled_albums, :class=>'EagerAlbum', :key=>:band_id, :allow_eager=>false do |ds|
        ds.filter(:name=>name)
      end
      one_to_many :albums_by_name, :class=>'EagerAlbum', :key=>:band_id, :order=>:name, :allow_eager=>false
      one_to_many :top_10_albums, :class=>'EagerAlbum', :key=>:band_id, :limit=>10
    end
    
    class ::EagerTrack < Sequel::Model(:tracks)
      columns :id, :album_id
      many_to_one :album, :class=>'EagerAlbum', :key=>:album_id
    end
    
    class ::EagerGenre < Sequel::Model(:genres)
      columns :id, :xxx
      many_to_many :albums, :class=>'EagerAlbum', :left_key=>:genre_id, :right_key=>:album_id, :join_table=>:ag
    end
    
    class ::EagerBandMember < Sequel::Model(:members)
      columns :id
      many_to_many :bands, :class=>'EagerBand', :left_key=>:member_id, :right_key=>:band_id, :join_table=>:bm, :order =>:id
    end
    
    EagerAlbum.dataset.columns(:id, :band_id)
    EagerAlbum.dataset._fetch = proc do |sql|
      h = if sql =~ /101/
        {:id => 101, :band_id=> 101}
      else
        {:id => 1, :band_id=> 2}
      end
      h[:x_foreign_key_x] = 4 if sql =~ /ag\.genre_id/
      h
    end

    EagerBand.dataset._fetch = proc do |sql|
      case sql
      when /id IN (101)/
        # nothing
      when /id > 100/
        [{:id => 101}, {:id => 102}]
      else
        h = {:id => 2}
        h[:x_foreign_key_x] = 5 if sql =~ /bm\.member_id/
        h
      end
    end
    
    EagerTrack.dataset._fetch = {:id => 3, :album_id => 1}
    
    EagerGenre.dataset._fetch = proc do |sql|
      h = {:id => 4}
      h[:x_foreign_key_x] = 1 if sql =~ /ag\.album_id/
      h
    end
    
    EagerBandMember.dataset._fetch = proc do |sql|
      h = {:id => 5}
      h[:x_foreign_key_x] = 2 if sql =~ /bm\.band_id/
      h
    end

    MODEL_DB.reset
  end
  after do
    [:EagerAlbum, :EagerBand, :EagerTrack, :EagerGenre, :EagerBandMember].each{|x| Object.send(:remove_const, x)}
  end
  
  it "should raise an error if called without a symbol or hash" do
    proc{EagerAlbum.eager(Object.new)}.should raise_error(Sequel::Error)
  end

  it "should eagerly load a single many_to_one association" do
    a = EagerAlbum.eager(:band).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT * FROM bands WHERE (bands.id IN (2))']
    a.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    a.first.band.should == EagerBand.load(:id=>2)
    MODEL_DB.sqls.should == []
  end
  
  it "should eagerly load a single one_to_one association" do
    EagerAlbum.one_to_one :track, :class=>'EagerTrack', :key=>:album_id
    a = EagerAlbum.eager(:track).all
    a.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT * FROM tracks WHERE (tracks.album_id IN (1))']
    a.first.track.should == EagerTrack.load(:id => 3, :album_id=>1)
    MODEL_DB.sqls.should == []
  end
  
  it "should eagerly load a single one_to_one association using the :distinct_on strategy" do
    EagerTrack.dataset.meta_def(:supports_distinct_on?){true}
    EagerAlbum.one_to_one :track, :class=>'EagerTrack', :key=>:album_id, :eager_limit_strategy=>true
    a = EagerAlbum.eager(:track).all
    a.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT DISTINCT ON (tracks.album_id) * FROM tracks WHERE (tracks.album_id IN (1)) ORDER BY tracks.album_id']
    a.first.track.should == EagerTrack.load(:id => 3, :album_id=>1)
    MODEL_DB.sqls.should == []
  end
  
  it "should eagerly load a single one_to_one association using the :window_function strategy" do
    EagerTrack.dataset.meta_def(:supports_window_functions?){true}
    EagerAlbum.one_to_one :track, :class=>'EagerTrack', :key=>:album_id, :eager_limit_strategy=>true, :order=>:name
    a = EagerAlbum.eager(:track).all
    a.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY tracks.album_id ORDER BY name) AS x_sequel_row_number_x FROM tracks WHERE (tracks.album_id IN (1))) AS t1 WHERE (x_sequel_row_number_x = 1)']
    a.first.track.should == EagerTrack.load(:id => 3, :album_id=>1)
    MODEL_DB.sqls.should == []
  end
  
  it "should eagerly load a single one_to_one association using the :correlated_subquery strategy" do
    EagerAlbum.one_to_one :track, :class=>'EagerTrack', :key=>:album_id, :eager_limit_strategy=>:correlated_subquery, :order=>:name
    a = EagerAlbum.eager(:track).all
    a.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT * FROM tracks WHERE ((tracks.album_id IN (1)) AND (tracks.id IN (SELECT t1.id FROM tracks AS t1 WHERE (t1.album_id = tracks.album_id) ORDER BY name LIMIT 1))) ORDER BY name']
    a.first.track.should == EagerTrack.load(:id => 3, :album_id=>1)
    MODEL_DB.sqls.should == []
  end
  
  it "should handle qualified order clauses when eagerly loading a single one_to_one association using the :correlated_subquery strategy" do
    EagerAlbum.one_to_one :track, :class=>'EagerTrack', :key=>:album_id, :eager_limit_strategy=>:correlated_subquery, :order=>[:tracks__name, :tracks__name.desc, :name.qualify(:tracks), :name.qualify(:t), 1]
    a = EagerAlbum.eager(:track).all
    a.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT * FROM tracks WHERE ((tracks.album_id IN (1)) AND (tracks.id IN (SELECT t1.id FROM tracks AS t1 WHERE (t1.album_id = tracks.album_id) ORDER BY t1.name, t1.name DESC, t1.name, t.name, 1 LIMIT 1))) ORDER BY tracks.name, tracks.name DESC, tracks.name, t.name, 1']
    a.first.track.should == EagerTrack.load(:id => 3, :album_id=>1)
    MODEL_DB.sqls.should == []
  end
  
  it "should handle qualified composite keys when eagerly loading a single one_to_one association using the :correlated_subquery strategy" do
    c1 = Class.new(EagerAlbum)
    c2 = Class.new(EagerTrack)
    c1.set_primary_key [:id, :band_id]
    c2.set_primary_key [:id, :album_id]
    c1.one_to_one :track, :class=>c2, :key=>[:album_id, :id], :eager_limit_strategy=>:correlated_subquery
    c2.dataset._fetch = {:id => 2, :album_id=>1}
    a = c1.eager(:track).all
    a.should == [c1.load(:id => 1, :band_id => 2)]
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT * FROM tracks WHERE (((tracks.album_id, tracks.id) IN ((1, 2))) AND ((tracks.id, tracks.album_id) IN (SELECT t1.id, t1.album_id FROM tracks AS t1 WHERE ((t1.album_id = tracks.album_id) AND (t1.id = tracks.id)) LIMIT 1)))']
    a.first.track.should == c2.load(:id => 2, :album_id=>1)
    MODEL_DB.sqls.should == []
  end
  
  it "should eagerly load a single one_to_many association" do
    a = EagerAlbum.eager(:tracks).all
    a.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT * FROM tracks WHERE (tracks.album_id IN (1))']
    a.first.tracks.should == [EagerTrack.load(:id => 3, :album_id=>1)]
    MODEL_DB.sqls.should == []
  end
  
  it "should eagerly load a single many_to_many association" do
    a = EagerAlbum.eager(:genres).all
    a.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT genres.*, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON ((ag.genre_id = genres.id) AND (ag.album_id IN (1)))"]
    a.first.genres.should == [EagerGenre.load(:id=>4)]
    MODEL_DB.sqls.should == []
  end
  
  it "should correctly handle a :select=>[] option to many_to_many" do
    EagerAlbum.many_to_many :sgenres, :clone=>:genres, :select=>[]
    a = EagerAlbum.eager(:sgenres).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT *, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON ((ag.genre_id = genres.id) AND (ag.album_id IN (1)))"]
  end
  
  it "should correctly handle an aliased join table in many_to_many" do
    EagerAlbum.many_to_many :sgenres, :clone=>:genres, :join_table=>:ag___ga
    a = EagerAlbum.eager(:sgenres).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT genres.*, ga.album_id AS x_foreign_key_x FROM genres INNER JOIN ag AS ga ON ((ga.genre_id = genres.id) AND (ga.album_id IN (1)))"]
  end
  
  it "should eagerly load multiple associations in a single call" do
    a = EagerAlbum.eager(:genres, :tracks, :band).all
    a.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    sqls = MODEL_DB.sqls
    sqls.shift.should == 'SELECT * FROM albums'
    sqls.sort.should == ['SELECT * FROM bands WHERE (bands.id IN (2))',
      'SELECT * FROM tracks WHERE (tracks.album_id IN (1))',
      'SELECT genres.*, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON ((ag.genre_id = genres.id) AND (ag.album_id IN (1)))']
    a = a.first
    a.band.should == EagerBand.load(:id=>2)
    a.tracks.should == [EagerTrack.load(:id => 3, :album_id=>1)]
    a.genres.should == [EagerGenre.load(:id => 4)]
    MODEL_DB.sqls.should == []
  end
  
  it "should eagerly load multiple associations in separate calls" do
    a = EagerAlbum.eager(:genres).eager(:tracks).eager(:band).all
    a.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    sqls = MODEL_DB.sqls
    sqls.shift.should == 'SELECT * FROM albums'
    sqls.sort.should == ['SELECT * FROM bands WHERE (bands.id IN (2))',
      'SELECT * FROM tracks WHERE (tracks.album_id IN (1))',
      'SELECT genres.*, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON ((ag.genre_id = genres.id) AND (ag.album_id IN (1)))']
    a = a.first
    a.band.should == EagerBand.load(:id=>2)
    a.tracks.should == [EagerTrack.load(:id => 3, :album_id=>1)]
    a.genres.should == [EagerGenre.load(:id => 4)]
    MODEL_DB.sqls.should == []
  end
  
  it "should allow cascading of eager loading for associations of associated models" do
    a = EagerTrack.eager(:album=>{:band=>:members}).all
    a.should == [EagerTrack.load(:id => 3, :album_id => 1)]
    MODEL_DB.sqls.should == ['SELECT * FROM tracks', 
      'SELECT * FROM albums WHERE (albums.id IN (1))',
      'SELECT * FROM bands WHERE (bands.id IN (2))',
      "SELECT members.*, bm.band_id AS x_foreign_key_x FROM members INNER JOIN bm ON ((bm.member_id = members.id) AND (bm.band_id IN (2)))"]
    a = a.first
    a.album.should == EagerAlbum.load(:id => 1, :band_id => 2)
    a.album.band.should == EagerBand.load(:id => 2)
    a.album.band.members.should == [EagerBandMember.load(:id => 5)]
    MODEL_DB.sqls.should == []
  end
  
  it "should cascade eagerly loading when the :eager association option is used" do
    a = EagerBand.eager(:albums).all
    a.should == [EagerBand.load(:id=>2)]
    MODEL_DB.sqls.should == ['SELECT * FROM bands', 
      'SELECT * FROM albums WHERE (albums.band_id IN (2))',
      'SELECT * FROM tracks WHERE (tracks.album_id IN (1))']
    a.first.albums.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    a.first.albums.first.tracks.should == [EagerTrack.load(:id => 3, :album_id => 1)]
    MODEL_DB.sqls.should == []
  end
  
  it "should respect :eager when lazily loading an association" do
    a = EagerBand.all
    a.should == [EagerBand.load(:id=>2)]
    MODEL_DB.sqls.should == ['SELECT * FROM bands']
    a = a.first.albums
    MODEL_DB.sqls.should == ['SELECT * FROM albums WHERE (albums.band_id = 2)',
      'SELECT * FROM tracks WHERE (tracks.album_id IN (1))']
    a.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    a.first.tracks.should == [EagerTrack.load(:id => 3, :album_id => 1)]
    MODEL_DB.sqls.should == []
  end
  
  it "should cascade eagerly loading when the :eager_graph association option is used" do
    EagerAlbum.dataset._fetch = {:id=>1, :band_id=>2, :tracks_id=>3, :album_id=>1}
    a = EagerBand.eager(:graph_albums).all
    a.should == [EagerBand.load(:id=>2)]
    MODEL_DB.sqls.should == ['SELECT * FROM bands', 
      'SELECT albums.id, albums.band_id, tracks.id AS tracks_id, tracks.album_id FROM albums LEFT OUTER JOIN tracks ON (tracks.album_id = albums.id) WHERE (albums.band_id IN (2))']
    a.first.graph_albums.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    a.first.graph_albums.first.tracks.should == [EagerTrack.load(:id => 3, :album_id => 1)]
    MODEL_DB.sqls.should == []
  end
  
  it "should raise an Error when eager loading a many_to_many association with the :eager_graph option" do
    proc{EagerBand.eager(:graph_members).all}.should raise_error(Sequel::Error)
  end
  
  it "should respect :eager_graph when lazily loading an association" do
    a = EagerBand.all
    a.should == [EagerBand.load(:id=>2)]
    MODEL_DB.sqls.should == ['SELECT * FROM bands']
    a = a.first
    EagerAlbum.dataset._fetch = {:id=>1, :band_id=>2, :tracks_id=>3, :album_id=>1}
    a.graph_albums
    MODEL_DB.sqls.should == ['SELECT albums.id, albums.band_id, tracks.id AS tracks_id, tracks.album_id FROM albums LEFT OUTER JOIN tracks ON (tracks.album_id = albums.id) WHERE (albums.band_id = 2)']
    a.graph_albums.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    a.graph_albums.first.tracks.should == [EagerTrack.load(:id => 3, :album_id => 1)]
    MODEL_DB.sqls.should == []
  end
  
  it "should respect :eager_graph when lazily loading a many_to_many association" do
    ds = EagerBandMember.dataset
    def ds.columns() [:id] end
    ds._fetch = [{:id=>5, :bands_id=>2, :p_k=>6}, {:id=>5, :bands_id=>3, :p_k=>6}]
    a = EagerBand.load(:id=>2)
    a.graph_members.should == [EagerBandMember.load(:id=>5)]
    MODEL_DB.sqls.should == ['SELECT members.id, bands.id AS bands_id, bands.p_k FROM (SELECT members.* FROM members INNER JOIN bm ON ((bm.member_id = members.id) AND (bm.band_id = 2))) AS members LEFT OUTER JOIN bm AS bm_0 ON (bm_0.member_id = members.id) LEFT OUTER JOIN bands ON (bands.id = bm_0.band_id) ORDER BY bands.id']
    a.graph_members.first.bands.should == [EagerBand.load(:id=>2, :p_k=>6), EagerBand.load(:id=>3, :p_k=>6)]
    MODEL_DB.sqls.should == []
  end
  
  it "should respect :conditions when eagerly loading" do
    EagerBandMember.many_to_many :good_bands, :clone=>:bands, :conditions=>{:a=>32}
    a = EagerBandMember.eager(:good_bands).all
    a.should == [EagerBandMember.load(:id => 5)]
    MODEL_DB.sqls.should == ['SELECT * FROM members', 'SELECT bands.*, bm.member_id AS x_foreign_key_x FROM bands INNER JOIN bm ON ((bm.band_id = bands.id) AND (bm.member_id IN (5))) WHERE (a = 32) ORDER BY id']
    a.first.good_bands.should == [EagerBand.load(:id => 2)]
    MODEL_DB.sqls.should == []

    EagerBandMember.many_to_many :good_bands, :clone=>:bands, :conditions=>"x = 1"
    a = EagerBandMember.eager(:good_bands).all
    MODEL_DB.sqls.should == ['SELECT * FROM members', 'SELECT bands.*, bm.member_id AS x_foreign_key_x FROM bands INNER JOIN bm ON ((bm.band_id = bands.id) AND (bm.member_id IN (5))) WHERE (x = 1) ORDER BY id']
  end
  
  it "should respect :order when eagerly loading" do
    a = EagerBandMember.eager(:bands).all
    a.should == [EagerBandMember.load(:id => 5)]
    MODEL_DB.sqls.should == ['SELECT * FROM members', 'SELECT bands.*, bm.member_id AS x_foreign_key_x FROM bands INNER JOIN bm ON ((bm.band_id = bands.id) AND (bm.member_id IN (5))) ORDER BY id']
    a.first.bands.should == [EagerBand.load(:id => 2)]
    MODEL_DB.sqls.should == []
  end
  
  it "should populate the reciprocal many_to_one association when eagerly loading the one_to_many association" do
    a = EagerAlbum.eager(:tracks).all
    a.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT * FROM tracks WHERE (tracks.album_id IN (1))']
    a.first.tracks.should == [EagerTrack.load(:id => 3, :album_id=>1)]
    a.first.tracks.first.album.should == a.first
    MODEL_DB.sqls.should == []
  end

  it "should cache the negative lookup when eagerly loading a many_to_one association" do
    a = EagerAlbum.eager(:band).filter(:id=>101).all
    a.should == [EagerAlbum.load(:id => 101, :band_id => 101)]
    MODEL_DB.sqls.should == ['SELECT * FROM albums WHERE (id = 101)', 'SELECT * FROM bands WHERE (bands.id IN (101))']
    a.first.associations.fetch(:band, 2).should be_nil
    a.first.band.should be_nil
    MODEL_DB.sqls.should == []
  end
  
  it "should cache the negative lookup when eagerly loading a *_to_many associations" do
    a = EagerBand.eager(:albums).filter('id > 100').all
    a.should == [EagerBand.load(:id => 101), EagerBand.load(:id =>102)]
    MODEL_DB.sqls.should == ['SELECT * FROM bands WHERE (id > 100)', 'SELECT * FROM albums WHERE (albums.band_id IN (101, 102))', "SELECT * FROM tracks WHERE (tracks.album_id IN (101))"]
    a.map{|b| b.associations[:albums]}.should == [[EagerAlbum.load({:band_id=>101, :id=>101})], []]
    MODEL_DB.sqls.should == []
  end
  
  it "should use the association's block when eager loading by default" do
    EagerAlbum.eager(:good_tracks).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT * FROM tracks WHERE ((tracks.album_id IN (1)) AND (name = 'Good'))"]
  end

  it "should use the eager_block option when eager loading if given" do
    EagerBand.eager(:good_albums).all
    MODEL_DB.sqls.should == ['SELECT * FROM bands', "SELECT * FROM albums WHERE ((albums.band_id IN (2)) AND (name = 'good'))"]
    EagerBand.eager(:good_albums=>:good_tracks).all
    MODEL_DB.sqls.should == ['SELECT * FROM bands', "SELECT * FROM albums WHERE ((albums.band_id IN (2)) AND (name = 'good'))", "SELECT * FROM tracks WHERE ((tracks.album_id IN (1)) AND (name = 'Good'))"]
  end

  it "should raise an error when attempting to eagerly load an association with the :allow_eager option set to false" do
    proc{EagerBand.eager(:self_titled_albums).all}.should raise_error(Sequel::Error)
    proc{EagerBand.eager(:albums_by_name).all}.should raise_error(Sequel::Error)
  end

  it "should respect the association's :select option" do
    EagerAlbum.eager(:band_name).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT id, name FROM bands WHERE (bands.id IN (2))"]
    EagerAlbum.eager(:track_names).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT id, name FROM tracks WHERE (tracks.album_id IN (1))"]
    EagerAlbum.eager(:genre_names).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT id, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON ((ag.genre_id = genres.id) AND (ag.album_id IN (1)))"]
  end

  it "should respect many_to_one association's :qualify option" do
    EagerAlbum.many_to_one :special_band, :class=>:EagerBand, :qualify=>false, :key=>:band_id
    EagerBand.dataset._fetch = {:id=>2}
    as = EagerAlbum.eager(:special_band).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT * FROM bands WHERE (id IN (2))"]
    as.map{|a| a.special_band}.should == [EagerBand.load(:id=>2)]
    MODEL_DB.sqls.should == []
  end

  it "should respect the association's :primary_key option" do
    EagerAlbum.many_to_one :special_band, :class=>:EagerBand, :primary_key=>:p_k, :key=>:band_id
    EagerBand.dataset._fetch = {:p_k=>2, :id=>1}
    as = EagerAlbum.eager(:special_band).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT * FROM bands WHERE (bands.p_k IN (2))"]
    as.length.should == 1
    as.first.special_band.should == EagerBand.load(:p_k=>2, :id=>1)

    EagerAlbum.one_to_many :special_tracks, :class=>:EagerTrack, :primary_key=>:band_id, :key=>:album_id
    EagerTrack.dataset._fetch = {:album_id=>2, :id=>1}
    as = EagerAlbum.eager(:special_tracks).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT * FROM tracks WHERE (tracks.album_id IN (2))"]
    as.length.should == 1
    as.first.special_tracks.should == [EagerTrack.load(:album_id=>2, :id=>1)]
  end
  
  it "should respect the many_to_one association's composite keys" do
    EagerAlbum.many_to_one :special_band, :class=>:EagerBand, :primary_key=>[:id, :p_k], :key=>[:band_id, :id]
    EagerBand.dataset._fetch = {:p_k=>1, :id=>2}
    as = EagerAlbum.eager(:special_band).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT * FROM bands WHERE ((bands.id, bands.p_k) IN ((2, 1)))"]
    as.length.should == 1
    as.first.special_band.should == EagerBand.load(:p_k=>1, :id=>2)
  end
  
  it "should respect the one_to_many association's composite keys" do
    EagerAlbum.one_to_many :special_tracks, :class=>:EagerTrack, :primary_key=>[:band_id, :id], :key=>[:id, :album_id]
    EagerTrack.dataset._fetch = {:album_id=>1, :id=>2}
    as = EagerAlbum.eager(:special_tracks).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT * FROM tracks WHERE ((tracks.id, tracks.album_id) IN ((2, 1)))"]
    as.length.should == 1
    as.first.special_tracks.should == [EagerTrack.load(:album_id=>1, :id=>2)]
  end

  it "should respect many_to_many association's composite keys" do
    EagerAlbum.many_to_many :special_genres, :class=>:EagerGenre, :left_primary_key=>[:band_id, :id], :left_key=>[:l1, :l2], :right_primary_key=>[:xxx, :id], :right_key=>[:r1, :r2], :join_table=>:ag
    EagerGenre.dataset._fetch = [{:x_foreign_key_0_x=>2, :x_foreign_key_1_x=>1, :id=>5}, {:x_foreign_key_0_x=>2, :x_foreign_key_1_x=>1, :id=>6}]
    as = EagerAlbum.eager(:special_genres).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT genres.*, ag.l1 AS x_foreign_key_0_x, ag.l2 AS x_foreign_key_1_x FROM genres INNER JOIN ag ON ((ag.r1 = genres.xxx) AND (ag.r2 = genres.id) AND ((ag.l1, ag.l2) IN ((2, 1))))"]
    as.length.should == 1
    as.first.special_genres.should == [EagerGenre.load(:id=>5), EagerGenre.load(:id=>6)]
  end
  
  it "should respect many_to_many association's :left_primary_key and :right_primary_key options" do
    EagerAlbum.many_to_many :special_genres, :class=>:EagerGenre, :left_primary_key=>:band_id, :left_key=>:album_id, :right_primary_key=>:xxx, :right_key=>:genre_id, :join_table=>:ag
    EagerGenre.dataset._fetch = [{:x_foreign_key_x=>2, :id=>5}, {:x_foreign_key_x=>2, :id=>6}]
    as = EagerAlbum.eager(:special_genres).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT genres.*, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON ((ag.genre_id = genres.xxx) AND (ag.album_id IN (2)))"]
    as.length.should == 1
    as.first.special_genres.should == [EagerGenre.load(:id=>5), EagerGenre.load(:id=>6)]
  end

  it "should respect the :limit option on a one_to_many association" do
    EagerAlbum.one_to_many :first_two_tracks, :class=>:EagerTrack, :key=>:album_id, :limit=>2
    EagerTrack.dataset._fetch = [{:album_id=>1, :id=>2}, {:album_id=>1, :id=>3}, {:album_id=>1, :id=>4}]
    as = EagerAlbum.eager(:first_two_tracks).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT * FROM tracks WHERE (tracks.album_id IN (1))"]
    as.length.should == 1
    as.first.first_two_tracks.should == [EagerTrack.load(:album_id=>1, :id=>2), EagerTrack.load(:album_id=>1, :id=>3)]

    MODEL_DB.reset
    EagerAlbum.one_to_many :first_two_tracks, :class=>:EagerTrack, :key=>:album_id, :limit=>[2,1]
    as = EagerAlbum.eager(:first_two_tracks).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT * FROM tracks WHERE (tracks.album_id IN (1))"]
    as.length.should == 1
    as.first.first_two_tracks.should == [EagerTrack.load(:album_id=>1, :id=>3), EagerTrack.load(:album_id=>1, :id=>4)]
  end

  it "should respect the :limit option on a one_to_many association using the :window_function strategy" do
    EagerTrack.dataset.meta_def(:supports_window_functions?){true}
    EagerAlbum.one_to_many :tracks, :class=>'EagerTrack', :key=>:album_id, :eager_limit_strategy=>true, :order=>:name, :limit=>2
    a = EagerAlbum.eager(:tracks).all
    a.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY tracks.album_id ORDER BY name) AS x_sequel_row_number_x FROM tracks WHERE (tracks.album_id IN (1))) AS t1 WHERE (x_sequel_row_number_x <= 2)']
    a.first.tracks.should == [EagerTrack.load(:id => 3, :album_id=>1)]
    MODEL_DB.sqls.should == []
  end
  
  it "should respect the :limit option with an offset on a one_to_many association using the :window_function strategy" do
    EagerTrack.dataset.meta_def(:supports_window_functions?){true}
    EagerAlbum.one_to_many :tracks, :class=>'EagerTrack', :key=>:album_id, :eager_limit_strategy=>true, :order=>:name, :limit=>[2, 1]
    a = EagerAlbum.eager(:tracks).all
    a.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY tracks.album_id ORDER BY name) AS x_sequel_row_number_x FROM tracks WHERE (tracks.album_id IN (1))) AS t1 WHERE ((x_sequel_row_number_x >= 2) AND (x_sequel_row_number_x < 4))']
    a.first.tracks.should == [EagerTrack.load(:id => 3, :album_id=>1)]
    MODEL_DB.sqls.should == []
  end
  
  it "should respect the :limit option on a one_to_many association using the :correlated_subquery strategy" do
    EagerAlbum.one_to_many :tracks, :class=>'EagerTrack', :key=>:album_id, :eager_limit_strategy=>:correlated_subquery, :order=>:name, :limit=>2
    a = EagerAlbum.eager(:tracks).all
    a.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT * FROM tracks WHERE ((tracks.album_id IN (1)) AND (tracks.id IN (SELECT t1.id FROM tracks AS t1 WHERE (t1.album_id = tracks.album_id) ORDER BY name LIMIT 2))) ORDER BY name']
    a.first.tracks.should == [EagerTrack.load(:id => 3, :album_id=>1)]
    MODEL_DB.sqls.should == []
  end
  
  it "should respect the :limit option with an offset on a one_to_many association using the :correlated_subquery strategy" do
    EagerAlbum.one_to_many :tracks, :class=>'EagerTrack', :key=>:album_id, :eager_limit_strategy=>:correlated_subquery, :order=>:name, :limit=>[2, 1]
    a = EagerAlbum.eager(:tracks).all
    a.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT * FROM tracks WHERE ((tracks.album_id IN (1)) AND (tracks.id IN (SELECT t1.id FROM tracks AS t1 WHERE (t1.album_id = tracks.album_id) ORDER BY name LIMIT 2 OFFSET 1))) ORDER BY name']
    a.first.tracks.should == [EagerTrack.load(:id => 3, :album_id=>1)]
    MODEL_DB.sqls.should == []
  end
  
  it "should respect the limit option on a many_to_many association" do
    EagerAlbum.many_to_many :first_two_genres, :class=>:EagerGenre, :left_primary_key=>:band_id, :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :limit=>2
    EagerGenre.dataset._fetch = [{:x_foreign_key_x=>2, :id=>5}, {:x_foreign_key_x=>2, :id=>6}, {:x_foreign_key_x=>2, :id=>7}]
    as = EagerAlbum.eager(:first_two_genres).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT genres.*, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON ((ag.genre_id = genres.id) AND (ag.album_id IN (2)))"]
    as.length.should == 1
    as.first.first_two_genres.should == [EagerGenre.load(:id=>5), EagerGenre.load(:id=>6)]
    
    EagerGenre.dataset._fetch = [{:x_foreign_key_x=>2, :id=>5}, {:x_foreign_key_x=>2, :id=>6}, {:x_foreign_key_x=>2, :id=>7}]
    EagerAlbum.many_to_many :first_two_genres, :class=>:EagerGenre, :left_primary_key=>:band_id, :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :limit=>[2, 1]
    as = EagerAlbum.eager(:first_two_genres).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT genres.*, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON ((ag.genre_id = genres.id) AND (ag.album_id IN (2)))"]
    as.length.should == 1
    as.first.first_two_genres.should == [EagerGenre.load(:id=>6), EagerGenre.load(:id=>7)]
  end

  it "should respect the limit option on a many_to_many association using the :window_function strategy" do
    EagerGenre.dataset.meta_def(:supports_window_functions?){true}
    EagerAlbum.many_to_many :first_two_genres, :class=>:EagerGenre, :left_primary_key=>:band_id, :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :eager_limit_strategy=>true, :limit=>2, :order=>:name
    EagerGenre.dataset._fetch = [{:x_foreign_key_x=>2, :id=>5}, {:x_foreign_key_x=>2, :id=>6}]
    as = EagerAlbum.eager(:first_two_genres).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT * FROM (SELECT genres.*, ag.album_id AS x_foreign_key_x, row_number() OVER (PARTITION BY ag.album_id ORDER BY name) AS x_sequel_row_number_x FROM genres INNER JOIN ag ON ((ag.genre_id = genres.id) AND (ag.album_id IN (2)))) AS t1 WHERE (x_sequel_row_number_x <= 2)"]
    as.length.should == 1
    as.first.first_two_genres.should == [EagerGenre.load(:id=>5), EagerGenre.load(:id=>6)]

    EagerGenre.dataset._fetch = [{:x_foreign_key_x=>2, :id=>5}, {:x_foreign_key_x=>2, :id=>6}]
    EagerAlbum.many_to_many :first_two_genres, :class=>:EagerGenre, :left_primary_key=>:band_id, :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :eager_limit_strategy=>true, :limit=>[2, 1], :order=>:name
    as = EagerAlbum.eager(:first_two_genres).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT * FROM (SELECT genres.*, ag.album_id AS x_foreign_key_x, row_number() OVER (PARTITION BY ag.album_id ORDER BY name) AS x_sequel_row_number_x FROM genres INNER JOIN ag ON ((ag.genre_id = genres.id) AND (ag.album_id IN (2)))) AS t1 WHERE ((x_sequel_row_number_x >= 2) AND (x_sequel_row_number_x < 4))"]
    as.length.should == 1
    as.first.first_two_genres.should == [EagerGenre.load(:id=>5), EagerGenre.load(:id=>6)]
  end

  it "should respect the limit option on a many_to_many association using the :correlated_subquery strategy" do
    EagerAlbum.many_to_many :first_two_genres, :class=>:EagerGenre, :left_primary_key=>:band_id, :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :eager_limit_strategy=>:correlated_subquery, :limit=>2, :order=>:name
    EagerGenre.dataset._fetch = [{:x_foreign_key_x=>2, :id=>5}, {:x_foreign_key_x=>2, :id=>6}]
    as = EagerAlbum.eager(:first_two_genres).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT genres.*, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON ((ag.genre_id = genres.id) AND (ag.album_id IN (2))) WHERE (genres.id IN (SELECT t1.id FROM genres AS t1 INNER JOIN ag AS t2 ON ((t2.genre_id = t1.id) AND (t2.album_id = ag.album_id)) ORDER BY name LIMIT 2)) ORDER BY name"]
    as.length.should == 1
    as.first.first_two_genres.should == [EagerGenre.load(:id=>5), EagerGenre.load(:id=>6)]

    EagerGenre.dataset._fetch = [{:x_foreign_key_x=>2, :id=>5}, {:x_foreign_key_x=>2, :id=>6}]
    EagerAlbum.many_to_many :first_two_genres, :class=>:EagerGenre, :left_primary_key=>:band_id, :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :eager_limit_strategy=>:correlated_subquery, :limit=>[2, 1], :order=>:name
    as = EagerAlbum.eager(:first_two_genres).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT genres.*, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON ((ag.genre_id = genres.id) AND (ag.album_id IN (2))) WHERE (genres.id IN (SELECT t1.id FROM genres AS t1 INNER JOIN ag AS t2 ON ((t2.genre_id = t1.id) AND (t2.album_id = ag.album_id)) ORDER BY name LIMIT 2 OFFSET 1)) ORDER BY name"]
    as.length.should == 1
    as.first.first_two_genres.should == [EagerGenre.load(:id=>5), EagerGenre.load(:id=>6)]
  end

  it "should use the :eager_loader association option when eager loading" do
    EagerAlbum.many_to_one :special_band, :eager_loader=>(proc do |key_hash, records, assocs| 
      item = EagerBand.filter(:album_id=>records.collect{|r| [r.pk, r.pk*2]}.flatten).order(:name).first
      records.each{|r| r.associations[:special_band] = item}
    end)
    EagerAlbum.one_to_many :special_tracks, :eager_loader=>(proc do |eo|
      items = EagerTrack.filter(:album_id=>eo[:rows].collect{|r| [r.pk, r.pk*2]}.flatten).all
      eo[:rows].each{|r| r.associations[:special_tracks] = items}
    end)
    EagerAlbum.many_to_many :special_genres, :class=>:EagerGenre, :eager_loader=>(proc do |key_hash, records, assocs| 
      items = EagerGenre.inner_join(:ag, [:genre_id]).filter(:album_id=>records.collect{|r| r.pk}).all
      records.each{|r| r.associations[:special_genres] = items}
    end)
    a = EagerAlbum.eager(:special_genres, :special_tracks, :special_band).all
    a.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    sqls = MODEL_DB.sqls
    sqls.shift.should == 'SELECT * FROM albums'
    sqls.sort.should == ['SELECT * FROM bands WHERE (album_id IN (1, 2)) ORDER BY name LIMIT 1',
      'SELECT * FROM genres INNER JOIN ag USING (genre_id) WHERE (album_id IN (1))',
      'SELECT * FROM tracks WHERE (album_id IN (1, 2))']
    a = a.first
    a.special_band.should == EagerBand.load(:id => 2)
    a.special_tracks.should == [EagerTrack.load(:id => 3, :album_id=>1)]
    a.special_genres.should == [EagerGenre.load(:id => 4)]
    MODEL_DB.sqls.should == []
  end

  it "should raise an error if you use an :eager_loader proc with the wrong arity" do
    proc{EagerAlbum.many_to_one :special_band, :eager_loader=>proc{|a, b|}}.should raise_error(Sequel::Error)
  end
  
  it "should respect :after_load callbacks on associations when eager loading" do
    EagerAlbum.many_to_one :al_band, :class=>'EagerBand', :key=>:band_id, :after_load=>proc{|o, a| a.id *=2}
    EagerAlbum.one_to_many :al_tracks, :class=>'EagerTrack', :key=>:album_id, :after_load=>proc{|o, os| os.each{|a| a.id *=2}}
    EagerAlbum.many_to_many :al_genres, :class=>'EagerGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :after_load=>proc{|o, os| os.each{|a| a.id *=2}}
    a = EagerAlbum.eager(:al_band, :al_tracks, :al_genres).all.first
    a.should == EagerAlbum.load(:id => 1, :band_id => 2)
    a.al_band.should == EagerBand.load(:id=>4)
    a.al_tracks.should == [EagerTrack.load(:id=>6, :album_id=>1)]
    a.al_genres.should == [EagerGenre.load(:id=>8)]
  end
  
  it "should respect :uniq option when eagerly loading many_to_many associations" do
    EagerAlbum.many_to_many :al_genres, :class=>'EagerGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :uniq=>true
    EagerGenre.dataset._fetch = [{:x_foreign_key_x=>1, :id=>8}, {:x_foreign_key_x=>1, :id=>8}]
    a = EagerAlbum.eager(:al_genres).all.first
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT genres.*, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON ((ag.genre_id = genres.id) AND (ag.album_id IN (1)))"]
    a.should == EagerAlbum.load(:id => 1, :band_id => 2)
    a.al_genres.should == [EagerGenre.load(:id=>8)]
  end
  
  it "should respect :distinct option when eagerly loading many_to_many associations" do
    EagerAlbum.many_to_many :al_genres, :class=>'EagerGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :distinct=>true
    a = EagerAlbum.eager(:al_genres).all.first
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT DISTINCT genres.*, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON ((ag.genre_id = genres.id) AND (ag.album_id IN (1)))"]
    a.should == EagerAlbum.load(:id => 1, :band_id => 2)
    a.al_genres.should == [EagerGenre.load(:id=>4)]
  end

  it "should eagerly load a many_to_one association with custom eager block" do
    a = EagerAlbum.eager(:band => proc {|ds| ds.select(:id, :name)}).all
    a.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT id, name FROM bands WHERE (bands.id IN (2))']
    a.first.band.should == EagerBand.load(:id => 2)
    MODEL_DB.sqls.should == []
  end

  it "should eagerly load a one_to_one association with custom eager block" do
    EagerAlbum.one_to_one :track, :class=>'EagerTrack', :key=>:album_id
    a = EagerAlbum.eager(:track => proc {|ds| ds.select(:id)}).all
    a.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT id FROM tracks WHERE (tracks.album_id IN (1))']
    a.first.track.should == EagerTrack.load(:id => 3, :album_id=>1)
    MODEL_DB.sqls.should == []
  end

  it "should eagerly load a one_to_many association with custom eager block" do
    a = EagerAlbum.eager(:tracks => proc {|ds| ds.select(:id)}).all
    a.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT id FROM tracks WHERE (tracks.album_id IN (1))']
    a.first.tracks.should == [EagerTrack.load(:id => 3, :album_id=>1)]
    MODEL_DB.sqls.should == []
  end

  it "should eagerly load a many_to_many association with custom eager block" do
    a = EagerAlbum.eager(:genres => proc {|ds| ds.select(:name)}).all
    a.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT name, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON ((ag.genre_id = genres.id) AND (ag.album_id IN (1)))"]
    a.first.genres.should == [EagerGenre.load(:id => 4)]
    MODEL_DB.sqls.should == []
  end

  it "should allow cascading of eager loading within a custom eager block" do
    a = EagerTrack.eager(:album => proc {|ds| ds.eager(:band => :members)}).all
    a.should == [EagerTrack.load(:id => 3, :album_id => 1)]
    MODEL_DB.sqls.should == ['SELECT * FROM tracks',
      'SELECT * FROM albums WHERE (albums.id IN (1))',
      'SELECT * FROM bands WHERE (bands.id IN (2))',
      "SELECT members.*, bm.band_id AS x_foreign_key_x FROM members INNER JOIN bm ON ((bm.member_id = members.id) AND (bm.band_id IN (2)))"]
    a = a.first
    a.album.should == EagerAlbum.load(:id => 1, :band_id => 2)
    a.album.band.should == EagerBand.load(:id => 2)
    a.album.band.members.should == [EagerBandMember.load(:id => 5)]
    MODEL_DB.sqls.should == []
  end

  it "should allow cascading of eager loading with custom callback with hash value" do
    a = EagerTrack.eager(:album=>{proc{|ds| ds.select(:id, :band_id)}=>{:band => :members}}).all
    a.should == [EagerTrack.load(:id => 3, :album_id => 1)]
    MODEL_DB.sqls.should == ['SELECT * FROM tracks',
      'SELECT id, band_id FROM albums WHERE (albums.id IN (1))',
      'SELECT * FROM bands WHERE (bands.id IN (2))',
      "SELECT members.*, bm.band_id AS x_foreign_key_x FROM members INNER JOIN bm ON ((bm.member_id = members.id) AND (bm.band_id IN (2)))"]
    a = a.first
    a.album.should == EagerAlbum.load(:id => 1, :band_id => 2)
    a.album.band.should == EagerBand.load(:id => 2)
    a.album.band.members.should == [EagerBandMember.load(:id => 5)]
    MODEL_DB.sqls.should == []
  end

  it "should allow cascading of eager loading with custom callback with symbol value" do
    a = EagerTrack.eager(:album=>{proc{|ds| ds.select(:id, :band_id)}=>:band}).all
    a.should == [EagerTrack.load(:id => 3, :album_id => 1)]
    MODEL_DB.sqls.should == ['SELECT * FROM tracks',
      'SELECT id, band_id FROM albums WHERE (albums.id IN (1))',
      'SELECT * FROM bands WHERE (bands.id IN (2))']
    a = a.first
    a.album.should == EagerAlbum.load(:id => 1, :band_id => 2)
    a.album.band.should == EagerBand.load(:id => 2)
    MODEL_DB.sqls.should == []
  end

  it "should allow cascading of eager loading with custom callback with array value" do
    a = EagerTrack.eager(:album=>{proc{|ds| ds.select(:id, :band_id)}=>[:band, :band_name]}).all
    a.should == [EagerTrack.load(:id => 3, :album_id => 1)]
    sqls = MODEL_DB.sqls
    sqls.slice!(0..1).should == ['SELECT * FROM tracks',
      'SELECT id, band_id FROM albums WHERE (albums.id IN (1))']
    sqls.sort.should == ['SELECT * FROM bands WHERE (bands.id IN (2))',
      'SELECT id, name FROM bands WHERE (bands.id IN (2))']
    a = a.first
    a.album.should == EagerAlbum.load(:id => 1, :band_id => 2)
    a.album.band.should == EagerBand.load(:id => 2)
    a.album.band_name.should == EagerBand.load(:id => 2)
    MODEL_DB.sqls.should == []
  end

  it "should call both association and custom eager blocks" do
    EagerBand.eager(:good_albums => proc {|ds| ds.select(:name)}).all
    MODEL_DB.sqls.should == ['SELECT * FROM bands', "SELECT name FROM albums WHERE ((albums.band_id IN (2)) AND (name = 'good'))"]
  end
end

describe Sequel::Model, "#eager_graph" do
  before(:all) do
    class ::GraphAlbum < Sequel::Model(:albums)
      dataset.opts[:from] = [:albums]
      columns :id, :band_id
      many_to_one :band, :class=>'GraphBand', :key=>:band_id
      one_to_many :tracks, :class=>'GraphTrack', :key=>:album_id
      many_to_many :genres, :class=>'GraphGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag
      many_to_one :previous_album, :class=>'GraphAlbum'
    end

    class ::GraphBand < Sequel::Model(:bands)
      dataset.opts[:from] = [:bands]
      columns :id, :vocalist_id
      many_to_one :vocalist, :class=>'GraphBandMember', :key=>:vocalist_id
      one_to_many :albums, :class=>'GraphAlbum', :key=>:band_id
      many_to_many :members, :class=>'GraphBandMember', :left_key=>:band_id, :right_key=>:member_id, :join_table=>:bm
      many_to_many :genres, :class=>'GraphGenre', :left_key=>:band_id, :right_key=>:genre_id, :join_table=>:bg
    end
    
    class ::GraphTrack < Sequel::Model(:tracks)
      dataset.opts[:from] = [:tracks]
      columns :id, :album_id
      many_to_one :album, :class=>'GraphAlbum', :key=>:album_id
    end
    
    class ::GraphGenre < Sequel::Model(:genres)
      dataset.opts[:from] = [:genres]
      columns :id
      many_to_many :albums, :class=>'GraphAlbum', :left_key=>:genre_id, :right_key=>:album_id, :join_table=>:ag
    end
    
    class ::GraphBandMember < Sequel::Model(:members)
      dataset.opts[:from] = [:members]
      columns :id
      many_to_many :bands, :class=>'GraphBand', :left_key=>:member_id, :right_key=>:band_id, :join_table=>:bm
    end
  end
  after(:all) do
    [:GraphAlbum, :GraphBand, :GraphTrack, :GraphGenre, :GraphBandMember].each{|x| Object.send(:remove_const, x)}
  end
    
  it "should raise an error if called without a symbol or hash" do
    proc{GraphAlbum.eager_graph(Object.new)}.should raise_error(Sequel::Error)
  end

  it "should not split results and assign associations if ungraphed is called" do
    ds = GraphAlbum.eager_graph(:band).ungraphed
    ds.sql.should == 'SELECT albums.id, albums.band_id, band.id AS band_id_0, band.vocalist_id FROM albums LEFT OUTER JOIN bands AS band ON (band.id = albums.band_id)'
    ds._fetch = {:id=>1, :band_id=>2, :band_id_0=>2, :vocalist_id=>3}
    ds.all.should == [GraphAlbum.load(:id=>1, :band_id=>2, :band_id_0=>2, :vocalist_id=>3)]
  end

  it "should eagerly load a single many_to_one association" do
    ds = GraphAlbum.eager_graph(:band)
    ds.sql.should == 'SELECT albums.id, albums.band_id, band.id AS band_id_0, band.vocalist_id FROM albums LEFT OUTER JOIN bands AS band ON (band.id = albums.band_id)'
    ds._fetch = {:id=>1, :band_id=>2, :band_id_0=>2, :vocalist_id=>3}
    a = ds.all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(GraphAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    a = a.first
    a.band.should be_a_kind_of(GraphBand)
    a.band.values.should == {:id => 2, :vocalist_id=>3}
  end
  
  it "should eagerly load a single one_to_one association" do
    GraphAlbum.one_to_one :track, :class=>'GraphTrack', :key=>:album_id
    ds = GraphAlbum.eager_graph(:track)
    ds.sql.should == 'SELECT albums.id, albums.band_id, track.id AS track_id, track.album_id FROM albums LEFT OUTER JOIN tracks AS track ON (track.album_id = albums.id)'
    ds._fetch = {:id=>1, :band_id=>2, :track_id=>3, :album_id=>1}
    a = ds.all
    a.should == [GraphAlbum.load(:id => 1, :band_id => 2)]
    a.first.track.should == GraphTrack.load(:id => 3, :album_id=>1)
  end

  it "should eagerly load a single one_to_many association" do
    ds = GraphAlbum.eager_graph(:tracks)
    ds.sql.should == 'SELECT albums.id, albums.band_id, tracks.id AS tracks_id, tracks.album_id FROM albums LEFT OUTER JOIN tracks ON (tracks.album_id = albums.id)'
    ds._fetch = {:id=>1, :band_id=>2, :tracks_id=>3, :album_id=>1}
    a = ds.all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(GraphAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    a = a.first
    a.tracks.should be_a_kind_of(Array)
    a.tracks.size.should == 1
    a.tracks.first.should be_a_kind_of(GraphTrack)
    a.tracks.first.values.should == {:id => 3, :album_id=>1}
  end

  it "should eagerly load a single many_to_many association" do
    ds = GraphAlbum.eager_graph(:genres)
    ds.sql.should == 'SELECT albums.id, albums.band_id, genres.id AS genres_id FROM albums LEFT OUTER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN genres ON (genres.id = ag.genre_id)'
    ds._fetch = {:id=>1, :band_id=>2, :genres_id=>4}
    a = ds.all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(GraphAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    a = a.first
    a.genres.should be_a_kind_of(Array)
    a.genres.size.should == 1
    a.genres.first.should be_a_kind_of(GraphGenre)
    a.genres.first.values.should == {:id => 4}
  end

  it "should correctly handle an aliased join table in many_to_many" do
    c = Class.new(GraphAlbum)
    c.many_to_many :genres, :clone=>:genres, :join_table=>:ag___ga
    c.eager_graph(:genres).sql.should == 'SELECT albums.id, albums.band_id, genres.id AS genres_id FROM albums LEFT OUTER JOIN ag AS ga ON (ga.album_id = albums.id) LEFT OUTER JOIN genres ON (genres.id = ga.genre_id)'

    c.many_to_many :genres, :clone=>:genres, :join_table=>:ag___albums
    c.eager_graph(:genres).sql.should == 'SELECT albums.id, albums.band_id, genres.id AS genres_id FROM albums LEFT OUTER JOIN ag AS albums_0 ON (albums_0.album_id = albums.id) LEFT OUTER JOIN genres ON (genres.id = albums_0.genre_id)'

    c.many_to_many :genres, :clone=>:genres, :join_table=>:ag___genres
    c.eager_graph(:genres).sql.should == 'SELECT albums.id, albums.band_id, genres.id AS genres_id FROM albums LEFT OUTER JOIN ag AS genres_0 ON (genres_0.album_id = albums.id) LEFT OUTER JOIN genres ON (genres.id = genres_0.genre_id)'
  end
  
  it "should eagerly load multiple associations in a single call" do 
    ds = GraphAlbum.eager_graph(:genres, :tracks, :band)
    ds.sql.should == 'SELECT albums.id, albums.band_id, genres.id AS genres_id, tracks.id AS tracks_id, tracks.album_id, band.id AS band_id_0, band.vocalist_id FROM albums LEFT OUTER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN genres ON (genres.id = ag.genre_id) LEFT OUTER JOIN tracks ON (tracks.album_id = albums.id) LEFT OUTER JOIN bands AS band ON (band.id = albums.band_id)'
    ds._fetch = {:id=>1, :band_id=>2, :genres_id=>4, :tracks_id=>3, :album_id=>1, :band_id_0=>2, :vocalist_id=>6}
    a = ds.all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(GraphAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    a = a.first
    a.band.should be_a_kind_of(GraphBand)
    a.band.values.should == {:id => 2, :vocalist_id=>6}
    a.tracks.should be_a_kind_of(Array)
    a.tracks.size.should == 1
    a.tracks.first.should be_a_kind_of(GraphTrack)
    a.tracks.first.values.should == {:id => 3, :album_id=>1}
    a.genres.should be_a_kind_of(Array)
    a.genres.size.should == 1
    a.genres.first.should be_a_kind_of(GraphGenre)
    a.genres.first.values.should == {:id => 4}
  end

  it "should eagerly load multiple associations in separate calls" do 
    ds = GraphAlbum.eager_graph(:genres).eager_graph(:tracks).eager_graph(:band)
    ds.sql.should == 'SELECT albums.id, albums.band_id, genres.id AS genres_id, tracks.id AS tracks_id, tracks.album_id, band.id AS band_id_0, band.vocalist_id FROM albums LEFT OUTER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN genres ON (genres.id = ag.genre_id) LEFT OUTER JOIN tracks ON (tracks.album_id = albums.id) LEFT OUTER JOIN bands AS band ON (band.id = albums.band_id)'
    ds._fetch = {:id=>1, :band_id=>2, :genres_id=>4, :tracks_id=>3, :album_id=>1, :band_id_0=>2, :vocalist_id=>6}
    a = ds.all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(GraphAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    a = a.first
    a.band.should be_a_kind_of(GraphBand)
    a.band.values.should == {:id => 2, :vocalist_id=>6}
    a.tracks.should be_a_kind_of(Array)
    a.tracks.size.should == 1
    a.tracks.first.should be_a_kind_of(GraphTrack)
    a.tracks.first.values.should == {:id => 3, :album_id=>1}
    a.genres.should be_a_kind_of(Array)
    a.genres.size.should == 1
    a.genres.first.should be_a_kind_of(GraphGenre)
    a.genres.first.values.should == {:id => 4}
  end

  it "should allow cascading of eager loading for associations of associated models" do
    ds = GraphTrack.eager_graph(:album=>{:band=>:members})
    ds.sql.should == 'SELECT tracks.id, tracks.album_id, album.id AS album_id_0, album.band_id, band.id AS band_id_0, band.vocalist_id, members.id AS members_id FROM tracks LEFT OUTER JOIN albums AS album ON (album.id = tracks.album_id) LEFT OUTER JOIN bands AS band ON (band.id = album.band_id) LEFT OUTER JOIN bm ON (bm.band_id = band.id) LEFT OUTER JOIN members ON (members.id = bm.member_id)'
    ds._fetch = {:id=>3, :album_id=>1, :album_id_0=>1, :band_id=>2, :members_id=>5, :band_id_0=>2, :vocalist_id=>6}
    a = ds.all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(GraphTrack)
    a.first.values.should == {:id => 3, :album_id => 1}
    a = a.first
    a.album.should be_a_kind_of(GraphAlbum)
    a.album.values.should == {:id => 1, :band_id => 2}
    a.album.band.should be_a_kind_of(GraphBand)
    a.album.band.values.should == {:id => 2, :vocalist_id=>6}
    a.album.band.members.should be_a_kind_of(Array)
    a.album.band.members.size.should == 1
    a.album.band.members.first.should be_a_kind_of(GraphBandMember)
    a.album.band.members.first.values.should == {:id => 5}
  end
  
  it "should allow cascading of eager loading for multiple *_to_many associations, eliminating duplicates caused by cartesian products" do
    ds = GraphBand.eager_graph({:albums=>:tracks}, :members)
    ds.sql.should == 'SELECT bands.id, bands.vocalist_id, albums.id AS albums_id, albums.band_id, tracks.id AS tracks_id, tracks.album_id, members.id AS members_id FROM bands LEFT OUTER JOIN albums ON (albums.band_id = bands.id) LEFT OUTER JOIN tracks ON (tracks.album_id = albums.id) LEFT OUTER JOIN bm ON (bm.band_id = bands.id) LEFT OUTER JOIN members ON (members.id = bm.member_id)'
    ds._fetch = [{:id=>1, :vocalist_id=>2, :albums_id=>3, :band_id=>1, :tracks_id=>4, :album_id=>3, :members_id=>5},
      {:id=>1, :vocalist_id=>2, :albums_id=>3, :band_id=>1, :tracks_id=>4, :album_id=>3, :members_id=>6},
      {:id=>1, :vocalist_id=>2, :albums_id=>3, :band_id=>1, :tracks_id=>5, :album_id=>3, :members_id=>5},
      {:id=>1, :vocalist_id=>2, :albums_id=>3, :band_id=>1, :tracks_id=>5, :album_id=>3, :members_id=>6},
      {:id=>1, :vocalist_id=>2, :albums_id=>4, :band_id=>1, :tracks_id=>6, :album_id=>4, :members_id=>5},
      {:id=>1, :vocalist_id=>2, :albums_id=>4, :band_id=>1, :tracks_id=>6, :album_id=>4, :members_id=>6},
      {:id=>1, :vocalist_id=>2, :albums_id=>4, :band_id=>1, :tracks_id=>7, :album_id=>4, :members_id=>5},
      {:id=>1, :vocalist_id=>2, :albums_id=>4, :band_id=>1, :tracks_id=>7, :album_id=>4, :members_id=>6},
      {:id=>2, :vocalist_id=>2, :albums_id=>5, :band_id=>2, :tracks_id=>8, :album_id=>5, :members_id=>5},
      {:id=>2, :vocalist_id=>2, :albums_id=>5, :band_id=>2, :tracks_id=>8, :album_id=>5, :members_id=>6},
      {:id=>2, :vocalist_id=>2, :albums_id=>5, :band_id=>2, :tracks_id=>9, :album_id=>5, :members_id=>5},
      {:id=>2, :vocalist_id=>2, :albums_id=>5, :band_id=>2, :tracks_id=>9, :album_id=>5, :members_id=>6},
      {:id=>2, :vocalist_id=>2, :albums_id=>6, :band_id=>2, :tracks_id=>1, :album_id=>6, :members_id=>5},
      {:id=>2, :vocalist_id=>2, :albums_id=>6, :band_id=>2, :tracks_id=>1, :album_id=>6, :members_id=>6},
      {:id=>2, :vocalist_id=>2, :albums_id=>6, :band_id=>2, :tracks_id=>2, :album_id=>6, :members_id=>5},
      {:id=>2, :vocalist_id=>2, :albums_id=>6, :band_id=>2, :tracks_id=>2, :album_id=>6, :members_id=>6}]
    a = ds.all
    a.should == [GraphBand.load(:id=>1, :vocalist_id=>2), GraphBand.load(:id=>2, :vocalist_id=>2)]
    members = a.map{|x| x.members}
    members.should == [[GraphBandMember.load(:id=>5), GraphBandMember.load(:id=>6)], [GraphBandMember.load(:id=>5), GraphBandMember.load(:id=>6)]]
    albums = a.map{|x| x.albums}
    albums.should == [[GraphAlbum.load(:id=>3, :band_id=>1), GraphAlbum.load(:id=>4, :band_id=>1)], [GraphAlbum.load(:id=>5, :band_id=>2), GraphAlbum.load(:id=>6, :band_id=>2)]]
    tracks = albums.map{|x| x.map{|y| y.tracks}}
    tracks.should == [[[GraphTrack.load(:id=>4, :album_id=>3), GraphTrack.load(:id=>5, :album_id=>3)], [GraphTrack.load(:id=>6, :album_id=>4), GraphTrack.load(:id=>7, :album_id=>4)]], [[GraphTrack.load(:id=>8, :album_id=>5), GraphTrack.load(:id=>9, :album_id=>5)], [GraphTrack.load(:id=>1, :album_id=>6), GraphTrack.load(:id=>2, :album_id=>6)]]]
  end
  
  it "should populate the reciprocal many_to_one association when eagerly loading the one_to_many association" do
    MODEL_DB.reset
    ds = GraphAlbum.eager_graph(:tracks)
    ds.sql.should == 'SELECT albums.id, albums.band_id, tracks.id AS tracks_id, tracks.album_id FROM albums LEFT OUTER JOIN tracks ON (tracks.album_id = albums.id)'
    ds._fetch = {:id=>1, :band_id=>2, :tracks_id=>3, :album_id=>1}
    a = ds.all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(GraphAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    a = a.first
    a.tracks.should be_a_kind_of(Array)
    a.tracks.size.should == 1
    a.tracks.first.should be_a_kind_of(GraphTrack)
    a.tracks.first.values.should == {:id => 3, :album_id=>1}
    a.tracks.first.album.should be_a_kind_of(GraphAlbum)
    a.tracks.first.album.should == a
    MODEL_DB.sqls.length.should == 1
  end

  it "should eager load multiple associations from the same table" do
    ds = GraphBand.eager_graph(:vocalist, :members)
    ds.sql.should == 'SELECT bands.id, bands.vocalist_id, vocalist.id AS vocalist_id_0, members.id AS members_id FROM bands LEFT OUTER JOIN members AS vocalist ON (vocalist.id = bands.vocalist_id) LEFT OUTER JOIN bm ON (bm.band_id = bands.id) LEFT OUTER JOIN members ON (members.id = bm.member_id)'
    ds._fetch = {:id=>2, :vocalist_id=>6, :vocalist_id_0=>6, :members_id=>5}
    a = ds.all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(GraphBand)
    a.first.values.should == {:id => 2, :vocalist_id => 6}
    a = a.first
    a.vocalist.should be_a_kind_of(GraphBandMember)
    a.vocalist.values.should == {:id => 6}
    a.members.should be_a_kind_of(Array)
    a.members.size.should == 1
    a.members.first.should be_a_kind_of(GraphBandMember)
    a.members.first.values.should == {:id => 5}
  end

  it "should give you a plain hash when called without .all" do 
    ds = GraphAlbum.eager_graph(:band)
    ds.sql.should == 'SELECT albums.id, albums.band_id, band.id AS band_id_0, band.vocalist_id FROM albums LEFT OUTER JOIN bands AS band ON (band.id = albums.band_id)'
    ds._fetch = {:id=>1, :band_id=>2, :band_id_0=>2, :vocalist_id=>3}
    ds.first.should == {:id=>1, :band_id=>2, :band_id_0=>2, :vocalist_id=>3}
  end

  it "should not drop any associated objects if the graph could not be a cartesian product" do
    ds = GraphBand.eager_graph(:members, :vocalist)
    ds.sql.should == 'SELECT bands.id, bands.vocalist_id, members.id AS members_id, vocalist.id AS vocalist_id_0 FROM bands LEFT OUTER JOIN bm ON (bm.band_id = bands.id) LEFT OUTER JOIN members ON (members.id = bm.member_id) LEFT OUTER JOIN members AS vocalist ON (vocalist.id = bands.vocalist_id)'
    ds._fetch = [{:id=>2, :vocalist_id=>6, :members_id=>5, :vocalist_id_0=>6}, {:id=>2, :vocalist_id=>6, :members_id=>5, :vocalist_id_0=>6}]
    a = ds.all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(GraphBand)
    a.first.values.should == {:id => 2, :vocalist_id => 6}
    a = a.first
    a.vocalist.should be_a_kind_of(GraphBandMember)
    a.vocalist.values.should == {:id => 6}
    a.members.should be_a_kind_of(Array)
    a.members.size.should == 2
    a.members.first.should be_a_kind_of(GraphBandMember)
    a.members.first.values.should == {:id => 5}
    a.members.last.should be_a_kind_of(GraphBandMember)
    a.members.last.values.should == {:id => 5}
  end

  it "should respect the :cartesian_product_number option" do 
    GraphBand.many_to_one :other_vocalist, :class=>'GraphBandMember', :key=>:vocalist_id, :cartesian_product_number=>1
    ds = GraphBand.eager_graph(:members, :other_vocalist)
    ds.sql.should == 'SELECT bands.id, bands.vocalist_id, members.id AS members_id, other_vocalist.id AS other_vocalist_id FROM bands LEFT OUTER JOIN bm ON (bm.band_id = bands.id) LEFT OUTER JOIN members ON (members.id = bm.member_id) LEFT OUTER JOIN members AS other_vocalist ON (other_vocalist.id = bands.vocalist_id)'
    ds._fetch = [{:id=>2, :vocalist_id=>6, :members_id=>5, :other_vocalist_id=>6}, {:id=>2, :vocalist_id=>6, :members_id=>5, :other_vocalist_id=>6}]
    a = ds.all
    a.should == [GraphBand.load(:id=>2, :vocalist_id => 6)]
    a.first.other_vocalist.should == GraphBandMember.load(:id=>6)
    a.first.members.should == [GraphBandMember.load(:id=>5)]
  end

  it "should drop duplicate items that occur in sequence if the graph could be a cartesian product" do
    ds = GraphBand.eager_graph(:members, :genres)
    ds.sql.should == 'SELECT bands.id, bands.vocalist_id, members.id AS members_id, genres.id AS genres_id FROM bands LEFT OUTER JOIN bm ON (bm.band_id = bands.id) LEFT OUTER JOIN members ON (members.id = bm.member_id) LEFT OUTER JOIN bg ON (bg.band_id = bands.id) LEFT OUTER JOIN genres ON (genres.id = bg.genre_id)'
    ds._fetch = [{:id=>2, :vocalist_id=>6, :members_id=>5, :genres_id=>7},
      {:id=>2, :vocalist_id=>6, :members_id=>5, :genres_id=>8},
      {:id=>2, :vocalist_id=>6, :members_id=>6, :genres_id=>7},
      {:id=>2, :vocalist_id=>6, :members_id=>6, :genres_id=>8}]
    a = ds.all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(GraphBand)
    a.first.values.should == {:id => 2, :vocalist_id => 6}
    a = a.first
    a.members.should be_a_kind_of(Array)
    a.members.size.should == 2
    a.members.first.should be_a_kind_of(GraphBandMember)
    a.members.first.values.should == {:id => 5}
    a.members.last.should be_a_kind_of(GraphBandMember)
    a.members.last.values.should == {:id => 6}
    a.genres.size.should == 2
    a.genres.first.should be_a_kind_of(GraphGenre)
    a.genres.first.values.should == {:id => 7}
    a.genres.last.should be_a_kind_of(GraphGenre)
    a.genres.last.values.should == {:id => 8}
  end

  it "should be able to be used in combination with #eager" do
    MODEL_DB.reset
    ds = GraphAlbum.eager_graph(:tracks).eager(:genres)
    ds._fetch = {:id=>1, :band_id=>2, :tracks_id=>3, :album_id=>1}
    ds2 = GraphGenre.dataset
    ds2._fetch = {:id=>6, :x_foreign_key_x=>1}
    a = ds.all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(GraphAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    a = a.first
    a.tracks.should be_a_kind_of(Array)
    a.tracks.size.should == 1
    a.tracks.first.should be_a_kind_of(GraphTrack)
    a.tracks.first.values.should == {:id=>3, :album_id=>1}
    a.genres.should be_a_kind_of(Array)
    a.genres.size.should == 1
    a.genres.first.should be_a_kind_of(GraphGenre)
    a.genres.first.values.should == {:id=>6}
    MODEL_DB.sqls.should == ['SELECT albums.id, albums.band_id, tracks.id AS tracks_id, tracks.album_id FROM albums LEFT OUTER JOIN tracks ON (tracks.album_id = albums.id)',
    "SELECT genres.*, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON ((ag.genre_id = genres.id) AND (ag.album_id IN (1)))"]
  end

  it "should handle no associated records for a single many_to_one association" do
    ds = GraphAlbum.eager_graph(:band)
    ds.sql.should == 'SELECT albums.id, albums.band_id, band.id AS band_id_0, band.vocalist_id FROM albums LEFT OUTER JOIN bands AS band ON (band.id = albums.band_id)'
    ds._fetch = {:id=>1, :band_id=>2, :band_id_0=>nil, :vocalist_id=>nil}
    a = ds.all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(GraphAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    a.first.associations.fetch(:band, 2).should == nil
  end

  it "should handle no associated records for a single one_to_many association" do
    ds = GraphAlbum.eager_graph(:tracks)
    ds.sql.should == 'SELECT albums.id, albums.band_id, tracks.id AS tracks_id, tracks.album_id FROM albums LEFT OUTER JOIN tracks ON (tracks.album_id = albums.id)'
    ds._fetch = {:id=>1, :band_id=>2, :tracks_id=>nil, :album_id=>nil}
    a = ds.all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(GraphAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    a.first.tracks.should == []
  end

  it "should handle no associated records for a single many_to_many association" do
    ds = GraphAlbum.eager_graph(:genres)
    ds.sql.should == 'SELECT albums.id, albums.band_id, genres.id AS genres_id FROM albums LEFT OUTER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN genres ON (genres.id = ag.genre_id)'
    ds._fetch = {:id=>1, :band_id=>2, :genres_id=>nil}
    a = ds.all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(GraphAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    a.first.genres.should == []
  end

  it "should handle missing associated records when loading multiple associations" do 
    ds = GraphAlbum.eager_graph(:genres, :tracks, :band)
    ds.sql.should == 'SELECT albums.id, albums.band_id, genres.id AS genres_id, tracks.id AS tracks_id, tracks.album_id, band.id AS band_id_0, band.vocalist_id FROM albums LEFT OUTER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN genres ON (genres.id = ag.genre_id) LEFT OUTER JOIN tracks ON (tracks.album_id = albums.id) LEFT OUTER JOIN bands AS band ON (band.id = albums.band_id)'
    ds._fetch = [{:id=>1, :band_id=>2, :genres_id=>nil, :tracks_id=>3, :album_id=>1, :band_id_0=>nil, :vocalist_id=>nil},
      {:id=>1, :band_id=>2, :genres_id=>nil, :tracks_id=>4, :album_id=>1, :band_id_0=>nil, :vocalist_id=>nil},
      {:id=>1, :band_id=>2, :genres_id=>nil, :tracks_id=>5, :album_id=>1, :band_id_0=>nil, :vocalist_id=>nil},
      {:id=>1, :band_id=>2, :genres_id=>nil, :tracks_id=>6, :album_id=>1, :band_id_0=>nil, :vocalist_id=>nil}]
    a = ds.all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(GraphAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    a = a.first
    a.tracks.should be_a_kind_of(Array)
    a.tracks.size.should == 4
    a.tracks.first.should be_a_kind_of(GraphTrack)
    a.tracks.collect{|x|x[:id]}.should == [3,4,5,6]
    a.associations.fetch(:band, 2).should == nil
    a.genres.should == []
  end

  it "should handle missing associated records when cascading eager loading for associations of associated models" do
    ds = GraphTrack.eager_graph(:album=>{:band=>:members})
    ds.sql.should == 'SELECT tracks.id, tracks.album_id, album.id AS album_id_0, album.band_id, band.id AS band_id_0, band.vocalist_id, members.id AS members_id FROM tracks LEFT OUTER JOIN albums AS album ON (album.id = tracks.album_id) LEFT OUTER JOIN bands AS band ON (band.id = album.band_id) LEFT OUTER JOIN bm ON (bm.band_id = band.id) LEFT OUTER JOIN members ON (members.id = bm.member_id)'
    ds._fetch = [{:id=>2, :album_id=>2, :album_id_0=>nil, :band_id=>nil, :members_id=>nil, :band_id_0=>nil, :vocalist_id=>nil},
      {:id=>3, :album_id=>3, :album_id_0=>3, :band_id=>3, :members_id=>nil, :band_id_0=>nil, :vocalist_id=>nil},
      {:id=>4, :album_id=>4, :album_id_0=>4, :band_id=>2, :members_id=>nil, :band_id_0=>2, :vocalist_id=>6},
      {:id=>5, :album_id=>1, :album_id_0=>1, :band_id=>4, :members_id=>5, :band_id_0=>4, :vocalist_id=>8},
      {:id=>5, :album_id=>1, :album_id_0=>1, :band_id=>4, :members_id=>6, :band_id_0=>4, :vocalist_id=>8}]
    a = ds.all
    a.should be_a_kind_of(Array)
    a.size.should == 4
    a.first.should be_a_kind_of(GraphTrack)
    a.collect{|x|x[:id]}.should == [2,3,4,5]
    a[0].associations.fetch(:album, 2).should == nil
    a[1].album.should be_a_kind_of(GraphAlbum)
    a[1].album.values.should == {:id => 3, :band_id => 3}
    a[1].album.associations.fetch(:band, 2).should == nil
    a[2].album.should be_a_kind_of(GraphAlbum)
    a[2].album.values.should == {:id => 4, :band_id => 2}
    a[2].album.band.should be_a_kind_of(GraphBand)
    a[2].album.band.values.should == {:id => 2, :vocalist_id=>6}
    a[2].album.band.members.should == []
    a[3].album.should be_a_kind_of(GraphAlbum)
    a[3].album.values.should == {:id => 1, :band_id => 4}
    a[3].album.band.should be_a_kind_of(GraphBand)
    a[3].album.band.values.should == {:id => 4, :vocalist_id=>8}
    a[3].album.band.members.size.should == 2
    a[3].album.band.members.first.should be_a_kind_of(GraphBandMember)
    a[3].album.band.members.first.values.should == {:id => 5}
    a[3].album.band.members.last.should be_a_kind_of(GraphBandMember)
    a[3].album.band.members.last.values.should == {:id => 6}
  end

  it "should respect the association's :primary_key option" do 
    GraphAlbum.many_to_one :inner_band, :class=>'GraphBand', :key=>:band_id, :primary_key=>:vocalist_id
    ds = GraphAlbum.eager_graph(:inner_band)
    ds.sql.should == 'SELECT albums.id, albums.band_id, inner_band.id AS inner_band_id, inner_band.vocalist_id FROM albums LEFT OUTER JOIN bands AS inner_band ON (inner_band.vocalist_id = albums.band_id)'
    ds._fetch = {:id=>3, :band_id=>2, :inner_band_id=>5, :vocalist_id=>2}
    as = ds.all
    as.should == [GraphAlbum.load(:id=>3, :band_id=>2)]
    as.first.inner_band.should == GraphBand.load(:id=>5, :vocalist_id=>2)

    GraphAlbum.one_to_many :right_tracks, :class=>'GraphTrack', :key=>:album_id, :primary_key=>:band_id
    ds = GraphAlbum.eager_graph(:right_tracks)
    ds.sql.should == 'SELECT albums.id, albums.band_id, right_tracks.id AS right_tracks_id, right_tracks.album_id FROM albums LEFT OUTER JOIN tracks AS right_tracks ON (right_tracks.album_id = albums.band_id)'
    ds._fetch = [{:id=>3, :band_id=>2, :right_tracks_id=>5, :album_id=>2}, {:id=>3, :band_id=>2, :right_tracks_id=>6, :album_id=>2}]
    as = ds.all
    as.should == [GraphAlbum.load(:id=>3, :band_id=>2)]
    as.first.right_tracks.should == [GraphTrack.load(:id=>5, :album_id=>2), GraphTrack.load(:id=>6, :album_id=>2)]
  end
  
  it "should respect many_to_one association's composite keys" do 
    GraphAlbum.many_to_one :inner_band, :class=>'GraphBand', :key=>[:band_id, :id], :primary_key=>[:vocalist_id, :id]
    ds = GraphAlbum.eager_graph(:inner_band)
    ds.sql.should == 'SELECT albums.id, albums.band_id, inner_band.id AS inner_band_id, inner_band.vocalist_id FROM albums LEFT OUTER JOIN bands AS inner_band ON ((inner_band.vocalist_id = albums.band_id) AND (inner_band.id = albums.id))'
    ds._fetch = {:id=>3, :band_id=>2, :inner_band_id=>3, :vocalist_id=>2}
    as = ds.all
    as.should == [GraphAlbum.load(:id=>3, :band_id=>2)]
    as.first.inner_band.should == GraphBand.load(:id=>3, :vocalist_id=>2)
  end

  it "should respect one_to_many association's composite keys" do 
    GraphAlbum.one_to_many :right_tracks, :class=>'GraphTrack', :key=>[:album_id, :id], :primary_key=>[:band_id, :id]
    ds = GraphAlbum.eager_graph(:right_tracks)
    ds.sql.should == 'SELECT albums.id, albums.band_id, right_tracks.id AS right_tracks_id, right_tracks.album_id FROM albums LEFT OUTER JOIN tracks AS right_tracks ON ((right_tracks.album_id = albums.band_id) AND (right_tracks.id = albums.id))'
    ds._fetch = {:id=>3, :band_id=>2, :right_tracks_id=>3, :album_id=>2}
    as = ds.all
    as.should == [GraphAlbum.load(:id=>3, :band_id=>2)]
    as.first.right_tracks.should == [GraphTrack.load(:id=>3, :album_id=>2)]
  end
  
  it "should respect many_to_many association's composite keys" do 
    GraphAlbum.many_to_many :sbands, :class=>'GraphBand', :left_key=>[:l1, :l2], :left_primary_key=>[:band_id, :id], :right_key=>[:r1, :r2], :right_primary_key=>[:vocalist_id, :id], :join_table=>:b
    ds = GraphAlbum.eager_graph(:sbands)
    ds.sql.should == 'SELECT albums.id, albums.band_id, sbands.id AS sbands_id, sbands.vocalist_id FROM albums LEFT OUTER JOIN b ON ((b.l1 = albums.band_id) AND (b.l2 = albums.id)) LEFT OUTER JOIN bands AS sbands ON ((sbands.vocalist_id = b.r1) AND (sbands.id = b.r2))'
    ds._fetch = [{:id=>3, :band_id=>2, :sbands_id=>5, :vocalist_id=>6}, {:id=>3, :band_id=>2, :sbands_id=>6, :vocalist_id=>22}]
    as = ds.all
    as.should == [GraphAlbum.load(:id=>3, :band_id=>2)]
    as.first.sbands.should == [GraphBand.load(:id=>5, :vocalist_id=>6), GraphBand.load(:id=>6, :vocalist_id=>22)]
  end

  it "should respect many_to_many association's :left_primary_key and :right_primary_key options" do 
    GraphAlbum.many_to_many :inner_genres, :class=>'GraphGenre', :left_key=>:album_id, :left_primary_key=>:band_id, :right_key=>:genre_id, :right_primary_key=>:xxx, :join_table=>:ag
    ds = GraphAlbum.eager_graph(:inner_genres)
    ds.sql.should == 'SELECT albums.id, albums.band_id, inner_genres.id AS inner_genres_id FROM albums LEFT OUTER JOIN ag ON (ag.album_id = albums.band_id) LEFT OUTER JOIN genres AS inner_genres ON (inner_genres.xxx = ag.genre_id)'
    ds._fetch = [{:id=>3, :band_id=>2, :inner_genres_id=>5, :xxx=>12}, {:id=>3, :band_id=>2, :inner_genres_id=>6, :xxx=>22}]
    as = ds.all
    as.should == [GraphAlbum.load(:id=>3, :band_id=>2)]
    as.first.inner_genres.should == [GraphGenre.load(:id=>5), GraphGenre.load(:id=>6)]
  end

  it "should respect composite primary keys for classes when eager loading" do 
    c1 = Class.new(GraphAlbum)
    c2 = Class.new(GraphBand)
    c1.set_primary_key [:band_id, :id]
    c2.set_primary_key [:vocalist_id, :id]
    c1.many_to_many :sbands, :class=>c2, :left_key=>[:l1, :l2], :right_key=>[:r1, :r2], :join_table=>:b
    c2.one_to_many :salbums, :class=>c1, :key=>[:band_id, :id]
    ds = c1.eager_graph(:sbands=>:salbums)
    ds.sql.should == 'SELECT albums.id, albums.band_id, sbands.id AS sbands_id, sbands.vocalist_id, salbums.id AS salbums_id, salbums.band_id AS salbums_band_id FROM albums LEFT OUTER JOIN b ON ((b.l1 = albums.band_id) AND (b.l2 = albums.id)) LEFT OUTER JOIN bands AS sbands ON ((sbands.vocalist_id = b.r1) AND (sbands.id = b.r2)) LEFT OUTER JOIN albums AS salbums ON ((salbums.band_id = sbands.vocalist_id) AND (salbums.id = sbands.id))'
    ds._fetch = [{:id=>3, :band_id=>2, :sbands_id=>5, :vocalist_id=>6, :salbums_id=>7, :salbums_band_id=>8},
      {:id=>3, :band_id=>2, :sbands_id=>5, :vocalist_id=>6, :salbums_id=>9, :salbums_band_id=>10},
      {:id=>3, :band_id=>2, :sbands_id=>6, :vocalist_id=>22, :salbums_id=>nil, :salbums_band_id=>nil},
      {:id=>7, :band_id=>8, :sbands_id=>nil, :vocalist_id=>nil, :salbums_id=>nil, :salbums_band_id=>nil}]
    as = ds.all
    as.should == [c1.load(:id=>3, :band_id=>2), c1.load(:id=>7, :band_id=>8)]
    as.map{|x| x.sbands}.should == [[c2.load(:id=>5, :vocalist_id=>6), c2.load(:id=>6, :vocalist_id=>22)], []]
    as.map{|x| x.sbands.map{|y| y.salbums}}.should == [[[c1.load(:id=>7, :band_id=>8), c1.load(:id=>9, :band_id=>10)], []], []]
  end

  it "should respect the association's :graph_select option" do 
    GraphAlbum.many_to_one :inner_band, :class=>'GraphBand', :key=>:band_id, :graph_select=>:vocalist_id
    GraphAlbum.eager_graph(:inner_band).sql.should == 'SELECT albums.id, albums.band_id, inner_band.vocalist_id FROM albums LEFT OUTER JOIN bands AS inner_band ON (inner_band.id = albums.band_id)'

    GraphAlbum.one_to_many :right_tracks, :class=>'GraphTrack', :key=>:album_id, :graph_select=>[:album_id]
    GraphAlbum.eager_graph(:right_tracks).sql.should == 'SELECT albums.id, albums.band_id, right_tracks.album_id FROM albums LEFT OUTER JOIN tracks AS right_tracks ON (right_tracks.album_id = albums.id)'

    GraphAlbum.many_to_many :inner_genres, :class=>'GraphGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :graph_select=>[]
    GraphAlbum.eager_graph(:inner_genres).sql.should == 'SELECT albums.id, albums.band_id FROM albums LEFT OUTER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN genres AS inner_genres ON (inner_genres.id = ag.genre_id)'
  end

  it "should respect the association's :graph_join_type option" do 
    GraphAlbum.many_to_one :inner_band, :class=>'GraphBand', :key=>:band_id, :graph_join_type=>:inner
    GraphAlbum.eager_graph(:inner_band).sql.should == 'SELECT albums.id, albums.band_id, inner_band.id AS inner_band_id, inner_band.vocalist_id FROM albums INNER JOIN bands AS inner_band ON (inner_band.id = albums.band_id)'

    GraphAlbum.one_to_many :right_tracks, :class=>'GraphTrack', :key=>:album_id, :graph_join_type=>:right_outer
    GraphAlbum.eager_graph(:right_tracks).sql.should == 'SELECT albums.id, albums.band_id, right_tracks.id AS right_tracks_id, right_tracks.album_id FROM albums RIGHT OUTER JOIN tracks AS right_tracks ON (right_tracks.album_id = albums.id)'

    GraphAlbum.many_to_many :inner_genres, :class=>'GraphGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :graph_join_type=>:inner
    GraphAlbum.eager_graph(:inner_genres).sql.should == 'SELECT albums.id, albums.band_id, inner_genres.id AS inner_genres_id FROM albums INNER JOIN ag ON (ag.album_id = albums.id) INNER JOIN genres AS inner_genres ON (inner_genres.id = ag.genre_id)'
  end

  it "should respect the association's :graph_join_table_join_type option" do 
    GraphAlbum.many_to_many :inner_genres, :class=>'GraphGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :graph_join_table_join_type=>:inner
    GraphAlbum.eager_graph(:inner_genres).sql.should == 'SELECT albums.id, albums.band_id, inner_genres.id AS inner_genres_id FROM albums INNER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN genres AS inner_genres ON (inner_genres.id = ag.genre_id)'

    GraphAlbum.many_to_many :inner_genres, :class=>'GraphGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :graph_join_table_join_type=>:inner, :graph_join_type=>:right_outer
    GraphAlbum.eager_graph(:inner_genres).sql.should == 'SELECT albums.id, albums.band_id, inner_genres.id AS inner_genres_id FROM albums INNER JOIN ag ON (ag.album_id = albums.id) RIGHT OUTER JOIN genres AS inner_genres ON (inner_genres.id = ag.genre_id)'
  end

  it "should respect the association's :conditions option" do 
    GraphAlbum.many_to_one :active_band, :class=>'GraphBand', :key=>:band_id, :conditions=>{:active=>true}
    GraphAlbum.eager_graph(:active_band).sql.should == "SELECT albums.id, albums.band_id, active_band.id AS active_band_id, active_band.vocalist_id FROM albums LEFT OUTER JOIN bands AS active_band ON ((active_band.id = albums.band_id) AND (active_band.active IS TRUE))"

    GraphAlbum.one_to_many :right_tracks, :class=>'GraphTrack', :key=>:album_id, :conditions=>{:id=>(0..100)}
    GraphAlbum.eager_graph(:right_tracks).sql.should == 'SELECT albums.id, albums.band_id, right_tracks.id AS right_tracks_id, right_tracks.album_id FROM albums LEFT OUTER JOIN tracks AS right_tracks ON ((right_tracks.album_id = albums.id) AND (right_tracks.id >= 0) AND (right_tracks.id <= 100))'

    GraphAlbum.many_to_many :active_genres, :class=>'GraphGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :conditions=>{true=>:active}
    GraphAlbum.eager_graph(:active_genres).sql.should == "SELECT albums.id, albums.band_id, active_genres.id AS active_genres_id FROM albums LEFT OUTER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN genres AS active_genres ON ((active_genres.id = ag.genre_id) AND ('t' = ag.active))"
  end

  it "should respect the association's :graph_conditions option" do 
    GraphAlbum.many_to_one :active_band, :class=>'GraphBand', :key=>:band_id, :graph_conditions=>{:active=>true}
    GraphAlbum.eager_graph(:active_band).sql.should == "SELECT albums.id, albums.band_id, active_band.id AS active_band_id, active_band.vocalist_id FROM albums LEFT OUTER JOIN bands AS active_band ON ((active_band.id = albums.band_id) AND (active_band.active IS TRUE))"

    GraphAlbum.one_to_many :right_tracks, :class=>'GraphTrack', :key=>:album_id, :graph_conditions=>{:id=>(0..100)}
    GraphAlbum.eager_graph(:right_tracks).sql.should == 'SELECT albums.id, albums.band_id, right_tracks.id AS right_tracks_id, right_tracks.album_id FROM albums LEFT OUTER JOIN tracks AS right_tracks ON ((right_tracks.album_id = albums.id) AND (right_tracks.id >= 0) AND (right_tracks.id <= 100))'

    GraphAlbum.many_to_many :active_genres, :class=>'GraphGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :graph_conditions=>{true=>:active}
    GraphAlbum.eager_graph(:active_genres).sql.should == "SELECT albums.id, albums.band_id, active_genres.id AS active_genres_id FROM albums LEFT OUTER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN genres AS active_genres ON ((active_genres.id = ag.genre_id) AND ('t' = ag.active))"
  end

  it "should respect the association's :graph_join_table_conditions option" do 
    GraphAlbum.many_to_many :active_genres, :class=>'GraphGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :graph_join_table_conditions=>{:active=>true}
    GraphAlbum.eager_graph(:active_genres).sql.should == "SELECT albums.id, albums.band_id, active_genres.id AS active_genres_id FROM albums LEFT OUTER JOIN ag ON ((ag.album_id = albums.id) AND (ag.active IS TRUE)) LEFT OUTER JOIN genres AS active_genres ON (active_genres.id = ag.genre_id)"

    GraphAlbum.many_to_many :active_genres, :class=>'GraphGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :graph_conditions=>{true=>:active}, :graph_join_table_conditions=>{true=>:active}
    GraphAlbum.eager_graph(:active_genres).sql.should == "SELECT albums.id, albums.band_id, active_genres.id AS active_genres_id FROM albums LEFT OUTER JOIN ag ON ((ag.album_id = albums.id) AND ('t' = albums.active)) LEFT OUTER JOIN genres AS active_genres ON ((active_genres.id = ag.genre_id) AND ('t' = ag.active))"
  end

  it "should respect the association's :graph_block option" do 
    GraphAlbum.many_to_one :active_band, :class=>'GraphBand', :key=>:band_id, :graph_block=>proc{|ja,lja,js| {:active.qualify(ja)=>true}}
    GraphAlbum.eager_graph(:active_band).sql.should == "SELECT albums.id, albums.band_id, active_band.id AS active_band_id, active_band.vocalist_id FROM albums LEFT OUTER JOIN bands AS active_band ON ((active_band.id = albums.band_id) AND (active_band.active IS TRUE))"

    GraphAlbum.one_to_many :right_tracks, :class=>'GraphTrack', :key=>:album_id, :graph_block=>proc{|ja,lja,js| {:id.qualify(ja)=>(0..100)}}
    GraphAlbum.eager_graph(:right_tracks).sql.should == 'SELECT albums.id, albums.band_id, right_tracks.id AS right_tracks_id, right_tracks.album_id FROM albums LEFT OUTER JOIN tracks AS right_tracks ON ((right_tracks.album_id = albums.id) AND (right_tracks.id >= 0) AND (right_tracks.id <= 100))'

    GraphAlbum.many_to_many :active_genres, :class=>'GraphGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :graph_block=>proc{|ja,lja,js| {true=>:active.qualify(lja)}}
    GraphAlbum.eager_graph(:active_genres).sql.should == "SELECT albums.id, albums.band_id, active_genres.id AS active_genres_id FROM albums LEFT OUTER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN genres AS active_genres ON ((active_genres.id = ag.genre_id) AND ('t' = ag.active))"
  end

  it "should respect the association's :graph_join_block option" do 
    GraphAlbum.many_to_many :active_genres, :class=>'GraphGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :graph_join_table_block=>proc{|ja,lja,js| {:active.qualify(ja)=>true}}
    GraphAlbum.eager_graph(:active_genres).sql.should == "SELECT albums.id, albums.band_id, active_genres.id AS active_genres_id FROM albums LEFT OUTER JOIN ag ON ((ag.album_id = albums.id) AND (ag.active IS TRUE)) LEFT OUTER JOIN genres AS active_genres ON (active_genres.id = ag.genre_id)"

    GraphAlbum.many_to_many :active_genres, :class=>'GraphGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :graph_block=>proc{|ja,lja,js| {true=>:active.qualify(lja)}}, :graph_join_table_block=>proc{|ja,lja,js| {true=>:active.qualify(lja)}}
    GraphAlbum.eager_graph(:active_genres).sql.should == "SELECT albums.id, albums.band_id, active_genres.id AS active_genres_id FROM albums LEFT OUTER JOIN ag ON ((ag.album_id = albums.id) AND ('t' = albums.active)) LEFT OUTER JOIN genres AS active_genres ON ((active_genres.id = ag.genre_id) AND ('t' = ag.active))"
  end

  it "should respect the association's :eager_grapher option" do 
    GraphAlbum.many_to_one :active_band, :class=>'GraphBand', :key=>:band_id, :eager_grapher=>proc{|ds, aa, ta| ds.graph(GraphBand, {:active=>true}, :table_alias=>aa, :join_type=>:inner)}
    GraphAlbum.eager_graph(:active_band).sql.should == "SELECT albums.id, albums.band_id, active_band.id AS active_band_id, active_band.vocalist_id FROM albums INNER JOIN bands AS active_band ON (active_band.active IS TRUE)"

    GraphAlbum.one_to_many :right_tracks, :class=>'GraphTrack', :key=>:album_id, :eager_grapher=>proc{|eo| eo[:self].graph(GraphTrack, nil, :join_type=>:natural, :table_alias=>eo[:table_alias])}
    GraphAlbum.eager_graph(:right_tracks).sql.should == 'SELECT albums.id, albums.band_id, right_tracks.id AS right_tracks_id, right_tracks.album_id FROM albums NATURAL JOIN tracks AS right_tracks'

    GraphAlbum.many_to_many :active_genres, :class=>'GraphGenre', :eager_grapher=>proc{|ds, aa, ta| ds.graph(:ag, {:album_id=>:id}, :table_alias=>:a123, :implicit_qualifier=>ta).graph(GraphGenre, [:album_id], :table_alias=>aa)}
    GraphAlbum.eager_graph(:active_genres).sql.should == "SELECT albums.id, albums.band_id, active_genres.id AS active_genres_id FROM albums LEFT OUTER JOIN ag AS a123 ON (a123.album_id = albums.id) LEFT OUTER JOIN genres AS active_genres USING (album_id)"
  end

  it "should raise an error if you use an :eager_grapher proc with the wrong arity" do
    proc{GraphAlbum.many_to_one :special_band, :eager_grapher=>proc{|a, b|}}.should raise_error(Sequel::Error)
  end

  it "should respect the association's :graph_only_conditions option" do 
    GraphAlbum.many_to_one :active_band, :class=>'GraphBand', :key=>:band_id, :graph_only_conditions=>{:active=>true}
    GraphAlbum.eager_graph(:active_band).sql.should == "SELECT albums.id, albums.band_id, active_band.id AS active_band_id, active_band.vocalist_id FROM albums LEFT OUTER JOIN bands AS active_band ON (active_band.active IS TRUE)"

    GraphAlbum.one_to_many :right_tracks, :class=>'GraphTrack', :key=>:album_id, :graph_only_conditions=>nil, :graph_join_type=>:natural
    GraphAlbum.eager_graph(:right_tracks).sql.should == 'SELECT albums.id, albums.band_id, right_tracks.id AS right_tracks_id, right_tracks.album_id FROM albums NATURAL JOIN tracks AS right_tracks'

    GraphAlbum.many_to_many :active_genres, :class=>'GraphGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :graph_only_conditions=>[:album_id]
    GraphAlbum.eager_graph(:active_genres).sql.should == "SELECT albums.id, albums.band_id, active_genres.id AS active_genres_id FROM albums LEFT OUTER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN genres AS active_genres USING (album_id)"
  end

  it "should respect the association's :graph_join_table_only_conditions option" do 
    GraphAlbum.many_to_many :active_genres, :class=>'GraphGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :graph_join_table_only_conditions=>{:active=>true}
    GraphAlbum.eager_graph(:active_genres).sql.should == "SELECT albums.id, albums.band_id, active_genres.id AS active_genres_id FROM albums LEFT OUTER JOIN ag ON (ag.active IS TRUE) LEFT OUTER JOIN genres AS active_genres ON (active_genres.id = ag.genre_id)"

    GraphAlbum.many_to_many :active_genres, :class=>'GraphGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :graph_only_conditions=>(:price + 2 > 100), :graph_join_table_only_conditions=>"active"
    GraphAlbum.eager_graph(:active_genres).sql.should == "SELECT albums.id, albums.band_id, active_genres.id AS active_genres_id FROM albums LEFT OUTER JOIN ag ON (active) LEFT OUTER JOIN genres AS active_genres ON ((price + 2) > 100)"
  end

  it "should create unique table aliases for all associations" do
    GraphAlbum.eager_graph(:previous_album=>{:previous_album=>:previous_album}).sql.should == "SELECT albums.id, albums.band_id, previous_album.id AS previous_album_id, previous_album.band_id AS previous_album_band_id, previous_album_0.id AS previous_album_0_id, previous_album_0.band_id AS previous_album_0_band_id, previous_album_1.id AS previous_album_1_id, previous_album_1.band_id AS previous_album_1_band_id FROM albums LEFT OUTER JOIN albums AS previous_album ON (previous_album.id = albums.previous_album_id) LEFT OUTER JOIN albums AS previous_album_0 ON (previous_album_0.id = previous_album.previous_album_id) LEFT OUTER JOIN albums AS previous_album_1 ON (previous_album_1.id = previous_album_0.previous_album_id)"
  end

  it "should respect the association's :order" do
    GraphAlbum.one_to_many :right_tracks, :class=>'GraphTrack', :key=>:album_id, :order=>[:id, :album_id]
    GraphAlbum.eager_graph(:right_tracks).sql.should == 'SELECT albums.id, albums.band_id, right_tracks.id AS right_tracks_id, right_tracks.album_id FROM albums LEFT OUTER JOIN tracks AS right_tracks ON (right_tracks.album_id = albums.id) ORDER BY right_tracks.id, right_tracks.album_id'
  end

  it "should only qualify unqualified symbols, identifiers, or ordered versions in association's :order" do
    GraphAlbum.one_to_many :right_tracks, :class=>'GraphTrack', :key=>:album_id, :order=>[:blah__id.identifier, :blah__id.identifier.desc, :blah__id.desc, :blah__id, :album_id, :album_id.desc, 1, 'RANDOM()'.lit, :a.qualify(:b)]
    GraphAlbum.eager_graph(:right_tracks).sql.should == 'SELECT albums.id, albums.band_id, right_tracks.id AS right_tracks_id, right_tracks.album_id FROM albums LEFT OUTER JOIN tracks AS right_tracks ON (right_tracks.album_id = albums.id) ORDER BY right_tracks.blah__id, right_tracks.blah__id DESC, blah.id DESC, blah.id, right_tracks.album_id, right_tracks.album_id DESC, 1, RANDOM(), b.a'
  end

  it "should not respect the association's :order if :order_eager_graph is false" do
    GraphAlbum.one_to_many :right_tracks, :class=>'GraphTrack', :key=>:album_id, :order=>[:id, :album_id], :order_eager_graph=>false
    GraphAlbum.eager_graph(:right_tracks).sql.should == 'SELECT albums.id, albums.band_id, right_tracks.id AS right_tracks_id, right_tracks.album_id FROM albums LEFT OUTER JOIN tracks AS right_tracks ON (right_tracks.album_id = albums.id)'
  end

  it "should add the association's :order to the existing order" do
    GraphAlbum.one_to_many :right_tracks, :class=>'GraphTrack', :key=>:album_id, :order=>[:id, :album_id]
    GraphAlbum.order(:band_id).eager_graph(:right_tracks).sql.should == 'SELECT albums.id, albums.band_id, right_tracks.id AS right_tracks_id, right_tracks.album_id FROM albums LEFT OUTER JOIN tracks AS right_tracks ON (right_tracks.album_id = albums.id) ORDER BY band_id, right_tracks.id, right_tracks.album_id'
  end

  it "should add the association's :order for cascading associations" do
    GraphBand.one_to_many :a_albums, :class=>'GraphAlbum', :key=>:band_id, :order=>:name
    GraphAlbum.one_to_many :b_tracks, :class=>'GraphTrack', :key=>:album_id, :order=>[:id, :album_id]
    GraphBand.eager_graph(:a_albums=>:b_tracks).sql.should == 'SELECT bands.id, bands.vocalist_id, a_albums.id AS a_albums_id, a_albums.band_id, b_tracks.id AS b_tracks_id, b_tracks.album_id FROM bands LEFT OUTER JOIN albums AS a_albums ON (a_albums.band_id = bands.id) LEFT OUTER JOIN tracks AS b_tracks ON (b_tracks.album_id = a_albums.id) ORDER BY a_albums.name, b_tracks.id, b_tracks.album_id'
    GraphAlbum.one_to_many :albums, :class=>'GraphAlbum', :key=>:band_id, :order=>[:band_id, :id]
    GraphAlbum.eager_graph(:albums=>{:albums=>:albums}).sql.should == 'SELECT albums.id, albums.band_id, albums_0.id AS albums_0_id, albums_0.band_id AS albums_0_band_id, albums_1.id AS albums_1_id, albums_1.band_id AS albums_1_band_id, albums_2.id AS albums_2_id, albums_2.band_id AS albums_2_band_id FROM albums LEFT OUTER JOIN albums AS albums_0 ON (albums_0.band_id = albums.id) LEFT OUTER JOIN albums AS albums_1 ON (albums_1.band_id = albums_0.id) LEFT OUTER JOIN albums AS albums_2 ON (albums_2.band_id = albums_1.id) ORDER BY albums_0.band_id, albums_0.id, albums_1.band_id, albums_1.id, albums_2.band_id, albums_2.id'
  end

  it "should add the associations :order for multiple associations" do
    GraphAlbum.many_to_many :a_genres, :class=>'GraphGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :order=>:id
    GraphAlbum.one_to_many :b_tracks, :class=>'GraphTrack', :key=>:album_id, :order=>[:id, :album_id]
    GraphAlbum.eager_graph(:a_genres, :b_tracks).sql.should == 'SELECT albums.id, albums.band_id, a_genres.id AS a_genres_id, b_tracks.id AS b_tracks_id, b_tracks.album_id FROM albums LEFT OUTER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN genres AS a_genres ON (a_genres.id = ag.genre_id) LEFT OUTER JOIN tracks AS b_tracks ON (b_tracks.album_id = albums.id) ORDER BY a_genres.id, b_tracks.id, b_tracks.album_id'
  end

  it "should use the correct qualifier when graphing multiple tables with extra conditions" do
    GraphAlbum.many_to_many :a_genres, :class=>'GraphGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag
    GraphAlbum.one_to_many :b_tracks, :class=>'GraphTrack', :key=>:album_id, :graph_conditions=>{:a=>:b}
    GraphAlbum.eager_graph(:a_genres, :b_tracks).sql.should == 'SELECT albums.id, albums.band_id, a_genres.id AS a_genres_id, b_tracks.id AS b_tracks_id, b_tracks.album_id FROM albums LEFT OUTER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN genres AS a_genres ON (a_genres.id = ag.genre_id) LEFT OUTER JOIN tracks AS b_tracks ON ((b_tracks.album_id = albums.id) AND (b_tracks.a = albums.b))'
  end

  it "should eagerly load associated records for classes that do not have a primary key" do
    GraphAlbum.no_primary_key
    GraphGenre.no_primary_key
    GraphAlbum.many_to_many :inner_genres, :class=>'GraphGenre', :left_key=>:album_id, :left_primary_key=>:band_id, :right_key=>:genre_id, :right_primary_key=>:xxx, :join_table=>:ag
    ds = GraphAlbum.eager_graph(:inner_genres)
    ds.sql.should == 'SELECT albums.id, albums.band_id, inner_genres.id AS inner_genres_id FROM albums LEFT OUTER JOIN ag ON (ag.album_id = albums.band_id) LEFT OUTER JOIN genres AS inner_genres ON (inner_genres.xxx = ag.genre_id)'
    ds._fetch = [{:id=>3, :band_id=>2, :inner_genres_id=>5, :xxx=>12}, {:id=>3, :band_id=>2, :inner_genres_id=>6, :xxx=>22}]
    as = ds.all
    as.should == [GraphAlbum.load(:id=>3, :band_id=>2)]
    as.first.inner_genres.should == [GraphGenre.load(:id=>5), GraphGenre.load(:id=>6)]
    GraphAlbum.set_primary_key :id
    GraphGenre.set_primary_key :id
  end
  
  it "should handle eager loading with schemas and aliases of different types" do
    GraphAlbum.eager_graph(:band).join(:s__genres, [:b_id]).eager_graph(:genres).sql.should == 'SELECT albums.id, albums.band_id, band.id AS band_id_0, band.vocalist_id, genres_0.id AS genres_0_id FROM albums LEFT OUTER JOIN bands AS band ON (band.id = albums.band_id) INNER JOIN s.genres USING (b_id) LEFT OUTER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN genres AS genres_0 ON (genres_0.id = ag.genre_id)'
    GraphAlbum.eager_graph(:band).join(:genres.qualify(:s), [:b_id]).eager_graph(:genres).sql.should == 'SELECT albums.id, albums.band_id, band.id AS band_id_0, band.vocalist_id, genres_0.id AS genres_0_id FROM albums LEFT OUTER JOIN bands AS band ON (band.id = albums.band_id) INNER JOIN s.genres USING (b_id) LEFT OUTER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN genres AS genres_0 ON (genres_0.id = ag.genre_id)'
    GraphAlbum.eager_graph(:band).join(:s__b.as('genres'), [:b_id]).eager_graph(:genres).sql.should ==  'SELECT albums.id, albums.band_id, band.id AS band_id_0, band.vocalist_id, genres_0.id AS genres_0_id FROM albums LEFT OUTER JOIN bands AS band ON (band.id = albums.band_id) INNER JOIN s.b AS genres USING (b_id) LEFT OUTER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN genres AS genres_0 ON (genres_0.id = ag.genre_id)'
    GraphAlbum.eager_graph(:band).join(:s__b, [:b_id], :genres.identifier).eager_graph(:genres).sql.should ==  'SELECT albums.id, albums.band_id, band.id AS band_id_0, band.vocalist_id, genres_0.id AS genres_0_id FROM albums LEFT OUTER JOIN bands AS band ON (band.id = albums.band_id) INNER JOIN s.b AS genres USING (b_id) LEFT OUTER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN genres AS genres_0 ON (genres_0.id = ag.genre_id)'
    GraphAlbum.eager_graph(:band).join(:genres.identifier, [:b_id]).eager_graph(:genres).sql.should ==  'SELECT albums.id, albums.band_id, band.id AS band_id_0, band.vocalist_id, genres_0.id AS genres_0_id FROM albums LEFT OUTER JOIN bands AS band ON (band.id = albums.band_id) INNER JOIN genres USING (b_id) LEFT OUTER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN genres AS genres_0 ON (genres_0.id = ag.genre_id)'
    GraphAlbum.eager_graph(:band).join('genres', [:b_id]).eager_graph(:genres).sql.should ==  'SELECT albums.id, albums.band_id, band.id AS band_id_0, band.vocalist_id, genres_0.id AS genres_0_id FROM albums LEFT OUTER JOIN bands AS band ON (band.id = albums.band_id) INNER JOIN genres USING (b_id) LEFT OUTER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN genres AS genres_0 ON (genres_0.id = ag.genre_id)'
  end
  
  it "should raise errors if invalid aliases or table styles are used" do
    proc{GraphAlbum.from_self(:alias=>:bands.qualify(:s)).eager_graph(:band)}.should raise_error(Sequel::Error)
    proc{GraphAlbum.from('?'.lit(:bands)).eager_graph(:band)}.should raise_error(Sequel::Error)
  end

  it "should eagerly load schema qualified tables correctly with joins" do
    c1 = Class.new(GraphAlbum)
    c2 = Class.new(GraphGenre)
    ds = c1.dataset = c1.dataset.from(:s__a)
    def ds.columns() [:id] end
    c2.dataset = c2.dataset.from(:s__g)
    c1.many_to_many :a_genres, :class=>c2, :left_primary_key=>:id, :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:s__ag
    ds = c1.join(:s__t, [:b_id]).eager_graph(:a_genres)
    ds.sql.should == 'SELECT a.id, a_genres.id AS a_genres_id FROM (SELECT * FROM s.a INNER JOIN s.t USING (b_id)) AS a LEFT OUTER JOIN s.ag AS ag ON (ag.album_id = a.id) LEFT OUTER JOIN s.g AS a_genres ON (a_genres.id = ag.genre_id)'
  end

  it "should respect :after_load callbacks on associations when eager graphing" do
    GraphAlbum.many_to_one :al_band, :class=>GraphBand, :key=>:band_id, :after_load=>proc{|o, a| a.id *=2}
    GraphAlbum.one_to_many :al_tracks, :class=>GraphTrack, :key=>:album_id, :after_load=>proc{|o, os| os.each{|a| a.id *=2}}
    GraphAlbum.many_to_many :al_genres, :class=>GraphGenre, :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :after_load=>proc{|o, os| os.each{|a| a.id *=2}}
    ds = GraphAlbum.eager_graph(:al_band, :al_tracks, :al_genres)
    ds.sql.should == "SELECT albums.id, albums.band_id, al_band.id AS al_band_id, al_band.vocalist_id, al_tracks.id AS al_tracks_id, al_tracks.album_id, al_genres.id AS al_genres_id FROM albums LEFT OUTER JOIN bands AS al_band ON (al_band.id = albums.band_id) LEFT OUTER JOIN tracks AS al_tracks ON (al_tracks.album_id = albums.id) LEFT OUTER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN genres AS al_genres ON (al_genres.id = ag.genre_id)"
    ds._fetch = {:id=>1, :band_id=>2, :al_band_id=>3, :vocalist_id=>4, :al_tracks_id=>5, :album_id=>6, :al_genres_id=>7}
    a = ds.all.first
    a.should == GraphAlbum.load(:id => 1, :band_id => 2)
    a.al_band.should == GraphBand.load(:id=>6, :vocalist_id=>4)
    a.al_tracks.should == [GraphTrack.load(:id=>10, :album_id=>6)]
    a.al_genres.should == [GraphGenre.load(:id=>14)]
  end

  it "should respect limits on associations when eager graphing" do
    GraphAlbum.many_to_one :al_band, :class=>GraphBand, :key=>:band_id
    GraphAlbum.one_to_many :al_tracks, :class=>GraphTrack, :key=>:album_id, :limit=>2
    GraphAlbum.many_to_many :al_genres, :class=>GraphGenre, :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag, :limit=>2
    ds = GraphAlbum.eager_graph(:al_band, :al_tracks, :al_genres)
    ds.sql.should == "SELECT albums.id, albums.band_id, al_band.id AS al_band_id, al_band.vocalist_id, al_tracks.id AS al_tracks_id, al_tracks.album_id, al_genres.id AS al_genres_id FROM albums LEFT OUTER JOIN bands AS al_band ON (al_band.id = albums.band_id) LEFT OUTER JOIN tracks AS al_tracks ON (al_tracks.album_id = albums.id) LEFT OUTER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN genres AS al_genres ON (al_genres.id = ag.genre_id)"
    ds._fetch = [{:id=>1, :band_id=>2, :al_band_id=>3, :vocalist_id=>4, :al_tracks_id=>5, :album_id=>6, :al_genres_id=>7},
      {:id=>1, :band_id=>2, :al_band_id=>8, :vocalist_id=>9, :al_tracks_id=>10, :album_id=>11, :al_genres_id=>12},
      {:id=>1, :band_id=>2, :al_band_id=>13, :vocalist_id=>14, :al_tracks_id=>15, :album_id=>16, :al_genres_id=>17}]
    a = ds.all.first
    a.should == GraphAlbum.load(:id => 1, :band_id => 2)
    a.al_band.should == GraphBand.load(:id=>3, :vocalist_id=>4)
    a.al_tracks.should == [GraphTrack.load(:id=>5, :album_id=>6), GraphTrack.load(:id=>10, :album_id=>11)]
    a.al_genres.should == [GraphGenre.load(:id=>7), GraphGenre.load(:id=>12)]
  end

  it "should eagerly load a many_to_one association with a custom callback" do
    ds = GraphAlbum.eager_graph(:band => proc {|ds| ds.select(:id).columns(:id)})
    ds.sql.should == 'SELECT albums.id, albums.band_id, band.id AS band_id_0 FROM albums LEFT OUTER JOIN (SELECT id FROM bands) AS band ON (band.id = albums.band_id)'
    ds._fetch = {:id=>1, :band_id=>2, :band_id_0=>2}
    a = ds.all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(GraphAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    a = a.first
    a.band.should be_a_kind_of(GraphBand)
    a.band.values.should == {:id => 2}
  end

  it "should eagerly load a one_to_one association with a custom callback" do
    GraphAlbum.one_to_one :track, :class=>'GraphTrack', :key=>:album_id
    ds = GraphAlbum.eager_graph(:track => proc {|ds| ds.select(:album_id).columns(:album_id)})
    ds.sql.should == 'SELECT albums.id, albums.band_id, track.album_id FROM albums LEFT OUTER JOIN (SELECT album_id FROM tracks) AS track ON (track.album_id = albums.id)'
    ds._fetch = {:id=>1, :band_id=>2, :album_id=>1}
    a = ds.all
    a.should == [GraphAlbum.load(:id => 1, :band_id => 2)]
    a.first.track.should == GraphTrack.load(:album_id=>1)
  end

  it "should eagerly load a one_to_many association with a custom callback" do
    ds = GraphAlbum.eager_graph(:tracks => proc {|ds| ds.select(:album_id).columns(:album_id)})
    ds.sql.should == 'SELECT albums.id, albums.band_id, tracks.album_id FROM albums LEFT OUTER JOIN (SELECT album_id FROM tracks) AS tracks ON (tracks.album_id = albums.id)'
    ds._fetch = {:id=>1, :band_id=>2, :album_id=>1}
    a = ds.all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(GraphAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    a = a.first
    a.tracks.should be_a_kind_of(Array)
    a.tracks.size.should == 1
    a.tracks.first.should be_a_kind_of(GraphTrack)
    a.tracks.first.values.should == {:album_id=>1}
  end

  it "should eagerly load a many_to_many association with a custom callback" do
    ds = GraphAlbum.eager_graph(:genres => proc {|ds| ds.select(:id).columns(:id)})
    ds.sql.should == 'SELECT albums.id, albums.band_id, genres.id AS genres_id FROM albums LEFT OUTER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN (SELECT id FROM genres) AS genres ON (genres.id = ag.genre_id)'
    ds._fetch = {:id=>1, :band_id=>2, :genres_id=>4}
    a = ds.all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(GraphAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    a = a.first
    a.genres.should be_a_kind_of(Array)
    a.genres.size.should == 1
    a.genres.first.should be_a_kind_of(GraphGenre)
    a.genres.first.values.should == {:id => 4}
  end

  it "should allow cascading of eager loading with a custom callback with hash value" do
    ds = GraphTrack.eager_graph(:album=>{proc{|ds| ds.select(:id, :band_id).columns(:id, :band_id)}=>{:band=>:members}})
    ds.sql.should == 'SELECT tracks.id, tracks.album_id, album.id AS album_id_0, album.band_id, band.id AS band_id_0, band.vocalist_id, members.id AS members_id FROM tracks LEFT OUTER JOIN (SELECT id, band_id FROM albums) AS album ON (album.id = tracks.album_id) LEFT OUTER JOIN bands AS band ON (band.id = album.band_id) LEFT OUTER JOIN bm ON (bm.band_id = band.id) LEFT OUTER JOIN members ON (members.id = bm.member_id)'
    ds._fetch = {:id=>3, :album_id=>1, :album_id_0=>1, :band_id=>2, :members_id=>5, :band_id_0=>2, :vocalist_id=>6}
    a = ds.all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(GraphTrack)
    a.first.values.should == {:id => 3, :album_id => 1}
    a = a.first
    a.album.should be_a_kind_of(GraphAlbum)
    a.album.values.should == {:id => 1, :band_id => 2}
    a.album.band.should be_a_kind_of(GraphBand)
    a.album.band.values.should == {:id => 2, :vocalist_id=>6}
    a.album.band.members.should be_a_kind_of(Array)
    a.album.band.members.size.should == 1
    a.album.band.members.first.should be_a_kind_of(GraphBandMember)
    a.album.band.members.first.values.should == {:id => 5}
  end
  
  it "should allow cascading of eager loading with a custom callback with array value" do
    ds = GraphTrack.eager_graph(:album=>{proc{|ds| ds.select(:id, :band_id).columns(:id, :band_id)}=>[:band, :tracks]})
    ds.sql.should == 'SELECT tracks.id, tracks.album_id, album.id AS album_id_0, album.band_id, band.id AS band_id_0, band.vocalist_id, tracks_0.id AS tracks_0_id, tracks_0.album_id AS tracks_0_album_id FROM tracks LEFT OUTER JOIN (SELECT id, band_id FROM albums) AS album ON (album.id = tracks.album_id) LEFT OUTER JOIN bands AS band ON (band.id = album.band_id) LEFT OUTER JOIN tracks AS tracks_0 ON (tracks_0.album_id = album.id)'
    ds._fetch = {:id=>3, :album_id=>1, :album_id_0=>1, :band_id=>2, :band_id_0=>2, :vocalist_id=>6, :tracks_0_id=>3, :tracks_0_album_id=>1}
    a = ds.all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(GraphTrack)
    a.first.values.should == {:id => 3, :album_id => 1}
    a = a.first
    a.album.should be_a_kind_of(GraphAlbum)
    a.album.values.should == {:id => 1, :band_id => 2}
    a.album.band.should be_a_kind_of(GraphBand)
    a.album.band.values.should == {:id => 2, :vocalist_id=>6}
    a.album.tracks.should be_a_kind_of(Array)
    a.album.tracks.size.should == 1
    a.album.tracks.first.should be_a_kind_of(GraphTrack)
    a.album.tracks.first.values.should == {:id => 3, :album_id => 1}
  end
  
end
