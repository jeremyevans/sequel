require File.join(File.dirname(__FILE__), "spec_helper")

describe Sequel::Model, "#eager" do
  before(:each) do
    MODEL_DB.reset
    
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
    
    EagerAlbum.dataset.extend(Module.new {
      def columns
        [:id, :band_id]
      end

      def fetch_rows(sql)
        h = if sql =~ /101/
          {:id => 101, :band_id=> 101}
        else
          {:id => 1, :band_id=> 2}
        end
        h.merge!(:x_foreign_key_x=>4) if sql =~ /ag\.genre_id/
        @db << sql
        yield h
      end
    })

    EagerBand.dataset.extend(Module.new {
      def fetch_rows(sql)
        h = {:id => 2}
        h.merge!(:x_foreign_key_x=>5) if sql =~ /bm\.member_id/
        @db << sql
        case sql
        when /id IN (101)/
        when /id > 100/
          yield({:id => 101})
          yield({:id => 102})
        else
          yield h
        end
      end
    })
    
    EagerTrack.dataset.extend(Module.new {
      def fetch_rows(sql)
        @db << sql
        yield({:id => 3, :album_id => 1})
      end
    })
    
    EagerGenre.dataset.extend(Module.new {
      def fetch_rows(sql)
        h = {:id => 4}
        h.merge!(:x_foreign_key_x=>1) if sql =~ /ag\.album_id/
        @db << sql
        yield h
      end
    })
    
    EagerBandMember.dataset.extend(Module.new {
      def fetch_rows(sql)
        h = {:id => 5}
        h.merge!(:x_foreign_key_x=>2) if sql =~ /bm\.band_id/
        @db << sql
        yield h
      end
    })
  end
  
  it "should raise an error if called without a symbol or hash" do
    proc{EagerAlbum.eager(Object.new)}.should raise_error(Sequel::Error)
  end

  it "should eagerly load a single many_to_one association" do
    a = EagerAlbum.eager(:band).all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(EagerAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT * FROM bands WHERE (bands.id IN (2))']
    a = a.first
    a.band.should be_a_kind_of(EagerBand)
    a.band.values.should == {:id => 2}
    MODEL_DB.sqls.length.should == 2
  end
  
  it "should eagerly load a single one_to_one association" do
    EagerAlbum.one_to_one :track, :class=>'EagerTrack', :key=>:album_id
    a = EagerAlbum.eager(:track).all
    a.should == [EagerAlbum.load(:id => 1, :band_id => 2)]
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT * FROM tracks WHERE (tracks.album_id IN (1))']
    a.first.track.should == EagerTrack.load(:id => 3, :album_id=>1)
    MODEL_DB.sqls.length.should == 2
  end
  
  it "should eagerly load a single one_to_many association" do
    a = EagerAlbum.eager(:tracks).all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(EagerAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT * FROM tracks WHERE (tracks.album_id IN (1))']
    a = a.first
    a.tracks.should be_a_kind_of(Array)
    a.tracks.size.should == 1
    a.tracks.first.should be_a_kind_of(EagerTrack)
    a.tracks.first.values.should == {:id => 3, :album_id=>1}
    MODEL_DB.sqls.length.should == 2
  end
  
  it "should eagerly load a single many_to_many association" do
    a = EagerAlbum.eager(:genres).all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(EagerAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT genres.*, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON ((ag.genre_id = genres.id) AND (ag.album_id IN (1)))"]
    a = a.first
    a.genres.should be_a_kind_of(Array)
    a.genres.size.should == 1
    a.genres.first.should be_a_kind_of(EagerGenre)
    a.genres.first.values.should == {:id => 4}
    MODEL_DB.sqls.length.should == 2
  end
  
  it "should eagerly load multiple associations in a single call" do
    a = EagerAlbum.eager(:genres, :tracks, :band).all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(EagerAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    MODEL_DB.sqls.length.should == 4
    MODEL_DB.sqls[0].should == 'SELECT * FROM albums'
    MODEL_DB.sqls[1..-1].should(include('SELECT * FROM bands WHERE (bands.id IN (2))'))
    MODEL_DB.sqls[1..-1].should(include('SELECT * FROM tracks WHERE (tracks.album_id IN (1))'))
    MODEL_DB.sqls[1..-1].should(include('SELECT genres.*, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON ((ag.genre_id = genres.id) AND (ag.album_id IN (1)))'))
    a = a.first
    a.band.should be_a_kind_of(EagerBand)
    a.band.values.should == {:id => 2}
    a.tracks.should be_a_kind_of(Array)
    a.tracks.size.should == 1
    a.tracks.first.should be_a_kind_of(EagerTrack)
    a.tracks.first.values.should == {:id => 3, :album_id=>1}
    a.genres.should be_a_kind_of(Array)
    a.genres.size.should == 1
    a.genres.first.should be_a_kind_of(EagerGenre)
    a.genres.first.values.should == {:id => 4}
    MODEL_DB.sqls.length.should == 4
  end
  
  it "should eagerly load multiple associations in separate calls" do
    a = EagerAlbum.eager(:genres).eager(:tracks).eager(:band).all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(EagerAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    MODEL_DB.sqls.length.should == 4
    MODEL_DB.sqls[0].should == 'SELECT * FROM albums'
    MODEL_DB.sqls[1..-1].should(include('SELECT * FROM bands WHERE (bands.id IN (2))'))
    MODEL_DB.sqls[1..-1].should(include('SELECT * FROM tracks WHERE (tracks.album_id IN (1))'))
    MODEL_DB.sqls[1..-1].should(include('SELECT genres.*, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON ((ag.genre_id = genres.id) AND (ag.album_id IN (1)))'))
    a = a.first
    a.band.should be_a_kind_of(EagerBand)
    a.band.values.should == {:id => 2}
    a.tracks.should be_a_kind_of(Array)
    a.tracks.size.should == 1
    a.tracks.first.should be_a_kind_of(EagerTrack)
    a.tracks.first.values.should == {:id => 3, :album_id=>1}
    a.genres.should be_a_kind_of(Array)
    a.genres.size.should == 1
    a.genres.first.should be_a_kind_of(EagerGenre)
    a.genres.first.values.should == {:id => 4}
    MODEL_DB.sqls.length.should == 4
  end
  
  it "should allow cascading of eager loading for associations of associated models" do
    a = EagerTrack.eager(:album=>{:band=>:members}).all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(EagerTrack)
    a.first.values.should == {:id => 3, :album_id => 1}
    MODEL_DB.sqls.length.should == 4
    MODEL_DB.sqls.should == ['SELECT * FROM tracks', 
      'SELECT * FROM albums WHERE (albums.id IN (1))',
      'SELECT * FROM bands WHERE (bands.id IN (2))',
      "SELECT members.*, bm.band_id AS x_foreign_key_x FROM members INNER JOIN bm ON ((bm.member_id = members.id) AND (bm.band_id IN (2)))"]
    a = a.first
    a.album.should be_a_kind_of(EagerAlbum)
    a.album.values.should == {:id => 1, :band_id => 2}
    a.album.band.should be_a_kind_of(EagerBand)
    a.album.band.values.should == {:id => 2}
    a.album.band.members.should be_a_kind_of(Array)
    a.album.band.members.size.should == 1
    a.album.band.members.first.should be_a_kind_of(EagerBandMember)
    a.album.band.members.first.values.should == {:id => 5}
    MODEL_DB.sqls.length.should == 4
  end
  
  it "should cascade eagerly loading when the :eager association option is used" do
    a = EagerBand.eager(:albums).all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(EagerBand)
    a.first.values.should == {:id => 2}
    MODEL_DB.sqls.should == ['SELECT * FROM bands', 
                             'SELECT * FROM albums WHERE (albums.band_id IN (2))',
                             'SELECT * FROM tracks WHERE (tracks.album_id IN (1))']
    a = a.first
    a.albums.should be_a_kind_of(Array)
    a.albums.size.should == 1
    a.albums.first.should be_a_kind_of(EagerAlbum)
    a.albums.first.values.should == {:id => 1, :band_id => 2}
    a = a.albums.first
    a.tracks.should be_a_kind_of(Array)
    a.tracks.size.should == 1
    a.tracks.first.should be_a_kind_of(EagerTrack)
    a.tracks.first.values.should == {:id => 3, :album_id => 1}
    MODEL_DB.sqls.length.should == 3
  end
  
  it "should respect :eager when lazily loading an association" do
    a = EagerBand.all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(EagerBand)
    a.first.values.should == {:id => 2}
    MODEL_DB.sqls.should == ['SELECT * FROM bands']
    a = a.first
    a.albums
    MODEL_DB.sqls.should == ['SELECT * FROM bands', 
                             'SELECT * FROM albums WHERE (albums.band_id = 2)',
                             'SELECT * FROM tracks WHERE (tracks.album_id IN (1))']
    a.albums.should be_a_kind_of(Array)
    a.albums.size.should == 1
    a.albums.first.should be_a_kind_of(EagerAlbum)
    a.albums.first.values.should == {:id => 1, :band_id => 2}
    a = a.albums.first
    a.tracks.should be_a_kind_of(Array)
    a.tracks.size.should == 1
    a.tracks.first.should be_a_kind_of(EagerTrack)
    a.tracks.first.values.should == {:id => 3, :album_id => 1}
    MODEL_DB.sqls.length.should == 3
  end
  
  it "should cascade eagerly loading when the :eager_graph association option is used" do
    EagerAlbum.dataset.extend(Module.new {
      def fetch_rows(sql)
        @db << sql
        yield({:id=>1, :band_id=>2, :tracks_id=>3, :album_id=>1})
      end
    })
    a = EagerBand.eager(:graph_albums).all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(EagerBand)
    a.first.values.should == {:id => 2}
    MODEL_DB.sqls.should == ['SELECT * FROM bands', 
                             'SELECT albums.id, albums.band_id, tracks.id AS tracks_id, tracks.album_id FROM albums LEFT OUTER JOIN tracks ON (tracks.album_id = albums.id) WHERE (albums.band_id IN (2))']
    a = a.first
    a.graph_albums.should be_a_kind_of(Array)
    a.graph_albums.size.should == 1
    a.graph_albums.first.should be_a_kind_of(EagerAlbum)
    a.graph_albums.first.values.should == {:id => 1, :band_id => 2}
    a = a.graph_albums.first
    a.tracks.should be_a_kind_of(Array)
    a.tracks.size.should == 1
    a.tracks.first.should be_a_kind_of(EagerTrack)
    a.tracks.first.values.should == {:id => 3, :album_id => 1}
    MODEL_DB.sqls.length.should == 2
  end
  
  it "should cascade eagerly loading when the :eager_graph association option is used with a many_to_many association" do
    EagerBandMember.dataset.extend(Module.new {
      def columns
        [:id]
      end
      def fetch_rows(sql)
        @db << sql
        yield({:id=>5, :bands_id=>2, :p_k=>6, :x_foreign_key_x=>2})
        yield({:id=>5, :bands_id=>3, :p_k=>6, :x_foreign_key_x=>2})
      end
    })
    a = EagerBand.eager(:graph_members).all
    a.should == [EagerBand.load(:id=>2)]
    MODEL_DB.sqls.should == ['SELECT * FROM bands', 
                             'SELECT members.id, bands.id AS bands_id, bands.p_k, bm.band_id AS x_foreign_key_x FROM (SELECT members.* FROM members INNER JOIN bm ON ((bm.member_id = members.id) AND (bm.band_id IN (2)))) AS members LEFT OUTER JOIN bm AS bm_0 ON (bm_0.member_id = members.id) LEFT OUTER JOIN bands ON (bands.id = bm_0.band_id) ORDER BY bands.id']
    a = a.first
    a.graph_members.should == [EagerBandMember.load(:id=>5)]
    a.graph_members.first.bands.should == [EagerBand.load(:id=>2, :p_k=>6), EagerBand.load(:id=>3, :p_k=>6)]
    MODEL_DB.sqls.length.should == 2
  end
  
  it "should respect :eager_graph when lazily loading an association" do
    a = EagerBand.all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(EagerBand)
    a.first.values.should == {:id => 2}
    MODEL_DB.sqls.should == ['SELECT * FROM bands']
    a = a.first
    EagerAlbum.dataset.extend(Module.new {
      def fetch_rows(sql)
        @db << sql
        yield({:id=>1, :band_id=>2, :tracks_id=>3, :album_id=>1})
      end
    })
    a.graph_albums
    MODEL_DB.sqls.should == ['SELECT * FROM bands', 
                             'SELECT albums.id, albums.band_id, tracks.id AS tracks_id, tracks.album_id FROM albums LEFT OUTER JOIN tracks ON (tracks.album_id = albums.id) WHERE (albums.band_id = 2)']
    a.graph_albums.should be_a_kind_of(Array)
    a.graph_albums.size.should == 1
    a.graph_albums.first.should be_a_kind_of(EagerAlbum)
    a.graph_albums.first.values.should == {:id => 1, :band_id => 2}
    a = a.graph_albums.first
    a.tracks.should be_a_kind_of(Array)
    a.tracks.size.should == 1
    a.tracks.first.should be_a_kind_of(EagerTrack)
    a.tracks.first.values.should == {:id => 3, :album_id => 1}
    MODEL_DB.sqls.length.should == 2
  end
  
  it "should respect :eager_graph when lazily loading a many_to_many association" do
    EagerBandMember.dataset.extend(Module.new {
      def columns
        [:id]
      end
      def fetch_rows(sql)
        @db << sql
        yield({:id=>5, :bands_id=>2, :p_k=>6})
        yield({:id=>5, :bands_id=>3, :p_k=>6})
      end
    })
    a = EagerBand.load(:id=>2)
    a.graph_members.should == [EagerBandMember.load(:id=>5)]
    MODEL_DB.sqls.should == ['SELECT members.id, bands.id AS bands_id, bands.p_k FROM (SELECT members.* FROM members INNER JOIN bm ON ((bm.member_id = members.id) AND (bm.band_id = 2))) AS members LEFT OUTER JOIN bm AS bm_0 ON (bm_0.member_id = members.id) LEFT OUTER JOIN bands ON (bands.id = bm_0.band_id) ORDER BY bands.id']
    a.graph_members.first.bands.should == [EagerBand.load(:id=>2, :p_k=>6), EagerBand.load(:id=>3, :p_k=>6)]
    MODEL_DB.sqls.length.should == 1
  end
  
  it "should respect :conditions when eagerly loading" do
    EagerBandMember.many_to_many :good_bands, :clone=>:bands, :conditions=>{:a=>32}
    a = EagerBandMember.eager(:good_bands).all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(EagerBandMember)
    a.first.values.should == {:id => 5}
    MODEL_DB.sqls.should == ['SELECT * FROM members', 'SELECT bands.*, bm.member_id AS x_foreign_key_x FROM bands INNER JOIN bm ON ((bm.band_id = bands.id) AND (bm.member_id IN (5))) WHERE (a = 32) ORDER BY id']
    a = a.first
    a.good_bands.should be_a_kind_of(Array)
    a.good_bands.size.should == 1
    a.good_bands.first.should be_a_kind_of(EagerBand)
    a.good_bands.first.values.should == {:id => 2}
    MODEL_DB.sqls.length.should == 2

    MODEL_DB.sqls.clear
    EagerBandMember.many_to_many :good_bands, :clone=>:bands, :conditions=>"x = 1"
    a = EagerBandMember.eager(:good_bands).all
    MODEL_DB.sqls.should == ['SELECT * FROM members', 'SELECT bands.*, bm.member_id AS x_foreign_key_x FROM bands INNER JOIN bm ON ((bm.band_id = bands.id) AND (bm.member_id IN (5))) WHERE (x = 1) ORDER BY id']
  end
  
  it "should respect :order when eagerly loading" do
    a = EagerBandMember.eager(:bands).all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(EagerBandMember)
    a.first.values.should == {:id => 5}
    MODEL_DB.sqls.should == ['SELECT * FROM members', 'SELECT bands.*, bm.member_id AS x_foreign_key_x FROM bands INNER JOIN bm ON ((bm.band_id = bands.id) AND (bm.member_id IN (5))) ORDER BY id']
    a = a.first
    a.bands.should be_a_kind_of(Array)
    a.bands.size.should == 1
    a.bands.first.should be_a_kind_of(EagerBand)
    a.bands.first.values.should == {:id => 2}
    MODEL_DB.sqls.length.should == 2
  end
  
  it "should populate the reciprocal many_to_one association when eagerly loading the one_to_many association" do
    a = EagerAlbum.eager(:tracks).all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(EagerAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT * FROM tracks WHERE (tracks.album_id IN (1))']
    a = a.first
    a.tracks.should be_a_kind_of(Array)
    a.tracks.size.should == 1
    a.tracks.first.should be_a_kind_of(EagerTrack)
    a.tracks.first.values.should == {:id => 3, :album_id=>1}
    a.tracks.first.album.should be_a_kind_of(EagerAlbum)
    a.tracks.first.album.should == a
    MODEL_DB.sqls.length.should == 2
  end

  it "should cache the negative lookup when eagerly loading a many_to_one association" do
    a = EagerAlbum.eager(:band).filter(:id=>101).all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(EagerAlbum)
    a.first.values.should == {:id => 101, :band_id => 101}
    MODEL_DB.sqls.should == ['SELECT * FROM albums WHERE (id = 101)', 'SELECT * FROM bands WHERE (bands.id IN (101))']
    a = a.first
    a.associations.fetch(:band, 2).should == nil
    a.band.should == nil
    MODEL_DB.sqls.length.should == 2
  end
  
  it "should cache the negative lookup when eagerly loading a *_to_many associations" do
    a = EagerBand.eager(:albums).filter('id > 100').all
    a.should be_a_kind_of(Array)
    a.size.should == 2
    a.first.should be_a_kind_of(EagerBand)
    a.first.values.should == {:id => 101}
    a.last.values.should == {:id => 102}
    MODEL_DB.sqls.should == ['SELECT * FROM bands WHERE (id > 100)', 'SELECT * FROM albums WHERE (albums.band_id IN (101, 102))', "SELECT * FROM tracks WHERE (tracks.album_id IN (101))"]
    a.first.associations[:albums].should be_a_kind_of(Array)
    a.first.albums.length.should == 1
    a.first.albums.first.should be_a_kind_of(EagerAlbum)
    a.last.associations[:albums].should == []
    a.last.albums.should == []
    MODEL_DB.sqls.length.should == 3
  end
  
  it "should use the association's block when eager loading by default" do
    EagerAlbum.eager(:good_tracks).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT * FROM tracks WHERE ((tracks.album_id IN (1)) AND (name = 'Good'))"]
  end

  it "should use the eager_block option when eager loading if given" do
    EagerBand.eager(:good_albums).all
    MODEL_DB.sqls.should == ['SELECT * FROM bands', "SELECT * FROM albums WHERE ((albums.band_id IN (2)) AND (name = 'good'))"]
    MODEL_DB.sqls.clear
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
    MODEL_DB.sqls.clear
    EagerAlbum.eager(:track_names).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT id, name FROM tracks WHERE (tracks.album_id IN (1))"]
    MODEL_DB.sqls.clear
    EagerAlbum.eager(:genre_names).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT id, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON ((ag.genre_id = genres.id) AND (ag.album_id IN (1)))"]
  end

  it "should respect the association's :primary_key option" do
    EagerAlbum.many_to_one :special_band, :class=>:EagerBand, :primary_key=>:p_k, :key=>:band_id
    EagerBand.dataset.extend(Module.new {
      def fetch_rows(sql)
        MODEL_DB.sqls << sql
        yield({:p_k=>2, :id=>1})
      end
    })
    as = EagerAlbum.eager(:special_band).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT * FROM bands WHERE (bands.p_k IN (2))"]
    as.length.should == 1
    as.first.special_band.should == EagerBand.load(:p_k=>2, :id=>1)
    MODEL_DB.sqls.clear
    EagerAlbum.one_to_many :special_tracks, :class=>:EagerTrack, :primary_key=>:band_id, :key=>:album_id
    EagerTrack.dataset.extend(Module.new {
      def fetch_rows(sql)
        MODEL_DB.sqls << sql
        yield({:album_id=>2, :id=>1})
      end
    })
    as = EagerAlbum.eager(:special_tracks).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT * FROM tracks WHERE (tracks.album_id IN (2))"]
    as.length.should == 1
    as.first.special_tracks.should == [EagerTrack.load(:album_id=>2, :id=>1)]
  end
  
  it "should respect the many_to_one association's composite keys" do
    EagerAlbum.many_to_one :special_band, :class=>:EagerBand, :primary_key=>[:id, :p_k], :key=>[:band_id, :id]
    EagerBand.dataset.extend(Module.new {
      def fetch_rows(sql)
        MODEL_DB.sqls << sql
        yield({:p_k=>1, :id=>2})
      end
    })
    as = EagerAlbum.eager(:special_band).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT * FROM bands WHERE ((bands.id, bands.p_k) IN ((2, 1)))"]
    as.length.should == 1
    as.first.special_band.should == EagerBand.load(:p_k=>1, :id=>2)
  end
  
  it "should respect the one_to_many association's composite keys" do
    EagerAlbum.one_to_many :special_tracks, :class=>:EagerTrack, :primary_key=>[:band_id, :id], :key=>[:id, :album_id]
    EagerTrack.dataset.extend(Module.new {
      def fetch_rows(sql)
        MODEL_DB.sqls << sql
        yield({:album_id=>1, :id=>2})
      end
    })
    as = EagerAlbum.eager(:special_tracks).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT * FROM tracks WHERE ((tracks.id, tracks.album_id) IN ((2, 1)))"]
    as.length.should == 1
    as.first.special_tracks.should == [EagerTrack.load(:album_id=>1, :id=>2)]
  end

  it "should respect many_to_many association's composite keys" do
    EagerAlbum.many_to_many :special_genres, :class=>:EagerGenre, :left_primary_key=>[:band_id, :id], :left_key=>[:l1, :l2], :right_primary_key=>[:xxx, :id], :right_key=>[:r1, :r2], :join_table=>:ag
    EagerGenre.dataset.extend(Module.new {
      def fetch_rows(sql)
        MODEL_DB.sqls << sql
        yield({:x_foreign_key_0_x=>2, :x_foreign_key_1_x=>1, :id=>5})
        yield({:x_foreign_key_0_x=>2, :x_foreign_key_1_x=>1, :id=>6})
      end
    })
    as = EagerAlbum.eager(:special_genres).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT genres.*, ag.l1 AS x_foreign_key_0_x, ag.l2 AS x_foreign_key_1_x FROM genres INNER JOIN ag ON ((ag.r1 = genres.xxx) AND (ag.r2 = genres.id) AND ((ag.l1, ag.l2) IN ((2, 1))))"]
    as.length.should == 1
    as.first.special_genres.should == [EagerGenre.load(:id=>5), EagerGenre.load(:id=>6)]
  end
  
  it "should respect many_to_many association's :left_primary_key and :right_primary_key options" do
    EagerAlbum.many_to_many :special_genres, :class=>:EagerGenre, :left_primary_key=>:band_id, :left_key=>:album_id, :right_primary_key=>:xxx, :right_key=>:genre_id, :join_table=>:ag
    EagerGenre.dataset.extend(Module.new {
      def fetch_rows(sql)
        MODEL_DB.sqls << sql
        yield({:x_foreign_key_x=>2, :id=>5})
        yield({:x_foreign_key_x=>2, :id=>6})
      end
    })
    as = EagerAlbum.eager(:special_genres).all
    MODEL_DB.sqls.should == ['SELECT * FROM albums', "SELECT genres.*, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON ((ag.genre_id = genres.xxx) AND (ag.album_id IN (2)))"]
    as.length.should == 1
    as.first.special_genres.should == [EagerGenre.load(:id=>5), EagerGenre.load(:id=>6)]
  end

  it "should use the :eager_loader association option when eager loading" do
    EagerAlbum.many_to_one :special_band, :eager_loader=>(proc do |key_hash, records, assocs| 
      item = EagerBand.filter(:album_id=>records.collect{|r| [r.pk, r.pk*2]}.flatten).order(:name).first
      records.each{|r| r.associations[:special_band] = item}
    end)
    EagerAlbum.one_to_many :special_tracks, :eager_loader=>(proc do |key_hash, records, assocs| 
      items = EagerTrack.filter(:album_id=>records.collect{|r| [r.pk, r.pk*2]}.flatten).all
      records.each{|r| r.associations[:special_tracks] = items}
    end)
    EagerAlbum.many_to_many :special_genres, :class=>:EagerGenre, :eager_loader=>(proc do |key_hash, records, assocs| 
      items = EagerGenre.inner_join(:ag, [:genre_id]).filter(:album_id=>records.collect{|r| r.pk}).all
      records.each{|r| r.associations[:special_genres] = items}
    end)
    a = EagerAlbum.eager(:special_genres, :special_tracks, :special_band).all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(EagerAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    MODEL_DB.sqls.length.should == 4
    MODEL_DB.sqls[0].should == 'SELECT * FROM albums'
    MODEL_DB.sqls[1..-1].should(include('SELECT * FROM bands WHERE (album_id IN (1, 2)) ORDER BY name LIMIT 1'))
    MODEL_DB.sqls[1..-1].should(include('SELECT * FROM tracks WHERE (album_id IN (1, 2))'))
    MODEL_DB.sqls[1..-1].should(include('SELECT * FROM genres INNER JOIN ag USING (genre_id) WHERE (album_id IN (1))'))
    a = a.first
    a.special_band.should be_a_kind_of(EagerBand)
    a.special_band.values.should == {:id => 2}
    a.special_tracks.should be_a_kind_of(Array)
    a.special_tracks.size.should == 1
    a.special_tracks.first.should be_a_kind_of(EagerTrack)
    a.special_tracks.first.values.should == {:id => 3, :album_id=>1}
    a.special_genres.should be_a_kind_of(Array)
    a.special_genres.size.should == 1
    a.special_genres.first.should be_a_kind_of(EagerGenre)
    a.special_genres.first.values.should == {:id => 4}
    MODEL_DB.sqls.length.should == 4
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
    EagerGenre.dataset.extend(Module.new {
      def fetch_rows(sql)
        MODEL_DB.sqls << sql
        yield({:x_foreign_key_x=>1, :id=>8})
        yield({:x_foreign_key_x=>1, :id=>8})
      end
    })
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
end

describe Sequel::Model, "#eager_graph" do
  after(:all) do
    class ::MockDataset
      alias clone orig_clone
    end
  end

  before(:all) do
    class ::MockDataset
      alias orig_clone clone
      def clone(opts = {})
        c = super()
        c.opts = @opts.merge(opts)
        c.instance_variable_set(:@columns, (@columns.dup if @columns))
        c
      end
    end

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
    
  it "should raise an error if called without a symbol or hash" do
    proc{GraphAlbum.eager_graph(Object.new)}.should raise_error(Sequel::Error)
  end

  it "should not split results and assign associations if ungraphed is called" do
    ds = GraphAlbum.eager_graph(:band).ungraphed
    ds.sql.should == 'SELECT albums.id, albums.band_id, band.id AS band_id_0, band.vocalist_id FROM albums LEFT OUTER JOIN bands AS band ON (band.id = albums.band_id)'
    def ds.fetch_rows(sql, &block)
      yield({:id=>1, :band_id=>2, :band_id_0=>2, :vocalist_id=>3})
    end
    ds.all.should == [GraphAlbum.load(:id=>1, :band_id=>2, :band_id_0=>2, :vocalist_id=>3)]
  end

  it "should eagerly load a single many_to_one association" do
    ds = GraphAlbum.eager_graph(:band)
    ds.sql.should == 'SELECT albums.id, albums.band_id, band.id AS band_id_0, band.vocalist_id FROM albums LEFT OUTER JOIN bands AS band ON (band.id = albums.band_id)'
    def ds.fetch_rows(sql, &block)
      yield({:id=>1, :band_id=>2, :band_id_0=>2, :vocalist_id=>3})
    end
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
    def ds.fetch_rows(sql, &block)
      yield({:id=>1, :band_id=>2, :track_id=>3, :album_id=>1})
    end
    a = ds.all
    a.should == [GraphAlbum.load(:id => 1, :band_id => 2)]
    a.first.track.should == GraphTrack.load(:id => 3, :album_id=>1)
  end

  it "should eagerly load a single one_to_many association" do
    ds = GraphAlbum.eager_graph(:tracks)
    ds.sql.should == 'SELECT albums.id, albums.band_id, tracks.id AS tracks_id, tracks.album_id FROM albums LEFT OUTER JOIN tracks ON (tracks.album_id = albums.id)'
    def ds.fetch_rows(sql, &block)
      yield({:id=>1, :band_id=>2, :tracks_id=>3, :album_id=>1})
    end
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
    def ds.fetch_rows(sql, &block)
      yield({:id=>1, :band_id=>2, :genres_id=>4})
    end
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

  it "should eagerly load multiple associations in a single call" do 
    ds = GraphAlbum.eager_graph(:genres, :tracks, :band)
    ds.sql.should == 'SELECT albums.id, albums.band_id, genres.id AS genres_id, tracks.id AS tracks_id, tracks.album_id, band.id AS band_id_0, band.vocalist_id FROM albums LEFT OUTER JOIN ag ON (ag.album_id = albums.id) LEFT OUTER JOIN genres ON (genres.id = ag.genre_id) LEFT OUTER JOIN tracks ON (tracks.album_id = albums.id) LEFT OUTER JOIN bands AS band ON (band.id = albums.band_id)'
    def ds.fetch_rows(sql, &block)
      yield({:id=>1, :band_id=>2, :genres_id=>4, :tracks_id=>3, :album_id=>1, :band_id_0=>2, :vocalist_id=>6})
    end
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
    def ds.fetch_rows(sql, &block)
      yield({:id=>1, :band_id=>2, :genres_id=>4, :tracks_id=>3, :album_id=>1, :band_id_0=>2, :vocalist_id=>6})
    end
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
    def ds.fetch_rows(sql, &block)
      yield({:id=>3, :album_id=>1, :album_id_0=>1, :band_id=>2, :members_id=>5, :band_id_0=>2, :vocalist_id=>6})
    end
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
    def ds.fetch_rows(sql, &block)
      yield({:id=>1, :vocalist_id=>2, :albums_id=>3, :band_id=>1, :tracks_id=>4, :album_id=>3, :members_id=>5})
      yield({:id=>1, :vocalist_id=>2, :albums_id=>3, :band_id=>1, :tracks_id=>4, :album_id=>3, :members_id=>6})
      yield({:id=>1, :vocalist_id=>2, :albums_id=>3, :band_id=>1, :tracks_id=>5, :album_id=>3, :members_id=>5})
      yield({:id=>1, :vocalist_id=>2, :albums_id=>3, :band_id=>1, :tracks_id=>5, :album_id=>3, :members_id=>6})
      yield({:id=>1, :vocalist_id=>2, :albums_id=>4, :band_id=>1, :tracks_id=>6, :album_id=>4, :members_id=>5})
      yield({:id=>1, :vocalist_id=>2, :albums_id=>4, :band_id=>1, :tracks_id=>6, :album_id=>4, :members_id=>6})
      yield({:id=>1, :vocalist_id=>2, :albums_id=>4, :band_id=>1, :tracks_id=>7, :album_id=>4, :members_id=>5})
      yield({:id=>1, :vocalist_id=>2, :albums_id=>4, :band_id=>1, :tracks_id=>7, :album_id=>4, :members_id=>6})
      yield({:id=>2, :vocalist_id=>2, :albums_id=>5, :band_id=>2, :tracks_id=>8, :album_id=>5, :members_id=>5})
      yield({:id=>2, :vocalist_id=>2, :albums_id=>5, :band_id=>2, :tracks_id=>8, :album_id=>5, :members_id=>6})
      yield({:id=>2, :vocalist_id=>2, :albums_id=>5, :band_id=>2, :tracks_id=>9, :album_id=>5, :members_id=>5})
      yield({:id=>2, :vocalist_id=>2, :albums_id=>5, :band_id=>2, :tracks_id=>9, :album_id=>5, :members_id=>6})
      yield({:id=>2, :vocalist_id=>2, :albums_id=>6, :band_id=>2, :tracks_id=>1, :album_id=>6, :members_id=>5})
      yield({:id=>2, :vocalist_id=>2, :albums_id=>6, :band_id=>2, :tracks_id=>1, :album_id=>6, :members_id=>6})
      yield({:id=>2, :vocalist_id=>2, :albums_id=>6, :band_id=>2, :tracks_id=>2, :album_id=>6, :members_id=>5})
      yield({:id=>2, :vocalist_id=>2, :albums_id=>6, :band_id=>2, :tracks_id=>2, :album_id=>6, :members_id=>6})
    end
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
    def ds.fetch_rows(sql, &block)
      @db << sql
      yield({:id=>1, :band_id=>2, :tracks_id=>3, :album_id=>1})
    end
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
    def ds.fetch_rows(sql, &block)
      yield({:id=>2, :vocalist_id=>6, :vocalist_id_0=>6, :members_id=>5})
    end
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

  it "should give you a graph of tables when called without .all" do 
    ds = GraphAlbum.eager_graph(:band)
    ds.sql.should == 'SELECT albums.id, albums.band_id, band.id AS band_id_0, band.vocalist_id FROM albums LEFT OUTER JOIN bands AS band ON (band.id = albums.band_id)'
    def ds.fetch_rows(sql, &block)
      yield({:id=>1, :band_id=>2, :band_id_0=>2, :vocalist_id=>3})
    end
    ds.first.should == {:albums=>GraphAlbum.load(:id => 1, :band_id => 2), :band=>GraphBand.load(:id => 2, :vocalist_id=>3)}
  end

  it "should not drop any associated objects if the graph could not be a cartesian product" do
    ds = GraphBand.eager_graph(:members, :vocalist)
    ds.sql.should == 'SELECT bands.id, bands.vocalist_id, members.id AS members_id, vocalist.id AS vocalist_id_0 FROM bands LEFT OUTER JOIN bm ON (bm.band_id = bands.id) LEFT OUTER JOIN members ON (members.id = bm.member_id) LEFT OUTER JOIN members AS vocalist ON (vocalist.id = bands.vocalist_id)'
    def ds.fetch_rows(sql, &block)
      yield({:id=>2, :vocalist_id=>6, :members_id=>5, :vocalist_id_0=>6})
      yield({:id=>2, :vocalist_id=>6, :members_id=>5, :vocalist_id_0=>6})
    end
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
    def ds.fetch_rows(sql, &block)
      yield({:id=>2, :vocalist_id=>6, :members_id=>5, :other_vocalist_id=>6})
      yield({:id=>2, :vocalist_id=>6, :members_id=>5, :other_vocalist_id=>6})
    end
    a = ds.all
    a.should == [GraphBand.load(:id=>2, :vocalist_id => 6)]
    a.first.other_vocalist.should == GraphBandMember.load(:id=>6)
    a.first.members.should == [GraphBandMember.load(:id=>5)]
  end

  it "should drop duplicate items that occur in sequence if the graph could be a cartesian product" do
    ds = GraphBand.eager_graph(:members, :genres)
    ds.sql.should == 'SELECT bands.id, bands.vocalist_id, members.id AS members_id, genres.id AS genres_id FROM bands LEFT OUTER JOIN bm ON (bm.band_id = bands.id) LEFT OUTER JOIN members ON (members.id = bm.member_id) LEFT OUTER JOIN bg ON (bg.band_id = bands.id) LEFT OUTER JOIN genres ON (genres.id = bg.genre_id)'
    def ds.fetch_rows(sql, &block)
      yield({:id=>2, :vocalist_id=>6, :members_id=>5, :genres_id=>7})
      yield({:id=>2, :vocalist_id=>6, :members_id=>5, :genres_id=>8})
      yield({:id=>2, :vocalist_id=>6, :members_id=>6, :genres_id=>7})
      yield({:id=>2, :vocalist_id=>6, :members_id=>6, :genres_id=>8})
    end
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
    def ds.fetch_rows(sql, &block)
      @db << sql
      yield({:id=>1, :band_id=>2, :tracks_id=>3, :album_id=>1})
    end
    ds2 = GraphGenre.dataset
    def ds2.fetch_rows(sql, &block)
      @db << sql
      yield({:id=>6, :x_foreign_key_x=>1})
    end
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
    def ds.fetch_rows(sql, &block)
      yield({:id=>1, :band_id=>2, :band_id_0=>nil, :vocalist_id=>nil})
    end
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
    def ds.fetch_rows(sql, &block)
      yield({:id=>1, :band_id=>2, :tracks_id=>nil, :album_id=>nil})
    end
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
    def ds.fetch_rows(sql, &block)
      yield({:id=>1, :band_id=>2, :genres_id=>nil})
    end
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
    def ds.fetch_rows(sql, &block)
      yield({:id=>1, :band_id=>2, :genres_id=>nil, :tracks_id=>3, :album_id=>1, :band_id_0=>nil, :vocalist_id=>nil})
      yield({:id=>1, :band_id=>2, :genres_id=>nil, :tracks_id=>4, :album_id=>1, :band_id_0=>nil, :vocalist_id=>nil})
      yield({:id=>1, :band_id=>2, :genres_id=>nil, :tracks_id=>5, :album_id=>1, :band_id_0=>nil, :vocalist_id=>nil})
      yield({:id=>1, :band_id=>2, :genres_id=>nil, :tracks_id=>6, :album_id=>1, :band_id_0=>nil, :vocalist_id=>nil})
    end
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
    def ds.fetch_rows(sql, &block)
      yield({:id=>2, :album_id=>2, :album_id_0=>nil, :band_id=>nil, :members_id=>nil, :band_id_0=>nil, :vocalist_id=>nil})
      yield({:id=>3, :album_id=>3, :album_id_0=>3, :band_id=>3, :members_id=>nil, :band_id_0=>nil, :vocalist_id=>nil})
      yield({:id=>4, :album_id=>4, :album_id_0=>4, :band_id=>2, :members_id=>nil, :band_id_0=>2, :vocalist_id=>6})
      yield({:id=>5, :album_id=>1, :album_id_0=>1, :band_id=>4, :members_id=>5, :band_id_0=>4, :vocalist_id=>8})
      yield({:id=>5, :album_id=>1, :album_id_0=>1, :band_id=>4, :members_id=>6, :band_id_0=>4, :vocalist_id=>8})
    end
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
    def ds.fetch_rows(sql, &block)
      yield({:id=>3, :band_id=>2, :inner_band_id=>5, :vocalist_id=>2})
    end
    as = ds.all
    as.should == [GraphAlbum.load(:id=>3, :band_id=>2)]
    as.first.inner_band.should == GraphBand.load(:id=>5, :vocalist_id=>2)

    GraphAlbum.one_to_many :right_tracks, :class=>'GraphTrack', :key=>:album_id, :primary_key=>:band_id
    ds = GraphAlbum.eager_graph(:right_tracks)
    ds.sql.should == 'SELECT albums.id, albums.band_id, right_tracks.id AS right_tracks_id, right_tracks.album_id FROM albums LEFT OUTER JOIN tracks AS right_tracks ON (right_tracks.album_id = albums.band_id)'
    def ds.fetch_rows(sql, &block)
      yield({:id=>3, :band_id=>2, :right_tracks_id=>5, :album_id=>2})
      yield({:id=>3, :band_id=>2, :right_tracks_id=>6, :album_id=>2})
    end
    as = ds.all
    as.should == [GraphAlbum.load(:id=>3, :band_id=>2)]
    as.first.right_tracks.should == [GraphTrack.load(:id=>5, :album_id=>2), GraphTrack.load(:id=>6, :album_id=>2)]
  end
  
  it "should respect many_to_one association's composite keys" do 
    GraphAlbum.many_to_one :inner_band, :class=>'GraphBand', :key=>[:band_id, :id], :primary_key=>[:vocalist_id, :id]
    ds = GraphAlbum.eager_graph(:inner_band)
    ds.sql.should == 'SELECT albums.id, albums.band_id, inner_band.id AS inner_band_id, inner_band.vocalist_id FROM albums LEFT OUTER JOIN bands AS inner_band ON ((inner_band.vocalist_id = albums.band_id) AND (inner_band.id = albums.id))'
    def ds.fetch_rows(sql, &block)
      yield({:id=>3, :band_id=>2, :inner_band_id=>3, :vocalist_id=>2})
    end
    as = ds.all
    as.should == [GraphAlbum.load(:id=>3, :band_id=>2)]
    as.first.inner_band.should == GraphBand.load(:id=>3, :vocalist_id=>2)
  end

  it "should respect one_to_many association's composite keys" do 
    GraphAlbum.one_to_many :right_tracks, :class=>'GraphTrack', :key=>[:album_id, :id], :primary_key=>[:band_id, :id]
    ds = GraphAlbum.eager_graph(:right_tracks)
    ds.sql.should == 'SELECT albums.id, albums.band_id, right_tracks.id AS right_tracks_id, right_tracks.album_id FROM albums LEFT OUTER JOIN tracks AS right_tracks ON ((right_tracks.album_id = albums.band_id) AND (right_tracks.id = albums.id))'
    def ds.fetch_rows(sql, &block)
      yield({:id=>3, :band_id=>2, :right_tracks_id=>3, :album_id=>2})
    end
    as = ds.all
    as.should == [GraphAlbum.load(:id=>3, :band_id=>2)]
    as.first.right_tracks.should == [GraphTrack.load(:id=>3, :album_id=>2)]
  end
  
  it "should respect many_to_many association's composite keys" do 
    GraphAlbum.many_to_many :sbands, :class=>'GraphBand', :left_key=>[:l1, :l2], :left_primary_key=>[:band_id, :id], :right_key=>[:r1, :r2], :right_primary_key=>[:vocalist_id, :id], :join_table=>:b
    ds = GraphAlbum.eager_graph(:sbands)
    ds.sql.should == 'SELECT albums.id, albums.band_id, sbands.id AS sbands_id, sbands.vocalist_id FROM albums LEFT OUTER JOIN b ON ((b.l1 = albums.band_id) AND (b.l2 = albums.id)) LEFT OUTER JOIN bands AS sbands ON ((sbands.vocalist_id = b.r1) AND (sbands.id = b.r2))'
    def ds.fetch_rows(sql, &block)
      yield({:id=>3, :band_id=>2, :sbands_id=>5, :vocalist_id=>6})
      yield({:id=>3, :band_id=>2, :sbands_id=>6, :vocalist_id=>22})
    end
    as = ds.all
    as.should == [GraphAlbum.load(:id=>3, :band_id=>2)]
    as.first.sbands.should == [GraphBand.load(:id=>5, :vocalist_id=>6), GraphBand.load(:id=>6, :vocalist_id=>22)]
  end

  it "should respect many_to_many association's :left_primary_key and :right_primary_key options" do 
    GraphAlbum.many_to_many :inner_genres, :class=>'GraphGenre', :left_key=>:album_id, :left_primary_key=>:band_id, :right_key=>:genre_id, :right_primary_key=>:xxx, :join_table=>:ag
    ds = GraphAlbum.eager_graph(:inner_genres)
    ds.sql.should == 'SELECT albums.id, albums.band_id, inner_genres.id AS inner_genres_id FROM albums LEFT OUTER JOIN ag ON (ag.album_id = albums.band_id) LEFT OUTER JOIN genres AS inner_genres ON (inner_genres.xxx = ag.genre_id)'
    def ds.fetch_rows(sql, &block)
      yield({:id=>3, :band_id=>2, :inner_genres_id=>5, :xxx=>12})
      yield({:id=>3, :band_id=>2, :inner_genres_id=>6, :xxx=>22})
    end
    as = ds.all
    as.should == [GraphAlbum.load(:id=>3, :band_id=>2)]
    as.first.inner_genres.should == [GraphGenre.load(:id=>5), GraphGenre.load(:id=>6)]
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

    GraphAlbum.one_to_many :right_tracks, :class=>'GraphTrack', :key=>:album_id, :eager_grapher=>proc{|ds, aa, ta| ds.graph(GraphTrack, nil, :join_type=>:natural, :table_alias=>aa)}
    GraphAlbum.eager_graph(:right_tracks).sql.should == 'SELECT albums.id, albums.band_id, right_tracks.id AS right_tracks_id, right_tracks.album_id FROM albums NATURAL JOIN tracks AS right_tracks'

    GraphAlbum.many_to_many :active_genres, :class=>'GraphGenre', :eager_grapher=>proc{|ds, aa, ta| ds.graph(:ag, {:album_id=>:id}, :table_alias=>:a123, :implicit_qualifier=>ta).graph(GraphGenre, [:album_id], :table_alias=>aa)}
    GraphAlbum.eager_graph(:active_genres).sql.should == "SELECT albums.id, albums.band_id, active_genres.id AS active_genres_id FROM albums LEFT OUTER JOIN ag AS a123 ON (a123.album_id = albums.id) LEFT OUTER JOIN genres AS active_genres USING (album_id)"
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
    def ds.fetch_rows(sql, &block)
      yield({:id=>3, :band_id=>2, :inner_genres_id=>5, :xxx=>12})
      yield({:id=>3, :band_id=>2, :inner_genres_id=>6, :xxx=>22})
    end
    as = ds.all
    as.should == [GraphAlbum.load(:id=>3, :band_id=>2)]
    as.first.inner_genres.should == [GraphGenre.load(:id=>5), GraphGenre.load(:id=>6)]
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
    c1.dataset = c1.dataset.from(:s__a)
    c2.dataset = c2.dataset.from(:s__g)
    c1.many_to_many :a_genres, :class=>c2, :left_primary_key=>:id, :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:s__ag
    ds = c1.join(:s__t, [:b_id]).eager_graph(:a_genres)
    ds.sql.should == 'SELECT a.id, a_genres.id AS a_genres_id FROM (SELECT * FROM s.a INNER JOIN s.t USING (b_id)) AS a LEFT OUTER JOIN s.ag AS ag ON (ag.album_id = a.id) LEFT OUTER JOIN s.g AS a_genres ON (a_genres.id = ag.genre_id)'
  end
end
