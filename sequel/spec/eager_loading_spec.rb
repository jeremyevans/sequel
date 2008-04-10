require File.join(File.dirname(__FILE__), "spec_helper")

describe Sequel::Model, "eager" do
  before(:each) do
    MODEL_DB.reset
    
    class EagerAlbum < Sequel::Model(:albums)
      columns :id, :band_id
      many_to_one :band, :class=>'EagerBand', :key=>:band_id
      one_to_many :tracks, :class=>'EagerTrack', :key=>:album_id
      many_to_many :genres, :class=>'EagerGenre', :left_key=>:album_id, :right_key=>:genre_id, :join_table=>:ag
    end

    class EagerBand < Sequel::Model(:bands)
      columns :id
      one_to_many :albums, :class=>'EagerAlbum', :key=>:band_id, :eager=>:tracks
      many_to_many :members, :class=>'EagerBandMember', :left_key=>:band_id, :right_key=>:member_id, :join_table=>:bm
    end
    
    class EagerTrack < Sequel::Model(:tracks)
      columns :id, :album_id
      many_to_one :album, :class=>'EagerAlbum', :key=>:album_id
    end
    
    class EagerGenre < Sequel::Model(:genres)
      columns :id
      many_to_many :albums, :class=>'EagerAlbum', :left_key=>:genre_id, :right_key=>:album_id, :join_table=>:ag
    end
    
    class EagerBandMember < Sequel::Model(:members)
      columns :id
      many_to_many :bands, :class=>'EagerBand', :left_key=>:member_id, :right_key=>:band_id, :join_table=>:bm, :order =>:id
    end
    
    EagerAlbum.dataset.extend(Module.new {
      def fetch_rows(sql)
        h = {:id => 1, :band_id=> 2}
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
        yield h
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
  
  it "should eagerly load a single many_to_one association" do
    a = EagerAlbum.eager(:band).all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(EagerAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT * FROM bands WHERE (id IN (2))']
    a = a.first
    a.band.should be_a_kind_of(EagerBand)
    a.band.values.should == {:id => 2}
    MODEL_DB.sqls.length.should == 2
  end
  
  it "should eagerly load a single one_to_many association" do
    a = EagerAlbum.eager(:tracks).all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(EagerAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT * FROM tracks WHERE (album_id IN (1))']
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
    MODEL_DB.sqls.length.should == 2
    MODEL_DB.sqls[0].should == 'SELECT * FROM albums'
    ["SELECT genres.*, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON (ag.genre_id = genres.id) AND (ag.album_id IN (1))",
     "SELECT genres.*, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON (ag.album_id IN (1)) AND (ag.genre_id = genres.id)"
    ].should(include(MODEL_DB.sqls[1]))
    a = a.first
    a.genres.should be_a_kind_of(Array)
    a.genres.size.should == 1
    a.genres.first.should be_a_kind_of(EagerGenre)
    a.genres.first.values.should == {:id => 4}
    MODEL_DB.sqls.length.should == 2
  end
  
  it "should eagerly load multiple associations" do
    a = EagerAlbum.eager(:genres, :tracks, :band).all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(EagerAlbum)
    a.first.values.should == {:id => 1, :band_id => 2}
    MODEL_DB.sqls.length.should == 4
    MODEL_DB.sqls[0].should == 'SELECT * FROM albums'
    MODEL_DB.sqls[1..-1].should(include('SELECT * FROM bands WHERE (id IN (2))'))
    MODEL_DB.sqls[1..-1].should(include('SELECT * FROM tracks WHERE (album_id IN (1))'))
    sqls = MODEL_DB.sqls[1..-1] - ['SELECT * FROM bands WHERE (id IN (2))', 'SELECT * FROM tracks WHERE (album_id IN (1))']
    ["SELECT genres.*, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON (ag.genre_id = genres.id) AND (ag.album_id IN (1))",
     "SELECT genres.*, ag.album_id AS x_foreign_key_x FROM genres INNER JOIN ag ON (ag.album_id IN (1)) AND (ag.genre_id = genres.id)"
    ].should(include(sqls[0]))
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
    MODEL_DB.sqls[0...-1].should == ['SELECT * FROM tracks', 
                             'SELECT * FROM albums WHERE (id IN (1))',
                             'SELECT * FROM bands WHERE (id IN (2))']
    ["SELECT members.*, bm.band_id AS x_foreign_key_x FROM members INNER JOIN bm ON (bm.member_id = members.id) AND (bm.band_id IN (2))",
     "SELECT members.*, bm.band_id AS x_foreign_key_x FROM members INNER JOIN bm ON (bm.band_id IN (2)) AND (bm.member_id = members.id)"
    ].should(include(MODEL_DB.sqls[-1]))
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
                             'SELECT * FROM albums WHERE (band_id IN (2))',
                             'SELECT * FROM tracks WHERE (album_id IN (1))']
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
    a.albums.all
    MODEL_DB.sqls.should == ['SELECT * FROM bands', 
                             'SELECT * FROM albums WHERE (band_id = 2)',
                             'SELECT * FROM tracks WHERE (album_id IN (1))']
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
  
  it "should respect :order when eagerly loading" do
    a = EagerBandMember.eager(:bands).all
    a.should be_a_kind_of(Array)
    a.size.should == 1
    a.first.should be_a_kind_of(EagerBandMember)
    a.first.values.should == {:id => 5}
    MODEL_DB.sqls.length.should == 2
    MODEL_DB.sqls[0].should == 'SELECT * FROM members'
    ['SELECT bands.*, bm.member_id AS x_foreign_key_x FROM bands INNER JOIN bm ON (bm.band_id = bands.id) AND (bm.member_id IN (5)) ORDER BY id',
     'SELECT bands.*, bm.member_id AS x_foreign_key_x FROM bands INNER JOIN bm ON (bm.member_id IN (5)) AND (bm.band_id = bands.id) ORDER BY id'
    ].should(include(MODEL_DB.sqls[1]))
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
    MODEL_DB.sqls.should == ['SELECT * FROM albums', 'SELECT * FROM tracks WHERE (album_id IN (1))']
    a = a.first
    a.tracks.should be_a_kind_of(Array)
    a.tracks.size.should == 1
    a.tracks.first.should be_a_kind_of(EagerTrack)
    a.tracks.first.values.should == {:id => 3, :album_id=>1}
    a.tracks.first.album.should be_a_kind_of(EagerAlbum)
    a.tracks.first.album.should == a
    MODEL_DB.sqls.length.should == 2
  end
end
