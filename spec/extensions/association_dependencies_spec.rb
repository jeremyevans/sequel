require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "AssociationDependencies plugin" do
  before do
    mods = @mods = []
    @c = Class.new(Sequel::Model)
    @c.plugin :association_dependencies
    @Artist = Class.new(@c).set_dataset(:artists)
    ds1 = @Artist.dataset
    def ds1.fetch_rows(s)
      (MODEL_DB.sqls ||= []) << s
      yield({:id=>2, :name=>'Ar'})
    end
    @Album = Class.new(@c).set_dataset(:albums)
    ds1 = @Album.dataset
    def ds1.fetch_rows(s)
      (MODEL_DB.sqls ||= []) << s
      yield({:id=>1, :name=>'Al', :artist_id=>2})
    end
    @Artist.columns :id, :name
    @Album.columns :id, :name, :artist_id
    @Artist.one_to_many :albums, :class=>@Album, :key=>:artist_id
    @Artist.one_to_one :first_album, :class=>@Album, :key=>:artist_id, :conditions=>{:position=>1}
    @Artist.many_to_many :other_artists, :class=>@Artist, :join_table=>:aoa, :left_key=>:l, :right_key=>:r
    @Album.many_to_one :artist, :class=>@Artist
    MODEL_DB.reset
  end

  specify "should allow destroying associated many_to_one associated object" do
    @Album.add_association_dependencies :artist=>:destroy
    @Album.load(:id=>1, :name=>'Al', :artist_id=>2).destroy
    MODEL_DB.sqls.should == ['DELETE FROM albums WHERE (id = 1)', 'SELECT * FROM artists WHERE (artists.id = 2) LIMIT 1', 'DELETE FROM artists WHERE (id = 2)']
  end

  specify "should allow deleting associated many_to_one associated object" do
    @Album.add_association_dependencies :artist=>:delete
    @Album.load(:id=>1, :name=>'Al', :artist_id=>2).destroy
    MODEL_DB.sqls.should == ['DELETE FROM albums WHERE (id = 1)', 'DELETE FROM artists WHERE (artists.id = 2)']
  end
  
  specify "should allow destroying associated one_to_one associated object" do
    @Artist.add_association_dependencies :first_album=>:destroy
    @Artist.load(:id=>2, :name=>'Ar').destroy
    MODEL_DB.sqls.should == ['SELECT * FROM albums WHERE ((albums.artist_id = 2) AND (position = 1)) LIMIT 1', 'DELETE FROM albums WHERE (id = 1)', 'DELETE FROM artists WHERE (id = 2)']
  end

  specify "should allow deleting associated one_to_one associated object" do
    @Artist.add_association_dependencies :first_album=>:delete
    @Artist.load(:id=>2, :name=>'Ar').destroy
    MODEL_DB.sqls.should == ['DELETE FROM albums WHERE ((albums.artist_id = 2) AND (position = 1))', 'DELETE FROM artists WHERE (id = 2)']
  end

  specify "should allow destroying associated one_to_many objects" do
    @Artist.add_association_dependencies :albums=>:destroy
    @Artist.load(:id=>2, :name=>'Ar').destroy
    MODEL_DB.sqls.should == ['SELECT * FROM albums WHERE (albums.artist_id = 2)', 'DELETE FROM albums WHERE (id = 1)', 'DELETE FROM artists WHERE (id = 2)']
  end

  specify "should allow deleting associated one_to_many objects" do
    @Artist.add_association_dependencies :albums=>:delete
    @Artist.load(:id=>2, :name=>'Ar').destroy
    MODEL_DB.sqls.should == ['DELETE FROM albums WHERE (albums.artist_id = 2)', 'DELETE FROM artists WHERE (id = 2)']
  end
  
  specify "should allow nullifying associated one_to_one objects" do
    @Artist.add_association_dependencies :first_album=>:nullify
    @Artist.load(:id=>2, :name=>'Ar').destroy
    MODEL_DB.sqls.should == ['UPDATE albums SET artist_id = NULL WHERE ((artist_id = 2) AND (position = 1))', 'DELETE FROM artists WHERE (id = 2)']
  end

  specify "should allow nullifying associated one_to_many objects" do
    @Artist.add_association_dependencies :albums=>:nullify
    @Artist.load(:id=>2, :name=>'Ar').destroy
    MODEL_DB.sqls.should == ['UPDATE albums SET artist_id = NULL WHERE (artist_id = 2)', 'DELETE FROM artists WHERE (id = 2)']
  end

  specify "should allow nullifying associated many_to_many associations" do
    @Artist.add_association_dependencies :other_artists=>:nullify
    @Artist.load(:id=>2, :name=>'Ar').destroy
    MODEL_DB.sqls.should == ['DELETE FROM aoa WHERE (l = 2)', 'DELETE FROM artists WHERE (id = 2)']
  end

  specify "should raise an error if attempting to nullify a many_to_one association" do
    proc{@Album.add_association_dependencies :artist=>:nullify}.should raise_error(Sequel::Error)
  end

  specify "should raise an error if using an unrecognized dependence action" do
    proc{@Album.add_association_dependencies :artist=>:blah}.should raise_error(Sequel::Error)
  end

  specify "should raise an error if a nonexistent association is used" do
    proc{@Album.add_association_dependencies :blah=>:delete}.should raise_error(Sequel::Error)
  end

  specify "should raise an error if a invalid association type is used" do
    @Artist.plugin :many_through_many
    @Artist.many_through_many :other_albums, [[:id, :id, :id]]
    proc{@Artist.add_association_dependencies :other_albums=>:nullify}.should raise_error(Sequel::Error)
  end

  specify "should raise an error if using a many_to_many association type without nullify" do
    proc{@Artist.add_association_dependencies :other_artists=>:delete}.should raise_error(Sequel::Error)
  end

  specify "should allow specifying association dependencies in the plugin call" do
    @Album.plugin :association_dependencies, :artist=>:destroy
    @Album.load(:id=>1, :name=>'Al', :artist_id=>2).destroy
    MODEL_DB.sqls.should == ['DELETE FROM albums WHERE (id = 1)', 'SELECT * FROM artists WHERE (artists.id = 2) LIMIT 1', 'DELETE FROM artists WHERE (id = 2)']
  end

  specify "should work with subclasses" do
    c = Class.new(@Album)
    c.add_association_dependencies :artist=>:destroy
    c.load(:id=>1, :name=>'Al', :artist_id=>2).destroy
    MODEL_DB.sqls.should == ['DELETE FROM albums WHERE (id = 1)', 'SELECT * FROM artists WHERE (artists.id = 2) LIMIT 1', 'DELETE FROM artists WHERE (id = 2)']
    MODEL_DB.reset

    @Album.load(:id=>1, :name=>'Al', :artist_id=>2).destroy
    MODEL_DB.sqls.should == ['DELETE FROM albums WHERE (id = 1)']
    MODEL_DB.reset

    @Album.add_association_dependencies :artist=>:destroy
    c2 = Class.new(@Album)
    c2.load(:id=>1, :name=>'Al', :artist_id=>2).destroy
    MODEL_DB.sqls.should == ['DELETE FROM albums WHERE (id = 1)', 'SELECT * FROM artists WHERE (artists.id = 2) LIMIT 1', 'DELETE FROM artists WHERE (id = 2)']
  end
end
