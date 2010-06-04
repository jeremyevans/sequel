require File.join(File.dirname(__FILE__), "spec_helper")

begin
  require 'json'
rescue LoadError => e
  skip_warn "json_serializer plugin: can't load json (#{e.class}: #{e})"
else
describe "Sequel::Plugins::JsonSerializer" do
  before do
    class ::Artist < Sequel::Model
      plugin :json_serializer
      columns :id, :name
      one_to_many :albums
    end
    class ::Album < Sequel::Model
      attr_accessor :blah
      plugin :json_serializer
      columns :id, :name, :artist_id
      many_to_one :artist
    end
    @artist = Artist.load(:id=>2, :name=>'YJM')
    @artist.associations[:albums] = []
    @album = Album.load(:id=>1, :name=>'RF')
    @album.artist = @artist
    @album.blah = 'Blah'
  end
  after do
    Object.send(:remove_const, :Artist)
    Object.send(:remove_const, :Album)
  end

  it "should round trip successfully" do
    JSON.parse(@artist.to_json).should == @artist
    JSON.parse(@album.to_json).should == @album
  end

  it "should handle the :only option" do
    JSON.parse(@artist.to_json(:only=>:name)).should == Artist.load(:name=>@artist.name)
    JSON.parse(@album.to_json(:only=>[:id, :name])).should == Album.load(:id=>@album.id, :name=>@album.name)
  end

  it "should handle the :except option" do
    JSON.parse(@artist.to_json(:except=>:id)).should == Artist.load(:name=>@artist.name)
    JSON.parse(@album.to_json(:except=>[:id, :artist_id])).should == Album.load(:name=>@album.name)
  end

  it "should handle the :include option for associations" do
    JSON.parse(@artist.to_json(:include=>:albums)).albums.should == [@album]
    JSON.parse(@album.to_json(:include=>:artist)).artist.should == @artist
  end

  it "should handle the :include option for arbitrary attributes" do
    JSON.parse(@album.to_json(:include=>:blah)).blah.should == @album.blah
  end

  it "should handle multiple inclusions using an array for the :include option" do
    a = JSON.parse(@album.to_json(:include=>[:blah, :artist]))
    a.blah.should == @album.blah
    a.artist.should == @artist
  end

  it "should handle cascading using a hash for the :include option" do
    JSON.parse(@artist.to_json(:include=>{:albums=>{:include=>:artist}})).albums.map{|a| a.artist}.should == [@artist]
    JSON.parse(@album.to_json(:include=>{:artist=>{:include=>:albums}})).artist.albums.should == [@album]

    JSON.parse(@artist.to_json(:include=>{:albums=>{:only=>:name}})).albums.should == [Album.load(:name=>@album.name)]
    JSON.parse(@album.to_json(:include=>{:artist=>{:except=>:name}})).artist.should == Artist.load(:id=>@artist.id)

    JSON.parse(@artist.to_json(:include=>{:albums=>{:include=>{:artist=>{:include=>:albums}}}})).albums.map{|a| a.artist.albums}.should == [[@album]]
    JSON.parse(@album.to_json(:include=>{:artist=>{:include=>{:albums=>{:only=>:name}}}})).artist.albums.should == [Album.load(:name=>@album.name)]
  end

  it "should handle the :include option cascading with an empty hash" do
    JSON.parse(@album.to_json(:include=>{:artist=>{}})).artist.should == @artist
    JSON.parse(@album.to_json(:include=>{:blah=>{}})).blah.should == @album.blah
  end

  it "should accept a :naked option to not include the JSON.create_id, so parsing yields a plain hash" do
    JSON.parse(@album.to_json(:naked=>true)).should == @album.values.inject({}){|h, (k, v)| h[k.to_s] = v; h}
  end

  it "should support #from_json to set column values" do
    @artist.from_json('{"name": "AS"}')
    @artist.name.should == 'AS'
    @artist.id.should == 2
  end

  it "should raise an exception for json keys that aren't associations, columns, or setter methods" do
    Album.send(:undef_method, :blah=)
    proc{JSON.parse(@album.to_json(:include=>:blah))}.should raise_error(Sequel::Error)
  end

  it "should support a to_json class and dataset method" do
    album = @album
    Album.dataset.meta_def(:all){[album]}
    JSON.parse(Album.to_json).should == [@album]
    JSON.parse(Album.to_json(:include=>:artist)).map{|x| x.artist}.should == [@artist]
    JSON.parse(Album.dataset.to_json(:only=>:name)).should == [Album.load(:name=>@album.name)]
  end

  it "should have dataset to_json method work with naked datasets" do
    album = @album
    ds = Album.dataset.naked
    ds.meta_def(:all){[album.values]}
    JSON.parse(ds.to_json).should == [@album.values.inject({}){|h, (k, v)| h[k.to_s] = v; h}]
  end
end
end
