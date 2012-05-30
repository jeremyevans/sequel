require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

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
      def_column_accessor :id, :name
      one_to_many :albums
    end
    class ::Album < Sequel::Model
      attr_accessor :blah
      plugin :json_serializer
      columns :id, :name, :artist_id
      def_column_accessor :id, :name, :artist_id
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

  it "should handle ruby objects in values" do
    class ::Artist
      def name=(v)
        super(Date.parse(v))
      end
    end
    JSON.parse(Artist.load(:name=>Date.today).to_json).should == Artist.load(:name=>Date.today)
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

  it "should support #from_json to support specific :fields" do
    @album.from_json('{"name": "AS", "artist_id": 3}', :fields=>['name'])
    @album.name.should == 'AS'
    @album.artist_id.should == 2
  end

  it "should support #from_json to support :missing=>:skip option" do
    @album.from_json('{"artist_id": 3}', :fields=>['name'], :missing=>:skip)
    @album.name.should == 'RF'
    @album.artist_id.should == 2
  end

  it "should support #from_json to support :missing=>:raise option" do
    proc{@album.from_json('{"artist_id": 3}', :fields=>['name'], :missing=>:raise)}.should raise_error(Sequel::Error)
  end

  it "should raise an exception for json keys that aren't associations, columns, or setter methods" do
    Album.send(:undef_method, :blah=)
    proc{JSON.parse(@album.to_json(:include=>:blah))}.should raise_error(Sequel::Error)
  end

  it "should support a to_json class and dataset method" do
    Album.dataset._fetch = {:id=>1, :name=>'RF', :artist_id=>2}
    Artist.dataset._fetch = {:id=>2, :name=>'YJM'}
    JSON.parse(Album.to_json).should == [@album]
    JSON.parse(Album.to_json(:include=>:artist)).map{|x| x.artist}.should == [@artist]
    JSON.parse(Album.dataset.to_json(:only=>:name)).should == [Album.load(:name=>@album.name)]
  end

  it "should have dataset to_json method work with naked datasets" do
    album = @album
    ds = Album.dataset.naked
    ds._fetch = {:id=>1, :name=>'RF', :artist_id=>2}
    JSON.parse(ds.to_json).should == [@album.values.inject({}){|h, (k, v)| h[k.to_s] = v; h}]
  end

  it "should have dataset to_json method respect :array option for the array to use" do
    a = Album.load(:id=>1, :name=>'RF', :artist_id=>3)
    JSON.parse(Album.to_json(:array=>[a])).should == [a]

    a.associations[:artist] = artist = Artist.load(:id=>3, :name=>'YJM')
    JSON.parse(Album.to_json(:array=>[a], :include=>:artist)).first.artist.should == artist

    artist.associations[:albums] = [a]
    x = JSON.parse(Artist.to_json(:array=>[artist], :include=>:albums))
    x.should == [artist]
    x.first.albums.should == [a]
  end

  it "should propagate class default options to instance to_json output" do
    class ::Album2 < Sequel::Model
      attr_accessor :blah
      plugin :json_serializer, :naked => true, :except => :id
      columns :id, :name, :artist_id
      many_to_one :artist
    end
    @album2 = Album2.load(:id=>2, :name=>'JK')
    @album2.artist = @artist
    @album2.blah = 'Gak'
    JSON.parse(@album2.to_json).should == @album2.values.reject{|k,v| k.to_s == 'id'}.inject({}){|h, (k, v)| h[k.to_s] = v; h}
    JSON.parse(@album2.to_json(:only => :name)).should == @album2.values.reject{|k,v| k.to_s != 'name'}.inject({}){|h, (k, v)| h[k.to_s] = v; h}
    JSON.parse(@album2.to_json(:except => :artist_id)).should == @album2.values.reject{|k,v| k.to_s == 'artist_id'}.inject({}){|h, (k, v)| h[k.to_s] = v; h}
  end
  
  it "should handle the :root option to qualify single records" do
    @album.to_json(:root=>true, :except => [:name, :artist_id]).to_s.should == '{"album":{"id":1}}'
    @album.to_json(:root=>true, :only => :name).to_s.should == '{"album":{"name":"RF"}}'
  end
  
  it "should handle the :root=>:both option to qualify a dataset of records" do
    Album.dataset._fetch = [{:id=>1, :name=>'RF'}, {:id=>1, :name=>'RF'}]
    Album.dataset.to_json(:root=>true, :only => :id).to_s.should == '{"albums":[{"album":{"id":1}},{"album":{"id":1}}]}'
  end

  it "should handle the :root=>:collection option to qualify just the collection" do
    Album.dataset._fetch = [{:id=>1, :name=>'RF'}, {:id=>1, :name=>'RF'}]
    Album.dataset.to_json(:root=>:collection, :only => :id).to_s.should == '{"albums":[{"id":1},{"id":1}]}'
  end

  it "should handle the :root=>:instance option to qualify just the instances" do
    Album.dataset._fetch = [{:id=>1, :name=>'RF'}, {:id=>1, :name=>'RF'}]
    Album.dataset.to_json(:root=>:instance, :only => :id).to_s.should == '[{"album":{"id":1}},{"album":{"id":1}}]'
  end

  it "should handle the :root=>true option be the same as :root=>:both for backwards compatibility" do
    Album.dataset._fetch = [{:id=>1, :name=>'RF'}, {:id=>1, :name=>'RF'}]
    Album.dataset.to_json(:root=>true, :only => :id).to_s.should == '{"albums":[{"album":{"id":1}},{"album":{"id":1}}]}'
  end

  it "should store the default options in json_serializer_opts" do
    Album.json_serializer_opts.should == {}
    c = Class.new(Album)
    c.plugin :json_serializer, :naked=>true
    c.json_serializer_opts.should == {:naked=>true}
  end

  it "should work correctly when subclassing" do
    class ::Artist2 < Artist
      plugin :json_serializer, :only=>:name
    end
    JSON.parse(Artist2.load(:id=>2, :name=>'YYY').to_json).should == Artist2.load(:name=>'YYY')
    class ::Artist3 < Artist2
      plugin :json_serializer, :naked=>:true
    end
    JSON.parse(Artist3.load(:id=>2, :name=>'YYY').to_json).should == {"name"=>'YYY'}
    Object.send(:remove_const, :Artist2)
    Object.send(:remove_const, :Artist3)
  end
end
end
