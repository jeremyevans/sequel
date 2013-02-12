require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

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
    Artist.from_json(@artist.to_json).should == @artist
    Album.from_json(@album.to_json).should == @album
  end

  it "should handle ruby objects in values" do
    class ::Artist
      def name=(v)
        super(Date.parse(v))
      end
    end
    Artist.from_json(Artist.load(:name=>Date.today).to_json).should == Artist.load(:name=>Date.today)
  end

  it "should have .json_create method for creating an instance from a hash parsed from JSON" do
    Artist.json_create(Sequel.parse_json(@artist.to_json)).should == @artist
  end

  it "should have .json_create method raise error if not given a hash" do
    proc{Artist.json_create([])}.should raise_error(Sequel::Error)
  end

  it "should handle the :only option" do
    Artist.from_json(@artist.to_json(:only=>:name)).should == Artist.load(:name=>@artist.name)
    Album.from_json(@album.to_json(:only=>[:id, :name])).should == Album.load(:id=>@album.id, :name=>@album.name)
  end

  it "should handle the :except option" do
    Artist.from_json(@artist.to_json(:except=>:id)).should == Artist.load(:name=>@artist.name)
    Album.from_json(@album.to_json(:except=>[:id, :artist_id])).should == Album.load(:name=>@album.name)
  end

  it "should handle the :include option for associations" do
    Artist.from_json(@artist.to_json(:include=>:albums)).albums.should == [@album]
    Album.from_json(@album.to_json(:include=>:artist)).artist.should == @artist
  end

  it "should raise an error if attempting to parse json when providing array to non-array association or vice-versa" do
    proc{Artist.from_json('{"albums":{"id":1,"name":"RF","artist_id":2,"json_class":"Album"},"id":2,"name":"YJM","json_class":"Artist"}')}.should raise_error(Sequel::Error)
    proc{Album.from_json('{"artist":[{"id":2,"name":"YJM","json_class":"Artist"}],"id":1,"name":"RF","json_class":"Album","artist_id":2}')}.should raise_error(Sequel::Error)
  end

  it "should raise an error if attempting to parse an array containing non-hashes" do
    proc{Artist.from_json('[{"id":2,"name":"YJM","json_class":"Artist"}, 2]')}.should raise_error(Sequel::Error)
  end

  it "should raise an error if attempting to parse invalid JSON" do
    begin
      Sequel.instance_eval do
        alias pj parse_json
        def parse_json(v)
          v
        end
      end
      proc{Album.from_json('1')}.should raise_error(Sequel::Error)
    ensure
      Sequel.instance_eval do
        alias parse_json pj
      end
    end
  end

  it "should handle case where Sequel.parse_json already returns an instance" do
    begin
      Sequel.instance_eval do
        alias pj parse_json
        def parse_json(v)
          Album.load(:id=>3)
        end
      end
      ::Album.from_json('1').should == Album.load(:id=>3)
    ensure
      Sequel.instance_eval do
        alias parse_json pj
      end
    end
  end

  it "should handle the :include option for arbitrary attributes" do
    Album.from_json(@album.to_json(:include=>:blah)).blah.should == @album.blah
  end

  it "should handle multiple inclusions using an array for the :include option" do
    a = Album.from_json(@album.to_json(:include=>[:blah, :artist]))
    a.blah.should == @album.blah
    a.artist.should == @artist
  end

  it "should handle cascading using a hash for the :include option" do
    Artist.from_json(@artist.to_json(:include=>{:albums=>{:include=>:artist}})).albums.map{|a| a.artist}.should == [@artist]
    Album.from_json(@album.to_json(:include=>{:artist=>{:include=>:albums}})).artist.albums.should == [@album]

    Artist.from_json(@artist.to_json(:include=>{:albums=>{:only=>:name}})).albums.should == [Album.load(:name=>@album.name)]
    Album.from_json(@album.to_json(:include=>{:artist=>{:except=>:name}})).artist.should == Artist.load(:id=>@artist.id)

    Artist.from_json(@artist.to_json(:include=>{:albums=>{:include=>{:artist=>{:include=>:albums}}}})).albums.map{|a| a.artist.albums}.should == [[@album]]
    Album.from_json(@album.to_json(:include=>{:artist=>{:include=>{:albums=>{:only=>:name}}}})).artist.albums.should == [Album.load(:name=>@album.name)]
  end

  it "should handle the :include option cascading with an empty hash" do
    Album.from_json(@album.to_json(:include=>{:artist=>{}})).artist.should == @artist
    Album.from_json(@album.to_json(:include=>{:blah=>{}})).blah.should == @album.blah
  end

  it "should accept a :naked option to not include the JSON.create_id, so parsing yields a plain hash" do
    Sequel.parse_json(@album.to_json(:naked=>true)).should == @album.values.inject({}){|h, (k, v)| h[k.to_s] = v; h}
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

  it "should have #from_json raise an error if parsed json isn't a hash" do
    proc{@artist.from_json('[]')}.should raise_error(Sequel::Error)
  end

  it "should raise an exception for json keys that aren't associations, columns, or setter methods" do
    Album.send(:undef_method, :blah=)
    proc{Album.from_json(@album.to_json(:include=>:blah))}.should raise_error(Sequel::Error)
  end

  it "should support a to_json class and dataset method" do
    Album.dataset._fetch = {:id=>1, :name=>'RF', :artist_id=>2}
    Artist.dataset._fetch = {:id=>2, :name=>'YJM'}
    Album.array_from_json(Album.to_json).should == [@album]
    Album.array_from_json(Album.to_json(:include=>:artist)).map{|x| x.artist}.should == [@artist]
    Album.array_from_json(Album.dataset.to_json(:only=>:name)).should == [Album.load(:name=>@album.name)]
  end

  it "should have dataset to_json method work with naked datasets" do
    album = @album
    ds = Album.dataset.naked
    ds._fetch = {:id=>1, :name=>'RF', :artist_id=>2}
    Sequel.parse_json(ds.to_json).should == [@album.values.inject({}){|h, (k, v)| h[k.to_s] = v; h}]
  end

  it "should have dataset to_json method respect :array option for the array to use" do
    a = Album.load(:id=>1, :name=>'RF', :artist_id=>3)
    Album.array_from_json(Album.to_json(:array=>[a])).should == [a]

    a.associations[:artist] = artist = Artist.load(:id=>3, :name=>'YJM')
    Album.array_from_json(Album.to_json(:array=>[a], :include=>:artist)).first.artist.should == artist

    artist.associations[:albums] = [a]
    x = Artist.array_from_json(Artist.to_json(:array=>[artist], :include=>:albums))
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
    Artist2.from_json(Artist2.load(:id=>2, :name=>'YYY').to_json).should == Artist2.load(:name=>'YYY')
    class ::Artist3 < Artist2
      plugin :json_serializer, :naked=>:true
    end
    Sequel.parse_json(Artist3.load(:id=>2, :name=>'YYY').to_json).should == {"name"=>'YYY'}
    Object.send(:remove_const, :Artist2)
    Object.send(:remove_const, :Artist3)
  end
end
