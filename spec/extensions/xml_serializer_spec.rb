require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

begin
  require 'nokogiri'
rescue LoadError => e
  skip_warn "xml_serializer plugin: can't load nokogiri (#{e.class}: #{e})"
else
describe "Sequel::Plugins::XmlSerializer" do
  before do
    class ::Artist < Sequel::Model
      plugin :xml_serializer
      columns :id, :name
      @db_schema = {:id=>{:type=>:integer}, :name=>{:type=>:string}}
      one_to_many :albums
    end
    class ::Album < Sequel::Model
      attr_accessor :blah
      plugin :xml_serializer
      columns :id, :name, :artist_id
      @db_schema = {:id=>{:type=>:integer}, :name=>{:type=>:string}, :artist_id=>{:type=>:integer}}
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
    Artist.from_xml(@artist.to_xml).should == @artist
    Album.from_xml(@album.to_xml).should == @album
  end

  it "should handle the :only option" do
    Artist.from_xml(@artist.to_xml(:only=>:name)).should == Artist.load(:name=>@artist.name)
    Album.from_xml(@album.to_xml(:only=>[:id, :name])).should == Album.load(:id=>@album.id, :name=>@album.name)
  end

  it "should handle the :except option" do
    Artist.from_xml(@artist.to_xml(:except=>:id)).should == Artist.load(:name=>@artist.name)
    Album.from_xml(@album.to_xml(:except=>[:id, :artist_id])).should == Album.load(:name=>@album.name)
  end

  it "should handle the :include option for associations" do
    Artist.from_xml(@artist.to_xml(:include=>:albums)).albums.should == [@album]
    Album.from_xml(@album.to_xml(:include=>:artist)).artist.should == @artist
  end

  it "should handle the :include option for arbitrary attributes" do
    Album.from_xml(@album.to_xml(:include=>:blah)).blah.should == @album.blah
  end

  it "should handle multiple inclusions using an array for the :include option" do
    a = Album.from_xml(@album.to_xml(:include=>[:blah, :artist]))
    a.blah.should == @album.blah
    a.artist.should == @artist
  end

  it "should handle cascading using a hash for the :include option" do
    Artist.from_xml(@artist.to_xml(:include=>{:albums=>{:include=>:artist}})).albums.map{|a| a.artist}.should == [@artist]
    Album.from_xml(@album.to_xml(:include=>{:artist=>{:include=>:albums}})).artist.albums.should == [@album]

    Artist.from_xml(@artist.to_xml(:include=>{:albums=>{:only=>:name}})).albums.should == [Album.load(:name=>@album.name)]
    Album.from_xml(@album.to_xml(:include=>{:artist=>{:except=>:name}})).artist.should == Artist.load(:id=>@artist.id)

    Artist.from_xml(@artist.to_xml(:include=>{:albums=>{:include=>{:artist=>{:include=>:albums}}}})).albums.map{|a| a.artist.albums}.should == [[@album]]
    Album.from_xml(@album.to_xml(:include=>{:artist=>{:include=>{:albums=>{:only=>:name}}}})).artist.albums.should == [Album.load(:name=>@album.name)]
  end

  it "should handle the :include option cascading with an empty hash" do
    Album.from_xml(@album.to_xml(:include=>{:artist=>{}})).artist.should == @artist
    Album.from_xml(@album.to_xml(:include=>{:blah=>{}})).blah.should == @album.blah
  end

  it "should support #from_xml to set column values" do
    @artist.from_xml('<album><name>AS</name></album>')
    @artist.name.should == 'AS'
    @artist.id.should == 2
  end

  it "should support a :name_proc option when serializing and deserializing" do
    Album.from_xml(@album.to_xml(:name_proc=>proc{|s| s.reverse}), :name_proc=>proc{|s| s.reverse}).should == @album
  end

  it "should support a :camelize option when serializing and :underscore option when deserializing" do
    Album.from_xml(@album.to_xml(:camelize=>true), :underscore=>true).should == @album
  end

  it "should support a :camelize option when serializing and :underscore option when deserializing" do
    Album.from_xml(@album.to_xml(:dasherize=>true), :underscore=>true).should == @album
  end

  it "should support an :encoding option when serializing" do
    ["<?xml version=\"1.0\" encoding=\"utf-8\"?><artist><id>2</id><name>YJM</name></artist>",
     "<?xml version=\"1.0\" encoding=\"utf-8\"?><artist><name>YJM</name><id>2</id></artist>"].should include(@artist.to_xml(:encoding=>'utf-8').gsub(/\n */m, ''))
  end

  it "should support a :builder_opts option when serializing" do
    ["<?xml version=\"1.0\" encoding=\"utf-8\"?><artist><id>2</id><name>YJM</name></artist>",
     "<?xml version=\"1.0\" encoding=\"utf-8\"?><artist><name>YJM</name><id>2</id></artist>"].should include(@artist.to_xml(:builder_opts=>{:encoding=>'utf-8'}).gsub(/\n */m, ''))
  end

  it "should support an :types option when serializing" do
    ["<?xml version=\"1.0\"?><artist><id type=\"integer\">2</id><name type=\"string\">YJM</name></artist>",
     "<?xml version=\"1.0\"?><artist><name type=\"string\">YJM</name><id type=\"integer\">2</id></artist>"].should include(@artist.to_xml(:types=>true).gsub(/\n */m, ''))
  end

  it "should support an :root_name option when serializing" do
    ["<?xml version=\"1.0\"?><ar><id>2</id><name>YJM</name></ar>",
     "<?xml version=\"1.0\"?><ar><name>YJM</name><id>2</id></ar>"].should include(@artist.to_xml(:root_name=>'ar').gsub(/\n */m, ''))
  end

  it "should support an :array_root_name option when serializing arrays" do
    artist = @artist
    Artist.dataset.meta_def(:all){[artist]}
    ["<?xml version=\"1.0\"?><ars><ar><id>2</id><name>YJM</name></ar></ars>",
     "<?xml version=\"1.0\"?><ars><ar><name>YJM</name><id>2</id></ar></ars>"].should include(Artist.to_xml(:array_root_name=>'ars', :root_name=>'ar').gsub(/\n */m, ''))
  end

  it "should raise an exception for xml tags that aren't associations, columns, or setter methods" do
    Album.send(:undef_method, :blah=)
    proc{Album.from_xml(@album.to_xml(:include=>:blah))}.should raise_error(Sequel::Error)
  end

  it "should support a to_xml class and dataset method" do
    album = @album
    Album.dataset.meta_def(:all){[album]}
    Album.array_from_xml(Album.to_xml).should == [@album]
    Album.array_from_xml(Album.to_xml(:include=>:artist)).map{|x| x.artist}.should == [@artist]
    Album.array_from_xml(Album.dataset.to_xml(:only=>:name)).should == [Album.load(:name=>@album.name)]
  end

  it "should raise an error if the dataset does not have a row_proc" do
    proc{Album.dataset.naked.to_xml}.should raise_error(Sequel::Error)
  end
end
end
