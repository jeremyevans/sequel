require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::CsvSerializer" do
  before do
    class ::Artist < Sequel::Model
      unrestrict_primary_key
      plugin :csv_serializer
      columns :id, :name
      def_column_accessor :id, :name
      @db_schema = {:id=>{:type=>:integer}}
      one_to_many :albums
    end
    class ::Album < Sequel::Model
      unrestrict_primary_key
      attr_accessor :blah
      plugin :csv_serializer
      columns :id, :name, :artist_id
      def_column_accessor :id, :name, :artist_id
      @db_schema = {:id=>{:type=>:integer}, :artist_id=>{:type=>:integer}}
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
    Artist.from_csv(@artist.to_csv).should == @artist
    Album.from_csv(@album.to_csv).should == @album
  end

  it "should handle ruby objects in values" do
    class ::Artist
      def name=(v)
        super(Date.parse(v))
      end
    end
    Artist.from_csv(Artist.load(:name=>Date.today).to_csv).
      should == Artist.load(:name=>Date.today)
  end

  it "should handle the :only option" do
    Artist.from_csv(@artist.to_csv(:only=>:name), :only=>:name).
      should == Artist.load(:name=>@artist.name)
    Album.from_csv(@album.to_csv(:only=>[:id, :name]), :only=>[:id, :name]).
      should == Album.load(:id=>@album.id, :name=>@album.name)
  end

  it "should handle the :except option" do
    Artist.from_csv(@artist.to_csv(:except=>:id), :except=>:id).
      should == Artist.load(:name=>@artist.name)
    Album.from_csv(@album.to_csv(:except=>[:id, :artist_id]),
                   :except=>[:id, :artist_id]).
      should == Album.load(:name=>@album.name)
  end

  it "should handle the :include option for arbitrary attributes" do
    Album.from_csv(@album.to_csv(:include=>:blah), :include=>:blah).
      blah.should == @album.blah
  end

  it "should handle multiple inclusions using an array for the :include option" do
    a = Album.from_csv(@album.to_csv(:include=>[:blah]), :include=>:blah)
    a.blah.should == @album.blah
  end

  it "should support #from_csv to set column values" do
    @artist.from_csv('AS', :only=>:name)
    @artist.name.should == 'AS'
    @artist.id.should == 2

    @artist.from_csv('1', :only=>:id)
    @artist.name.should == 'AS'
    @artist.id.should == 1
  end

  it "should support #from_csv to support specific :fields" do
    @album.from_csv('AS,2', :headers=>['name'])
    @album.name.should == 'AS'
    @album.artist_id.should == 2

    @album.from_csv('2,AS', :headers=>[nil, 'name'])
    @album.name.should == 'AS'
    @album.artist_id.should == 2
  end

  it "should support a to_csv class and dataset method" do
    Album.dataset._fetch = {:id=>1, :name=>'RF', :artist_id=>2}
    Artist.dataset._fetch = {:id=>2, :name=>'YJM'}
    Album.array_from_csv(Album.to_csv).should == [@album]
    Album.array_from_csv(Album.dataset.to_csv(:only=>:name), :only=>:name).
      should == [Album.load(:name=>@album.name)]
  end

  it "should have dataset to_csv method respect :array option" do
    a = Album.new(:id=>1, :name=>'RF', :artist_id=>3)
    Album.array_from_csv(Album.to_csv(:array=>[a])).should == [a]
  end

  it "should propagate class default options to instance to_csv output" do
    class ::Album2 < Sequel::Model
      attr_accessor :blah
      plugin :csv_serializer, :except => :id
      columns :id, :name, :artist_id
      many_to_one :artist
    end
    @album2 = Album2.load(:id=>2, :name=>'JK')
    @album2.artist = @artist
    @album2.blah = 'Gak'

    csv = Sequel::Plugins::CsvSerializer::CSV
    csv.parse(@album2.to_csv(:write_headers=>true), :headers=>true).
      first.to_hash.
      should == @album2.
               values.
               reject { |k, _v| k.to_s == 'id' }.
               reduce({}) { |h, (k, v)| h[k.to_s] = v.to_s; h }
    csv.parse(@album2.to_csv(:only=>:name, :write_headers=>true),
              :headers=>true).
      first.to_hash.
      should == @album2.
               values.
               reject { |k, _v| k.to_s != 'name' }.
               reduce({}) { |h, (k, v)| h[k.to_s] = v.to_s; h }
    csv.parse(@album2.to_csv(:except=>:artist_id, :write_headers=>true),
              :headers=>true).
      first.to_hash.
      should == @album2.
               values.
               reject { |k, _v| k.to_s == 'artist_id' }.
               reduce({}) { |h, (k, v)| h[k.to_s] = v.to_s; h }
  end

  it "should store the default options in csv_serializer_opts" do
    Album.csv_serializer_opts.should == {}
    c = Class.new(Album)
    c.plugin :csv_serializer, :naked=>false
    c.csv_serializer_opts.should == {:naked=>false}
  end

  it "should work correctly when subclassing" do
    ::Artist2 = Class.new(Artist)
    Artist2.plugin :csv_serializer, :only=>:name
    ::Artist3 = Class.new(Artist2)
    Artist3.from_csv(Artist3.load(:id=>2, :name=>'YYY').to_csv).
      should == Artist3.load(:name=>'YYY')
    Object.send(:remove_const, :Artist2)
    Object.send(:remove_const, :Artist3)
  end

  it "should raise an error if attempting to set a restricted column "\
     "and :all_columns is not used" do
    Artist.restrict_primary_key
    proc{Artist.from_csv(@artist.to_csv)}.should raise_error(Sequel::Error)
  end

  it "should use a dataset's selected columns" do
    columns = [:id]
    ds = Artist.select(*columns).limit(1)
    ds.instance_variable_set(:@columns, columns)
    ds._fetch = [:id => 10]
    ds.to_csv(:write_headers => true).should == "id\n10\n"
  end

  it "should pass all the examples from the documentation" do
    @album.to_csv(:write_headers=>true).should == "id,name,artist_id\n1,RF,2\n"
    @album.to_csv(:only=>:name).should == "RF\n"
    @album.to_csv(:except=>[:id, :artist_id]).should == "RF\n"
  end
end
