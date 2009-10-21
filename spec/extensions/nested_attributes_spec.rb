require File.join(File.dirname(__FILE__), "spec_helper")

describe "NestedAttributes plugin" do
  before do
    mods = @mods = []
    i = 0
    gi = lambda{i}
    ii = lambda{i+=1}
    ds_mod = Module.new do
      def empty?; false; end
      define_method(:insert) do |h|
        x = ii.call
        mods << [:i, first_source, h, x]
        x
      end
      define_method(:insert_select) do |h|
        x = ii.call
        mods << [:is, first_source, h, x]
        h.merge(:id=>x)
      end
      define_method(:update) do |h|
        mods << [:u, first_source, h, literal(opts[:where])]
        1
      end
      define_method(:delete) do
        mods << [:d, first_source, literal(opts[:where])]
        1
      end
    end
    db = Sequel::Database.new({})
    db.meta_def(:dataset) do |*a|
      x = super(*a)
      x.extend(ds_mod)
      x
    end
    @c = Class.new(Sequel::Model(db))
    @c.plugin :nested_attributes
    @Artist = Class.new(@c).set_dataset(:artists)
    @Album = Class.new(@c).set_dataset(:albums)
    @Tag = Class.new(@c).set_dataset(:tags)
    [@Artist, @Album, @Tag].each do |m|
      m.dataset.extend(ds_mod)
    end
    @Artist.columns :id, :name
    @Album.columns :id, :name, :artist_id
    @Tag.columns :id, :name
    @Artist.one_to_many :albums, :class=>@Album, :key=>:artist_id
    @Album.many_to_one :artist, :class=>@Artist
    @Album.many_to_many :tags, :class=>@Tag, :left_key=>:album_id, :right_key=>:tag_id, :join_table=>:at
    @Artist.nested_attributes :albums, :destroy=>true, :remove=>true
    @Album.nested_attributes :artist, :tags, :destroy=>true, :remove=>true
  end
  
  it "should support creating new many_to_one objects" do
    a = @Album.new({:name=>'Al', :artist_attributes=>{:name=>'Ar'}})
    @mods.should == []
    a.save
    @mods.should == [[:is, :artists, {:name=>"Ar"}, 1], [:is, :albums, {:name=>"Al", :artist_id=>1}, 2]]
  end
  
  it "should support creating new one_to_many objects" do
    a = @Artist.new({:name=>'Ar', :albums_attributes=>[{:name=>'Al'}]})
    @mods.should == []
    a.save
    @mods.should == [[:is, :artists, {:name=>"Ar"}, 1], [:is, :albums, {:name=>"Al", :artist_id=>1}, 2]]
  end
  
  it "should support creating new many_to_many objects" do
    a = @Album.new({:name=>'Al', :tags_attributes=>[{:name=>'T'}]})
    @mods.should == []
    a.save
    @mods.should == [[:is, :albums, {:name=>"Al"}, 1], [:is, :tags, {:name=>"T"}, 2], [:i, :at, {:album_id=>1, :tag_id=>2}, 3]]
  end
  
  it "should support updating many_to_one objects" do
    al = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    al.associations[:artist] = ar
    al.set(:artist_attributes=>{:id=>'20', :name=>'Ar2'})
    @mods.should == []
    al.save
    @mods.should == [[:u, :albums, {:name=>"Al"}, '(id = 10)'], [:u, :artists, {:name=>"Ar2"}, '(id = 20)']]
  end
  
  it "should support updating one_to_many objects" do
    al = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    ar.associations[:albums] = [al]
    ar.set(:albums_attributes=>[{:id=>10, :name=>'Al2'}])
    @mods.should == []
    ar.save
    @mods.should == [[:u, :artists, {:name=>"Ar"}, '(id = 20)'], [:u, :albums, {:name=>"Al2"}, '(id = 10)']]
  end
  
  it "should support updating many_to_many objects" do
    a = @Album.load(:id=>10, :name=>'Al')
    t = @Tag.load(:id=>20, :name=>'T')
    a.associations[:tags] = [t]
    a.set(:tags_attributes=>[{:id=>20, :name=>'T2'}])
    @mods.should == []
    a.save
    @mods.should == [[:u, :albums, {:name=>"Al"}, '(id = 10)'], [:u, :tags, {:name=>"T2"}, '(id = 20)']]
  end
  
  it "should support removing many_to_one objects" do
    al = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    al.associations[:artist] = ar
    al.set(:artist_attributes=>{:id=>'20', :_remove=>'1'})
    @mods.should == []
    al.save
    @mods.should == [[:u, :albums, {:artist_id=>nil, :name=>'Al'}, '(id = 10)']]
  end
  
  it "should support removing one_to_many objects" do
    al = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    ar.associations[:albums] = [al]
    ar.set(:albums_attributes=>[{:id=>10, :_remove=>'t'}])
    @mods.should == []
    ar.save
    @mods.should == [[:u, :albums, {:name=>"Al", :artist_id=>nil}, '(id = 10)'], [:u, :artists, {:name=>"Ar"}, '(id = 20)']]
  end
  
  it "should support removing many_to_many objects" do
    a = @Album.load(:id=>10, :name=>'Al')
    t = @Tag.load(:id=>20, :name=>'T')
    a.associations[:tags] = [t]
    a.set(:tags_attributes=>[{:id=>20, :_remove=>true}])
    @mods.should == []
    a.save
    @mods.should == [[:d, :at, '((album_id = 10) AND (tag_id = 20))'], [:u, :albums, {:name=>"Al"}, '(id = 10)']]
  end
  
  it "should support destroying many_to_one objects" do
    al = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    al.associations[:artist] = ar
    al.set(:artist_attributes=>{:id=>'20', :_delete=>'1'})
    @mods.should == []
    al.save
    @mods.should == [[:u, :albums, {:artist_id=>nil, :name=>'Al'}, '(id = 10)'], [:d, :artists, '(id = 20)']]
  end
  
  it "should support destroying one_to_many objects" do
    al = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    ar.associations[:albums] = [al]
    ar.set(:albums_attributes=>[{:id=>10, :_delete=>'t'}])
    @mods.should == []
    ar.save
    @mods.should == [[:u, :albums, {:name=>"Al", :artist_id=>nil}, '(id = 10)'], [:u, :artists, {:name=>"Ar"}, '(id = 20)'], [:d, :albums, '(id = 10)']]
  end
  
  it "should support destroying many_to_many objects" do
    a = @Album.load(:id=>10, :name=>'Al')
    t = @Tag.load(:id=>20, :name=>'T')
    a.associations[:tags] = [t]
    a.set(:tags_attributes=>[{:id=>20, :_delete=>true}])
    @mods.should == []
    a.save
    @mods.should == [[:d, :at, '((album_id = 10) AND (tag_id = 20))'], [:u, :albums, {:name=>"Al"}, '(id = 10)'], [:d, :tags, '(id = 20)']]
  end
  
  it "should support both string and symbol keys in nested attribute hashes" do
    a = @Album.load(:id=>10, :name=>'Al')
    t = @Tag.load(:id=>20, :name=>'T')
    a.associations[:tags] = [t]
    a.set('tags_attributes'=>[{'id'=>20, '_delete'=>true}])
    @mods.should == []
    a.save
    @mods.should == [[:d, :at, '((album_id = 10) AND (tag_id = 20))'], [:u, :albums, {:name=>"Al"}, '(id = 10)'], [:d, :tags, '(id = 20)']]
  end
  
  it "should support using a hash instead of an array for to_many nested attributes" do
    a = @Album.load(:id=>10, :name=>'Al')
    t = @Tag.load(:id=>20, :name=>'T')
    a.associations[:tags] = [t]
    a.set('tags_attributes'=>{'1'=>{'id'=>20, '_delete'=>true}})
    @mods.should == []
    a.save
    @mods.should == [[:d, :at, '((album_id = 10) AND (tag_id = 20))'], [:u, :albums, {:name=>"Al"}, '(id = 10)'], [:d, :tags, '(id = 20)']]
  end
  
  it "should only allow destroying associated objects if :destroy option is used in the nested_attributes call" do
    a = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    a.associations[:artist] = ar
    @Album.nested_attributes :artist
    proc{a.set(:artist_attributes=>{:id=>'20', :_delete=>'1'})}.should raise_error(Sequel::Error)
    @Album.nested_attributes :artist, :destroy=>true
    proc{a.set(:artist_attributes=>{:id=>'20', :_delete=>'1'})}.should_not raise_error(Sequel::Error)
  end
  
  it "should only allow removing associated objects if :remove option is used in the nested_attributes call" do
    a = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    a.associations[:artist] = ar
    @Album.nested_attributes :artist
    proc{a.set(:artist_attributes=>{:id=>'20', :_remove=>'1'})}.should raise_error(Sequel::Error)
    @Album.nested_attributes :artist, :remove=>true
    proc{a.set(:artist_attributes=>{:id=>'20', :_remove=>'1'})}.should_not raise_error(Sequel::Error)
  end
  
  it "should raise an Error if a primary key is given in a nested attribute hash, but no matching associated object exists" do
    al = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    ar.associations[:albums] = [al]
    proc{ar.set(:albums_attributes=>[{:id=>30, :_delete=>'t'}])}.should raise_error(Sequel::Error)
    proc{ar.set(:albums_attributes=>[{:id=>10, :_delete=>'t'}])}.should_not raise_error(Sequel::Error)
  end
  
  it "should not raise an Error if an unmatched primary key is given, if the :strict=>false option is used" do
    @Artist.nested_attributes :albums, :strict=>false
    al = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    ar.associations[:albums] = [al]
    ar.set(:albums_attributes=>[{:id=>30, :_delete=>'t'}])
    @mods.should == []
    ar.save
    @mods.should == [[:u, :artists, {:name=>"Ar"}, '(id = 20)']]
  end
  
  it "should not save if nested attribute is not valid and should include nested attribute validation errors in the main object's validation errors" do
    @Artist.class_eval do
      def validate
        super
        errors.add(:name, 'cannot be Ar') if name == 'Ar'
      end
    end
    a = @Album.new(:name=>'Al', :artist_attributes=>{:name=>'Ar'})
    @mods.should == []
    proc{a.save}.should raise_error(Sequel::ValidationFailed)
    a.errors.full_messages.should == ['artist name cannot be Ar']
    @mods.should == []
  end
  
  it "should not attempt to validate nested attributes if the :validate=>false association option is used" do
    @Album.many_to_one :artist, :class=>@Artist, :validate=>false
    @Album.nested_attributes :artist, :tags, :destroy=>true, :remove=>true
    @Artist.class_eval do
      def validate
        super
        errors.add(:name, 'cannot be Ar') if name == 'Ar'
      end
    end
    a = @Album.new(:name=>'Al', :artist_attributes=>{:name=>'Ar'})
    @mods.should == []
    a.save
    @mods.should == [[:is, :artists, {:name=>"Ar"}, 1], [:is, :albums, {:name=>"Al", :artist_id=>1}, 2]]
  end
  
  it "should not attempt to validate nested attributes if the :validate=>false option is passed to save" do
    @Artist.class_eval do
      def validate
        super
        errors.add(:name, 'cannot be Ar') if name == 'Ar'
      end
    end
    a = @Album.new(:name=>'Al', :artist_attributes=>{:name=>'Ar'})
    @mods.should == []
    a.save(:validate=>false)
    @mods.should == [[:is, :artists, {:name=>"Ar"}, 1], [:is, :albums, {:name=>"Al", :artist_id=>1}, 2]]
  end
  
  it "should not accept nested attributes unless explicitly specified" do
    @Artist.many_to_many :tags, :class=>@Tag, :left_key=>:album_id, :right_key=>:tag_id, :join_table=>:at
    proc{@Artist.create({:name=>'Ar', :tags_attributes=>[{:name=>'T'}]})}.should raise_error(Sequel::Error)
    @mods.should == []
  end
  
  it "should save when save_changes or update is called if nested attribute associated objects changed but there are no changes to the main object" do
    al = @Album.load(:id=>10, :name=>'Al')
    ar = @Artist.load(:id=>20, :name=>'Ar')
    al.associations[:artist] = ar
    al.update(:artist_attributes=>{:id=>'20', :name=>'Ar2'})
    @mods.should == [[:u, :artists, {:name=>"Ar2"}, '(id = 20)']]
  end
  
  it "should have a :limit option limiting the amount of entries" do
    @Album.nested_attributes :tags, :limit=>2
    arr = [{:name=>'T'}]
    proc{@Album.new({:name=>'Al', :tags_attributes=>arr*3})}.should raise_error(Sequel::Error)
    a = @Album.new({:name=>'Al', :tags_attributes=>arr*2})
    @mods.should == []
    a.save
    @mods.should == [[:is, :albums, {:name=>"Al"}, 1], [:is, :tags, {:name=>"T"}, 2], [:i, :at, {:album_id=>1, :tag_id=>2}, 3], [:is, :tags, {:name=>"T"}, 4], [:i, :at, {:album_id=>1, :tag_id=>4}, 5]]
  end
  
  it "should accept a block that each hash gets passed to determine if it should be processed" do
    @Album.nested_attributes(:tags){|h| h[:name].empty?}
    a = @Album.new({:name=>'Al', :tags_attributes=>[{:name=>'T'}, {:name=>''}, {:name=>'T2'}]})
    @mods.should == []
    a.save
    @mods.should == [[:is, :albums, {:name=>"Al"}, 1], [:is, :tags, {:name=>"T"}, 2], [:i, :at, {:album_id=>1, :tag_id=>2}, 3], [:is, :tags, {:name=>"T2"}, 4], [:i, :at, {:album_id=>1, :tag_id=>4}, 5]]
  end
  
  it "should return objects created/modified in the internal methods" do
    @Album.nested_attributes :tags, :remove=>true, :strict=>false
    objs = []
    @Album.class_eval do
      define_method(:nested_attributes_create){|*a| objs << [super(*a), :create]}
      define_method(:nested_attributes_remove){|*a| objs << [super(*a), :remove]}
      define_method(:nested_attributes_update){|*a| objs << [super(*a), :update]}
    end
    a = @Album.new(:name=>'Al')
    a.associations[:tags] = [@Tag.load(:id=>6, :name=>'A'), @Tag.load(:id=>7, :name=>'A2')] 
    a.tags_attributes = [{:id=>6, :name=>'T'}, {:id=>7, :name=>'T2', :_remove=>true}, {:name=>'T3'}, {:id=>8, :name=>'T4'}, {:id=>9, :name=>'T5', :_remove=>true}]
    objs.should == [[@Tag.load(:id=>6, :name=>'T'), :update], [@Tag.load(:id=>7, :name=>'A2'), :remove], [@Tag.new(:name=>'T3'), :create], [nil, :update], [nil, :remove]]
  end
end
