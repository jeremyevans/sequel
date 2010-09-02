require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

# H2 and MSSQL don't support USING joins
unless [:h2, :mssql].include?(INTEGRATION_DB.database_type)
describe "Class Table Inheritance Plugin" do
  before do
    @db = INTEGRATION_DB
    @db.instance_variable_set(:@schemas, {})
    @db.create_table!(:employees) do
      primary_key :id
      String :name
      String :kind
    end
    @db.create_table!(:managers) do
      foreign_key :id, :employees, :primary_key=>true
      Integer :num_staff
    end
    @db.create_table!(:executives) do
      foreign_key :id, :managers, :primary_key=>true
      Integer :num_managers
    end
    @db.create_table!(:staff) do
      foreign_key :id, :employees, :primary_key=>true
      foreign_key :manager_id, :managers
    end
    class ::Employee < Sequel::Model(@db)
      plugin :class_table_inheritance, :key=>:kind, :table_map=>{:Staff=>:staff}
    end 
    class ::Manager < Employee
      one_to_many :staff_members, :class=>:Staff
    end 
    class ::Executive < Manager
    end 
    class ::Staff < Employee
      many_to_one :manager
    end 
    
    @i1 =@db[:employees].insert(:name=>'E', :kind=>'Employee')
    @i2 = @db[:employees].insert(:name=>'S', :kind=>'Staff')
    @i3 = @db[:employees].insert(:name=>'M', :kind=>'Manager')
    @i4 = @db[:employees].insert(:name=>'Ex', :kind=>'Executive')
    @db[:managers].insert(:id=>@i3, :num_staff=>7)
    @db[:managers].insert(:id=>@i4, :num_staff=>5)
    @db[:executives].insert(:id=>@i4, :num_managers=>6)
    @db[:staff].insert(:id=>@i2, :manager_id=>@i4)
    
    clear_sqls
  end
  after do
    @db.drop_table :staff, :executives, :managers, :employees
    [:Executive, :Manager, :Staff, :Employee].each{|s| Object.send(:remove_const, s)}
  end

  specify "should return rows as subclass instances" do
    Employee.order(:id).all.should == [
      Employee.load(:id=>@i1, :name=>'E', :kind=>'Employee'),
      Staff.load(:id=>@i2, :name=>'S', :kind=>'Staff'),
      Manager.load(:id=>@i3, :name=>'M', :kind=>'Manager'),
      Executive.load(:id=>@i4, :name=>'Ex', :kind=>'Executive')
    ]
  end
  
  specify "should lazily load columns in subclass tables" do
    a = Employee.order(:id).all
    a[1][:manager_id].should == nil
    a[1].manager_id.should == @i4
  end
  
  specify "should include schema for columns for tables for ancestor classes" do
    Employee.db_schema.keys.sort_by{|x| x.to_s}.should == [:id, :kind, :name]
    Staff.db_schema.keys.sort_by{|x| x.to_s}.should == [:id, :kind, :manager_id, :name]
    Manager.db_schema.keys.sort_by{|x| x.to_s}.should == [:id, :kind, :name, :num_staff]
    Executive.db_schema.keys.sort_by{|x| x.to_s}.should == [:id, :kind, :name, :num_managers, :num_staff]
  end
  
  specify "should include columns for tables for ancestor classes" do
    Employee.columns.should == [:id, :name, :kind]
    Staff.columns.should == [:id, :name, :kind, :manager_id]
    Manager.columns.should == [:id, :name, :kind, :num_staff]
    Executive.columns.should == [:id, :name, :kind, :num_staff, :num_managers]
  end
  
  specify "should delete rows from all tables" do
    e = Executive.first
    i = e.id
    e.staff_members_dataset.destroy
    e.destroy
    @db[:executives][:id=>i].should == nil
    @db[:managers][:id=>i].should == nil
    @db[:employees][:id=>i].should == nil
  end
  
  # See http://www.sqlite.org/src/tktview/3338b3fa19ac4abee6c475126a2e6d9d61f26ab1
  cspecify "should insert rows into all tables", :sqlite do
    e = Executive.create(:name=>'Ex2', :num_managers=>8, :num_staff=>9)
    i = e.id
    @db[:employees][:id=>i].should == {:id=>i, :name=>'Ex2', :kind=>'Executive'}
    @db[:managers][:id=>i].should == {:id=>i, :num_staff=>9}
    @db[:executives][:id=>i].should == {:id=>i, :num_managers=>8}
  end
  
  specify "should update rows in all tables" do
    Executive.first.update(:name=>'Ex2', :num_managers=>8, :num_staff=>9)
    @db[:employees][:id=>@i4].should == {:id=>@i4, :name=>'Ex2', :kind=>'Executive'}
    @db[:managers][:id=>@i4].should == {:id=>@i4, :num_staff=>9}
    @db[:executives][:id=>@i4].should == {:id=>@i4, :num_managers=>8}
  end
  
  cspecify "should handle many_to_one relationships", :sqlite do
    m = Staff.first.manager
    m.should == Manager[@i4]
    m.should be_a_kind_of(Executive)
  end
  
  cspecify "should handle eagerly loading many_to_one relationships", :sqlite do
    Staff.limit(1).eager(:manager).all.map{|x| x.manager}.should == [Manager[@i4]]
  end
  
  cspecify "should handle eagerly graphing many_to_one relationships", :sqlite do
    ss = Staff.eager_graph(:manager).all
    ss.should == [Staff[@i2]]
    ss.map{|x| x.manager}.should == [Manager[@i4]]
  end
  
  specify "should handle one_to_many relationships" do
    Executive.first.staff_members.should == [Staff[@i2]]
  end
  
  specify "should handle eagerly loading one_to_many relationships" do
    Executive.limit(1).eager(:staff_members).first.staff_members.should == [Staff[@i2]]
  end
  
  cspecify "should handle eagerly graphing one_to_many relationships", :sqlite do
    es = Executive.limit(1).eager_graph(:staff_members).all
    es.should == [Executive[@i4]]
    es.map{|x| x.staff_members}.should == [[Staff[@i2]]]
  end
end
end

describe "Many Through Many Plugin" do
  before do
    @db = INTEGRATION_DB
    @db.instance_variable_set(:@schemas, {})
    @db.create_table!(:albums) do
      primary_key :id
      String :name
    end
    @db.create_table!(:artists) do
      primary_key :id
      String :name
    end
    @db.create_table!(:albums_artists) do
      foreign_key :album_id, :albums
      foreign_key :artist_id, :artists
    end
    class ::Album < Sequel::Model(@db)
      many_to_many :artists
    end 
    class ::Artist < Sequel::Model(@db)
      plugin :many_through_many
    end 
    
    @artist1 = Artist.create(:name=>'1')
    @artist2 = Artist.create(:name=>'2')
    @artist3 = Artist.create(:name=>'3')
    @artist4 = Artist.create(:name=>'4')
    @album1 = Album.create(:name=>'A')
    @album1.add_artist(@artist1)
    @album1.add_artist(@artist2)
    @album2 = Album.create(:name=>'B')
    @album2.add_artist(@artist3)
    @album2.add_artist(@artist4)
    @album3 = Album.create(:name=>'C')
    @album3.add_artist(@artist2)
    @album3.add_artist(@artist3)
    @album4 = Album.create(:name=>'D')
    @album4.add_artist(@artist1)
    @album4.add_artist(@artist4)
    
    clear_sqls
  end
  after do
    @db.drop_table :albums_artists, :albums, :artists
    [:Album, :Artist].each{|s| Object.send(:remove_const, s)}
  end
  
  specify "should handle super simple case with 1 join table" do
    Artist.many_through_many :albums, [[:albums_artists, :artist_id, :album_id]]
    Artist[1].albums.map{|x| x.name}.sort.should == %w'A D'
    Artist[2].albums.map{|x| x.name}.sort.should == %w'A C'
    Artist[3].albums.map{|x| x.name}.sort.should == %w'B C'
    Artist[4].albums.map{|x| x.name}.sort.should == %w'B D'
    
    Artist.filter(:id=>1).eager(:albums).all.map{|x| x.albums.map{|a| a.name}}.flatten.sort.should == %w'A D'
    Artist.filter(:id=>2).eager(:albums).all.map{|x| x.albums.map{|a| a.name}}.flatten.sort.should == %w'A C'
    Artist.filter(:id=>3).eager(:albums).all.map{|x| x.albums.map{|a| a.name}}.flatten.sort.should == %w'B C'
    Artist.filter(:id=>4).eager(:albums).all.map{|x| x.albums.map{|a| a.name}}.flatten.sort.should == %w'B D'
    
    Artist.filter(:artists__id=>1).eager_graph(:albums).all.map{|x| x.albums.map{|a| a.name}}.flatten.sort.should == %w'A D'
    Artist.filter(:artists__id=>2).eager_graph(:albums).all.map{|x| x.albums.map{|a| a.name}}.flatten.sort.should == %w'A C'
    Artist.filter(:artists__id=>3).eager_graph(:albums).all.map{|x| x.albums.map{|a| a.name}}.flatten.sort.should == %w'B C'
    Artist.filter(:artists__id=>4).eager_graph(:albums).all.map{|x| x.albums.map{|a| a.name}}.flatten.sort.should == %w'B D'
  end

  specify "should handle typical case with 3 join tables" do
    Artist.many_through_many :related_artists, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_artists, :album_id, :artist_id]], :class=>Artist, :distinct=>true
    Artist[1].related_artists.map{|x| x.name}.sort.should == %w'1 2 4'
    Artist[2].related_artists.map{|x| x.name}.sort.should == %w'1 2 3'
    Artist[3].related_artists.map{|x| x.name}.sort.should == %w'2 3 4'
    Artist[4].related_artists.map{|x| x.name}.sort.should == %w'1 3 4'
    
    Artist.filter(:id=>1).eager(:related_artists).all.map{|x| x.related_artists.map{|a| a.name}}.flatten.sort.should == %w'1 2 4'
    Artist.filter(:id=>2).eager(:related_artists).all.map{|x| x.related_artists.map{|a| a.name}}.flatten.sort.should == %w'1 2 3'
    Artist.filter(:id=>3).eager(:related_artists).all.map{|x| x.related_artists.map{|a| a.name}}.flatten.sort.should == %w'2 3 4'
    Artist.filter(:id=>4).eager(:related_artists).all.map{|x| x.related_artists.map{|a| a.name}}.flatten.sort.should == %w'1 3 4'
    
    Artist.filter(:artists__id=>1).eager_graph(:related_artists).all.map{|x| x.related_artists.map{|a| a.name}}.flatten.sort.should == %w'1 2 4'
    Artist.filter(:artists__id=>2).eager_graph(:related_artists).all.map{|x| x.related_artists.map{|a| a.name}}.flatten.sort.should == %w'1 2 3'
    Artist.filter(:artists__id=>3).eager_graph(:related_artists).all.map{|x| x.related_artists.map{|a| a.name}}.flatten.sort.should == %w'2 3 4'
    Artist.filter(:artists__id=>4).eager_graph(:related_artists).all.map{|x| x.related_artists.map{|a| a.name}}.flatten.sort.should == %w'1 3 4'
  end

  specify "should handle extreme case with 5 join tables" do
    Artist.many_through_many :related_albums, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_artists, :album_id, :artist_id], [:artists, :id, :id], [:albums_artists, :artist_id, :album_id]], :class=>Album, :distinct=>true
    @db[:albums_artists].delete
    @album1.add_artist(@artist1)
    @album1.add_artist(@artist2)
    @album2.add_artist(@artist2)
    @album2.add_artist(@artist3)
    @album3.add_artist(@artist1)
    @album4.add_artist(@artist3)
    @album4.add_artist(@artist4)
    
    Artist[1].related_albums.map{|x| x.name}.sort.should == %w'A B C'
    Artist[2].related_albums.map{|x| x.name}.sort.should == %w'A B C D'
    Artist[3].related_albums.map{|x| x.name}.sort.should == %w'A B D'
    Artist[4].related_albums.map{|x| x.name}.sort.should == %w'B D'
    
    Artist.filter(:id=>1).eager(:related_albums).all.map{|x| x.related_albums.map{|a| a.name}}.flatten.sort.should == %w'A B C'
    Artist.filter(:id=>2).eager(:related_albums).all.map{|x| x.related_albums.map{|a| a.name}}.flatten.sort.should == %w'A B C D'
    Artist.filter(:id=>3).eager(:related_albums).all.map{|x| x.related_albums.map{|a| a.name}}.flatten.sort.should == %w'A B D'
    Artist.filter(:id=>4).eager(:related_albums).all.map{|x| x.related_albums.map{|a| a.name}}.flatten.sort.should == %w'B D'
    
    Artist.filter(:artists__id=>1).eager_graph(:related_albums).all.map{|x| x.related_albums.map{|a| a.name}}.flatten.sort.should == %w'A B C'
    Artist.filter(:artists__id=>2).eager_graph(:related_albums).all.map{|x| x.related_albums.map{|a| a.name}}.flatten.sort.should == %w'A B C D'
    Artist.filter(:artists__id=>3).eager_graph(:related_albums).all.map{|x| x.related_albums.map{|a| a.name}}.flatten.sort.should == %w'A B D'
    Artist.filter(:artists__id=>4).eager_graph(:related_albums).all.map{|x| x.related_albums.map{|a| a.name}}.flatten.sort.should == %w'B D'
  end
end

describe "Lazy Attributes plugin" do 
  before do
    @db = INTEGRATION_DB
    @db.create_table!(:items) do
      primary_key :id
      String :name
      Integer :num
    end
    class ::Item < Sequel::Model(@db)
      plugin :lazy_attributes, :num
    end
    Item.create(:name=>'J', :num=>1)
  end
  after do
    @db.drop_table(:items)
    Object.send(:remove_const, :Item)
  end
  
  specify "should not include lazy attribute columns by default" do
    Item.first.should == Item.load(:id=>1, :name=>'J')
  end
  
  specify "should load lazy attribute on access" do
    Item.first.num.should == 1
  end
  
  specify "should typecast lazy attribute in setter" do
    i = Item.new
    i.num = '1'
    i.num.should == 1
  end
  
  specify "should load lazy attribute for all items returned when accessing any item if using identity map " do
    Item.create(:name=>'K', :num=>2)
    Item.with_identity_map do
      a = Item.order(:name).all
      a.should == [Item.load(:id=>1, :name=>'J'), Item.load(:id=>2, :name=>'K')]
      a.map{|x| x[:num]}.should == [nil, nil]
      a.first.num.should == 1
      a.map{|x| x[:num]}.should == [1, 2]
      a.last.num.should == 2
    end
  end
end

describe "Tactical Eager Loading Plugin" do
  before do
    @db = INTEGRATION_DB
    @db.instance_variable_set(:@schemas, {})
    @db.create_table!(:artists) do
      primary_key :id
      String :name
    end
    @db.create_table!(:albums) do
      primary_key :id
      String :name
      foreign_key :artist_id, :artists
    end
    class ::Album < Sequel::Model(@db)
      plugin :tactical_eager_loading
      many_to_one :artist
    end
    class ::Artist < Sequel::Model(@db)
      plugin :tactical_eager_loading
      one_to_many :albums, :order=>:name
    end 
    
    @artist1 = Artist.create(:name=>'1')
    @artist2 = Artist.create(:name=>'2')
    @artist3 = Artist.create(:name=>'3')
    @artist4 = Artist.create(:name=>'4')
    @album1 = Album.create(:name=>'A', :artist=>@artist1)
    @album2 = Album.create(:name=>'B', :artist=>@artist1)
    @album3 = Album.create(:name=>'C', :artist=>@artist2)
    @album4 = Album.create(:name=>'D', :artist=>@artist3)
    
    clear_sqls
  end
  after do
    @db.drop_table :albums, :artists
    [:Album, :Artist].each{|s| Object.send(:remove_const, s)}
  end

  specify "should eagerly load associations for all items when accessing any item" do
    a = Artist.order(:name).all
    a.map{|x| x.associations}.should == [{}, {}, {}, {}]
    a.first.albums.should == [@album1, @album2]
    a.map{|x| x.associations}.should == [{:albums=>[@album1, @album2]}, {:albums=>[@album3]}, {:albums=>[@album4]}, {:albums=>[]}]
    
    a = Album.order(:name).all
    a.map{|x| x.associations}.should == [{}, {}, {}, {}]
    a.first.artist.should == @artist1
    a.map{|x| x.associations}.should == [{:artist=>@artist1}, {:artist=>@artist1}, {:artist=>@artist2}, {:artist=>@artist3}]
  end
end

describe "Identity Map plugin" do 
  before do
    @db = INTEGRATION_DB
    @db.create_table!(:items) do
      primary_key :id
      String :name
      Integer :num
    end
    class ::Item < Sequel::Model(@db)
      plugin :identity_map
    end
    Item.create(:name=>'J', :num=>3)
  end
  after do
    @db.drop_table(:items)
    Object.send(:remove_const, :Item)
  end

  specify "should return the same instance if retrieved more than once" do
    Item.with_identity_map{Item.first.object_id.should == Item.first.object_id}
  end
  
  specify "should merge attributes that don't exist in the model" do
    Item.with_identity_map do 
      i = Item.select(:id, :name).first
      i.values.should == {:id=>1, :name=>'J'}
      Item.first
      i.values.should == {:id=>1, :name=>'J', :num=>3}
    end
  end
end

describe "Touch plugin" do
  before do
    @db = INTEGRATION_DB
    @db.instance_variable_set(:@schemas, {})
    @db.create_table!(:artists) do
      primary_key :id
      String :name
      DateTime :updated_at
    end
    @db.create_table!(:albums) do
      primary_key :id
      String :name
      foreign_key :artist_id, :artists
      DateTime :updated_at
    end
    class ::Album < Sequel::Model(@db)
      many_to_one :artist
      plugin :touch, :associations=>:artist
    end
    class ::Artist < Sequel::Model(@db)
    end 
    
    @artist = Artist.create(:name=>'1')
    @album = Album.create(:name=>'A', :artist=>@artist)
  end
  after do
    @db.drop_table :albums, :artists
    [:Album, :Artist].each{|s| Object.send(:remove_const, s)}
  end

  specify "should update the timestamp column when touching the record" do
    @album.updated_at.should == nil
    @album.touch
    @album.updated_at.to_i.should be_close(Time.now.to_i, 2)
  end
  
  cspecify "should update the timestamp column for associated records when the record is updated or destroyed", [:do, :sqlite], [:jdbc, :sqlite] do
    @artist.updated_at.should == nil
    @album.update(:name=>'B')
    ua = @artist.reload.updated_at
    if ua.is_a?(Time)
      ua.to_i.should be_close(Time.now.to_i, 2)
    else
      (DateTime.now - ua).should be_close(0, 2.0/86400)
    end
    @artist.update(:updated_at=>nil)
    @album.destroy
    if ua.is_a?(Time)
      ua.to_i.should be_close(Time.now.to_i, 2)
    else
      (DateTime.now - ua).should be_close(0, 2.0/86400)
    end
  end
end

describe "Serialization plugin" do 
  before do
    @db = INTEGRATION_DB
    @db.create_table!(:items) do
      primary_key :id
      String :stuff
    end
    class ::Item < Sequel::Model(@db)
      plugin :serialization, :marshal, :stuff
    end
  end
  after do
    @db.drop_table(:items)
    Object.send(:remove_const, :Item)
  end

  specify "should serialize and deserialize items as needed" do
    i = Item.create(:stuff=>{:a=>1})
    i.stuff.should == {:a=>1}
    i.stuff = [1, 2, 3]
    i.save
    Item.first.stuff.should == [1, 2, 3]
    i.update(:stuff=>Item.new)
    Item.first.stuff.should == Item.new
  end
end

describe "OptimisticLocking plugin" do 
  before do
    @db = INTEGRATION_DB
    @db.create_table!(:people) do
      primary_key :id
      String :name
      Integer :lock_version, :default=>0, :null=>false
    end
    class ::Person < Sequel::Model(@db)
      plugin :optimistic_locking
      create(:name=>'John')
    end
  end
  after do
    @db.drop_table(:people)
    Object.send(:remove_const, :Person)
  end

  specify "should raise an error when updating a stale record" do
    p1 = Person[1]
    p2 = Person[1]
    p1.update(:name=>'Jim')
    proc{p2.update(:name=>'Bob')}.should raise_error(Sequel::Plugins::OptimisticLocking::Error)
  end

  specify "should raise an error when destroying a stale record" do
    p1 = Person[1]
    p2 = Person[1]
    p1.update(:name=>'Jim')
    proc{p2.destroy}.should raise_error(Sequel::Plugins::OptimisticLocking::Error)
  end

  specify "should not raise an error when updating the same record twice" do
    p1 = Person[1]
    p1.update(:name=>'Jim')
    proc{p1.update(:name=>'Bob')}.should_not raise_error
  end
end

describe "Composition plugin" do 
  before do
    @db = INTEGRATION_DB
    @db.create_table!(:events) do
      primary_key :id
      Integer :year
      Integer :month
      Integer :day
    end
    class ::Event < Sequel::Model(@db)
      plugin :composition
      composition :date, :composer=>proc{Date.new(year, month, day) if year && month && day}, :decomposer=>(proc do
          if date
            self.year = date.year
            self.month = date.month
            self.day = date.day
          else
            self.year, self.month, self.day = nil
          end
        end)
      composition :date, :mapping=>[:year, :month, :day]
    end
    @e1 = Event.create(:year=>2010, :month=>2, :day=>15)
    @e2 = Event.create({})
  end
  after do
    @db.drop_table(:events)
    Object.send(:remove_const, :Event)
  end

  specify "should return a composed object if the underlying columns have a value" do
    @e1.date.should == Date.civil(2010, 2, 15)
    @e2.date.should == nil
  end

  specify "should decompose the object when saving the record" do
    @e1.date = Date.civil(2009, 1, 2)
    @e1.save
    @e1.year.should == 2009
    @e1.month.should == 1
    @e1.day.should == 2
  end

  specify "should save all columns when saving changes" do
    @e2.date = Date.civil(2009, 10, 2)
    @e2.save_changes
    @e2.reload
    @e2.year.should == 2009
    @e2.month.should == 10
    @e2.day.should == 2
  end
end

if INTEGRATION_DB.dataset.supports_cte?
  describe "RcteTree Plugin" do
    before do
      @db = INTEGRATION_DB
      @db.create_table!(:nodes) do
        primary_key :id
        Integer :parent_id
        String :name
      end
      class ::Node < Sequel::Model(@db)
        plugin :rcte_tree, :order=>:name
      end
      
      @a = Node.create(:name=>'a')
      @b = Node.create(:name=>'b')
      @aa = Node.create(:name=>'aa', :parent=>@a)
      @ab = Node.create(:name=>'ab', :parent=>@a)
      @ba = Node.create(:name=>'ba', :parent=>@b)
      @bb = Node.create(:name=>'bb', :parent=>@b)
      @aaa = Node.create(:name=>'aaa', :parent=>@aa)
      @aab = Node.create(:name=>'aab', :parent=>@aa)
      @aba = Node.create(:name=>'aba', :parent=>@ab)
      @abb = Node.create(:name=>'abb', :parent=>@ab)
      @aaaa = Node.create(:name=>'aaaa', :parent=>@aaa)
      @aaab = Node.create(:name=>'aaab', :parent=>@aaa)
      @aaaaa = Node.create(:name=>'aaaaa', :parent=>@aaaa)
    end
    after do
      @db.drop_table :nodes
      Object.send(:remove_const, :Node)
    end
    
    specify "should load all standard (not-CTE) methods correctly" do
      @a.children.should == [@aa, @ab]
      @b.children.should == [@ba, @bb]
      @aa.children.should == [@aaa, @aab]
      @ab.children.should == [@aba, @abb]
      @ba.children.should == []
      @bb.children.should == []
      @aaa.children.should == [@aaaa, @aaab]
      @aab.children.should == []
      @aba.children.should == []
      @abb.children.should == []
      @aaaa.children.should == [@aaaaa]
      @aaab.children.should == []
      @aaaaa.children.should == []
      
      @a.parent.should == nil
      @b.parent.should == nil
      @aa.parent.should == @a
      @ab.parent.should == @a
      @ba.parent.should == @b
      @bb.parent.should == @b
      @aaa.parent.should == @aa
      @aab.parent.should == @aa
      @aba.parent.should == @ab
      @abb.parent.should == @ab
      @aaaa.parent.should == @aaa
      @aaab.parent.should == @aaa
      @aaaaa.parent.should == @aaaa
    end
    
    specify "should load all ancestors and descendants lazily for a given instance" do
      @a.descendants.should == [@aa, @aaa, @aaaa, @aaaaa, @aaab, @aab, @ab, @aba, @abb]
      @b.descendants.should == [@ba, @bb]
      @aa.descendants.should == [@aaa, @aaaa, @aaaaa, @aaab, @aab]
      @ab.descendants.should == [@aba, @abb]
      @ba.descendants.should == []
      @bb.descendants.should == []
      @aaa.descendants.should == [@aaaa, @aaaaa, @aaab]
      @aab.descendants.should == []
      @aba.descendants.should == []
      @abb.descendants.should == []
      @aaaa.descendants.should == [@aaaaa]
      @aaab.descendants.should == []
      @aaaaa.descendants.should == []
      
      @a.ancestors.should == []
      @b.ancestors.should == []
      @aa.ancestors.should == [@a]
      @ab.ancestors.should == [@a]
      @ba.ancestors.should == [@b]
      @bb.ancestors.should == [@b]
      @aaa.ancestors.should == [@a, @aa]
      @aab.ancestors.should == [@a, @aa]
      @aba.ancestors.should == [@a, @ab]
      @abb.ancestors.should == [@a, @ab]
      @aaaa.ancestors.should == [@a, @aa, @aaa]
      @aaab.ancestors.should == [@a, @aa, @aaa]
      @aaaaa.ancestors.should == [@a, @aa, @aaa, @aaaa]
    end
    
    specify "should eagerly load all ancestors and descendants for a dataset" do
      nodes = Node.filter(:id=>[@a.id, @b.id, @aaa.id]).order(:name).eager(:ancestors, :descendants).all
      nodes.should == [@a, @aaa, @b]
      nodes[0].descendants.should == [@aa, @aaa, @aaaa, @aaaaa, @aaab, @aab, @ab, @aba, @abb]
      nodes[1].descendants.should == [@aaaa, @aaaaa, @aaab]
      nodes[2].descendants.should == [@ba, @bb]
      nodes[0].ancestors.should == []
      nodes[1].ancestors.should == [@a, @aa]
      nodes[2].ancestors.should == []
    end
    
    specify "should eagerly load descendants to a given level" do
      nodes = Node.filter(:id=>[@a.id, @b.id, @aaa.id]).order(:name).eager(:descendants=>1).all
      nodes.should == [@a, @aaa, @b]
      nodes[0].descendants.should == [@aa, @ab]
      nodes[1].descendants.should == [@aaaa, @aaab]
      nodes[2].descendants.should == [@ba, @bb]
      
      nodes = Node.filter(:id=>[@a.id, @b.id, @aaa.id]).order(:name).eager(:descendants=>2).all
      nodes.should == [@a, @aaa, @b]
      nodes[0].descendants.should == [@aa, @aaa, @aab, @ab, @aba, @abb]
      nodes[1].descendants.should == [@aaaa, @aaaaa, @aaab]
      nodes[2].descendants.should == [@ba, @bb]
    end
    
    specify "should populate all :children associations when eagerly loading descendants for a dataset" do
      nodes = Node.filter(:id=>[@a.id, @b.id, @aaa.id]).order(:name).eager(:descendants).all
      nodes[0].associations[:children].should == [@aa, @ab]
      nodes[1].associations[:children].should == [@aaaa, @aaab]
      nodes[2].associations[:children].should == [@ba, @bb]
      nodes[0].associations[:children].map{|c1| c1.associations[:children]}.should == [[@aaa, @aab], [@aba, @abb]]
      nodes[1].associations[:children].map{|c1| c1.associations[:children]}.should == [[@aaaaa], []]
      nodes[2].associations[:children].map{|c1| c1.associations[:children]}.should == [[], []]
      nodes[0].associations[:children].map{|c1| c1.associations[:children].map{|c2| c2.associations[:children]}}.should == [[[@aaaa, @aaab], []], [[], []]]
      nodes[1].associations[:children].map{|c1| c1.associations[:children].map{|c2| c2.associations[:children]}}.should == [[[]], []]
      nodes[0].associations[:children].map{|c1| c1.associations[:children].map{|c2| c2.associations[:children].map{|c3| c3.associations[:children]}}}.should == [[[[@aaaaa], []], []], [[], []]]
      nodes[0].associations[:children].map{|c1| c1.associations[:children].map{|c2| c2.associations[:children].map{|c3| c3.associations[:children].map{|c4| c4.associations[:children]}}}}.should == [[[[[]], []], []], [[], []]]
    end
    
    specify "should not populate :children associations for final level when loading descendants to a given level" do
      nodes = Node.filter(:id=>[@a.id, @b.id, @aaa.id]).order(:name).eager(:descendants=>1).all
      nodes[0].associations[:children].should == [@aa, @ab]
      nodes[0].associations[:children].map{|c1| c1.associations[:children]}.should == [nil, nil]
      nodes[1].associations[:children].should == [@aaaa, @aaab]
      nodes[1].associations[:children].map{|c1| c1.associations[:children]}.should == [nil, nil]
      nodes[2].associations[:children].should == [@ba, @bb]
      nodes[2].associations[:children].map{|c1| c1.associations[:children]}.should == [nil, nil]
      
      nodes[0].associations[:children].map{|c1| c1.children}.should == [[@aaa, @aab], [@aba, @abb]]
      nodes[1].associations[:children].map{|c1| c1.children}.should == [[@aaaaa], []]
      nodes[2].associations[:children].map{|c1| c1.children}.should == [[], []]
      
      nodes = Node.filter(:id=>[@a.id, @b.id, @aaa.id]).order(:name).eager(:descendants=>2).all
      nodes[0].associations[:children].should == [@aa, @ab]
      nodes[0].associations[:children].map{|c1| c1.associations[:children]}.should == [[@aaa, @aab], [@aba, @abb]]
      nodes[0].associations[:children].map{|c1| c1.associations[:children].map{|c2| c2.associations[:children]}}.should == [[[@aaaa, @aaab], nil], [nil, nil]]
      nodes[0].associations[:children].map{|c1| c1.associations[:children].map{|c2| (cc2 = c2.associations[:children]) ? cc2.map{|c3| c3.associations[:children]} : nil}}.should == [[[[@aaaaa], []], nil], [nil, nil]]
      nodes[0].associations[:children].map{|c1| c1.associations[:children].map{|c2| (cc2 = c2.associations[:children]) ? cc2.map{|c3| (cc3 = c3.associations[:children]) ? cc3.map{|c4| c4.associations[:children]} : nil} : nil}}.should == [[[[nil], []], nil], [nil, nil]]
      
      nodes[1].associations[:children].should == [@aaaa, @aaab]
      nodes[1].associations[:children].map{|c1| c1.associations[:children]}.should == [[@aaaaa], []]
      nodes[1].associations[:children].map{|c1| c1.associations[:children].map{|c2| c2.associations[:children]}}.should == [[nil], []]
      
      nodes[2].associations[:children].should == [@ba, @bb]
      nodes[2].associations[:children].map{|c1| c1.associations[:children]}.should == [[], []]
      
      nodes[0].associations[:children].map{|c1| c1.associations[:children].map{|c2| c2.children}}.should == [[[@aaaa, @aaab], []], [[], []]]
      nodes[0].associations[:children].map{|c1| c1.associations[:children].map{|c2| c2.children.map{|c3| c3.children}}}.should == [[[[@aaaaa], []], []], [[], []]]
      nodes[0].associations[:children].map{|c1| c1.associations[:children].map{|c2| c2.children.map{|c3| c3.children.map{|c4| c4.children}}}}.should == [[[[[]], []], []], [[], []]]
      nodes[1].associations[:children].map{|c1| c1.associations[:children].map{|c2| c2.children}}.should == [[[]], []]
    end
    
    specify "should populate all :children associations when lazily loading descendants" do
      @a.descendants
      @a.associations[:children].should == [@aa, @ab]
      @a.associations[:children].map{|c1| c1.associations[:children]}.should == [[@aaa, @aab], [@aba, @abb]]
      @a.associations[:children].map{|c1| c1.associations[:children].map{|c2| c2.associations[:children]}}.should == [[[@aaaa, @aaab], []], [[], []]]
      @a.associations[:children].map{|c1| c1.associations[:children].map{|c2| c2.associations[:children].map{|c3| c3.associations[:children]}}}.should == [[[[@aaaaa], []], []], [[], []]]
      @a.associations[:children].map{|c1| c1.associations[:children].map{|c2| c2.associations[:children].map{|c3| c3.associations[:children].map{|c4| c4.associations[:children]}}}}.should == [[[[[]], []], []], [[], []]]
      
      @b.descendants
      @b.associations[:children].should == [@ba, @bb]
      @b.associations[:children].map{|c1| c1.associations[:children]}.should == [[], []]
      
      @aaa.descendants
      @aaa.associations[:children].map{|c1| c1.associations[:children]}.should == [[@aaaaa], []]
      @aaa.associations[:children].map{|c1| c1.associations[:children].map{|c2| c2.associations[:children]}}.should == [[[]], []]
    end
    
    specify "should populate all :parent associations when eagerly loading ancestors for a dataset" do
      nodes = Node.filter(:id=>[@a.id, @ba.id, @aaa.id, @aaaaa.id]).order(:name).eager(:ancestors).all
      nodes[0].associations.fetch(:parent, 1).should == nil
      nodes[1].associations[:parent].should == @aa
      nodes[1].associations[:parent].associations[:parent].should == @a
      nodes[1].associations[:parent].associations[:parent].associations.fetch(:parent, 1) == nil
      nodes[2].associations[:parent].should == @aaaa
      nodes[2].associations[:parent].associations[:parent].should == @aaa
      nodes[2].associations[:parent].associations[:parent].associations[:parent].should == @aa
      nodes[2].associations[:parent].associations[:parent].associations[:parent].associations[:parent].should == @a
      nodes[2].associations[:parent].associations[:parent].associations[:parent].associations[:parent].associations.fetch(:parent, 1).should == nil
      nodes[3].associations[:parent].should == @b
      nodes[3].associations[:parent].associations.fetch(:parent, 1).should == nil
    end
    
    specify "should populate all :parent associations when lazily loading ancestors" do
      @a.reload
      @a.ancestors
      @a.associations[:parent].should == nil
      
      @ba.reload
      @ba.ancestors
      @ba.associations[:parent].should == @b
      @ba.associations[:parent].associations.fetch(:parent, 1).should == nil
      
      @ba.reload
      @aaaaa.ancestors
      @aaaaa.associations[:parent].should == @aaaa
      @aaaaa.associations[:parent].associations[:parent].should == @aaa
      @aaaaa.associations[:parent].associations[:parent].associations[:parent].should == @aa
      @aaaaa.associations[:parent].associations[:parent].associations[:parent].associations[:parent].should == @a
      @aaaaa.associations[:parent].associations[:parent].associations[:parent].associations[:parent].associations.fetch(:parent, 1).should == nil
    end
  end
end

describe "Instance Filters plugin" do 
  before do
    @db = INTEGRATION_DB
    @db.create_table!(:items) do
      primary_key :id
      String :name
      Integer :cost
      Integer :number
    end
    class ::Item < Sequel::Model(@db)
      plugin :instance_filters
    end
    @i = Item.create(:name=>'J', :number=>1, :cost=>2)
    @i.instance_filter(:number=>1)
    @i.set(:name=>'K')
  end
  after do
    @db.drop_table(:items)
    Object.send(:remove_const, :Item)
  end
  
  specify "should not raise an error if saving only updates one row" do
    @i.save
    @i.refresh.name.should == 'K'
  end
  
  specify "should raise error if saving doesn't update a row" do
    @i.this.update(:number=>2)
    proc{@i.save}.should raise_error(Sequel::Error)
  end
  
  specify "should apply all instance filters" do
    @i.instance_filter{cost <= 2}
    @i.this.update(:number=>2)
    proc{@i.save}.should raise_error(Sequel::Error)
    @i.this.update(:number=>1, :cost=>3)
    proc{@i.save}.should raise_error(Sequel::Error)
    @i.this.update(:cost=>2)
    @i.save
    @i.refresh.name.should == 'K'
  end
  
  specify "should clear instance filters after successful save" do
    @i.save
    @i.this.update(:number=>2)
    @i.update(:name=>'L')
    @i.refresh.name.should == 'L'
  end
  
  specify "should not raise an error if deleting only deletes one row" do
    @i.destroy
    proc{@i.refresh}.should raise_error(Sequel::Error, 'Record not found')
  end
  
  specify "should raise error if destroying doesn't delete a row" do
    @i.this.update(:number=>2)
    proc{@i.destroy}.should raise_error(Sequel::Error)
  end
end

describe "UpdatePrimaryKey plugin" do 
  before do
    @db = INTEGRATION_DB
    @db.create_table!(:t) do
      Integer :a, :primary_key=>true
      Integer :b
    end
    @ds = @db[:t]
    @ds.insert(:a=>1, :b=>3)
    @c = Class.new(Sequel::Model(@ds))
    @c.set_primary_key(:a)
    @c.unrestrict_primary_key
    @c.plugin :update_primary_key
  end
  after do
    @db.drop_table(:t)
  end

  specify "should handle regular updates" do
    @c.first.update(:b=>4)
    @db[:t].all.should == [{:a=>1, :b=>4}]
    @c.first.set(:b=>5).save
    @db[:t].all.should == [{:a=>1, :b=>5}]
    @c.first.set(:b=>6).save(:b)
    @db[:t].all.should == [{:a=>1, :b=>6}]
  end

  specify "should handle updating the primary key field with another field" do
    @c.first.update(:a=>2, :b=>4)
    @db[:t].all.should == [{:a=>2, :b=>4}]
  end

  specify "should handle updating just the primary key field when saving changes" do
    @c.first.update(:a=>2)
    @db[:t].all.should == [{:a=>2, :b=>3}]
    @c.first.set(:a=>3).save(:a)
    @db[:t].all.should == [{:a=>3, :b=>3}]
  end

  specify "should handle saving after modifying the primary key field with another field" do
    @c.first.set(:a=>2, :b=>4).save
    @db[:t].all.should == [{:a=>2, :b=>4}]
  end

  specify "should handle saving after modifying just the primary key field" do
    @c.first.set(:a=>2).save
    @db[:t].all.should == [{:a=>2, :b=>3}]
  end

  specify "should handle saving after updating the primary key" do
    @c.first.update(:a=>2).update(:b=>4).set(:b=>5).save
    @db[:t].all.should == [{:a=>2, :b=>5}]
  end
end

describe "AssociationPks plugin" do 
  before do
    @db = INTEGRATION_DB
    @db.create_table!(:artists) do
      primary_key :id
      String :name
    end
    @db.create_table!(:albums) do
      primary_key :id
      String :name
      foreign_key :artist_id, :artists
    end
    @db.create_table!(:tags) do
      primary_key :id
      String :name
    end
    @db.create_table!(:albums_tags) do
      foreign_key :album_id, :albums
      foreign_key :tag_id, :tags
    end
    class ::Artist < Sequel::Model
      plugin :association_pks
      one_to_many :albums, :order=>:id
    end 
    class ::Album < Sequel::Model
      plugin :association_pks
      many_to_many :tags, :order=>:id
    end 
    class ::Tag < Sequel::Model
    end 
    
    @ar1 =@db[:artists].insert(:name=>'YJM')
    @ar2 =@db[:artists].insert(:name=>'AS')
    @al1 =@db[:albums].insert(:name=>'RF', :artist_id=>@ar1)
    @al2 =@db[:albums].insert(:name=>'MO', :artist_id=>@ar1)
    @al3 =@db[:albums].insert(:name=>'T', :artist_id=>@ar1)
    @t1 = @db[:tags].insert(:name=>'A')
    @t2 = @db[:tags].insert(:name=>'B')
    @t3 = @db[:tags].insert(:name=>'C')
    {@al1=>[@t1, @t2, @t3], @al2=>[@t2]}.each do |aid, tids|
      tids.each{|tid| @db[:albums_tags].insert([aid, tid])}
    end
  end
  after do
    @db.drop_table :albums_tags, :tags, :albums, :artists
    [:Artist, :Album, :Tag].each{|s| Object.send(:remove_const, s)}
  end

  specify "should return correct associated pks for one_to_many associations" do
    Artist.order(:id).all.map{|a| a.album_pks}.should == [[@al1, @al2, @al3], []]
  end

  specify "should return correct associated pks for many_to_many associations" do
    Album.order(:id).all.map{|a| a.tag_pks.sort}.should == [[@t1, @t2, @t3], [@t2], []]
  end

  specify "should set associated pks correctly for a one_to_many association" do
    Artist.use_transactions = true
    Album.order(:id).select_map(:artist_id).should == [@ar1, @ar1, @ar1]

    Artist[@ar2].album_pks = [@t1, @t3]
    Artist[@ar1].album_pks.should == [@t2]
    Album.order(:id).select_map(:artist_id).should == [@ar2, @ar1, @ar2]

    Artist[@ar1].album_pks = [@t1]
    Artist[@ar2].album_pks.should == [@t3]
    Album.order(:id).select_map(:artist_id).should == [@ar1, nil, @ar2]

    Artist[@ar1].album_pks = [@t1, @t2]
    Artist[@ar2].album_pks.should == [@t3]
    Album.order(:id).select_map(:artist_id).should == [@ar1, @ar1, @ar2]
  end

  specify "should set associated pks correctly for a many_to_many association" do
    Artist.use_transactions = true
    @db[:albums_tags].filter(:album_id=>@al1).select_order_map(:tag_id).should == [@t1, @t2, @t3]
    Album[@al1].tag_pks = [@t1, @t3]
    @db[:albums_tags].filter(:album_id=>@al1).select_order_map(:tag_id).should == [@t1, @t3]
    Album[@al1].tag_pks = []
    @db[:albums_tags].filter(:album_id=>@al1).select_order_map(:tag_id).should == []

    @db[:albums_tags].filter(:album_id=>@al2).select_order_map(:tag_id).should == [@t2]
    Album[@al2].tag_pks = [@t1, @t2]
    @db[:albums_tags].filter(:album_id=>@al2).select_order_map(:tag_id).should == [@t1, @t2]
    Album[@al2].tag_pks = []
    @db[:albums_tags].filter(:album_id=>@al1).select_order_map(:tag_id).should == []

    @db[:albums_tags].filter(:album_id=>@al3).select_order_map(:tag_id).should == []
    Album[@al3].tag_pks = [@t1, @t3]
    @db[:albums_tags].filter(:album_id=>@al3).select_order_map(:tag_id).should == [@t1, @t3]
    Album[@al3].tag_pks = []
    @db[:albums_tags].filter(:album_id=>@al1).select_order_map(:tag_id).should == []
  end
end


describe "List plugin without a scope" do
  before do
    @db = INTEGRATION_DB
    @db.create_table :sites do
      primary_key :id
      String :name
      Integer :position
    end

    @c = Class.new(Sequel::Model(@db[:sites]))
    @c.plugin :list
    @c.delete
    @c.create :name => "hig", :position => 3
    @c.create :name => "def", :position => 2
    @c.create :name => "abc", :position => 1
  end

  after do
    @db.drop_table(:sites)
  end

  it "should return rows in order of position" do
    @c.map(:position).should == [1,2,3]
    @c.map(:name).should == %w[ abc def hig ]
  end

  it "should define prev and next" do
    i = @c[:name => "abc"]
    i.prev.should == nil
    i = @c[:name => "def"]
    i.prev.should == @c[:name => "abc"]
    i.next.should == @c[:name => "hig"]
    i = @c[:name => "hig"]
    i.next.should == nil
  end

  it "should define move_to" do
    @c[:name => "def"].move_to(1)
    @c.map(:name).should == %w[ def abc hig ]

    @c[:name => "abc"].move_to(3)
    @c.map(:name).should == %w[ def hig abc ]

    proc { @c[:name => "abc"].move_to(-1) }.should raise_error(Sequel::Error)
    proc { @c[:name => "abc"].move_to(10) }.should raise_error(Sequel::Error)
  end

  it "should define move_to_top and move_to_bottom" do
    @c[:name => "def"].move_to_top
    @c.map(:name).should == %w[ def abc hig ]

    @c[:name => "def"].move_to_bottom
    @c.map(:name).should == %w[ abc hig def ]
  end

  it "should define move_up and move_down" do
    @c[:name => "def"].move_up
    @c.map(:name).should == %w[ def abc hig ]

    @c[:name => "abc"].move_down
    @c.map(:name).should == %w[ def hig abc ]

    @c[:name => "abc"].move_up(2)
    @c.map(:name).should == %w[ abc def hig ]

    @c[:name => "abc"].move_down(2)
    @c.map(:name).should == %w[ def hig abc ]

    proc { @c[:name => "def"].move_up(10) }.should raise_error(Sequel::Error)
    proc { @c[:name => "def"].move_down(10) }.should raise_error(Sequel::Error)
  end
end

describe "List plugin with a scope" do
  before do
    @db = INTEGRATION_DB
    @db.create_table :pages do
      primary_key :id
      String :name
      Integer :pos
      Integer :parent_id
    end

    @c = Class.new(Sequel::Model(@db[:pages]))
    @c.plugin :list, :field => :pos, :scope => :parent_id
    p1 = @c.create :name => "Hm", :pos => 1, :parent_id => 0
    p2 = @c.create :name => "Ps", :pos => 1, :parent_id => p1.id
    @c.create :name => "P1", :pos => 1, :parent_id => p2.id
    @c.create :name => "P2", :pos => 2, :parent_id => p2.id
    @c.create :name => "P3", :pos => 3, :parent_id => p2.id
    @c.create :name => "Au", :pos => 2, :parent_id => p1.id
  end

  after do
    @db.drop_table(:pages)
  end

  it "should return rows in order of position" do
    @c.map(:name).should == %w[ Hm Ps Au P1 P2 P3 ]
  end

  it "should define prev and next" do
    @c[:name => "Ps"].next.name.should == 'Au'
    @c[:name => "Au"].prev.name.should == 'Ps'
    @c[:name => "P1"].next.name.should == 'P2'
    @c[:name => "P2"].prev.name.should == 'P1'

    @c[:name => "P1"].next(2).name.should == 'P3'
    @c[:name => "P2"].next(-1).name.should == 'P1'
    @c[:name => "P3"].prev(2).name.should == 'P1'
    @c[:name => "P2"].prev(-1).name.should == 'P3'

    @c[:name => "Ps"].prev.should == nil
    @c[:name => "Au"].next.should == nil
    @c[:name => "P1"].prev.should == nil
    @c[:name => "P3"].next.should == nil
  end

  it "should define move_to" do
    @c[:name => "P2"].move_to(1)
    @c.map(:name).should == %w[ Hm Ps Au P2 P1 P3 ]

    @c[:name => "P2"].move_to(3)
    @c.map(:name).should == %w[ Hm Ps Au P1 P3 P2 ]

    proc { @c[:name => "P2"].move_to(-1) }.should raise_error(Sequel::Error)
    proc { @c[:name => "P2"].move_to(10) }.should raise_error(Sequel::Error)
  end

  it "should define move_to_top and move_to_bottom" do
    @c[:name => "Au"].move_to_top
    @c.map(:name).should == %w[ Hm Au Ps P1 P2 P3 ]

    @c[:name => "Au"].move_to_bottom
    @c.map(:name).should == %w[ Hm Ps Au P1 P2 P3 ]
  end

  it "should define move_up and move_down" do
    @c[:name => "P2"].move_up
    @c.map(:name).should == %w[ Hm Ps Au P2 P1 P3 ]

    @c[:name => "P1"].move_down
    @c.map(:name).should == %w[ Hm Ps Au P2 P3 P1 ]

    proc { @c[:name => "P1"].move_up(10) }.should raise_error(Sequel::Error)
    proc { @c[:name => "P1"].move_down(10) }.should raise_error(Sequel::Error)
  end
end

describe "Sequel::Plugins::Tree" do
  before do
    @db = INTEGRATION_DB
  end

  describe "with natural database order" do
    before do
      @db.create_table!(:nodes) do
        Integer :id, :primary_key=>true
        String :name
        Integer :parent_id
        Integer :position 
      end

      @nodes = [{:id => 1, :name => 'one', :parent_id => nil, :position => 1}, 
        {:id => 2, :name => 'two', :parent_id => nil, :position => 2}, 
        {:id => 3, :name => 'three', :parent_id => nil, :position => 3}, 
        {:id => 4, :name => "two.one", :parent_id => 2, :position => 1},
        {:id => 5, :name => "two.two", :parent_id => 2, :position => 2},
        {:id => 6, :name => "two.two.one", :parent_id => 5, :position => 1},
        {:id => 7, :name => "one.two", :parent_id => 1, :position => 2},
        {:id => 8, :name => "one.one", :parent_id => 1, :position => 1},
        {:id => 9, :name => "five", :parent_id => nil, :position => 5},
        {:id => 10, :name => "four", :parent_id => nil, :position => 4},
        {:id => 11, :name => "five.one", :parent_id => 9, :position => 1},
        {:id => 12, :name => "two.three", :parent_id => 2, :position => 3}]
      @nodes.each{|node| @db[:nodes].insert(node)}

      class ::Node < Sequel::Model
        plugin :tree
      end
    end
    after do
      @db.drop_table(:nodes)
      Object.send(:remove_const, :Node)
    end

    it "should instantiate" do
      Node.all.size.should == 12
    end

    it "should find top level nodes" do
      Node.roots_dataset.count.should == 5
    end

    it "should find all descendants of a node" do 
      two = Node.find(:id => 2)
      two.name.should == "two"
      two.descendants.map{|m| m[:id]}.should == [4, 5, 12, 6]
    end

    it "should find all ancestors of a node" do 
      twotwoone = Node.find(:id => 6)
      twotwoone.name.should == "two.two.one"
      twotwoone.ancestors.map{|m| m[:id]}.should == [5, 2]
    end
    
    it "should find all siblings of a node, excepting self" do 
      twoone = Node.find(:id => 4)
      twoone.name.should == "two.one"
      twoone.siblings.map{|m| m[:id]}.should == [5, 12]
    end

    it "should find all siblings of a node, including self" do 
      twoone = Node.find(:id => 4)
      twoone.name.should == "two.one"
      twoone.self_and_siblings.map{|m| m[:id]}.should == [4, 5, 12]
    end

    it "should find siblings for root nodes" do 
      three = Node.find(:id => 3)
      three.name.should == "three"
      three.self_and_siblings.map{|m| m[:id]}.should == [1, 2, 3, 9, 10]
    end

    it "should find correct root for a node" do
      twotwoone = Node.find(:id => 6)
      twotwoone.name.should == "two.two.one"
      twotwoone.root[:id].should == 2
    
      three = Node.find(:id => 3)
      three.name.should == "three"
      three.root[:id].should == 3
    
      fiveone = Node.find(:id => 11)
      fiveone.name.should == "five.one"
      fiveone.root[:id].should == 9
    end

    it "iterate top-level nodes in natural database order" do
      Node.roots_dataset.count.should == 5
      Node.roots.inject([]){|ids, p| ids << p.position}.should == [1, 2, 3, 5, 4]
    end
  
    it "should have children" do
      one = Node.find(:id => 1)
      one.name.should == "one"
      one.children.size.should == 2
    end
  
    it "children should be natural database order" do 
      one = Node.find(:id => 1)
      one.name.should == "one"
      one.children.map{|m| m[:position]}.should == [2, 1]
    end

    describe "Nodes in specified order" do
      before do
        class ::OrderedNode < Sequel::Model(:nodes)
          plugin :tree, :order => :position
        end
      end
      after do
        Object.send(:remove_const, :OrderedNode)
      end

      it "iterate top-level nodes in order by position" do
        OrderedNode.roots_dataset.count.should == 5
        OrderedNode.roots.inject([]){|ids, p| ids << p.position}.should == [1, 2, 3, 4, 5]
      end

      it "children should be in specified order" do 
        one = OrderedNode.find(:id => 1)
        one.name.should == "one"
        one.children.map{|m| m[:position]}.should == [1, 2]
      end
    end
  end

  describe "Lorems in specified order" do
    before do
      @db.create_table!(:lorems) do
        Integer :id, :primary_key=>true
        String :name
        Integer :ipsum_id
        Integer :neque
      end

      @lorems = [{:id => 1, :name => 'Lorem', :ipsum_id => nil, :neque => 4}, 
        {:id => 2, :name => 'Ipsum', :ipsum_id => nil, :neque => 3}, 
        {:id => 4, :name => "Neque", :ipsum_id => 2, :neque => 2},
        {:id => 5, :name => "Porro", :ipsum_id => 2, :neque => 1}]  
      @lorems.each{|lorem| @db[:lorems].insert(lorem)}

      class ::Lorem < Sequel::Model
        plugin :tree, :key => :ipsum_id, :order => :neque
      end
    end
    after do
      @db.drop_table(:lorems)
      Object.send(:remove_const, :Lorem)
    end

    it "iterate top-level nodes in order by position" do
      Lorem.roots_dataset.count.should == 2
      Lorem.roots.inject([]){|ids, p| ids << p.neque}.should == [3, 4]
    end

    it "children should be specified order" do 
      one = Lorem.find(:id => 2)
      one.children.map{|m| m[:neque]}.should == [1, 2]
    end
  end
end
