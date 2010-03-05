require File.join(File.dirname(__FILE__), 'spec_helper.rb')

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
    @db.drop_table :executives, :managers, :staff, :employees
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
  
  cspecify "should update the timestamp column for associated records when the record is updated or destroyed", [:do], [:jdbc, :sqlite] do
    @artist.updated_at.should == nil
    @album.update(:name=>'B')
    @artist.reload.updated_at.to_i.should be_close(Time.now.to_i, 2)
    @artist.update(:updated_at=>nil)
    @album.destroy
    @artist.reload.updated_at.to_i.should be_close(Time.now.to_i, 2)
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
