require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

# DB2 does not seem to support USING joins in every version; it seems to be
# valid expression in DB2 iSeries UDB though.
unless !INTEGRATION_DB.dataset.supports_join_using? || Sequel.guarded?(:db2)
describe "Class Table Inheritance Plugin" do
  before(:all) do
    @db = INTEGRATION_DB
    @db.instance_variable_set(:@schemas, {})
    @db.drop_table?(:staff, :executives, :managers, :employees)
    @db.create_table(:employees) do
      primary_key :id
      String :name
      String :kind
    end
    @db.create_table(:managers) do
      foreign_key :id, :employees, :primary_key=>true
      Integer :num_staff
    end
    @db.create_table(:executives) do
      foreign_key :id, :managers, :primary_key=>true
      Integer :num_managers
    end
    @db.create_table(:staff) do
      foreign_key :id, :employees, :primary_key=>true
      foreign_key :manager_id, :managers
    end
  end
  before do
    [:staff, :executives, :managers, :employees].each{|t| @db[t].delete}
    class ::Employee < Sequel::Model(@db)
      plugin :class_table_inheritance, :key=>:kind, :table_map=>{:Staff=>:staff}
    end 
    class ::Manager < Employee
      one_to_many :staff_members, :class=>:Staff
    end 
    class ::Executive < Manager
    end 
    class ::Staff < Employee
      many_to_one :manager, :qualify=>false
    end 
    
    @i1 =@db[:employees].insert(:name=>'E', :kind=>'Employee')
    @i2 = @db[:employees].insert(:name=>'S', :kind=>'Staff')
    @i3 = @db[:employees].insert(:name=>'M', :kind=>'Manager')
    @i4 = @db[:employees].insert(:name=>'Ex', :kind=>'Executive')
    @db[:managers].insert(:id=>@i3, :num_staff=>7)
    @db[:managers].insert(:id=>@i4, :num_staff=>5)
    @db[:executives].insert(:id=>@i4, :num_managers=>6)
    @db[:staff].insert(:id=>@i2, :manager_id=>@i4)
  end
  after do
    [:Executive, :Manager, :Staff, :Employee].each{|s| Object.send(:remove_const, s)}
  end
  after(:all) do
    @db.drop_table? :staff, :executives, :managers, :employees
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
  
  specify "should handle associations only defined in subclasses" do
    Employee.filter(:id=>@i2).all.first.manager.id.should == @i4
  end

  cspecify "should insert rows into all tables", [proc{|db| db.sqlite_version < 30709}, :sqlite] do
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
  
  specify "should handle many_to_one relationships" do
    m = Staff.first.manager
    m.should == Manager[@i4]
    m.should be_a_kind_of(Executive)
  end
  
  specify "should handle eagerly loading many_to_one relationships" do
    Staff.limit(1).eager(:manager).all.map{|x| x.manager}.should == [Manager[@i4]]
  end
  
  specify "should handle eagerly graphing many_to_one relationships" do
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
  
  cspecify "should handle eagerly graphing one_to_many relationships", [proc{|db| db.sqlite_version < 30709}, :sqlite] do
    es = Executive.limit(1).eager_graph(:staff_members).all
    es.should == [Executive[@i4]]
    es.map{|x| x.staff_members}.should == [[Staff[@i2]]]
  end
end
end

describe "Many Through Many Plugin" do
  before(:all) do
    @db = INTEGRATION_DB
    @db.instance_variable_set(:@schemas, {})
    @db.drop_table?(:albums_artists, :albums, :artists)
    @db.create_table(:albums) do
      primary_key :id
      String :name
    end
    @db.create_table(:artists) do
      primary_key :id
      String :name
    end
    @db.create_table(:albums_artists) do
      foreign_key :album_id, :albums
      foreign_key :artist_id, :artists
    end
  end
  before do
    [:albums_artists, :albums, :artists].each{|t| @db[t].delete}
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
  end
  after do
    [:Album, :Artist].each{|s| Object.send(:remove_const, s)}
  end
  after(:all) do
    @db.drop_table? :albums_artists, :albums, :artists
  end
  
  def self_join(c)
    c.join(Sequel.as(c.table_name, :b), Array(c.primary_key).zip(Array(c.primary_key))).select_all(c.table_name)
  end

  specify "should handle super simple case with 1 join table" do
    Artist.many_through_many :albums, [[:albums_artists, :artist_id, :album_id]]
    Artist[@artist1.id].albums.map{|x| x.name}.sort.should == %w'A D'
    Artist[@artist2.id].albums.map{|x| x.name}.sort.should == %w'A C'
    Artist[@artist3.id].albums.map{|x| x.name}.sort.should == %w'B C'
    Artist[@artist4.id].albums.map{|x| x.name}.sort.should == %w'B D'
    
    Artist.plugin :prepared_statements_associations
    Artist[@artist1.id].albums.map{|x| x.name}.sort.should == %w'A D'
    Artist[@artist2.id].albums.map{|x| x.name}.sort.should == %w'A C'
    Artist[@artist3.id].albums.map{|x| x.name}.sort.should == %w'B C'
    Artist[@artist4.id].albums.map{|x| x.name}.sort.should == %w'B D'

    Artist.filter(:id=>1).eager(:albums).all.map{|x| x.albums.map{|a| a.name}}.flatten.sort.should == %w'A D'
    Artist.filter(:id=>2).eager(:albums).all.map{|x| x.albums.map{|a| a.name}}.flatten.sort.should == %w'A C'
    Artist.filter(:id=>3).eager(:albums).all.map{|x| x.albums.map{|a| a.name}}.flatten.sort.should == %w'B C'
    Artist.filter(:id=>4).eager(:albums).all.map{|x| x.albums.map{|a| a.name}}.flatten.sort.should == %w'B D'
    
    Artist.filter(:artists__id=>1).eager_graph(:albums).all.map{|x| x.albums.map{|a| a.name}}.flatten.sort.should == %w'A D'
    Artist.filter(:artists__id=>2).eager_graph(:albums).all.map{|x| x.albums.map{|a| a.name}}.flatten.sort.should == %w'A C'
    Artist.filter(:artists__id=>3).eager_graph(:albums).all.map{|x| x.albums.map{|a| a.name}}.flatten.sort.should == %w'B C'
    Artist.filter(:artists__id=>4).eager_graph(:albums).all.map{|x| x.albums.map{|a| a.name}}.flatten.sort.should == %w'B D'

    Artist.filter(:albums=>@album1).all.map{|a| a.name}.sort.should == %w'1 2'
    Artist.filter(:albums=>@album2).all.map{|a| a.name}.sort.should == %w'3 4'
    Artist.filter(:albums=>@album3).all.map{|a| a.name}.sort.should == %w'2 3'
    Artist.filter(:albums=>@album4).all.map{|a| a.name}.sort.should == %w'1 4'

    Artist.exclude(:albums=>@album1).all.map{|a| a.name}.sort.should == %w'3 4'
    Artist.exclude(:albums=>@album2).all.map{|a| a.name}.sort.should == %w'1 2'
    Artist.exclude(:albums=>@album3).all.map{|a| a.name}.sort.should == %w'1 4'
    Artist.exclude(:albums=>@album4).all.map{|a| a.name}.sort.should == %w'2 3'

    Artist.filter(:albums=>[@album1, @album3]).all.map{|a| a.name}.sort.should == %w'1 2 3'
    Artist.filter(:albums=>[@album2, @album4]).all.map{|a| a.name}.sort.should == %w'1 3 4'

    Artist.exclude(:albums=>[@album1, @album3]).all.map{|a| a.name}.sort.should == %w'4'
    Artist.exclude(:albums=>[@album2, @album4]).all.map{|a| a.name}.sort.should == %w'2'

    Artist.filter(:albums=>Album.filter(:id=>[@album1.id, @album3.id])).all.map{|a| a.name}.sort.should == %w'1 2 3'
    Artist.exclude(:albums=>Album.filter(:id=>[@album1.id, @album3.id])).all.map{|a| a.name}.sort.should == %w'4'

    c = self_join(Artist)
    c.filter(:albums=>@album1).all.map{|a| a.name}.sort.should == %w'1 2'
    c.filter(:albums=>@album2).all.map{|a| a.name}.sort.should == %w'3 4'
    c.filter(:albums=>@album3).all.map{|a| a.name}.sort.should == %w'2 3'
    c.filter(:albums=>@album4).all.map{|a| a.name}.sort.should == %w'1 4'

    c.exclude(:albums=>@album1).all.map{|a| a.name}.sort.should == %w'3 4'
    c.exclude(:albums=>@album2).all.map{|a| a.name}.sort.should == %w'1 2'
    c.exclude(:albums=>@album3).all.map{|a| a.name}.sort.should == %w'1 4'
    c.exclude(:albums=>@album4).all.map{|a| a.name}.sort.should == %w'2 3'

    c.filter(:albums=>[@album1, @album3]).all.map{|a| a.name}.sort.should == %w'1 2 3'
    c.filter(:albums=>[@album2, @album4]).all.map{|a| a.name}.sort.should == %w'1 3 4'

    c.exclude(:albums=>[@album1, @album3]).all.map{|a| a.name}.sort.should == %w'4'
    c.exclude(:albums=>[@album2, @album4]).all.map{|a| a.name}.sort.should == %w'2'

    c.filter(:albums=>self_join(Album).filter(:albums__id=>[@album1.id, @album3.id])).all.map{|a| a.name}.sort.should == %w'1 2 3'
    c.exclude(:albums=>self_join(Album).filter(:albums__id=>[@album1.id, @album3.id])).all.map{|a| a.name}.sort.should == %w'4'
  end

  specify "should handle typical case with 3 join tables" do
    Artist.many_through_many :related_artists, [[:albums_artists, :artist_id, :album_id], [:albums, :id, :id], [:albums_artists, :album_id, :artist_id]], :class=>Artist, :distinct=>true
    Artist[@artist1.id].related_artists.map{|x| x.name}.sort.should == %w'1 2 4'
    Artist[@artist2.id].related_artists.map{|x| x.name}.sort.should == %w'1 2 3'
    Artist[@artist3.id].related_artists.map{|x| x.name}.sort.should == %w'2 3 4'
    Artist[@artist4.id].related_artists.map{|x| x.name}.sort.should == %w'1 3 4'
    
    Artist.plugin :prepared_statements_associations
    Artist[@artist1.id].related_artists.map{|x| x.name}.sort.should == %w'1 2 4'
    Artist[@artist2.id].related_artists.map{|x| x.name}.sort.should == %w'1 2 3'
    Artist[@artist3.id].related_artists.map{|x| x.name}.sort.should == %w'2 3 4'
    Artist[@artist4.id].related_artists.map{|x| x.name}.sort.should == %w'1 3 4'
    
    Artist.filter(:id=>@artist1.id).eager(:related_artists).all.map{|x| x.related_artists.map{|a| a.name}}.flatten.sort.should == %w'1 2 4'
    Artist.filter(:id=>@artist2.id).eager(:related_artists).all.map{|x| x.related_artists.map{|a| a.name}}.flatten.sort.should == %w'1 2 3'
    Artist.filter(:id=>@artist3.id).eager(:related_artists).all.map{|x| x.related_artists.map{|a| a.name}}.flatten.sort.should == %w'2 3 4'
    Artist.filter(:id=>@artist4.id).eager(:related_artists).all.map{|x| x.related_artists.map{|a| a.name}}.flatten.sort.should == %w'1 3 4'
    
    Artist.filter(:artists__id=>@artist1.id).eager_graph(:related_artists).all.map{|x| x.related_artists.map{|a| a.name}}.flatten.sort.should == %w'1 2 4'
    Artist.filter(:artists__id=>@artist2.id).eager_graph(:related_artists).all.map{|x| x.related_artists.map{|a| a.name}}.flatten.sort.should == %w'1 2 3'
    Artist.filter(:artists__id=>@artist3.id).eager_graph(:related_artists).all.map{|x| x.related_artists.map{|a| a.name}}.flatten.sort.should == %w'2 3 4'
    Artist.filter(:artists__id=>@artist4.id).eager_graph(:related_artists).all.map{|x| x.related_artists.map{|a| a.name}}.flatten.sort.should == %w'1 3 4'

    Artist.filter(:related_artists=>@artist1).all.map{|a| a.name}.sort.should == %w'1 2 4'
    Artist.filter(:related_artists=>@artist2).all.map{|a| a.name}.sort.should == %w'1 2 3'
    Artist.filter(:related_artists=>@artist3).all.map{|a| a.name}.sort.should == %w'2 3 4'
    Artist.filter(:related_artists=>@artist4).all.map{|a| a.name}.sort.should == %w'1 3 4'

    Artist.exclude(:related_artists=>@artist1).all.map{|a| a.name}.sort.should == %w'3'
    Artist.exclude(:related_artists=>@artist2).all.map{|a| a.name}.sort.should == %w'4'
    Artist.exclude(:related_artists=>@artist3).all.map{|a| a.name}.sort.should == %w'1'
    Artist.exclude(:related_artists=>@artist4).all.map{|a| a.name}.sort.should == %w'2'

    Artist.filter(:related_artists=>[@artist1, @artist4]).all.map{|a| a.name}.sort.should == %w'1 2 3 4'
    Artist.exclude(:related_artists=>[@artist1, @artist4]).all.map{|a| a.name}.sort.should == %w''

    Artist.filter(:related_artists=>Artist.filter(:id=>@artist1.id)).all.map{|a| a.name}.sort.should == %w'1 2 4'
    Artist.exclude(:related_artists=>Artist.filter(:id=>@artist1.id)).all.map{|a| a.name}.sort.should == %w'3'

    c = self_join(Artist)
    c.filter(:related_artists=>@artist1).all.map{|a| a.name}.sort.should == %w'1 2 4'
    c.filter(:related_artists=>@artist2).all.map{|a| a.name}.sort.should == %w'1 2 3'
    c.filter(:related_artists=>@artist3).all.map{|a| a.name}.sort.should == %w'2 3 4'
    c.filter(:related_artists=>@artist4).all.map{|a| a.name}.sort.should == %w'1 3 4'

    c.exclude(:related_artists=>@artist1).all.map{|a| a.name}.sort.should == %w'3'
    c.exclude(:related_artists=>@artist2).all.map{|a| a.name}.sort.should == %w'4'
    c.exclude(:related_artists=>@artist3).all.map{|a| a.name}.sort.should == %w'1'
    c.exclude(:related_artists=>@artist4).all.map{|a| a.name}.sort.should == %w'2'

    c.filter(:related_artists=>[@artist1, @artist4]).all.map{|a| a.name}.sort.should == %w'1 2 3 4'
    c.exclude(:related_artists=>[@artist1, @artist4]).all.map{|a| a.name}.sort.should == %w''

    c.filter(:related_artists=>c.filter(:artists__id=>@artist1.id)).all.map{|a| a.name}.sort.should == %w'1 2 4'
    c.exclude(:related_artists=>c.filter(:artists__id=>@artist1.id)).all.map{|a| a.name}.sort.should == %w'3'
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
    
    Artist[@artist1.id].related_albums.map{|x| x.name}.sort.should == %w'A B C'
    Artist[@artist2.id].related_albums.map{|x| x.name}.sort.should == %w'A B C D'
    Artist[@artist3.id].related_albums.map{|x| x.name}.sort.should == %w'A B D'
    Artist[@artist4.id].related_albums.map{|x| x.name}.sort.should == %w'B D'
    
    Artist.plugin :prepared_statements_associations
    Artist[@artist1.id].related_albums.map{|x| x.name}.sort.should == %w'A B C'
    Artist[@artist2.id].related_albums.map{|x| x.name}.sort.should == %w'A B C D'
    Artist[@artist3.id].related_albums.map{|x| x.name}.sort.should == %w'A B D'
    Artist[@artist4.id].related_albums.map{|x| x.name}.sort.should == %w'B D'
    
    Artist.filter(:id=>@artist1.id).eager(:related_albums).all.map{|x| x.related_albums.map{|a| a.name}}.flatten.sort.should == %w'A B C'
    Artist.filter(:id=>@artist2.id).eager(:related_albums).all.map{|x| x.related_albums.map{|a| a.name}}.flatten.sort.should == %w'A B C D'
    Artist.filter(:id=>@artist3.id).eager(:related_albums).all.map{|x| x.related_albums.map{|a| a.name}}.flatten.sort.should == %w'A B D'
    Artist.filter(:id=>@artist4.id).eager(:related_albums).all.map{|x| x.related_albums.map{|a| a.name}}.flatten.sort.should == %w'B D'
    
    Artist.filter(:artists__id=>@artist1.id).eager_graph(:related_albums).all.map{|x| x.related_albums.map{|a| a.name}}.flatten.sort.should == %w'A B C'
    Artist.filter(:artists__id=>@artist2.id).eager_graph(:related_albums).all.map{|x| x.related_albums.map{|a| a.name}}.flatten.sort.should == %w'A B C D'
    Artist.filter(:artists__id=>@artist3.id).eager_graph(:related_albums).all.map{|x| x.related_albums.map{|a| a.name}}.flatten.sort.should == %w'A B D'
    Artist.filter(:artists__id=>@artist4.id).eager_graph(:related_albums).all.map{|x| x.related_albums.map{|a| a.name}}.flatten.sort.should == %w'B D'

    Artist.filter(:related_albums=>@album1).all.map{|a| a.name}.sort.should == %w'1 2 3'
    Artist.filter(:related_albums=>@album2).all.map{|a| a.name}.sort.should == %w'1 2 3 4'
    Artist.filter(:related_albums=>@album3).all.map{|a| a.name}.sort.should == %w'1 2'
    Artist.filter(:related_albums=>@album4).all.map{|a| a.name}.sort.should == %w'2 3 4'

    Artist.exclude(:related_albums=>@album1).all.map{|a| a.name}.sort.should == %w'4'
    Artist.exclude(:related_albums=>@album2).all.map{|a| a.name}.sort.should == %w''
    Artist.exclude(:related_albums=>@album3).all.map{|a| a.name}.sort.should == %w'3 4'
    Artist.exclude(:related_albums=>@album4).all.map{|a| a.name}.sort.should == %w'1'

    Artist.filter(:related_albums=>[@album1, @album3]).all.map{|a| a.name}.sort.should == %w'1 2 3'
    Artist.filter(:related_albums=>[@album3, @album4]).all.map{|a| a.name}.sort.should == %w'1 2 3 4'

    Artist.exclude(:related_albums=>[@album1, @album3]).all.map{|a| a.name}.sort.should == %w'4'
    Artist.exclude(:related_albums=>[@album2, @album4]).all.map{|a| a.name}.sort.should == %w''

    Artist.filter(:related_albums=>Album.filter(:id=>[@album1.id, @album3.id])).all.map{|a| a.name}.sort.should == %w'1 2 3'
    Artist.exclude(:related_albums=>Album.filter(:id=>[@album1.id, @album3.id])).all.map{|a| a.name}.sort.should == %w'4'

    c = self_join(Artist)
    c.filter(:related_albums=>@album1).all.map{|a| a.name}.sort.should == %w'1 2 3'
    c.filter(:related_albums=>@album2).all.map{|a| a.name}.sort.should == %w'1 2 3 4'
    c.filter(:related_albums=>@album3).all.map{|a| a.name}.sort.should == %w'1 2'
    c.filter(:related_albums=>@album4).all.map{|a| a.name}.sort.should == %w'2 3 4'

    c.exclude(:related_albums=>@album1).all.map{|a| a.name}.sort.should == %w'4'
    c.exclude(:related_albums=>@album2).all.map{|a| a.name}.sort.should == %w''
    c.exclude(:related_albums=>@album3).all.map{|a| a.name}.sort.should == %w'3 4'
    c.exclude(:related_albums=>@album4).all.map{|a| a.name}.sort.should == %w'1'

    c.filter(:related_albums=>[@album1, @album3]).all.map{|a| a.name}.sort.should == %w'1 2 3'
    c.filter(:related_albums=>[@album3, @album4]).all.map{|a| a.name}.sort.should == %w'1 2 3 4'

    c.exclude(:related_albums=>[@album1, @album3]).all.map{|a| a.name}.sort.should == %w'4'
    c.exclude(:related_albums=>[@album2, @album4]).all.map{|a| a.name}.sort.should == %w''

    c.filter(:related_albums=>self_join(Album).filter(:albums__id=>[@album1.id, @album3.id])).all.map{|a| a.name}.sort.should == %w'1 2 3'
    c.exclude(:related_albums=>self_join(Album).filter(:albums__id=>[@album1.id, @album3.id])).all.map{|a| a.name}.sort.should == %w'4'
  end
end

describe "Lazy Attributes plugin" do 
  before(:all) do
    @db = INTEGRATION_DB
    @db.create_table!(:items) do
      primary_key :id
      String :name
      Integer :num
    end
    @db[:items].delete
    class ::Item < Sequel::Model(@db)
      plugin :lazy_attributes, :num
    end
    Item.create(:name=>'J', :num=>1)
  end
  after(:all) do
    @db.drop_table?(:items)
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
    a = Item.order(:name).all
    a.should == [Item.load(:id=>1, :name=>'J'), Item.load(:id=>2, :name=>'K')]
    a.map{|x| x[:num]}.should == [nil, nil]
    a.first.num.should == 1
    a.map{|x| x[:num]}.should == [1, 2]
    a.last.num.should == 2
  end
end

describe "Tactical Eager Loading Plugin" do
  before(:all) do
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
  end
  before do
    @db[:albums].delete
    @db[:artists].delete
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
  end
  after do
    [:Album, :Artist].each{|s| Object.send(:remove_const, s)}
  end
  after(:all) do
    @db.drop_table? :albums, :artists
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
    @db.drop_table?(:items)
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
  before(:all) do
    @db = INTEGRATION_DB
    @db.drop_table? :albums_artists, :albums, :artists
    @db.create_table(:artists) do
      primary_key :id
      String :name
      DateTime :updated_at
    end
    @db.create_table(:albums) do
      primary_key :id
      String :name
      foreign_key :artist_id, :artists
      DateTime :updated_at
    end
    @db.create_join_table({:album_id=>:albums, :artist_id=>:artists}, :no_index=>true)
  end
  before do
    @db[:albums].delete
    @db[:artists].delete
    class ::Album < Sequel::Model(@db)
    end
    class ::Artist < Sequel::Model(@db)
    end 
    
    @artist = Artist.create(:name=>'1')
    @album = Album.create(:name=>'A', :artist_id=>@artist.id)
  end
  after do
    [:Album, :Artist].each{|s| Object.send(:remove_const, s)}
  end
  after(:all) do
    @db.drop_table? :albums_artists, :albums, :artists
  end

  specify "should update the timestamp column when touching the record" do
    Album.plugin :touch
    @album.updated_at.should == nil
    @album.touch
    @album.updated_at.to_i.should be_within(2).of(Time.now.to_i)
  end
  
  cspecify "should update the timestamp column for many_to_one associated records when the record is updated or destroyed", [:do, :sqlite], [:jdbc, :sqlite], [:swift] do
    Album.many_to_one :artist
    Album.plugin :touch, :associations=>:artist
    @artist.updated_at.should == nil
    @album.update(:name=>'B')
    ua = @artist.reload.updated_at
    if ua.is_a?(Time)
      ua.to_i.should be_within(60).of(Time.now.to_i)
    else
      (DateTime.now - ua).should be_within(60.0/86400).of(0)
    end
    @artist.update(:updated_at=>nil)
    @album.destroy
    if ua.is_a?(Time)
      ua.to_i.should be_within(60).of(Time.now.to_i)
    else
      (DateTime.now - ua).should be_within(60.0/86400).of(0)
    end
  end

  cspecify "should update the timestamp column for one_to_many associated records when the record is updated", [:do, :sqlite], [:jdbc, :sqlite], [:swift] do
    Artist.one_to_many :albums
    Artist.plugin :touch, :associations=>:albums
    @album.updated_at.should == nil
    @artist.update(:name=>'B')
    ua = @album.reload.updated_at
    if ua.is_a?(Time)
      ua.to_i.should be_within(60).of(Time.now.to_i)
    else
      (DateTime.now - ua).should be_within(60.0/86400).of(0)
    end
  end

  cspecify "should update the timestamp column for many_to_many associated records when the record is updated", [:do, :sqlite], [:jdbc, :sqlite], [:swift] do
    Artist.many_to_many :albums
    Artist.plugin :touch, :associations=>:albums
    @artist.add_album(@album)
    @album.updated_at.should == nil
    @artist.update(:name=>'B')
    ua = @album.reload.updated_at
    if ua.is_a?(Time)
      ua.to_i.should be_within(60).of(Time.now.to_i)
    else
      (DateTime.now - ua).should be_within(60.0/86400).of(0)
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
    @db.drop_table?(:items)
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
  before(:all) do
    @db = INTEGRATION_DB
    @db.create_table!(:people) do
      primary_key :id
      String :name
      Integer :lock_version, :default=>0, :null=>false
    end
    class ::Person < Sequel::Model(@db)
      plugin :optimistic_locking
    end
  end
  before do
    @db[:people].delete
    @p = Person.create(:name=>'John')
  end
  after(:all) do
    @db.drop_table?(:people)
    Object.send(:remove_const, :Person)
  end

  specify "should raise an error when updating a stale record" do
    p1 = Person[@p.id]
    p2 = Person[@p.id]
    p1.update(:name=>'Jim')
    proc{p2.update(:name=>'Bob')}.should raise_error(Sequel::Plugins::OptimisticLocking::Error)
  end

  specify "should raise an error when destroying a stale record" do
    p1 = Person[@p.id]
    p2 = Person[@p.id]
    p1.update(:name=>'Jim')
    proc{p2.destroy}.should raise_error(Sequel::Plugins::OptimisticLocking::Error)
  end

  specify "should not raise an error when updating the same record twice" do
    p1 = Person[@p.id]
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
    @e2 = Event.create(:year=>nil)
  end
  after do
    @db.drop_table?(:events)
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

# DB2's implemention of CTE is too limited to use this plugin
if INTEGRATION_DB.dataset.supports_cte? and !Sequel.guarded?(:db2)
  describe "RcteTree Plugin" do
    before(:all) do
      @db = INTEGRATION_DB
      @db.create_table!(:nodes) do
        primary_key :id
        Integer :parent_id
        String :name
      end
      class ::Node < Sequel::Model(@db)
        plugin :rcte_tree, :order=>:name
      end
      
      @nodes = []
      @nodes << @a = Node.create(:name=>'a')
      @nodes << @b = Node.create(:name=>'b')
      @nodes << @aa = Node.create(:name=>'aa', :parent=>@a)
      @nodes << @ab = Node.create(:name=>'ab', :parent=>@a)
      @nodes << @ba = Node.create(:name=>'ba', :parent=>@b)
      @nodes << @bb = Node.create(:name=>'bb', :parent=>@b)
      @nodes << @aaa = Node.create(:name=>'aaa', :parent=>@aa)
      @nodes << @aab = Node.create(:name=>'aab', :parent=>@aa)
      @nodes << @aba = Node.create(:name=>'aba', :parent=>@ab)
      @nodes << @abb = Node.create(:name=>'abb', :parent=>@ab)
      @nodes << @aaaa = Node.create(:name=>'aaaa', :parent=>@aaa)
      @nodes << @aaab = Node.create(:name=>'aaab', :parent=>@aaa)
      @nodes << @aaaaa = Node.create(:name=>'aaaaa', :parent=>@aaaa)
    end
    before do
      @nodes.each{|n| n.associations.clear}
    end
    after(:all) do
      @db.drop_table? :nodes
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

    specify "should work correctly if not all columns are selected" do
      c = Class.new(Sequel::Model(@db[:nodes]))
      c.plugin :rcte_tree, :order=>:name
      c.plugin :lazy_attributes, :name
      c[:name=>'aaaa'].descendants.should == [c.load(:parent_id=>11, :id=>13)]
      c[:name=>'aa'].ancestors.should == [c.load(:parent_id=>nil, :id=>1)]
      nodes = c.filter(:id=>[@a.id, @b.id, @aaa.id]).order(:name).eager(:ancestors, :descendants).all
      nodes.should == [{:parent_id=>nil, :id=>1}, {:parent_id=>3, :id=>7}, {:parent_id=>nil, :id=>2}].map{|x| c.load(x)}
      nodes[2].descendants.should == [{:parent_id=>2, :id=>5}, {:parent_id=>2, :id=>6}].map{|x| c.load(x)}
      nodes[1].ancestors.should == [{:parent_id=>nil, :id=>1}, {:parent_id=>1, :id=>3}].map{|x| c.load(x)}
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
      nodes[1].associations[:parent].associations[:parent].associations.fetch(:parent, 1).should == nil
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
  before(:all) do
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
  end
  before do
    @db[:items].delete
    @i = Item.create(:name=>'J', :number=>1, :cost=>2)
    @i.instance_filter(:number=>1)
    @i.set(:name=>'K')
  end
  after(:all) do
    @db.drop_table?(:items)
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
  before(:all) do
    @db = INTEGRATION_DB
    @db.create_table!(:t) do
      Integer :a, :primary_key=>true
      Integer :b
    end
    @ds = @db[:t]
    @c = Class.new(Sequel::Model(@ds))
    @c.set_primary_key(:a)
    @c.unrestrict_primary_key
    @c.plugin :update_primary_key
  end
  before do
    @ds.delete
    @ds.insert(:a=>1, :b=>3)
  end
  after(:all) do
    @db.drop_table?(:t)
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
  before(:all) do
    @db = INTEGRATION_DB
    @db.drop_table?(:albums_tags, :albums_vocalists, :vocalists_instruments, :vocalists_hits, :hits, :instruments, :vocalists, :tags, :albums, :artists)
    @db.create_table(:artists) do
      primary_key :id
      String :name
    end
    @db.create_table(:albums) do
      primary_key :id
      String :name
      foreign_key :artist_id, :artists
    end
    @db.create_table(:tags) do
      primary_key :id
      String :name
    end
    @db.create_table(:albums_tags) do
      foreign_key :album_id, :albums
      foreign_key :tag_id, :tags
    end
    @db.create_table(:vocalists) do
      String :first
      String :last
      primary_key [:first, :last]
      foreign_key :album_id, :albums
    end
    @db.create_table(:albums_vocalists) do
      foreign_key :album_id, :albums
      String :first
      String :last
      foreign_key [:first, :last], :vocalists
    end
    @db.create_table(:instruments) do
      primary_key :id
      String :first
      String :last
      foreign_key [:first, :last], :vocalists
    end
    @db.create_table(:vocalists_instruments) do
      String :first
      String :last
      foreign_key [:first, :last], :vocalists
      foreign_key :instrument_id, :instruments
    end
    @db.create_table(:hits) do
      Integer :year
      Integer :week
      primary_key [:year, :week]
      String :first
      String :last
      foreign_key [:first, :last], :vocalists
    end
    @db.create_table(:vocalists_hits) do
      String :first
      String :last
      foreign_key [:first, :last], :vocalists
      Integer :year
      Integer :week
      foreign_key [:year, :week], :hits
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
    class ::Vocalist < Sequel::Model
      set_primary_key [:first, :last]
      plugin :association_pks
    end
    class ::Instrument < Sequel::Model
      plugin :association_pks
    end
    class ::Hit < Sequel::Model
      set_primary_key [:year, :week]
    end
  end
  before do
    [:albums_tags, :albums_vocalists, :vocalists_instruments, :vocalists_hits, :hits, :instruments, :vocalists, :tags, :albums, :artists].each{|t| @db[t].delete}
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
    @v1 = ['F1', 'L1']
    @v2 = ['F2', 'L2']
    @v3 = ['F3', 'L3']
    @db[:vocalists].insert(@v1 + [@al1])
    @db[:vocalists].insert(@v2 + [@al1])
    @db[:vocalists].insert(@v3 + [@al1])
    @i1 = @db[:instruments].insert([:first, :last], @v1)
    @i2 = @db[:instruments].insert([:first, :last], @v1)
    @i3 = @db[:instruments].insert([:first, :last], @v1)
    @h1 = [1997, 1]
    @h2 = [1997, 2]
    @h3 = [1997, 3]
    @db[:hits].insert(@h1 + @v1)
    @db[:hits].insert(@h2 + @v1)
    @db[:hits].insert(@h3 + @v1)
    {@al1=>[@v1, @v2, @v3], @al2=>[@v2]}.each do |aid, vids|
      vids.each{|vid| @db[:albums_vocalists].insert([aid] + vid)}
    end
    {@v1=>[@i1, @i2, @i3], @v2=>[@i2]}.each do |vid, iids|
      iids.each{|iid| @db[:vocalists_instruments].insert(vid + [iid])}
    end
    {@v1=>[@h1, @h2, @h3], @v2=>[@h2]}.each do |vid, hids|
      hids.each{|hid| @db[:vocalists_hits].insert(vid + hid)}
    end
  end
  after(:all) do
    @db.drop_table? :albums_tags, :albums_vocalists, :vocalists_instruments, :vocalists_hits, :hits, :instruments, :vocalists, :tags, :albums, :artists
    [:Artist, :Album, :Tag, :Vocalist, :Instrument, :Hit].each{|s| Object.send(:remove_const, s)}
  end

  specify "should return correct associated pks for one_to_many associations" do
    Artist.order(:id).all.map{|a| a.album_pks}.should == [[@al1, @al2, @al3], []]
  end

  specify "should return correct associated pks for many_to_many associations" do
    Album.order(:id).all.map{|a| a.tag_pks.sort}.should == [[@t1, @t2, @t3], [@t2], []]
  end

  specify "should return correct associated right-side cpks for one_to_many associations" do
    Album.one_to_many :vocalists, :order=>:first
    Album.order(:id).all.map{|a| a.vocalist_pks.sort}.should == [[@v1, @v2, @v3], [], []]
  end

  specify "should return correct associated right-side cpks for many_to_many associations" do
    Album.many_to_many :vocalists, :join_table=>:albums_vocalists, :right_key=>[:first, :last], :order=>:first
    Album.order(:id).all.map{|a| a.vocalist_pks.sort}.should == [[@v1, @v2, @v3], [@v2], []]
  end

  specify "should return correct associated pks for left-side cpks for one_to_many associations" do
    Vocalist.one_to_many :instruments, :key=>[:first, :last], :order=>:id
    Vocalist.order(:first, :last).all.map{|a| a.instrument_pks.sort}.should == [[@i1, @i2, @i3], [], []]
  end

  specify "should return correct associated pks for left-side cpks for many_to_many associations" do
    Vocalist.many_to_many :instruments, :join_table=>:vocalists_instruments, :left_key=>[:first, :last], :order=>:id
    Vocalist.order(:first, :last).all.map{|a| a.instrument_pks.sort}.should == [[@i1, @i2, @i3], [@i2], []]
  end

  specify "should return correct associated right-side cpks for left-side cpks for one_to_many associations" do
    Vocalist.one_to_many :hits, :key=>[:first, :last], :order=>:week
    Vocalist.order(:first, :last).all.map{|a| a.hit_pks.sort}.should == [[@h1, @h2, @h3], [], []]
  end

  specify "should return correct associated right-side cpks for left-side cpks for many_to_many associations" do
    Vocalist.many_to_many :hits, :join_table=>:vocalists_hits, :left_key=>[:first, :last], :right_key=>[:year, :week], :order=>:week
    Vocalist.order(:first, :last).all.map{|a| a.hit_pks.sort}.should == [[@h1, @h2, @h3], [@h2], []]
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

  specify "should set associated right-side cpks correctly for a one_to_many association" do
    Album.use_transactions = true
    Album.one_to_many :vocalists, :order=>:first
    Album.order(:id).all.map{|a| a.vocalist_pks.sort}.should == [[@v1, @v2, @v3], [], []]

    Album[@al2].vocalist_pks = [@v1, @v3]
    Album[@al1].vocalist_pks.should == [@v2]
    Vocalist.order(:first, :last).select_map(:album_id).should == [@al2, @al1, @al2]

    Album[@al1].vocalist_pks = [@v1]
    Album[@al2].vocalist_pks.should == [@v3]
    Vocalist.order(:first, :last).select_map(:album_id).should == [@al1, nil, @al2]

    Album[@al1].vocalist_pks = [@v1, @v2]
    Album[@al2].vocalist_pks.should == [@v3]
    Vocalist.order(:first, :last).select_map(:album_id).should == [@al1, @al1, @al2]
  end

  specify "should set associated right-side cpks correctly for a many_to_many association" do
    Album.use_transactions = true
    Album.many_to_many :vocalists, :join_table=>:albums_vocalists, :right_key=>[:first, :last], :order=>:first

    @db[:albums_vocalists].filter(:album_id=>@al1).select_order_map([:first, :last]).should == [@v1, @v2, @v3]
    Album[@al1].vocalist_pks = [@v1, @v3]
    @db[:albums_vocalists].filter(:album_id=>@al1).select_order_map([:first, :last]).should == [@v1, @v3]
    Album[@al1].vocalist_pks = []
    @db[:albums_vocalists].filter(:album_id=>@al1).select_order_map([:first, :last]).should == []

    @db[:albums_vocalists].filter(:album_id=>@al2).select_order_map([:first, :last]).should == [@v2]
    Album[@al2].vocalist_pks = [@v1, @v2]
    @db[:albums_vocalists].filter(:album_id=>@al2).select_order_map([:first, :last]).should == [@v1, @v2]
    Album[@al2].vocalist_pks = []
    @db[:albums_vocalists].filter(:album_id=>@al1).select_order_map([:first, :last]).should == []

    @db[:albums_vocalists].filter(:album_id=>@al3).select_order_map([:first, :last]).should == []
    Album[@al3].vocalist_pks = [@v1, @v3]
    @db[:albums_vocalists].filter(:album_id=>@al3).select_order_map([:first, :last]).should == [@v1, @v3]
    Album[@al3].vocalist_pks = []
    @db[:albums_vocalists].filter(:album_id=>@al1).select_order_map([:first, :last]).should == []
  end

  specify "should set associated pks correctly with left-side cpks for a one_to_many association" do
    Vocalist.use_transactions = true
    Vocalist.one_to_many :instruments, :key=>[:first, :last], :order=>:id
    Vocalist.order(:first, :last).all.map{|a| a.instrument_pks.sort}.should == [[@i1, @i2, @i3], [], []]

    Vocalist[@v2].instrument_pks = [@i1, @i3]
    Vocalist[@v1].instrument_pks.should == [@i2]
    Instrument.order(:id).select_map([:first, :last]).should == [@v2, @v1, @v2]

    Vocalist[@v1].instrument_pks = [@i1]
    Vocalist[@v2].instrument_pks.should == [@i3]
    Instrument.order(:id).select_map([:first, :last]).should == [@v1, [nil, nil], @v2]

    Vocalist[@v1].instrument_pks = [@i1, @i2]
    Vocalist[@v2].instrument_pks.should == [@i3]
    Instrument.order(:id).select_map([:first, :last]).should == [@v1, @v1, @v2]
  end

  specify "should set associated pks correctly with left-side cpks for a many_to_many association" do
    Vocalist.use_transactions = true
    Vocalist.many_to_many :instruments, :join_table=>:vocalists_instruments, :left_key=>[:first, :last], :order=>:id

    @db[:vocalists_instruments].filter([:first, :last]=>[@v1]).select_order_map(:instrument_id).should == [@i1, @i2, @i3]
    Vocalist[@v1].instrument_pks = [@i1, @i3]
    @db[:vocalists_instruments].filter([:first, :last]=>[@v1]).select_order_map(:instrument_id).should == [@i1, @i3]
    Vocalist[@v1].instrument_pks = []
    @db[:vocalists_instruments].filter([:first, :last]=>[@v1]).select_order_map(:instrument_id).should == []

    @db[:vocalists_instruments].filter([:first, :last]=>[@v2]).select_order_map(:instrument_id).should == [@i2]
    Vocalist[@v2].instrument_pks = [@i1, @i2]
    @db[:vocalists_instruments].filter([:first, :last]=>[@v2]).select_order_map(:instrument_id).should == [@i1, @i2]
    Vocalist[@v2].instrument_pks = []
    @db[:vocalists_instruments].filter([:first, :last]=>[@v1]).select_order_map(:instrument_id).should == []

    @db[:vocalists_instruments].filter([:first, :last]=>[@v3]).select_order_map(:instrument_id).should == []
    Vocalist[@v3].instrument_pks = [@i1, @i3]
    @db[:vocalists_instruments].filter([:first, :last]=>[@v3]).select_order_map(:instrument_id).should == [@i1, @i3]
    Vocalist[@v3].instrument_pks = []
    @db[:vocalists_instruments].filter([:first, :last]=>[@v1]).select_order_map(:instrument_id).should == []
  end

  specify "should set associated right-side cpks correctly with left-side cpks for a one_to_many association" do
    Vocalist.use_transactions = true
    Vocalist.one_to_many :hits, :key=>[:first, :last], :order=>:week
    Vocalist.order(:first, :last).all.map{|a| a.hit_pks.sort}.should == [[@h1, @h2, @h3], [], []]

    Vocalist[@v2].hit_pks = [@h1, @h3]
    Vocalist[@v1].hit_pks.should == [@h2]
    Hit.order(:year, :week).select_map([:first, :last]).should == [@v2, @v1, @v2]

    Vocalist[@v1].hit_pks = [@h1]
    Vocalist[@v2].hit_pks.should == [@h3]
    Hit.order(:year, :week).select_map([:first, :last]).should == [@v1, [nil, nil], @v2]

    Vocalist[@v1].hit_pks = [@h1, @h2]
    Vocalist[@v2].hit_pks.should == [@h3]
    Hit.order(:year, :week).select_map([:first, :last]).should == [@v1, @v1, @v2]
  end

  specify "should set associated right-side cpks correctly with left-side cpks for a many_to_many association" do
    Vocalist.use_transactions = true
    Vocalist.many_to_many :hits, :join_table=>:vocalists_hits, :left_key=>[:first, :last], :right_key=>[:year, :week], :order=>:week

    @db[:vocalists_hits].filter([:first, :last]=>[@v1]).select_order_map([:year, :week]).should == [@h1, @h2, @h3]
    Vocalist[@v1].hit_pks = [@h1, @h3]
    @db[:vocalists_hits].filter([:first, :last]=>[@v1]).select_order_map([:year, :week]).should == [@h1, @h3]
    Vocalist[@v1].hit_pks = []
    @db[:vocalists_hits].filter([:first, :last]=>[@v1]).select_order_map([:year, :week]).should == []

    @db[:vocalists_hits].filter([:first, :last]=>[@v2]).select_order_map([:year, :week]).should == [@h2]
    Vocalist[@v2].hit_pks = [@h1, @h2]
    @db[:vocalists_hits].filter([:first, :last]=>[@v2]).select_order_map([:year, :week]).should == [@h1, @h2]
    Vocalist[@v2].hit_pks = []
    @db[:vocalists_hits].filter([:first, :last]=>[@v1]).select_order_map([:year, :week]).should == []

    @db[:vocalists_hits].filter([:first, :last]=>[@v3]).select_order_map([:year, :week]).should == []
    Vocalist[@v3].hit_pks = [@h1, @h3]
    @db[:vocalists_hits].filter([:first, :last]=>[@v3]).select_order_map([:year, :week]).should == [@h1, @h3]
    Vocalist[@v3].hit_pks = []
    @db[:vocalists_hits].filter([:first, :last]=>[@v1]).select_order_map([:year, :week]).should == []
  end
end

describe "List plugin without a scope" do
  before(:all) do
    @db = INTEGRATION_DB
    @db.create_table!(:sites) do
      primary_key :id
      String :name
      Integer :position
    end

    @c = Class.new(Sequel::Model(@db[:sites]))
    @c.plugin :list
  end
  before do
    @c.delete
    @c.create :name => "abc"
    @c.create :name => "def"
    @c.create :name => "hig"
  end
  after(:all) do
    @db.drop_table?(:sites)
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
  before(:all) do
    @db = INTEGRATION_DB
    @db.create_table!(:pages) do
      primary_key :id
      String :name
      Integer :pos
      Integer :parent_id
    end

    @c = Class.new(Sequel::Model(@db[:pages]))
    @c.plugin :list, :field => :pos, :scope => :parent_id
  end
  before do
    @c.delete
    p1 = @c.create :name => "Hm", :parent_id => 0
    p2 = @c.create :name => "Ps", :parent_id => p1.id
    @c.create :name => "P1", :parent_id => p2.id
    @c.create :name => "P2", :parent_id => p2.id
    @c.create :name => "P3", :parent_id => p2.id
    @c.create :name => "Au", :parent_id => p1.id
  end
  after(:all) do
    @db.drop_table?(:pages)
  end

  specify "should return rows in order of position" do
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

  specify "should define move_to" do
    @c[:name => "P2"].move_to(1)
    @c.map(:name).should == %w[ Hm Ps Au P2 P1 P3 ]

    @c[:name => "P2"].move_to(3)
    @c.map(:name).should == %w[ Hm Ps Au P1 P3 P2 ]

    proc { @c[:name => "P2"].move_to(-1) }.should raise_error(Sequel::Error)
    proc { @c[:name => "P2"].move_to(10) }.should raise_error(Sequel::Error)
  end

  specify "should define move_to_top and move_to_bottom" do
    @c[:name => "Au"].move_to_top
    @c.map(:name).should == %w[ Hm Au Ps P1 P2 P3 ]

    @c[:name => "Au"].move_to_bottom
    @c.map(:name).should == %w[ Hm Ps Au P1 P2 P3 ]
  end

  specify "should define move_up and move_down" do
    @c[:name => "P2"].move_up
    @c.map(:name).should == %w[ Hm Ps Au P2 P1 P3 ]

    @c[:name => "P1"].move_down
    @c.map(:name).should == %w[ Hm Ps Au P2 P3 P1 ]

    proc { @c[:name => "P1"].move_up(10) }.should raise_error(Sequel::Error)
    proc { @c[:name => "P1"].move_down(10) }.should raise_error(Sequel::Error)
  end
end

describe "Sequel::Plugins::Tree" do
  before(:all) do
    @db = INTEGRATION_DB
  end

  describe "with natural database order" do
    before(:all) do
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
    after(:all) do
      @db.drop_table?(:nodes)
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
      before(:all) do
        class ::OrderedNode < Sequel::Model(:nodes)
          plugin :tree, :order => :position
        end
      end
      after(:all) do
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
    before(:all) do
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
    after(:all) do
      @db.drop_table?(:lorems)
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

describe "Sequel::Plugins::PreparedStatements" do
  before(:all) do
    @db = INTEGRATION_DB
    @db.create_table!(:ps_test) do
      primary_key :id
      String :name
      Integer :i
    end
    @c = Class.new(Sequel::Model(@db[:ps_test]))
    @c.plugin :prepared_statements_with_pk
  end
  before do
    @c.delete
    @foo = @c.create(:name=>'foo', :i=>10)
    @bar = @c.create(:name=>'bar', :i=>20)
  end
  after(:all) do
    @db.drop_table?(:ps_test)
  end

  it "should work with looking up using Model.[]" do 
    @c[@foo.id].should == @foo
    @c[@bar.id].should == @bar
    @c[0].should == nil
    @c[nil].should == nil
  end

  it "should work with looking up using Dataset#with_pk" do 
    @c.dataset.with_pk(@foo.id).should == @foo
    @c.dataset.with_pk(@bar.id).should == @bar
    @c.dataset.with_pk(0).should == nil
    @c.dataset.with_pk(nil).should == nil

    @c.dataset.filter(:i=>0).with_pk(@foo.id).should == nil
    @c.dataset.filter(:i=>10).with_pk(@foo.id).should == @foo
    @c.dataset.filter(:i=>20).with_pk(@bar.id).should == @bar
    @c.dataset.filter(:i=>10).with_pk(nil).should == nil
    @c.dataset.filter(:name=>'foo').with_pk(@foo.id).should == @foo
    @c.dataset.filter(:name=>'bar').with_pk(@bar.id).should == @bar
    @c.dataset.filter(:name=>'baz').with_pk(@bar.id).should == nil
    @c.dataset.filter(:name=>'bar').with_pk(nil).should == nil
  end

  it "should work with Model#destroy" do 
    @foo.destroy
    @bar.destroy
    @c[@foo.id].should == nil
    @c[@bar.id].should == nil
  end

  it "should work with Model#update" do 
    @foo.update(:name=>'foo2', :i=>30)
    @c[@foo.id].should == @c.load(:id=>@foo.id, :name=>'foo2', :i=>30)
    @foo.update(:name=>'foo3')
    @c[@foo.id].should == @c.load(:id=>@foo.id, :name=>'foo3', :i=>30)
    @foo.update(:i=>40)
    @c[@foo.id].should == @c.load(:id=>@foo.id, :name=>'foo3', :i=>40)
    @foo.update(:i=>nil)
    @c[@foo.id].should == @c.load(:id=>@foo.id, :name=>'foo3', :i=>nil)
  end

  it "should work with Model#create" do 
    o = @c.create(:name=>'foo2', :i=>30)
    @c[o.id].should == @c.load(:id=>o.id, :name=>'foo2', :i=>30)
    o = @c.create(:name=>'foo2')
    @c[o.id].should == @c.load(:id=>o.id, :name=>'foo2', :i=>nil)
    o = @c.create(:i=>30)
    @c[o.id].should == @c.load(:id=>o.id, :name=>nil, :i=>30)
    o = @c.create(:name=>nil, :i=>40)
    @c[o.id].should == @c.load(:id=>o.id, :name=>nil, :i=>40)
  end
end

describe "Caching plugins" do
  before(:all) do
    @db = INTEGRATION_DB
    @db.drop_table?(:albums, :artists)
    @db.create_table(:artists) do
      primary_key :id
    end
    @db.create_table(:albums) do
      primary_key :id
      foreign_key :artist_id, :artists
    end
    @db[:artists].insert
    @db[:albums].insert(:artist_id=>1)
  end
  before do
    @Album = Class.new(Sequel::Model(@db[:albums]))
    @Album.plugin :many_to_one_pk_lookup
  end
  after(:all) do
    @db.drop_table?(:albums, :artists)
  end

  shared_examples_for "a caching plugin" do
    it "should work with looking up using Model.[]" do 
      @Artist[1].should equal(@Artist[1])
      @Artist[:id=>1].should == @Artist[1]
      @Artist[0].should == nil
      @Artist[nil].should == nil
    end

    it "should work with lookup up many_to_one associated objects" do 
      a = @Artist[1]
      @Album.first.artist.should equal(a)
    end
  end

  describe "caching plugin" do
    before do
      @cache_class = Class.new(Hash) do
        def set(k, v, ttl) self[k] = v end
        alias get []
      end
      @cache = @cache_class.new

      @Artist = Class.new(Sequel::Model(@db[:artists]))
      @Artist.plugin :caching, @cache
      @Album.many_to_one :artist, :class=>@Artist
    end

    it_should_behave_like "a caching plugin"
  end

  describe "static_cache plugin" do
    before do
      @Artist = Class.new(Sequel::Model(@db[:artists]))
      @Artist.plugin :static_cache
      @Album.many_to_one :artist, :class=>@Artist
    end

    it_should_behave_like "a caching plugin"
  end
end

describe "Sequel::Plugins::ConstraintValidations" do
  before(:all) do
    @db = INTEGRATION_DB
    @db.extension(:constraint_validations)
    @db.create_constraint_validations_table
    @ds = @db[:cv_test]
    @regexp = regexp = @db.dataset.supports_regexp?
    @validation_opts = {}
    opts_proc = proc{@validation_opts}
    @validate_block = proc do |opts|
      opts = opts_proc.call
      presence :pre, opts.merge(:name=>:p)
      exact_length 5, :exactlen, opts.merge(:name=>:el)
      min_length 5, :minlen, opts.merge(:name=>:minl)
      max_length 5, :maxlen, opts.merge(:name=>:maxl)
      length_range 3..5, :lenrange, opts.merge(:name=>:lr)
      if regexp
        format(/^foo\d+/, :form, opts.merge(:name=>:f))
      end
      like 'foo%', :lik, opts.merge(:name=>:l)
      ilike 'foo%', :ilik, opts.merge(:name=>:il)
      includes %w'abc def', :inc, opts.merge(:name=>:i)
      unique :uniq, opts.merge(:name=>:u)
      max_length 6, :minlen, opts.merge(:name=>:maxl2)
    end
    @valid_row = {:pre=>'a', :exactlen=>'12345', :minlen=>'12345', :maxlen=>'12345', :lenrange=>'1234', :lik=>'fooabc', :ilik=>'FooABC', :inc=>'abc', :uniq=>'u'}
    @violations = [
      [:pre, [nil, '', ' ']],
      [:exactlen, [nil, '', '1234', '123456']],
      [:minlen, [nil, '', '1234']],
      [:maxlen, [nil, '123456']],
      [:lenrange, [nil, '', '12', '123456']],
      [:lik, [nil, '', 'fo', 'fotabc', 'FOOABC']],
      [:ilik, [nil, '', 'fo', 'fotabc']],
      [:inc, [nil, '', 'ab', 'abcd']],
    ]

    if @regexp
      @valid_row[:form] = 'foo1'
      @violations << [:form, [nil, '', 'foo', 'fooa']]
    end
  end
  after(:all) do
    @db.drop_constraint_validations_table
  end

  shared_examples_for "constraint validations" do
    cspecify "should set up constraints that work even outside the model", :mysql do 
      proc{@ds.insert(@valid_row)}.should_not raise_error

      # Test for unique constraint
      proc{@ds.insert(@valid_row)}.should raise_error(Sequel::DatabaseError)

      @ds.delete
      @violations.each do |col, vals|
        try = @valid_row.dup
        vals += ['1234567'] if col == :minlen
        vals.each do |val|
          next if val.nil? && @validation_opts[:allow_nil]
          try[col] = val
          proc{@ds.insert(try)}.should raise_error(Sequel::DatabaseError)
        end
      end

      # Test for dropping of constraint
      @db.alter_table(:cv_test){validate{drop :maxl2}}
      proc{@ds.insert(@valid_row.merge(:minlen=>'1234567'))}.should_not raise_error
    end

    it "should set up automatic validations inside the model" do 
      c = Class.new(Sequel::Model(@ds))
      c.plugin :constraint_validations
      c.delete
      proc{c.create(@valid_row)}.should_not raise_error

      # Test for unique validation 
      c.new(@valid_row).should_not be_valid

      c.delete
      @violations.each do |col, vals|
        try = @valid_row.dup
        vals.each do |val|
          next if val.nil? && @validation_opts[:allow_nil]
          try[col] = val
          c.new(try).should_not be_valid
        end
      end
      c.db.constraint_validations = nil
    end
  end

  describe "via create_table" do
    before(:all) do
      @table_block = proc do
        regexp = @regexp
        validate_block = @validate_block
        @db.create_table!(:cv_test) do
          primary_key :id
          String :pre
          String :exactlen
          String :minlen
          String :maxlen
          String :lenrange
          if regexp
            String :form
          end
          String :lik
          String :ilik
          String :inc
          String :uniq, :null=>false
          validate(&validate_block)
        end
      end
    end
    after(:all) do
      @db.drop_table?(:cv_test)
      @db.drop_constraint_validations_for(:table=>:cv_test)
    end

    describe "with :allow_nil=>true" do
      before(:all) do
        @validation_opts = {:allow_nil=>true}
        @table_block.call
      end
      it_should_behave_like "constraint validations"
    end
    describe "with :allow_nil=>false" do
      before(:all) do
        @table_block.call
      end
      it_should_behave_like "constraint validations"
    end
  end

  describe "via alter_table" do
    before(:all) do
      @table_block = proc do
        regexp = @regexp
        validate_block = @validate_block
        @db.create_table!(:cv_test) do
          primary_key :id
          String :lik
          String :ilik
          String :inc
          String :uniq, :null=>false
        end
        @db.alter_table(:cv_test) do
          add_column :pre, String
          add_column :exactlen, String
          add_column :minlen, String
          add_column :maxlen, String
          add_column :lenrange, String
          if regexp
            add_column :form, String
          end
          validate(&validate_block)
        end
      end
    end
    after(:all) do
      @db.drop_table?(:cv_test)
      @db.drop_constraint_validations_for(:table=>:cv_test)
    end

    describe "with :allow_nil=>true" do
      before(:all) do
        @validation_opts = {:allow_nil=>true}
        @table_block.call
      end
      it_should_behave_like "constraint validations"
    end
    describe "with :allow_nil=>false" do
      before(:all) do
        @table_block.call
      end
      it_should_behave_like "constraint validations"
    end
  end
end

describe "date_arithmetic extension" do
  asd = begin
    require 'active_support/duration'
    require 'active_support/inflector'
    require 'active_support/core_ext/string/inflections'
    true
  rescue LoadError
    false
  end

  before(:all) do
    @db = INTEGRATION_DB
    @db.extension(:date_arithmetic)
    if @db.database_type == :sqlite
      @db.use_timestamp_timezones = false
    end
    @date = Date.civil(2010, 7, 12)
    @dt = Time.local(2010, 7, 12)
    if asd
      @d0 = ActiveSupport::Duration.new(0, [[:days, 0]])
      @d1 = ActiveSupport::Duration.new(1, [[:days, 1]])
      @d2 = ActiveSupport::Duration.new(1, [[:years, 1], [:months, 1], [:days, 1], [:minutes, 61], [:seconds, 1]])
    end
    @h0 = {:days=>0}
    @h1 = {:days=>1, :years=>nil, :hours=>0}
    @h2 = {:years=>1, :months=>1, :days=>1, :hours=>1, :minutes=>1, :seconds=>1}
    @a1 = Time.local(2010, 7, 13)
    @a2 = Time.local(2011, 8, 13, 1, 1, 1)
    @s1 = Time.local(2010, 7, 11)
    @s2 = Time.local(2009, 6, 10, 22, 58, 59)
    @check = lambda do |meth, in_date, in_interval, should|
      output = @db.get(Sequel.send(meth, in_date, in_interval))
      output = Time.parse(output.to_s) unless output.is_a?(Time) || output.is_a?(DateTime)
      output.year.should == should.year
      output.month.should == should.month
      output.day.should == should.day
      output.hour.should == should.hour
      output.min.should == should.min
      output.sec.should == should.sec
    end
  end
  after(:all) do
    if @db.database_type == :sqlite
      @db.use_timestamp_timezones = true
    end
  end

  if asd
    specify "be able to use Sequel.date_add to add ActiveSupport::Duration objects to dates and datetimes" do
      @check.call(:date_add, @date, @d0, @dt)
      @check.call(:date_add, @date, @d1, @a1)
      @check.call(:date_add, @date, @d2, @a2)

      @check.call(:date_add, @dt, @d0, @dt)
      @check.call(:date_add, @dt, @d1, @a1)
      @check.call(:date_add, @dt, @d2, @a2)
    end

    specify "be able to use Sequel.date_sub to subtract ActiveSupport::Duration objects from dates and datetimes" do
      @check.call(:date_sub, @date, @d0, @dt)
      @check.call(:date_sub, @date, @d1, @s1)
      @check.call(:date_sub, @date, @d2, @s2)

      @check.call(:date_sub, @dt, @d0, @dt)
      @check.call(:date_sub, @dt, @d1, @s1)
      @check.call(:date_sub, @dt, @d2, @s2)
    end
  end

  specify "be able to use Sequel.date_add to add interval hashes to dates and datetimes" do
    @check.call(:date_add, @date, @h0, @dt)
    @check.call(:date_add, @date, @h1, @a1)
    @check.call(:date_add, @date, @h2, @a2)

    @check.call(:date_add, @dt, @h0, @dt)
    @check.call(:date_add, @dt, @h1, @a1)
    @check.call(:date_add, @dt, @h2, @a2)
  end

  specify "be able to use Sequel.date_sub to subtract interval hashes from dates and datetimes" do
    @check.call(:date_sub, @date, @h0, @dt)
    @check.call(:date_sub, @date, @h1, @s1)
    @check.call(:date_sub, @date, @h2, @s2)

    @check.call(:date_sub, @dt, @h0, @dt)
    @check.call(:date_sub, @dt, @h1, @s1)
    @check.call(:date_sub, @dt, @h2, @s2)
  end
end
