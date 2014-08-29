require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model, "pg_array_associations" do
  before do
    @db = Sequel.mock(:numrows=>1)
    class ::Artist < Sequel::Model(@db)
      attr_accessor :yyy
      columns :id, :tag_ids
      plugin :pg_array_associations
      pg_array_to_many :tags
      pg_array_to_many :a_tags, :clone=>:tags, :conditions=>{:name=>'A'}, :key=>:tag_ids
    end
    class ::Tag < Sequel::Model(@db)
      columns :id
      plugin :pg_array_associations
      many_to_pg_array :artists
      many_to_pg_array :a_artists, :clone=>:artists, :conditions=>{:name=>'A'}
      def id3
        id*3
      end
    end
    @c1 = Artist
    @c2 = Tag
    @c1.dataset._fetch = {:id=>1, :tag_ids=>Sequel.pg_array([1,2,3])}
    @c2.dataset._fetch = {:id=>2}
    @o1 = @c1.first
    @o2 = @c2.first
    @n1 = @c1.new
    @n2 = @c2.new
    @db.sqls
  end
  after do
    Object.send(:remove_const, :Artist)
    Object.send(:remove_const, :Tag)
  end

  it "should populate :key_hash and :id_map option correctly for custom eager loaders" do
    khs = []
    pr = proc{|h| khs << [h[:key_hash], h[:id_map]]}
    @c1.pg_array_to_many :tags, :clone=>:tags, :eager_loader=>pr
    @c2.many_to_pg_array :artists, :clone=>:artists, :eager_loader=>pr
    @c1.eager(:tags).all
    @c2.eager(:artists).all
    khs.should == [[{}, nil], [{:id=>{2=>[Tag.load(:id=>2)]}}, {2=>[Tag.load(:id=>2)]}]]
  end

  it "should not issue queries if the object cannot have associated objects" do
    @n1.tags.should == []
    @c1.load(:tag_ids=>[]).tags.should == []
    @n2.artists.should == []
    @db.sqls.should == []
  end

  it "should use correct SQL when loading associations lazily" do
    @o1.tags.should == [@o2]
    @o2.artists.should == [@o1]
    @db.sqls.should == ["SELECT * FROM tags WHERE (tags.id IN (1, 2, 3))", "SELECT * FROM artists WHERE (artists.tag_ids @> ARRAY[2]::integer[])"]
  end

  it "should accept :primary_key option for primary keys to use in current and associated table" do
    @c1.pg_array_to_many :tags, :clone=>:tags, :primary_key=>Sequel./(:id, 3)
    @c2.many_to_pg_array :artists, :clone=>:artists, :primary_key=>:id3
    @o1.tags_dataset.sql.should == "SELECT * FROM tags WHERE ((tags.id / 3) IN (1, 2, 3))"
    @o2.artists_dataset.sql.should == "SELECT * FROM artists WHERE (artists.tag_ids @> ARRAY[6]::integer[])"
  end
  
  it "should allowing filtering by associations" do
    @c1.filter(:tags=>@o2).sql.should == "SELECT * FROM artists WHERE (artists.tag_ids @> ARRAY[2]::integer[])"
    @c2.filter(:artists=>@o1).sql.should == "SELECT * FROM tags WHERE (tags.id IN (1, 2, 3))"
  end

  it "should allowing filtering by associations with :conditions" do
    @c1.filter(:a_tags=>@o2).sql.should == "SELECT * FROM artists WHERE coalesce((artists.tag_ids && (SELECT array_agg(tags.id) FROM tags WHERE ((name = 'A') AND (tags.id IS NOT NULL) AND (tags.id = 2)))), 'f')"
    @c2.filter(:a_artists=>@o1).sql.should == "SELECT * FROM tags WHERE (tags.id IN (SELECT unnest(artists.tag_ids) FROM artists WHERE ((name = 'A') AND (artists.tag_ids IS NOT NULL) AND (artists.id = 1))))"
  end

  it "should allowing excluding by associations" do
    @c1.exclude(:tags=>@o2).sql.should == "SELECT * FROM artists WHERE (NOT (artists.tag_ids @> ARRAY[2]::integer[]) OR (artists.tag_ids IS NULL))"
    @c2.exclude(:artists=>@o1).sql.should == "SELECT * FROM tags WHERE ((tags.id NOT IN (1, 2, 3)) OR (tags.id IS NULL))"
  end

  it "should allowing excluding by associations with :conditions" do
    @c1.exclude(:a_tags=>@o2).sql.should == "SELECT * FROM artists WHERE (NOT coalesce((artists.tag_ids && (SELECT array_agg(tags.id) FROM tags WHERE ((name = 'A') AND (tags.id IS NOT NULL) AND (tags.id = 2)))), 'f') OR (artists.tag_ids IS NULL))"
    @c2.exclude(:a_artists=>@o1).sql.should == "SELECT * FROM tags WHERE ((tags.id NOT IN (SELECT unnest(artists.tag_ids) FROM artists WHERE ((name = 'A') AND (artists.tag_ids IS NOT NULL) AND (artists.id = 1)))) OR (tags.id IS NULL))"
  end

  it "should allowing filtering by multiple associations" do
    @c1.filter(:tags=>[@c2.load(:id=>1), @c2.load(:id=>2)]).sql.should == "SELECT * FROM artists WHERE (artists.tag_ids && ARRAY[1,2]::integer[])"
    @c2.filter(:artists=>[@c1.load(:tag_ids=>Sequel.pg_array([3, 4])), @c1.load(:tag_ids=>Sequel.pg_array([4, 5]))]).sql.should == "SELECT * FROM tags WHERE (tags.id IN (3, 4, 5))"
  end

  it "should allowing filtering by multiple associations with :conditions" do
    @c1.filter(:a_tags=>[@c2.load(:id=>1), @c2.load(:id=>2)]).sql.should == "SELECT * FROM artists WHERE coalesce((artists.tag_ids && (SELECT array_agg(tags.id) FROM tags WHERE ((name = 'A') AND (tags.id IS NOT NULL) AND (tags.id IN (1, 2))))), 'f')"
    @c2.filter(:a_artists=>[@c1.load(:id=>7, :tag_ids=>Sequel.pg_array([3, 4])), @c1.load(:id=>8, :tag_ids=>Sequel.pg_array([4, 5]))]).sql.should == "SELECT * FROM tags WHERE (tags.id IN (SELECT unnest(artists.tag_ids) FROM artists WHERE ((name = 'A') AND (artists.tag_ids IS NOT NULL) AND (artists.id IN (7, 8)))))"
  end

  it "should allowing excluding by multiple associations" do
    @c1.exclude(:tags=>[@c2.load(:id=>1), @c2.load(:id=>2)]).sql.should == "SELECT * FROM artists WHERE (NOT (artists.tag_ids && ARRAY[1,2]::integer[]) OR (artists.tag_ids IS NULL))"
    @c2.exclude(:artists=>[@c1.load(:tag_ids=>Sequel.pg_array([3, 4])), @c1.load(:tag_ids=>Sequel.pg_array([4, 5]))]).sql.should == "SELECT * FROM tags WHERE ((tags.id NOT IN (3, 4, 5)) OR (tags.id IS NULL))"
  end

  it "should allowing excluding by multiple associations with :conditions" do
    @c1.exclude(:a_tags=>[@c2.load(:id=>1), @c2.load(:id=>2)]).sql.should == "SELECT * FROM artists WHERE (NOT coalesce((artists.tag_ids && (SELECT array_agg(tags.id) FROM tags WHERE ((name = 'A') AND (tags.id IS NOT NULL) AND (tags.id IN (1, 2))))), 'f') OR (artists.tag_ids IS NULL))"
    @c2.exclude(:a_artists=>[@c1.load(:id=>7, :tag_ids=>Sequel.pg_array([3, 4])), @c1.load(:id=>8, :tag_ids=>Sequel.pg_array([4, 5]))]).sql.should == "SELECT * FROM tags WHERE ((tags.id NOT IN (SELECT unnest(artists.tag_ids) FROM artists WHERE ((name = 'A') AND (artists.tag_ids IS NOT NULL) AND (artists.id IN (7, 8))))) OR (tags.id IS NULL))"
  end

  it "should allowing filtering/excluding associations with NULL or empty values" do
    @c1.filter(:tags=>@c2.new).sql.should == 'SELECT * FROM artists WHERE \'f\''
    @c1.exclude(:tags=>@c2.new).sql.should == 'SELECT * FROM artists WHERE \'t\''
    @c2.filter(:artists=>@c1.new).sql.should == 'SELECT * FROM tags WHERE \'f\''
    @c2.exclude(:artists=>@c1.new).sql.should == 'SELECT * FROM tags WHERE \'t\''

    @c2.filter(:artists=>@c1.load(:tag_ids=>[])).sql.should == 'SELECT * FROM tags WHERE \'f\''
    @c2.exclude(:artists=>@c1.load(:tag_ids=>[])).sql.should == 'SELECT * FROM tags WHERE \'t\''

    @c1.filter(:tags=>[@c2.new, @c2.load(:id=>2)]).sql.should == "SELECT * FROM artists WHERE (artists.tag_ids && ARRAY[2]::integer[])"
    @c2.filter(:artists=>[@c1.load(:tag_ids=>Sequel.pg_array([3, 4])), @c1.new]).sql.should == "SELECT * FROM tags WHERE (tags.id IN (3, 4))"
  end

  it "should allowing filtering by association datasets" do
    @c1.filter(:tags=>@c2.where(:id=>1)).sql.should == "SELECT * FROM artists WHERE coalesce((artists.tag_ids && (SELECT array_agg(tags.id) FROM tags WHERE (id = 1))), 'f')"
    @c2.filter(:artists=>@c1.where(:id=>1)).sql.should == "SELECT * FROM tags WHERE (tags.id IN (SELECT unnest(artists.tag_ids) FROM artists WHERE (id = 1)))"
  end

  it "should allowing filtering by association datasets with :conditions" do
    @c1.filter(:a_tags=>@c2.where(:id=>1)).sql.should == "SELECT * FROM artists WHERE coalesce((artists.tag_ids && (SELECT array_agg(tags.id) FROM tags WHERE ((name = 'A') AND (tags.id IS NOT NULL) AND (tags.id IN (SELECT tags.id FROM tags WHERE (id = 1)))))), 'f')"
    @c2.filter(:a_artists=>@c1.where(:id=>1)).sql.should == "SELECT * FROM tags WHERE (tags.id IN (SELECT unnest(artists.tag_ids) FROM artists WHERE ((name = 'A') AND (artists.tag_ids IS NOT NULL) AND (artists.id IN (SELECT artists.id FROM artists WHERE (id = 1))))))"
  end

  it "should allowing excluding by association datasets" do
    @c1.exclude(:tags=>@c2.where(:id=>1)).sql.should == "SELECT * FROM artists WHERE (NOT coalesce((artists.tag_ids && (SELECT array_agg(tags.id) FROM tags WHERE (id = 1))), 'f') OR (artists.tag_ids IS NULL))"
    @c2.exclude(:artists=>@c1.where(:id=>1)).sql.should == "SELECT * FROM tags WHERE ((tags.id NOT IN (SELECT unnest(artists.tag_ids) FROM artists WHERE (id = 1))) OR (tags.id IS NULL))"
  end

  it "should allowing excluding by association datasets with :conditions" do
    @c1.exclude(:a_tags=>@c2.where(:id=>1)).sql.should == "SELECT * FROM artists WHERE (NOT coalesce((artists.tag_ids && (SELECT array_agg(tags.id) FROM tags WHERE ((name = 'A') AND (tags.id IS NOT NULL) AND (tags.id IN (SELECT tags.id FROM tags WHERE (id = 1)))))), 'f') OR (artists.tag_ids IS NULL))"
    @c2.exclude(:a_artists=>@c1.where(:id=>1)).sql.should == "SELECT * FROM tags WHERE ((tags.id NOT IN (SELECT unnest(artists.tag_ids) FROM artists WHERE ((name = 'A') AND (artists.tag_ids IS NOT NULL) AND (artists.id IN (SELECT artists.id FROM artists WHERE (id = 1)))))) OR (tags.id IS NULL))"
  end

  it "filter by associations should respect key options" do 
    @c1.class_eval{def tag3_ids; tag_ids.map{|x| x*3} end}
    @c1.pg_array_to_many :tags, :clone=>:tags, :primary_key=>Sequel.*(:id, 3), :primary_key_method=>:id3, :key=>:tag3_ids, :key_column=>Sequel.pg_array(:tag_ids)[1..2]
    @c2.many_to_pg_array :artists, :clone=>:artists, :primary_key=>Sequel.*(:id, 3), :primary_key_method=>:id3, :key=>:tag3_ids, :key_column=>Sequel.pg_array(:tag_ids)[1..2]

    @c1.filter(:tags=>@o2).sql.should == "SELECT * FROM artists WHERE (artists.tag_ids[1:2] @> ARRAY[6]::integer[])"
    @c2.filter(:artists=>@o1).sql.should == "SELECT * FROM tags WHERE ((tags.id * 3) IN (3, 6, 9))"
    @c1.filter(:tags=>@c2.where(:id=>1)).sql.should == "SELECT * FROM artists WHERE coalesce((artists.tag_ids[1:2] && (SELECT array_agg((tags.id * 3)) FROM tags WHERE (id = 1))), 'f')"
    @c2.filter(:artists=>@c1.where(:id=>1)).sql.should == "SELECT * FROM tags WHERE ((tags.id * 3) IN (SELECT unnest(artists.tag_ids[1:2]) FROM artists WHERE (id = 1)))"
  end
  
  it "should support a :key option" do
    @c1.pg_array_to_many :tags, :clone=>:tags, :key=>:tag2_ids
    @c2.many_to_pg_array :artists, :clone=>:artists, :key=>:tag2_ids
    @c1.class_eval{def tag2_ids; tag_ids.map{|x| x * 2} end}
    @o1.tags_dataset.sql.should == "SELECT * FROM tags WHERE (tags.id IN (2, 4, 6))"
    @o2.artists_dataset.sql.should == "SELECT * FROM artists WHERE (artists.tag2_ids @> ARRAY[2]::integer[])"
  end
  
  it "should support a :key_column option" do
    @c2.many_to_pg_array :artists, :clone=>:artists, :key_column=>Sequel.pg_array(:tag_ids)[1..2], :key=>:tag2_ids
    @o2.artists_dataset.sql.should == "SELECT * FROM artists WHERE (artists.tag_ids[1:2] @> ARRAY[2]::integer[])"
  end
  
  it "should support a :primary_key option" do
    @c1.pg_array_to_many :tags, :clone=>:tags, :primary_key=>:id2
    @c2.many_to_pg_array :artists, :clone=>:artists, :primary_key=>:id2
    @o1.tags_dataset.sql.should == "SELECT * FROM tags WHERE (tags.id2 IN (1, 2, 3))"
    @c2.class_eval{def id2; id*2 end}
    @o2.artists_dataset.sql.should == "SELECT * FROM artists WHERE (artists.tag_ids @> ARRAY[4]::integer[])"
  end
  
  it "should support a :conditions option" do
    @c1.pg_array_to_many :tags, :clone=>:tags, :conditions=>{:a=>1}
    @c2.many_to_pg_array :artists, :clone=>:artists, :conditions=>{:a=>1}
    @o1.tags_dataset.sql.should == "SELECT * FROM tags WHERE ((a = 1) AND (tags.id IN (1, 2, 3)))"
    @o2.artists_dataset.sql.should == "SELECT * FROM artists WHERE ((a = 1) AND (artists.tag_ids @> ARRAY[2]::integer[]))"
  end
  
  it "should support an :order option" do
    @c1.pg_array_to_many :tags, :clone=>:tags, :order=>[:a, :b]
    @c2.many_to_pg_array :artists, :clone=>:artists, :order=>[:a, :b]
    @o1.tags_dataset.sql.should == "SELECT * FROM tags WHERE (tags.id IN (1, 2, 3)) ORDER BY a, b"
    @o2.artists_dataset.sql.should == "SELECT * FROM artists WHERE (artists.tag_ids @> ARRAY[2]::integer[]) ORDER BY a, b"
  end
  
  it "should support a select option" do
    @c1.pg_array_to_many :tags, :clone=>:tags, :select=>[:a, :b]
    @c2.many_to_pg_array :artists, :clone=>:artists, :select=>[:a, :b]
    @c1.load(:tag_ids=>Sequel.pg_array([1,2,3])).tags_dataset.sql.should == "SELECT a, b FROM tags WHERE (tags.id IN (1, 2, 3))"
    @c2.load(:id=>1).artists_dataset.sql.should == "SELECT a, b FROM artists WHERE (artists.tag_ids @> ARRAY[1]::integer[])"
  end
  
  it "should accept a block" do
    @c1.pg_array_to_many :tags, :clone=>:tags do |ds| ds.filter(:yyy=>@yyy) end
    @c2.many_to_pg_array :artists, :clone=>:artists do |ds| ds.filter(:a=>1) end
    @c1.new(:yyy=>6, :tag_ids=>Sequel.pg_array([1,2,3])).tags_dataset.sql.should == "SELECT * FROM tags WHERE ((tags.id IN (1, 2, 3)) AND (yyy = 6))"
    @o2.artists_dataset.sql.should == "SELECT * FROM artists WHERE ((artists.tag_ids @> ARRAY[2]::integer[]) AND (a = 1))"
  end

  it "should support a :dataset option that is used instead of the default" do
    @c1.pg_array_to_many :tags, :clone=>:tags, :dataset=>proc{Tag.where(:id=>tag_ids.map{|x| x*2})}
    @c2.many_to_pg_array :artists, :clone=>:artists, :dataset=>proc{Artist.where(Sequel.pg_array(Sequel.pg_array(:tag_ids)[1..2]).contains([id]))}
    @o1.tags_dataset.sql.should == "SELECT * FROM tags WHERE (id IN (2, 4, 6))"
    @o2.artists_dataset.sql.should == "SELECT * FROM artists WHERE (tag_ids[1:2] @> ARRAY[2])"
  end

  it "should support a :limit option" do
    @c1.pg_array_to_many :tags, :clone=>:tags, :limit=>[2, 3]
    @c2.many_to_pg_array :artists, :clone=>:artists, :limit=>[3, 2]
    @o1.tags_dataset.sql.should == "SELECT * FROM tags WHERE (tags.id IN (1, 2, 3)) LIMIT 2 OFFSET 3"
    @o2.artists_dataset.sql.should == "SELECT * FROM artists WHERE (artists.tag_ids @> ARRAY[2]::integer[]) LIMIT 3 OFFSET 2"
  end

  it "should support a :uniq option that removes duplicates from the association" do
    @c1.pg_array_to_many :tags, :clone=>:tags, :uniq=>true
    @c2.many_to_pg_array :artists, :clone=>:artists, :uniq=>true
    @c1.dataset._fetch = [{:id=>20}, {:id=>30}, {:id=>20}, {:id=>30}]
    @c2.dataset._fetch = [{:id=>20}, {:id=>30}, {:id=>20}, {:id=>30}]
    @o1.tags.should == [@c2.load(:id=>20), @c2.load(:id=>30)]
    @o2.artists.should == [@c1.load(:id=>20), @c1.load(:id=>30)]
  end

  it "reflection associated_object_keys should return correct values" do
    @c1.association_reflection(:tags).associated_object_keys.should == [:id]
    @c2.association_reflection(:artists).associated_object_keys.should == [:tag_ids]
  end
  
  it "reflection remove_before_destroy? should return correct values" do
    @c1.association_reflection(:tags).remove_before_destroy?.should == true
    @c2.association_reflection(:artists).remove_before_destroy?.should == false
  end
  
  it "reflection reciprocal should be correct" do
    @c1.association_reflection(:tags).reciprocal.should == :artists
    @c2.association_reflection(:artists).reciprocal.should == :tags
  end

  it "should eagerly load correctly" do
    a = @c1.eager(:tags).all
    a.should == [@o1]
    sqls = @db.sqls
    sqls.pop.should =~ /SELECT \* FROM tags WHERE \(tags\.id IN \([123], [123], [123]\)\)/ 
    sqls.should == ["SELECT * FROM artists"]
    a.first.tags.should == [@o2]
    @db.sqls.should == []

    a = @c2.eager(:artists).all
    a.should == [@o2]
    @db.sqls.should == ['SELECT * FROM tags', "SELECT * FROM artists WHERE (artists.tag_ids && ARRAY[2]::integer[])"]
    a.first.artists.should == [@o1]
    @db.sqls.should == []
  end
  
  it "should support using custom key options when eager loading associations" do
    @c1.class_eval{def tag3_ids; tag_ids.map{|x| x*3} end}
    @c1.pg_array_to_many :tags, :clone=>:tags, :primary_key=>Sequel.*(:id, 3), :primary_key_method=>:id3, :key=>:tag3_ids
    @c2.many_to_pg_array :artists, :clone=>:artists, :primary_key=>:id3, :key=>:tag3_ids, :key_column=>Sequel.pg_array(:tag_ids)[1..2]

    a = @c1.eager(:tags).all
    a.should == [@o1]
    sqls = @db.sqls
    sqls.pop.should =~ /SELECT \* FROM tags WHERE \(\(tags\.id \* 3\) IN \([369], [369], [369]\)\)/ 
    sqls.should == ["SELECT * FROM artists"]
    a.first.tags.should == [@o2]
    @db.sqls.should == []

    a = @c2.eager(:artists).all
    a.should == [@o2]
    @db.sqls.should == ["SELECT * FROM tags", "SELECT * FROM artists WHERE (artists.tag_ids[1:2] && ARRAY[6]::integer[])"]
    a.first.artists.should == [@o1]
    @db.sqls.should == []
  end

  it "should allow cascading of eager loading for associations of associated models" do
    a = @c1.eager(:tags=>:artists).all
    a.should == [@o1]
    sqls = @db.sqls
    sqls.slice!(1).should =~ /SELECT \* FROM tags WHERE \(tags\.id IN \([123], [123], [123]\)\)/ 
    sqls.should == ['SELECT * FROM artists', "SELECT * FROM artists WHERE (artists.tag_ids && ARRAY[2]::integer[])"]
    a.first.tags.should == [@o2]
    a.first.tags.first.artists.should == [@o1]
    @db.sqls.should == []
  end
  
  it "should respect :eager when lazily loading an association" do
    @c1.pg_array_to_many :tags2, :clone=>:tags, :eager=>:artists, :key=>:tag_ids
    @c2.many_to_pg_array :artists2, :clone=>:artists, :eager=>:tags

    @o1.tags2.should == [@o2]
    @db.sqls.should == ["SELECT * FROM tags WHERE (tags.id IN (1, 2, 3))", "SELECT * FROM artists WHERE (artists.tag_ids && ARRAY[2]::integer[])"]
    @o1.tags2.first.artists.should == [@o1]
    @db.sqls.should == []

    @o2.artists2.should == [@o1]
    sqls = @db.sqls
    sqls.pop.should =~ /SELECT \* FROM tags WHERE \(tags\.id IN \([123], [123], [123]\)\)/ 
    sqls.should == ["SELECT * FROM artists WHERE (artists.tag_ids @> ARRAY[2]::integer[])"]
    @o2.artists2.first.tags.should == [@o2]
    @db.sqls.should == []
  end
  
  it "should cascade eagerly loading when the :eager_graph association option is used" do
    @c1.pg_array_to_many :tags2, :clone=>:tags, :eager_graph=>:artists, :key=>:tag_ids
    @c2.many_to_pg_array :artists2, :clone=>:artists, :eager_graph=>:tags

    @c2.dataset._fetch = {:id=>2, :artists_id=>1, :tag_ids=>Sequel.pg_array([1,2,3])}
    @c1.dataset._fetch = {:id=>1, :tags_id=>2, :tag_ids=>Sequel.pg_array([1,2,3])}

    @o1.tags2.should == [@o2]
    @db.sqls.first.should =~ /SELECT tags\.id, artists\.id AS artists_id, artists\.tag_ids FROM tags LEFT OUTER JOIN artists ON \(artists.tag_ids @> ARRAY\[tags.id\]\) WHERE \(tags\.id IN \([123], [123], [123]\)\)/ 
    @o1.tags2.first.artists.should == [@o1]
    @db.sqls.should == []

    @o2.artists2.should == [@o1]
    @db.sqls.should == ["SELECT artists.id, artists.tag_ids, tags.id AS tags_id FROM artists LEFT OUTER JOIN tags ON (artists.tag_ids @> ARRAY[tags.id]) WHERE (artists.tag_ids @> ARRAY[2]::integer[])"]
    @o2.artists2.first.tags.should == [@o2]
    @db.sqls.should == []

    @c2.dataset._fetch = {:id=>2, :artists_id=>1, :tag_ids=>Sequel.pg_array([1,2,3])}
    @c1.dataset._fetch = {:id=>1, :tag_ids=>Sequel.pg_array([1,2,3])}

    a = @c1.eager(:tags2).all
    sqls = @db.sqls
    sqls.pop.should =~ /SELECT tags\.id, artists\.id AS artists_id, artists\.tag_ids FROM tags LEFT OUTER JOIN artists ON \(artists.tag_ids @> ARRAY\[tags.id\]\) WHERE \(tags\.id IN \([123], [123], [123]\)\)/ 
    sqls.should == ["SELECT * FROM artists"]
    a.should == [@o1]
    a.first.tags2.should == [@o2]
    a.first.tags2.first.artists.should == [@o1]
    @db.sqls.should == []

    @c2.dataset._fetch = {:id=>2}
    @c1.dataset._fetch = {:id=>1, :tags_id=>2, :tag_ids=>Sequel.pg_array([1,2,3])}

    a = @c2.eager(:artists2).all
    @db.sqls.should == ["SELECT * FROM tags", "SELECT artists.id, artists.tag_ids, tags.id AS tags_id FROM artists LEFT OUTER JOIN tags ON (artists.tag_ids @> ARRAY[tags.id]) WHERE (artists.tag_ids && ARRAY[2]::integer[])"]
    a.should == [@o2]
    a.first.artists2.should == [@o1]
    a.first.artists2.first.tags.should == [@o2]
    @db.sqls.should == []
  end
  
  it "should respect the :limit option when eager loading" do
    @c2.dataset._fetch = [{:id=>1},{:id=>2}, {:id=>3}]

    @c1.pg_array_to_many :tags, :clone=>:tags, :limit=>2
    a = @c1.eager(:tags).all
    a.should == [@o1]
    sqls = @db.sqls
    sqls.pop.should =~ /SELECT \* FROM tags WHERE \(tags\.id IN \([123], [123], [123]\)\)/ 
    sqls.should == ["SELECT * FROM artists"]
    a.first.tags.should == [@c2.load(:id=>1), @c2.load(:id=>2)]
    @db.sqls.should == []

    @c1.pg_array_to_many :tags, :clone=>:tags, :limit=>[1, 1]
    a = @c1.eager(:tags).all
    a.should == [@o1]
    sqls = @db.sqls
    sqls.pop.should =~ /SELECT \* FROM tags WHERE \(tags\.id IN \([123], [123], [123]\)\)/ 
    sqls.should == ["SELECT * FROM artists"]
    a.first.tags.should == [@c2.load(:id=>2)]
    @db.sqls.should == []

    @c1.pg_array_to_many :tags, :clone=>:tags, :limit=>[nil, 1]
    a = @c1.eager(:tags).all
    a.should == [@o1]
    sqls = @db.sqls
    sqls.pop.should =~ /SELECT \* FROM tags WHERE \(tags\.id IN \([123], [123], [123]\)\)/ 
    sqls.should == ["SELECT * FROM artists"]
    a.first.tags.should == [@c2.load(:id=>2), @c2.load(:id=>3)]
    @db.sqls.length.should == 0

    @c2.dataset._fetch = [{:id=>2}]
    @c1.dataset._fetch = [{:id=>5, :tag_ids=>Sequel.pg_array([1,2,3])},{:id=>6, :tag_ids=>Sequel.pg_array([2,3])}, {:id=>7, :tag_ids=>Sequel.pg_array([1,2])}]

    @c2.many_to_pg_array :artists, :clone=>:artists, :limit=>2
    a = @c2.eager(:artists).all
    a.should == [@o2]
    @db.sqls.should == ['SELECT * FROM tags', "SELECT * FROM artists WHERE (artists.tag_ids && ARRAY[2]::integer[])"]
    a.first.artists.should == [@c1.load(:id=>5, :tag_ids=>Sequel.pg_array([1,2,3])), @c1.load(:id=>6, :tag_ids=>Sequel.pg_array([2,3]))]
    @db.sqls.should == []

    @c2.many_to_pg_array :artists, :clone=>:artists, :limit=>[1, 1]
    a = @c2.eager(:artists).all
    a.should == [@o2]
    @db.sqls.should == ['SELECT * FROM tags', "SELECT * FROM artists WHERE (artists.tag_ids && ARRAY[2]::integer[])"]
    a.first.artists.should == [@c1.load(:id=>6, :tag_ids=>Sequel.pg_array([2,3]))]
    @db.sqls.should == []

    @c2.many_to_pg_array :artists, :clone=>:artists, :limit=>[nil, 1]
    a = @c2.eager(:artists).all
    a.should == [@o2]
    @db.sqls.should == ['SELECT * FROM tags', "SELECT * FROM artists WHERE (artists.tag_ids && ARRAY[2]::integer[])"]
    a.first.artists.should == [@c1.load(:id=>6, :tag_ids=>Sequel.pg_array([2,3])), @c1.load(:id=>7, :tag_ids=>Sequel.pg_array([1,2]))]
    @db.sqls.should == []
  end

  it "should support association_join" do
    @c1.association_join(:tags).sql.should == "SELECT * FROM artists INNER JOIN tags ON (artists.tag_ids @> ARRAY[tags.id])"
    @c2.association_join(:artists).sql.should == "SELECT * FROM tags INNER JOIN artists ON (artists.tag_ids @> ARRAY[tags.id])"
  end

  it "should support custom selects when using association_join" do
    @c1.select{a(b)}.association_join(:tags).sql.should == "SELECT a(b) FROM artists INNER JOIN tags ON (artists.tag_ids @> ARRAY[tags.id])"
    @c2.select{a(b)}.association_join(:artists).sql.should == "SELECT a(b) FROM tags INNER JOIN artists ON (artists.tag_ids @> ARRAY[tags.id])"
  end

  it "should eagerly graph associations" do
    @c2.dataset._fetch = {:id=>2, :artists_id=>1, :tag_ids=>Sequel.pg_array([1,2,3])}
    @c1.dataset._fetch = {:id=>1, :tags_id=>2, :tag_ids=>Sequel.pg_array([1,2,3])}

    a = @c1.eager_graph(:tags).all
    @db.sqls.should == ["SELECT artists.id, artists.tag_ids, tags.id AS tags_id FROM artists LEFT OUTER JOIN tags ON (artists.tag_ids @> ARRAY[tags.id])"]
    a.should == [@o1]
    a.first.tags.should == [@o2]
    @db.sqls.should == []

    a = @c2.eager_graph(:artists).all
    @db.sqls.should == ["SELECT tags.id, artists.id AS artists_id, artists.tag_ids FROM tags LEFT OUTER JOIN artists ON (artists.tag_ids @> ARRAY[tags.id])"]
    a.should == [@o2]
    a.first.artists.should == [@o1]
    @db.sqls.should == []
  end

  it "should allow cascading of eager graphing for associations of associated models" do
    @c2.dataset._fetch = {:id=>2, :artists_id=>1, :tag_ids=>Sequel.pg_array([1,2,3]), :tags_0_id=>2}
    @c1.dataset._fetch = {:id=>1, :tags_id=>2, :tag_ids=>Sequel.pg_array([1,2,3]), :artists_0_id=>1, :artists_0_tag_ids=>Sequel.pg_array([1,2,3])}

    a = @c1.eager_graph(:tags=>:artists).all
    @db.sqls.should == ["SELECT artists.id, artists.tag_ids, tags.id AS tags_id, artists_0.id AS artists_0_id, artists_0.tag_ids AS artists_0_tag_ids FROM artists LEFT OUTER JOIN tags ON (artists.tag_ids @> ARRAY[tags.id]) LEFT OUTER JOIN artists AS artists_0 ON (artists_0.tag_ids @> ARRAY[tags.id])"]
    a.should == [@o1]
    a.first.tags.should == [@o2]
    a.first.tags.first.artists.should == [@o1]
    @db.sqls.should == []

    a = @c2.eager_graph(:artists=>:tags).all
    @db.sqls.should == ["SELECT tags.id, artists.id AS artists_id, artists.tag_ids, tags_0.id AS tags_0_id FROM tags LEFT OUTER JOIN artists ON (artists.tag_ids @> ARRAY[tags.id]) LEFT OUTER JOIN tags AS tags_0 ON (artists.tag_ids @> ARRAY[tags_0.id])"]
    a.should == [@o2]
    a.first.artists.should == [@o1]
    a.first.artists.first.tags.should == [@o2]
    @db.sqls.should == []
  end
  
  it "eager graphing should respect key options" do 
    @c1.class_eval{def tag3_ids; tag_ids.map{|x| x*3} end}
    @c1.pg_array_to_many :tags, :clone=>:tags, :primary_key=>Sequel.*(:id, 3), :primary_key_method=>:id3, :key=>:tag3_ids, :key_column=>Sequel.pg_array(:tag_ids)[1..2]
    @c2.many_to_pg_array :artists, :clone=>:artists, :primary_key=>:id3, :key=>:tag3_ids, :key_column=>Sequel.pg_array(:tag_ids)[1..2]

    @c2.dataset._fetch = {:id=>2, :artists_id=>1, :tag_ids=>Sequel.pg_array([1,2,3]), :tags_0_id=>2}
    @c1.dataset._fetch = {:id=>1, :tags_id=>2, :tag_ids=>Sequel.pg_array([1,2,3]), :artists_0_id=>1, :artists_0_tag_ids=>Sequel.pg_array([1,2,3])}

    a = @c1.eager_graph(:tags).all
    a.should == [@o1]
    @db.sqls.should == ["SELECT artists.id, artists.tag_ids, tags.id AS tags_id FROM artists LEFT OUTER JOIN tags ON (artists.tag_ids[1:2] @> ARRAY[(tags.id * 3)])"]
    a.first.tags.should == [@o2]
    @db.sqls.should == []

    a = @c2.eager_graph(:artists).all
    a.should == [@o2]
    @db.sqls.should == ["SELECT tags.id, artists.id AS artists_id, artists.tag_ids FROM tags LEFT OUTER JOIN artists ON (artists.tag_ids[1:2] @> ARRAY[tags.id3])"]
    a.first.artists.should == [@o1]
    @db.sqls.should == []
  end
  
  it "should respect the association's :graph_select option" do 
    @c1.pg_array_to_many :tags, :clone=>:tags, :graph_select=>:id2
    @c2.many_to_pg_array :artists, :clone=>:artists, :graph_select=>:id

    @c2.dataset._fetch = {:id=>2, :artists_id=>1}
    @c1.dataset._fetch = {:id=>1, :id2=>2, :tag_ids=>Sequel.pg_array([1,2,3])}

    a = @c1.eager_graph(:tags).all
    @db.sqls.should == ["SELECT artists.id, artists.tag_ids, tags.id2 FROM artists LEFT OUTER JOIN tags ON (artists.tag_ids @> ARRAY[tags.id])"]
    a.should == [@o1]
    a.first.tags.should == [@c2.load(:id2=>2)]
    @db.sqls.should == []

    a = @c2.eager_graph(:artists).all
    @db.sqls.should == ["SELECT tags.id, artists.id AS artists_id FROM tags LEFT OUTER JOIN artists ON (artists.tag_ids @> ARRAY[tags.id])"]
    a.should == [@o2]
    a.first.artists.should == [@c1.load(:id=>1)]
    @db.sqls.should == []
  end

  it "should respect the association's :graph_join_type option" do 
    @c1.pg_array_to_many :tags, :clone=>:tags, :graph_join_type=>:inner
    @c2.many_to_pg_array :artists, :clone=>:artists, :graph_join_type=>:inner
    @c1.eager_graph(:tags).sql.should == "SELECT artists.id, artists.tag_ids, tags.id AS tags_id FROM artists INNER JOIN tags ON (artists.tag_ids @> ARRAY[tags.id])"
    @c2.eager_graph(:artists).sql.should == "SELECT tags.id, artists.id AS artists_id, artists.tag_ids FROM tags INNER JOIN artists ON (artists.tag_ids @> ARRAY[tags.id])"
  end

  it "should respect the association's :conditions option" do 
    @c1.pg_array_to_many :tags, :clone=>:tags, :conditions=>{:a=>1}
    @c2.many_to_pg_array :artists, :clone=>:artists, :conditions=>{:a=>1}
    @c1.eager_graph(:tags).sql.should == "SELECT artists.id, artists.tag_ids, tags.id AS tags_id FROM artists LEFT OUTER JOIN tags ON ((tags.a = 1) AND (artists.tag_ids @> ARRAY[tags.id]))"
    @c2.eager_graph(:artists).sql.should == "SELECT tags.id, artists.id AS artists_id, artists.tag_ids FROM tags LEFT OUTER JOIN artists ON ((artists.a = 1) AND (artists.tag_ids @> ARRAY[tags.id]))"
  end

  it "should respect the association's :graph_conditions option" do 
    @c1.pg_array_to_many :tags, :clone=>:tags, :graph_conditions=>{:a=>1}
    @c2.many_to_pg_array :artists, :clone=>:artists, :graph_conditions=>{:a=>1}
    @c1.eager_graph(:tags).sql.should == "SELECT artists.id, artists.tag_ids, tags.id AS tags_id FROM artists LEFT OUTER JOIN tags ON ((tags.a = 1) AND (artists.tag_ids @> ARRAY[tags.id]))"
    @c2.eager_graph(:artists).sql.should == "SELECT tags.id, artists.id AS artists_id, artists.tag_ids FROM tags LEFT OUTER JOIN artists ON ((artists.a = 1) AND (artists.tag_ids @> ARRAY[tags.id]))"
  end

  it "should respect the association's :graph_block option" do 
    @c1.pg_array_to_many :tags, :clone=>:tags, :graph_block=>proc{|ja,lja,js| {Sequel.qualify(ja, :a)=>1}}
    @c2.many_to_pg_array :artists, :clone=>:artists, :graph_block=>proc{|ja,lja,js| {Sequel.qualify(ja, :a)=>1}}
    @c1.eager_graph(:tags).sql.should == "SELECT artists.id, artists.tag_ids, tags.id AS tags_id FROM artists LEFT OUTER JOIN tags ON ((tags.a = 1) AND (artists.tag_ids @> ARRAY[tags.id]))"
    @c2.eager_graph(:artists).sql.should == "SELECT tags.id, artists.id AS artists_id, artists.tag_ids FROM tags LEFT OUTER JOIN artists ON ((artists.a = 1) AND (artists.tag_ids @> ARRAY[tags.id]))"
  end

  it "should respect the association's :graph_only_conditions option" do 
    @c1.pg_array_to_many :tags, :clone=>:tags, :graph_only_conditions=>{:a=>1}
    @c2.many_to_pg_array :artists, :clone=>:artists, :graph_only_conditions=>{:a=>1}
    @c1.eager_graph(:tags).sql.should == "SELECT artists.id, artists.tag_ids, tags.id AS tags_id FROM artists LEFT OUTER JOIN tags ON (tags.a = 1)"
    @c2.eager_graph(:artists).sql.should == "SELECT tags.id, artists.id AS artists_id, artists.tag_ids FROM tags LEFT OUTER JOIN artists ON (artists.a = 1)"
  end

  it "should respect the association's :graph_only_conditions with :graph_block option" do 
    @c1.pg_array_to_many :tags, :clone=>:tags, :graph_only_conditions=>{:a=>1}, :graph_block=>proc{|ja,lja,js| {Sequel.qualify(lja, :b)=>1}}
    @c2.many_to_pg_array :artists, :clone=>:artists, :graph_only_conditions=>{:a=>1}, :graph_block=>proc{|ja,lja,js| {Sequel.qualify(lja, :b)=>1}}
    @c1.eager_graph(:tags).sql.should == "SELECT artists.id, artists.tag_ids, tags.id AS tags_id FROM artists LEFT OUTER JOIN tags ON ((tags.a = 1) AND (artists.b = 1))"
    @c2.eager_graph(:artists).sql.should == "SELECT tags.id, artists.id AS artists_id, artists.tag_ids FROM tags LEFT OUTER JOIN artists ON ((artists.a = 1) AND (tags.b = 1))"
  end

  it "should define an add_ method for adding associated objects" do
    @o1.add_tag(@c2.load(:id=>4))
    @o1.tag_ids.should == [1,2,3,4]
    @db.sqls.should == []
    @o1.save_changes
    @db.sqls.should == ["UPDATE artists SET tag_ids = ARRAY[1,2,3,4] WHERE (id = 1)"]

    @o2.add_artist(@c1.load(:id=>1, :tag_ids=>Sequel.pg_array([4])))
    @db.sqls.should == ["UPDATE artists SET tag_ids = ARRAY[4,2] WHERE (id = 1)"]
  end

  it "should define a remove_ method for removing associated objects" do
    @o1.remove_tag(@o2)
    @o1.tag_ids.should == [1,3]
    @db.sqls.should == []
    @o1.save_changes
    @db.sqls.should == ["UPDATE artists SET tag_ids = ARRAY[1,3] WHERE (id = 1)"]

    @o2.remove_artist(@c1.load(:id=>1, :tag_ids=>Sequel.pg_array([1,2,3,4])))
    @db.sqls.should == ["UPDATE artists SET tag_ids = ARRAY[1,3,4] WHERE (id = 1)"]
  end

  it "should define a remove_all_ method for removing all associated objects" do
    @o1.remove_all_tags
    @o1.tag_ids.should == []
    @db.sqls.should == []
    @o1.save_changes
    @db.sqls.should == ["UPDATE artists SET tag_ids = ARRAY[] WHERE (id = 1)"]

    @o2.remove_all_artists
    @db.sqls.should == ["UPDATE artists SET tag_ids = array_remove(tag_ids, 2) WHERE (tag_ids @> ARRAY[2])"]
  end

  it "should allow calling add_ and remove_ methods on new objects for pg_array_to_many associations" do
    a = Artist.new
    a.add_tag(@c2.load(:id=>4))
    a.tag_ids.should == [4]
    a.remove_tag(@c2.load(:id=>4))
    a.tag_ids.should == []
    a.add_tag(@c2.load(:id=>4))
    a.tag_ids.should == [4]
    a.remove_all_tags
    a.tag_ids.should == []
  end

  it "should have pg_array_to_many association modification methods save if :save_after_modify option is used" do
    @c1.pg_array_to_many :tags, :clone=>:tags, :save_after_modify=>true

    @o1.add_tag(@c2.load(:id=>4))
    @o1.tag_ids.should == [1,2,3,4]
    @db.sqls.should == ["UPDATE artists SET tag_ids = ARRAY[1,2,3,4] WHERE (id = 1)"]

    @o1.remove_tag(@o2)
    @o1.tag_ids.should == [1,3,4]
    @db.sqls.should == ["UPDATE artists SET tag_ids = ARRAY[1,3,4] WHERE (id = 1)"]

    @o1.remove_all_tags
    @o1.tag_ids.should == []
    @db.sqls.should == ["UPDATE artists SET tag_ids = ARRAY[] WHERE (id = 1)"]
  end

  it "should have association modification methods deal with nil values" do
    v = @c1.load(:id=>1)
    v.add_tag(@c2.load(:id=>4))
    v.tag_ids.should == [4]
    @db.sqls.should == []
    v.save_changes
    @db.sqls.should == ["UPDATE artists SET tag_ids = ARRAY[4]::integer[] WHERE (id = 1)"]

    @o2.add_artist(@c1.load(:id=>1))
    @db.sqls.should == ["UPDATE artists SET tag_ids = ARRAY[2]::integer[] WHERE (id = 1)"]

    v = @c1.load(:id=>1)
    v.remove_tag(@c2.load(:id=>4))
    v.tag_ids.should == nil
    @db.sqls.should == []
    v.save_changes
    @db.sqls.should == []

    @o2.remove_artist(@c1.load(:id=>1))
    @db.sqls.should == []

    v = @c1.load(:id=>1)
    v.remove_all_tags
    v.tag_ids.should == nil
    @db.sqls.should == []
    v.save_changes
    @db.sqls.should == []
  end

  it "should have association modification methods deal with empty arrays values" do
    v = @c1.load(:id=>1, :tag_ids=>Sequel.pg_array([]))
    v.add_tag(@c2.load(:id=>4))
    v.tag_ids.should == [4]
    @db.sqls.should == []
    v.save_changes
    @db.sqls.should == ["UPDATE artists SET tag_ids = ARRAY[4] WHERE (id = 1)"]

    @o2.add_artist(@c1.load(:id=>1, :tag_ids=>Sequel.pg_array([])))
    @db.sqls.should == ["UPDATE artists SET tag_ids = ARRAY[2] WHERE (id = 1)"]

    v = @c1.load(:id=>1, :tag_ids=>Sequel.pg_array([]))
    v.remove_tag(@c2.load(:id=>4))
    v.tag_ids.should == []
    @db.sqls.should == []
    v.save_changes
    @db.sqls.should == []

    @o2.remove_artist(@c1.load(:id=>1, :tag_ids=>Sequel.pg_array([])))
    @db.sqls.should == []

    v = @c1.load(:id=>1, :tag_ids=>Sequel.pg_array([]))
    v.remove_all_tags
    v.tag_ids.should == []
    @db.sqls.should == []
    v.save_changes
    @db.sqls.should == []
  end

  it "should respect the :array_type option when manually creating arrays" do
    @c1.pg_array_to_many :tags, :clone=>:tags, :array_type=>:int8
    @c2.many_to_pg_array :artists, :clone=>:artists, :array_type=>:int8
    v = @c1.load(:id=>1)
    v.add_tag(@c2.load(:id=>4))
    v.tag_ids.should == [4]
    @db.sqls.should == []
    v.save_changes
    @db.sqls.should == ["UPDATE artists SET tag_ids = ARRAY[4]::int8[] WHERE (id = 1)"]

    @o2.add_artist(@c1.load(:id=>1))
    @db.sqls.should == ["UPDATE artists SET tag_ids = ARRAY[2]::int8[] WHERE (id = 1)"]
  end

  it "should respect the :array_type option in the associations dataset" do
    @c2.many_to_pg_array :artists, :clone=>:artists, :array_type=>:int8
    @c2.load(:id=>1).artists_dataset.sql.should == 'SELECT * FROM artists WHERE (artists.tag_ids @> ARRAY[1]::int8[])'
  end

  it "should respect the :array_type option when eager loading" do
    @c2.many_to_pg_array :artists, :clone=>:artists, :array_type=>:int8
    @c2.eager(:artists).all
    @db.sqls.should == ["SELECT * FROM tags", "SELECT * FROM artists WHERE (artists.tag_ids && ARRAY[2]::int8[])"]
  end

  it "should respect the :array_type option when filtering by associations" do
    @c1.pg_array_to_many :tags, :clone=>:tags, :array_type=>:int8
    @c1.where(:tags=>@c2.load(:id=>1)).sql.should == 'SELECT * FROM artists WHERE (artists.tag_ids @> ARRAY[1]::int8[])'
    @c1.where(:tags=>[@c2.load(:id=>1), @c2.load(:id=>2)]).sql.should == 'SELECT * FROM artists WHERE (artists.tag_ids && ARRAY[1,2]::int8[])'
  end

  it "should automatically determine the array type by looking at the schema" do
    @c1.db_schema[:tag_ids][:db_type] = 'int8'
    @c2.many_to_pg_array :artists, :clone=>:artists
    @c1.pg_array_to_many :tags, :clone=>:tags, :save_after_modify=>true
    @c2.load(:id=>1).artists_dataset.sql.should == 'SELECT * FROM artists WHERE (artists.tag_ids @> ARRAY[1]::int8[])'
    @c1.load(:id=>1).add_tag(@c2.load(:id=>1))
    @db.sqls.should == ["UPDATE artists SET tag_ids = ARRAY[1]::int8[] WHERE (id = 1)"]
  end

  it "should automatically determine the array type by looking at the schema" do
  end

  it "should not validate the current/associated object in add_ and remove_ if the :validate=>false option is used" do
    @c1.pg_array_to_many :tags, :clone=>:tags, :validate=>false, :save_after_modify=>true
    @c2.many_to_pg_array :artists, :clone=>:artists, :validate=>false
    a = @c1.load(:id=>1)
    t = @c2.load(:id=>2)
    def a.validate() errors.add(:id, 'foo') end
    a.associations[:tags] = []
    a.add_tag(t).should == t
    a.tags.should == [t]
    a.remove_tag(t).should == t
    a.tags.should == []

    t.associations[:artists] = []
    t.add_artist(a).should == a
    t.artists.should == [a]
    t.remove_artist(a).should == a
    t.artists.should == []
  end

  it "should not raise exception in add_ and remove_ if the :raise_on_save_failure=>false option is used" do
    @c1.pg_array_to_many :tags, :clone=>:tags, :raise_on_save_failure=>false, :save_after_modify=>true
    @c2.many_to_pg_array :artists, :clone=>:artists, :raise_on_save_failure=>false
    a = @c1.load(:id=>1)
    t = @c2.load(:id=>2)
    def a.validate() errors.add(:id, 'foo') end
    a.associations[:tags] = []
    a.add_tag(t).should == nil
    a.tags.should == []
    a.associations[:tags] = [t]
    a.remove_tag(t).should == nil
    a.tags.should == [t]

    t.associations[:artists] = []
    t.add_artist(a).should == nil
    t.artists.should == []
    t.associations[:artists] = [a]
    t.remove_artist(a).should == nil
    t.artists.should == [a]
  end
end
