require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "Sequel::Plugins::AssociationProxies" do
  before do
    class ::Tag < Sequel::Model
    end
    class ::Item < Sequel::Model
      plugin :association_proxies
      many_to_many :tags, :extend=>Module.new{def size; count end}
    end
    @i = Item.load(:id=>1)
    @t = @i.tags
    Item.db.reset
  end
  after do
    Object.send(:remove_const, :Tag)
    Object.send(:remove_const, :Item)
  end

  it "should send method calls to the associated object array if sent an array method" do
    @i.associations.has_key?(:tags).should == false
    @t.select{|x| false}.should == []
    @i.associations.has_key?(:tags).should == true
  end

  it "should send method calls to the association dataset if sent a non-array method" do
    @i.associations.has_key?(:tags).should == false
    @t.filter(:a=>1).sql.should == "SELECT tags.* FROM tags INNER JOIN items_tags ON ((items_tags.tag_id = tags.id) AND (items_tags.item_id = 1)) WHERE (a = 1)"
    @i.associations.has_key?(:tags).should == false
  end

  it "should accept block to plugin to specify which methods to proxy to dataset" do
    Item.plugin :association_proxies do |opts|
      opts[:method] == :where || opts[:arguments].length == 2 || opts[:block]
    end
    @i.associations.has_key?(:tags).should == false
    @t.where(:a=>1).sql.should == "SELECT tags.* FROM tags INNER JOIN items_tags ON ((items_tags.tag_id = tags.id) AND (items_tags.item_id = 1)) WHERE (a = 1)"
    @t.filter('a = ?', 1).sql.should == "SELECT tags.* FROM tags INNER JOIN items_tags ON ((items_tags.tag_id = tags.id) AND (items_tags.item_id = 1)) WHERE (a = 1)"
    @t.filter{{:a=>1}}.sql.should == "SELECT tags.* FROM tags INNER JOIN items_tags ON ((items_tags.tag_id = tags.id) AND (items_tags.item_id = 1)) WHERE (a = 1)"

    @i.associations.has_key?(:tags).should == false
    Item.plugin :association_proxies do |opts|
      proxy_arg = opts[:proxy_argument]
      proxy_block = opts[:proxy_block]
      cached = opts[:instance].associations[opts[:reflection][:name]]
      is_size = opts[:method] == :size
      is_size && !cached && !proxy_arg && !proxy_block
    end
    @t.size.should == 1
    Item.db.sqls.should == ["SELECT count(*) AS count FROM tags INNER JOIN items_tags ON ((items_tags.tag_id = tags.id) AND (items_tags.item_id = 1)) LIMIT 1"]
    @i.tags{|ds| ds}.size.should == 1
    Item.db.sqls.should == ["SELECT tags.* FROM tags INNER JOIN items_tags ON ((items_tags.tag_id = tags.id) AND (items_tags.item_id = 1))"]
    @i.tags(true).size.should == 1
    Item.db.sqls.should == ["SELECT tags.* FROM tags INNER JOIN items_tags ON ((items_tags.tag_id = tags.id) AND (items_tags.item_id = 1))"]
    @t.size.should == 1
    Item.db.sqls.should == []
  end

  it "should reload the cached association if sent an array method and the reload flag was given" do
    @t.select{|x| false}.should == []
    Item.db.sqls.length.should == 1
    @t.select{|x| false}.should == []
    Item.db.sqls.length.should == 0
    @i.tags(true).select{|x| false}.should == []
    Item.db.sqls.length.should == 1
    @t.filter(:a=>1).sql.should == "SELECT tags.* FROM tags INNER JOIN items_tags ON ((items_tags.tag_id = tags.id) AND (items_tags.item_id = 1)) WHERE (a = 1)"
    Item.db.sqls.length.should == 0
  end

  it "should not return a proxy object for associations that do not return an array" do
    Item.many_to_one :tag
    proc{@i.tag.filter(:a=>1)}.should raise_error(NoMethodError)

    Tag.one_to_one :item
    proc{Tag.load(:id=>1, :item_id=>2).item.filter(:a=>1)}.should raise_error(NoMethodError)
  end

  it "should work correctly in subclasses" do
    i = Class.new(Item).load(:id=>1)
    i.associations.has_key?(:tags).should == false
    i.tags.select{|x| false}.should == []
    i.associations.has_key?(:tags).should == true
    i.tags.filter(:a=>1).sql.should == "SELECT tags.* FROM tags INNER JOIN items_tags ON ((items_tags.tag_id = tags.id) AND (items_tags.item_id = 1)) WHERE (a = 1)"
  end

end
