require File.join(File.dirname(__FILE__), 'spec_helper.rb')

describe "Eagerly loading a tree structure" do
  before do
    class ::Node < Sequel::Model
      set_schema do
        primary_key :id
        foreign_key :parent_id, :nodes
      end
      create_table!
      
      many_to_one :parent
      one_to_many :children, :key=>:parent_id
    
      # Only useful when eager loading
      many_to_one :ancestors, :eager_loader=>(proc do |key_hash, nodes, associations|
        # Handle cases where the root node has the same parent_id as primary_key
        # and also when it is NULL
        non_root_nodes = nodes.reject do |n| 
          if [nil, n.pk].include?(n.parent_id)
            # Make sure root nodes have their parent association set to nil
            n.associations[:parent] = nil
            true
          else
            false
          end
        end
        unless non_root_nodes.empty?
          id_map = {}
          # Create an map of parent_ids to nodes that have that parent id
          non_root_nodes.each{|n| (id_map[n.parent_id] ||= []) << n}
          # Doesn't cause an infinte loop, because when only the root node
          # is left, this is not called.
          Node.filter(Node.primary_key=>id_map.keys.sort).eager(:ancestors).all do |node|
            # Populate the parent association for each node
            id_map[node.pk].each{|n| n.associations[:parent] = node}
          end
        end
      end)
      many_to_one :descendants, :eager_loader=>(proc do |key_hash, nodes, associations|
        id_map = {}
        nodes.each do |n|
          # Initialize an empty array of child associations for each parent node
          n.associations[:children] = []
          # Populate identity map of nodes
          id_map[n.pk] = n
        end
        # Doesn't cause an infinite loop, because the :eager_loader is not called
        # if no records are returned.  Exclude id = parent_id to avoid infinite loop
        # if the root note is one of the returned records and it has parent_id = id
        # instead of parent_id = NULL.
        Node.filter(:parent_id=>id_map.keys.sort).exclude(:id=>:parent_id).eager(:descendants).all do |node|
          # Get the parent from the identity map
          parent = id_map[node.parent_id]
          # Set the child's parent association to the parent 
          node.associations[:parent] = parent
          # Add the child association to the array of children in the parent
          parent.associations[:children] << node
        end
      end)
    end
    
    Node.insert(:parent_id=>1)
    Node.insert(:parent_id=>1)
    Node.insert(:parent_id=>1)
    Node.insert(:parent_id=>2)
    Node.insert(:parent_id=>4)
    Node.insert(:parent_id=>5)
    Node.insert(:parent_id=>6)
    clear_sqls
  end
  after do
    Node.drop_table
    Object.send(:remove_const, :Node)
  end

  it "#descendants should get all descendants in one call" do
    nodes = Node.filter(:id=>1).eager(:descendants).all
    sqls_should_be('SELECT * FROM nodes WHERE (id = 1)',
      'SELECT * FROM nodes WHERE ((parent_id IN (1)) AND (id != parent_id))',
      'SELECT * FROM nodes WHERE ((parent_id IN (2, 3)) AND (id != parent_id))',
      'SELECT * FROM nodes WHERE ((parent_id IN (4)) AND (id != parent_id))',
      'SELECT * FROM nodes WHERE ((parent_id IN (5)) AND (id != parent_id))',
      'SELECT * FROM nodes WHERE ((parent_id IN (6)) AND (id != parent_id))',
      'SELECT * FROM nodes WHERE ((parent_id IN (7)) AND (id != parent_id))')
    nodes.length.should == 1
    node = nodes.first
    node.pk.should == 1
    node.children.length.should == 2
    node.children.collect{|x| x.pk}.sort.should == [2, 3]
    node.children.collect{|x| x.parent}.should == [node, node]
    node = nodes.first.children.find{|x| x.pk == 2}
    node.children.length.should == 1
    node.children.first.pk.should == 4
    node.children.first.parent.should == node
    node = node.children.first
    node.children.length.should == 1
    node.children.first.pk.should == 5
    node.children.first.parent.should == node
    node = node.children.first
    node.children.length.should == 1
    node.children.first.pk.should == 6
    node.children.first.parent.should == node
    node = node.children.first
    node.children.length.should == 1
    node.children.first.pk.should == 7
    node.children.first.parent.should == node
    sqls_should_be
  end

  it "#ancestors should get all ancestors in one call" do
    nodes = Node.filter(:id=>[7,3]).order(:id).eager(:ancestors).all
    sqls_should_be('SELECT * FROM nodes WHERE (id IN (7, 3)) ORDER BY id',
      'SELECT * FROM nodes WHERE (id IN (1, 6))',
      'SELECT * FROM nodes WHERE (id IN (5))',
      'SELECT * FROM nodes WHERE (id IN (4))',
      'SELECT * FROM nodes WHERE (id IN (2))',
      'SELECT * FROM nodes WHERE (id IN (1))')
    nodes.length.should == 2
    nodes.collect{|x| x.pk}.should == [3, 7]
    nodes.first.parent.pk.should == 1
    nodes.first.parent.parent.should == nil
    node = nodes.last
    node.parent.pk.should == 6
    node = node.parent
    node.parent.pk.should == 5
    node = node.parent
    node.parent.pk.should == 4
    node = node.parent
    node.parent.pk.should == 2
    node = node.parent
    node.parent.pk.should == 1
    node.parent.parent.should == nil
    sqls_should_be
  end
end

describe "Association Extensions" do
  before do
    module ::FindOrCreate
      def find_or_create(vals)
        # Exploits the fact that Sequel filters are ruby objects that
        # can be introspected.
        author_id = @opts[:where].args[1]
        first(vals) || \
          @opts[:models][nil].create(vals.merge(:author_id=>author_id))
      end 
    end
    class ::Author < Sequel::Model
      set_schema do
        primary_key :id
      end
      create_table!
      one_to_many :authorships, :extend=>FindOrCreate, :dataset=>(proc do
        key = pk
        ds = Authorship.filter(:author_id=>key)
        ds.meta_def(:find_or_create_by_name) do |name|
          first(:name=>name) || Authorship.create(:name=>name, :author_id=>key)
        end 
        ds  
      end)
    end
    class ::Authorship < Sequel::Model
      set_schema do
        primary_key :id
        foreign_key :author_id, :authors
        text :name
      end
      create_table!
      many_to_one :author
    end
    @author = Author.create
    clear_sqls
  end
  after do
    Authorship.drop_table
    Author.drop_table
    Object.send(:remove_const, :Author)
    Object.send(:remove_const, :Authorship)
  end

  it "should allow methods to be called on the dataset method" do
    Authorship.count.should == 0
    sqls_should_be('SELECT COUNT(*) FROM authorships LIMIT 1')
    authorship = @author.authorships_dataset.find_or_create_by_name('Bob')
    sqls_should_be("SELECT * FROM authorships WHERE ((author_id = 1) AND (name = 'Bob')) LIMIT 1",
      /INSERT INTO authorships \((author_id, name|name, author_id)\) VALUES \((1, 'Bob'|'Bob', 1)\)/,
      "SELECT * FROM authorships WHERE (id = 1) LIMIT 1")
    Authorship.count.should == 1
    Authorship.first.should == authorship
    sqls_should_be('SELECT COUNT(*) FROM authorships LIMIT 1', "SELECT * FROM authorships LIMIT 1")
    authorship.name.should == 'Bob'
    authorship.author_id.should == @author.id
    @author.authorships_dataset.find_or_create_by_name('Bob').should == authorship
    sqls_should_be("SELECT * FROM authorships WHERE ((author_id = 1) AND (name = 'Bob')) LIMIT 1")
    Authorship.count.should == 1
    sqls_should_be('SELECT COUNT(*) FROM authorships LIMIT 1')
    authorship2 = @author.authorships_dataset.find_or_create(:name=>'Jim')
    sqls_should_be("SELECT * FROM authorships WHERE ((author_id = 1) AND (name = 'Jim')) LIMIT 1",
      /INSERT INTO authorships \((author_id, name|name, author_id)\) VALUES \((1, 'Jim'|'Jim', 1)\)/,
      "SELECT * FROM authorships WHERE (id = 2) LIMIT 1")
    Authorship.count.should == 2
    sqls_should_be('SELECT COUNT(*) FROM authorships LIMIT 1')
    Authorship.order(:name).map(:name).should == ['Bob', 'Jim']
    sqls_should_be('SELECT * FROM authorships ORDER BY name')
    authorship2.name.should == 'Jim'
    authorship2.author_id.should == @author.id
    @author.authorships_dataset.find_or_create(:name=>'Jim').should == authorship2
    sqls_should_be("SELECT * FROM authorships WHERE ((author_id = 1) AND (name = 'Jim')) LIMIT 1")
  end
end

describe "has_many :through has_many and has_one :through belongs_to" do
  before do
    class ::Firm < Sequel::Model
      set_schema do
        primary_key :id
      end
      create_table!
      one_to_many :clients
      one_to_many :invoices, :read_only=>true, \
        :dataset=>proc{Invoice.eager_graph(:client).filter(:client__firm_id=>pk)}, \
        :after_load=>(proc do |firm, invs|
          invs.each do |inv|
            inv.client.associations[:firm] = inv.associations[:firm] = firm
          end
        end), \
        :eager_loader=>(proc do |key_hash, firms, associations|
          id_map = key_hash[Firm.primary_key]
          firms.each{|firm| firm.associations[:invoices] = []}
          Invoice.eager_graph(:client).filter(:client__firm_id=>id_map.keys).all do |inv|
            id_map[inv.client.firm_id].each do |firm|
              inv.client.associations[:firm] = inv.associations[:firm] = firm
              firm.associations[:invoices] << inv
            end
          end
        end)
    end

    class ::Client < Sequel::Model
      set_schema do
        primary_key :id
        foreign_key :firm_id, :firms
      end
      create_table!
      many_to_one :firm
      one_to_many :invoices
    end

    class ::Invoice < Sequel::Model
      set_schema do
        primary_key :id
        foreign_key :client_id, :clients
      end
      create_table!
      many_to_one :client
      many_to_one :firm, :key=>nil, :read_only=>true, \
        :dataset=>proc{Firm.eager_graph(:clients).filter(:clients__id=>client_id)}, \
        :after_load=>(proc do |inv, firm|
          # Delete the cached associations from firm, because it only has the
          # client with this invoice, instead of all clients of the firm
          inv.associations[:client] = firm.associations.delete(:clients).first
        end), \
        :eager_loader=>(proc do |key_hash, invoices, associations|
          id_map = {}
          invoices.each do |inv|
            inv.associations[:firm] = nil
            inv.associations[:client] = nil
            (id_map[inv.client_id] ||= []) << inv
          end
          Firm.eager_graph(:clients).filter(:clients__id=>id_map.keys).all do |firm|
            # Delete the cached associations from firm, because it only has the
            # clients related the invoices being eagerly loaded, instead of all
            # clients of the firm.
            firm.associations.delete(:clients).each do |client|
              id_map[client.pk].each do |inv|
                inv.associations[:firm] = firm
                inv.associations[:client] = client
              end
            end
          end
        end)
    end
    @firm1 = Firm.create
    @firm2 = Firm.create
    @client1 = Client.create(:firm => @firm1)
    @client2 = Client.create(:firm => @firm1)
    @client3 = Client.create(:firm => @firm2)
    @invoice1 = Invoice.create(:client => @client1)
    @invoice2 = Invoice.create(:client => @client1)
    @invoice3 = Invoice.create(:client => @client2)
    @invoice4 = Invoice.create(:client => @client3)
    @invoice5 = Invoice.create(:client => @client3)
    clear_sqls
  end
  after do
    Invoice.drop_table
    Client.drop_table
    Firm.drop_table
    Object.send(:remove_const, :Firm)
    Object.send(:remove_const, :Client)
    Object.send(:remove_const, :Invoice)
  end

  it "should return has_many :through has_many records for a single object" do
    invs = @firm1.invoices.sort_by{|x| x.pk}
    sqls_should_be('SELECT invoices.id, invoices.client_id, client.id AS client_id_0, client.firm_id FROM invoices LEFT OUTER JOIN clients AS client ON (client.id = invoices.client_id) WHERE (client.firm_id = 1)')
    invs.should == [@invoice1, @invoice2, @invoice3]
    invs[0].client.should == @client1
    invs[1].client.should == @client1
    invs[2].client.should == @client2
    invs.collect{|i| i.firm}.should == [@firm1, @firm1, @firm1]
    invs.collect{|i| i.client.firm}.should == [@firm1, @firm1, @firm1]
    sqls_should_be
  end

  it "should eagerly load has_many :through has_many records for multiple objects" do
    firms = Firm.order(:id).eager(:invoices).all
    sqls_should_be("SELECT * FROM firms ORDER BY id", "SELECT invoices.id, invoices.client_id, client.id AS client_id_0, client.firm_id FROM invoices LEFT OUTER JOIN clients AS client ON (client.id = invoices.client_id) WHERE (client.firm_id IN (1, 2))")
    firms.should == [@firm1, @firm2]
    firm1, firm2 = firms
    invs1 = firm1.invoices.sort_by{|x| x.pk}
    invs2 = firm2.invoices.sort_by{|x| x.pk}
    invs1.should == [@invoice1, @invoice2, @invoice3]
    invs2.should == [@invoice4, @invoice5]
    invs1[0].client.should == @client1
    invs1[1].client.should == @client1
    invs1[2].client.should == @client2
    invs2[0].client.should == @client3
    invs2[1].client.should == @client3
    invs1.collect{|i| i.firm}.should == [@firm1, @firm1, @firm1]
    invs2.collect{|i| i.firm}.should == [@firm2, @firm2]
    invs1.collect{|i| i.client.firm}.should == [@firm1, @firm1, @firm1]
    invs2.collect{|i| i.client.firm}.should == [@firm2, @firm2]
    sqls_should_be
  end

  it "should return has_one :through belongs_to records for a single object" do
    firm = @invoice1.firm
    sqls_should_be('SELECT firms.id, clients.id AS clients_id, clients.firm_id FROM firms LEFT OUTER JOIN clients ON (clients.firm_id = firms.id) WHERE (clients.id = 1)')
    firm.should == @firm1
    @invoice1.client.should == @client1
    @invoice1.client.firm.should == @firm1
    sqls_should_be
    firm.associations[:clients].should == nil
  end

  it "should eagerly load has_one :through belongs_to records for multiple objects" do
    invs = Invoice.order(:id).eager(:firm).all
    sqls_should_be("SELECT * FROM invoices ORDER BY id", "SELECT firms.id, clients.id AS clients_id, clients.firm_id FROM firms LEFT OUTER JOIN clients ON (clients.firm_id = firms.id) WHERE (clients.id IN (1, 2, 3))")
    invs.should == [@invoice1, @invoice2, @invoice3, @invoice4, @invoice5]
    invs[0].firm.should == @firm1
    invs[0].client.should == @client1
    invs[0].client.firm.should == @firm1
    invs[0].firm.associations[:clients].should == nil
    invs[1].firm.should == @firm1
    invs[1].client.should == @client1
    invs[1].client.firm.should == @firm1
    invs[1].firm.associations[:clients].should == nil
    invs[2].firm.should == @firm1
    invs[2].client.should == @client2
    invs[2].client.firm.should == @firm1
    invs[2].firm.associations[:clients].should == nil
    invs[3].firm.should == @firm2
    invs[3].client.should == @client3
    invs[3].client.firm.should == @firm2
    invs[3].firm.associations[:clients].should == nil
    invs[4].firm.should == @firm2
    invs[4].client.should == @client3
    invs[4].client.firm.should == @firm2
    invs[4].firm.associations[:clients].should == nil
    sqls_should_be
  end
end

describe "Polymorphic Associations" do
  before do
    class ::Asset < Sequel::Model
      set_schema do
        primary_key :id
        integer :attachable_id
        text :attachable_type
      end
      create_table!
      many_to_one :attachable, :reciprocal=>:assets, \
        :dataset=>(proc do
          klass = attachable_type.constantize
          klass.filter(klass.primary_key=>attachable_id)
        end), \
        :eager_loader=>(proc do |key_hash, assets, associations|
          id_map = {}
          assets.each do |asset|
            asset.associations[:attachable] = nil 
            ((id_map[asset.attachable_type] ||= {})[asset.attachable_id] ||= []) << asset
          end 
          id_map.each do |klass_name, id_map|
            klass = klass_name.constantize
            klass.filter(klass.primary_key=>id_map.keys).all do |attach|
              id_map[attach.pk].each do |asset|
                asset.associations[:attachable] = attach
              end 
            end 
          end 
        end)
            
      private

      def _attachable=(attachable)
        self[:attachable_id] = (attachable.pk if attachable)
        self[:attachable_type] = (attachable.class.name if attachable)
      end 
    end 
  
    class ::Post < Sequel::Model
      set_schema do
        primary_key :id
      end
      create_table!
      one_to_many :assets, :key=>:attachable_id do |ds|
        ds.filter(:attachable_type=>'Post')
      end 
      
      private

      def _add_asset(asset)
        asset.attachable_id = pk
        asset.attachable_type = 'Post'
        asset.save
      end
      def _remove_asset(asset)
        asset.attachable_id = nil
        asset.attachable_type = nil
        asset.save
      end
      def _remove_all_assets
        Asset.filter(:attachable_id=>pk, :attachable_type=>'Post')\
          .update(:attachable_id=>nil, :attachable_type=>nil)
      end
    end 
  
    class ::Note < Sequel::Model
      set_schema do
        primary_key :id
      end
      create_table!
      one_to_many :assets, :key=>:attachable_id do |ds|
        ds.filter(:attachable_type=>'Note')
      end 
      
      private

      def _add_asset(asset)
        asset.attachable_id = pk
        asset.attachable_type = 'Note'
        asset.save
      end
      def _remove_asset(asset)
        asset.attachable_id = nil
        asset.attachable_type = nil
        asset.save
      end
      def _remove_all_assets
        Asset.filter(:attachable_id=>pk, :attachable_type=>'Note')\
          .update(:attachable_id=>nil, :attachable_type=>nil)
      end
    end
    @post = Post.create
    Note.create
    @note = Note.create
    @asset1 = Asset.create(:attachable=>@post)
    @asset2 = Asset.create(:attachable=>@note)
    @asset1.associations.clear
    @asset2.associations.clear
    clear_sqls
  end
  after do
    Asset.drop_table
    Post.drop_table
    Note.drop_table
    Object.send(:remove_const, :Asset)
    Object.send(:remove_const, :Post)
    Object.send(:remove_const, :Note)
  end

  it "should load the correct associated object for a single object" do
    @asset1.attachable.should == @post
    @asset2.attachable.should == @note
    sqls_should_be("SELECT * FROM posts WHERE (id = 1) LIMIT 1", "SELECT * FROM notes WHERE (id = 2) LIMIT 1")
  end

  it "should eagerly load the correct associated object for a group of objects" do
    assets = Asset.order(:id).eager(:attachable).all
    sqls_should_be("SELECT * FROM assets ORDER BY id", "SELECT * FROM posts WHERE (id IN (1))", "SELECT * FROM notes WHERE (id IN (2))")
    assets.should == [@asset1, @asset2]
    assets[0].attachable.should == @post
    assets[1].attachable.should == @note
    sqls_should_be
  end

  it "should set items correctly" do
    @asset1.attachable = @note
    @asset2.attachable = @post
    sqls_should_be("SELECT * FROM posts WHERE (id = 1) LIMIT 1", "SELECT * FROM notes WHERE (id = 2) LIMIT 1")
    @asset1.attachable.should == @note
    @asset1.attachable_id.should == @note.pk
    @asset1.attachable_type.should == 'Note'
    @asset2.attachable.should == @post
    @asset2.attachable_id.should == @post.pk
    @asset2.attachable_type.should == 'Post'
    @asset1.attachable = nil
    @asset1.attachable.should == nil
    @asset1.attachable_id.should == nil
    @asset1.attachable_type.should == nil
    sqls_should_be
  end

  it "should add items correctly" do
    @post.assets.should == [@asset1]
    sqls_should_be("SELECT * FROM assets WHERE ((assets.attachable_id = 1) AND (attachable_type = 'Post'))")
    @post.add_asset(@asset2)
    sqls_should_be(/UPDATE assets SET ((attachable_id = 1|attachable_type = 'Post'|id = 2)(, )?){3} WHERE \(id = 2\)/)
    @post.assets.should == [@asset1, @asset2]
    @asset2.attachable.should == @post
    @asset2.attachable_id.should == @post.pk
    @asset2.attachable_type.should == 'Post'
    sqls_should_be
  end

  it "should remove items correctly" do
    @note.assets.should == [@asset2]
    sqls_should_be("SELECT * FROM assets WHERE ((assets.attachable_id = 2) AND (attachable_type = 'Note'))")
    @note.remove_asset(@asset2)
    sqls_should_be(/UPDATE assets SET ((attachable_id = NULL|attachable_type = NULL|id = 2)(, )?){3} WHERE \(id = 2\)/)
    @note.assets.should == []
    @asset2.attachable.should == nil
    @asset2.attachable_id.should == nil
    @asset2.attachable_type.should == nil
    sqls_should_be
  end

  it "should remove all items correctly" do
    @post.remove_all_assets
    @note.remove_all_assets
    sqls_should_be(/UPDATE assets SET attachable_(id|type) = NULL, attachable_(type|id) = NULL WHERE \(\(attachable_(id|type) = (1|'Post')\) AND \(attachable_(type|id) = ('Post'|1)\)\)/,
      /UPDATE assets SET attachable_(id|type) = NULL, attachable_(type|id) = NULL WHERE \(\(attachable_(id|type) = (2|'Note')\) AND \(attachable_(type|id) = ('Note'|2)\)\)/)
    @asset1.reload.attachable.should == nil
    @asset2.reload.attachable.should == nil
  end
end

describe "many_to_one/one_to_many not referencing primary key" do
  before do
    class ::Client < Sequel::Model
      set_schema do
        primary_key :id
        text :name
      end
      create_table!
      one_to_many :invoices, :reciprocal=>:client, \
        :dataset=>proc{Invoice.filter(:client_name=>name)}, \
        :eager_loader=>(proc do |key_hash, clients, associations|
          id_map = {}
          clients.each do |client|
            id_map[client.name] = client
            client.associations[:invoices] = []
          end 
          Invoice.filter(:client_name=>id_map.keys.sort).all do |inv|
            inv.associations[:client] = client = id_map[inv.client_name]
            client.associations[:invoices] << inv 
          end 
        end)

      private

      def _add_invoice(invoice)
        invoice.client_name = name
        invoice.save
      end
      def _remove_invoice(invoice)
        invoice.client_name = nil
        invoice.save
      end
      def _remove_all_invoices
        Invoice.filter(:client_name=>name).update(:client_name=>nil)
      end
    end 
  
    class ::Invoice < Sequel::Model
      set_schema do
        primary_key :id
        text :client_name
      end
      create_table!
      many_to_one :client, :key=>:client_name, \
        :dataset=>proc{Client.filter(:name=>client_name)}, \
        :eager_loader=>(proc do |key_hash, invoices, associations|
          id_map = key_hash[:client_name]
          invoices.each{|inv| inv.associations[:client] = nil}
          Client.filter(:name=>id_map.keys).all do |client|
            id_map[client.name].each{|inv| inv.associations[:client] = client}
          end 
        end)
      
      private

      def _client=(client)
        self.client_name = (client.name if client)
      end
    end

    @client1 = Client.create(:name=>'X')
    @client2 = Client.create(:name=>'Y')
    @invoice1 = Invoice.create(:client_name=>'X')
    @invoice2 = Invoice.create(:client_name=>'X')
    clear_sqls
  end
  after do
    Invoice.drop_table
    Client.drop_table
    Object.send(:remove_const, :Client)
    Object.send(:remove_const, :Invoice)
  end

  it "should load all associated one_to_many objects for a single object" do
    invs = @client1.invoices
    sqls_should_be("SELECT * FROM invoices WHERE (client_name = 'X')")
    invs.should == [@invoice1, @invoice2]
    invs[0].client.should == @client1
    invs[1].client.should == @client1
    sqls_should_be
  end

  it "should load the associated many_to_one object for a single object" do
    client = @invoice1.client
    sqls_should_be("SELECT * FROM clients WHERE (name = 'X') LIMIT 1")
    client.should == @client1
  end

  it "should eagerly load all associated one_to_many objects for a group of objects" do
    clients = Client.order(:id).eager(:invoices).all
    sqls_should_be("SELECT * FROM clients ORDER BY id", "SELECT * FROM invoices WHERE (client_name IN ('X', 'Y'))")
    clients.should == [@client1, @client2]
    clients[1].invoices.should == []
    invs = clients[0].invoices.sort_by{|x| x.pk}
    invs.should == [@invoice1, @invoice2]
    invs[0].client.should == @client1
    invs[1].client.should == @client1
    sqls_should_be
  end

  it "should eagerly load the associated many_to_one object for a group of objects" do
    invoices = Invoice.order(:id).eager(:client).all
    sqls_should_be("SELECT * FROM invoices ORDER BY id", "SELECT * FROM clients WHERE (name IN ('X'))")
    invoices.should == [@invoice1, @invoice2]
    invoices[0].client.should == @client1
    invoices[1].client.should == @client1
    sqls_should_be
  end

  it "should set the associated object correctly" do
    @invoice1.client = @client2
    sqls_should_be("SELECT * FROM clients WHERE (name = 'X') LIMIT 1")
    @invoice1.client.should == @client2
    @invoice1.client_name.should == 'Y'
    @invoice1.client = nil
    @invoice1.client_name.should == nil
    sqls_should_be
  end

  it "should add the associated object correctly" do
    @client2.invoices.should == []
    sqls_should_be("SELECT * FROM invoices WHERE (client_name = 'Y')")
    @client2.add_invoice(@invoice1)
    sqls_should_be(/UPDATE invoices SET (client_name = 'Y'|id = 1), (client_name = 'Y'|id = 1) WHERE \(id = 1\)/)
    @client2.invoices.should == [@invoice1]
    @invoice1.client_name.should == 'Y'
    @invoice1.client = nil
    @invoice1.client_name.should == nil
    sqls_should_be
  end

  it "should remove the associated object correctly" do
    invs = @client1.invoices.sort_by{|x| x.pk}
    sqls_should_be("SELECT * FROM invoices WHERE (client_name = 'X')")
    invs.should == [@invoice1, @invoice2]
    @client1.remove_invoice(@invoice1)
    sqls_should_be(/UPDATE invoices SET (client_name = NULL|id = 1), (client_name = NULL|id = 1) WHERE \(id = 1\)/)
    @client1.invoices.should == [@invoice2]
    @invoice1.client_name.should == nil
    @invoice1.client.should == nil
    sqls_should_be
  end

  it "should remove all associated objects correctly" do
    invs = @client1.remove_all_invoices
    sqls_should_be("UPDATE invoices SET client_name = NULL WHERE (client_name = 'X')")
    @invoice1.refresh.client.should == nil
    @invoice1.client_name.should == nil
    @invoice2.refresh.client.should == nil
    @invoice2.client_name.should == nil
  end
end
