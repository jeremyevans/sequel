require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe Sequel::Model, "hybrid table inheritance plugin" do
  before do
    @db = Sequel.mock(:autoid=>proc{|sql| 1})
    def @db.supports_schema_parsing?() true end
    def @db.schema(table, opts={})
      {:employees=>[[:id, {:primary_key=>true, :type=>:integer}], [:name, {:type=>:string}], [:kind, {:type=>:string}]],
       :managers=>[[:id, {:type=>:integer}], [:num_staff, {:type=>:integer}]],
       :uber_managers=>[[:id, {:type=>:integer}], [:special, {:type=>:string}]],
       :executives=>[[:id, {:type=>:integer}], [:num_managers, {:type=>:integer}]],
       :staff=>[[:id, {:type=>:integer}], [:manager_id, {:type=>:integer}]],
       :cooks=>[[:id, {:type=>:integer}], [:speciality, {:type=>:string}]],
      }[table.is_a?(Sequel::Dataset) ? table.first_source_table : table]
    end
    @db.extend_datasets do
      def columns
        {[:employees]=>[:id, :name, :kind],
         [:managers]=>[:id, :num_staff],
         [:uber_managers]=>[:id, :special],
         [:executives]=>[:id, :num_managers],
         [:staff]=>[:id, :manager_id],
         [:staff, :cooks]=>[:id, :manager_id, :speciality],
         [:cooks]=>[:id, :speciality],
         [:employees, :managers]=>[:id, :name, :kind, :num_staff],
         [:employees, :managers, :uber_managers]=>[:id, :name, :kind, :num_staff, :special],
         [:employees, :managers, :executives]=>[:id, :name, :kind, :num_staff, :num_managers],
         [:employees, :staff]=>[:id, :name, :kind, :manager_id],
        }[opts[:from] + (opts[:join] || []).map{|x| x.table}]
      end
    end
    class ::Employee < Sequel::Model(@db)
      def _save_refresh; @values[:id] = 1 end
      def self.columns
        dataset.columns
      end
      plugin :class_table_inheritance, :key=>:kind, :table_map=>{:Staff=>:staff}, :subclass_load => :eager
    end
    class ::Unmanaged < Employee; end
    class ::Manager < Employee
      one_to_many :staff_members, :class=>:Staff
    end
    class ::SmartManager < Manager; end
    class ::GeniusManager < SmartManager; end
    class ::UberManager < SmartManager; end
    class ::DumbManager < Manager; end
    class ::Executive < Manager; end
    class ::Staff < Employee
      many_to_one :manager
    end
    class ::Cook < Staff; end

    @ds = Employee.dataset
  end

  def remove_subclasses
    Object.send(:remove_const, :Unmanaged)
    Object.send(:remove_const, :Executive)
    Object.send(:remove_const, :Manager)
    Object.send(:remove_const, :SmartManager)
    Object.send(:remove_const, :GeniusManager)
    Object.send(:remove_const, :UberManager)
    Object.send(:remove_const, :DumbManager)
    Object.send(:remove_const, :Staff)
    Object.send(:remove_const, :Cook)
  end

  after do
    remove_subclasses
    Object.send(:remove_const, :Employee)
  end

  def should_datasets
    Unmanaged.dataset.sql.must_equal "SELECT * FROM employees WHERE (employees.kind IN ('Unmanaged'))"
    Manager.dataset.sql.must_equal "SELECT employees.id, employees.name, employees.kind, managers.num_staff FROM employees INNER JOIN managers ON (managers.id = employees.id)"
    SmartManager.dataset.sql.must_equal "SELECT employees.id, employees.name, employees.kind, managers.num_staff FROM employees INNER JOIN managers ON (managers.id = employees.id) WHERE (employees.kind IN ('SmartManager', 'GeniusManager', 'UberManager'))"
    GeniusManager.dataset.sql.must_equal "SELECT employees.id, employees.name, employees.kind, managers.num_staff FROM employees INNER JOIN managers ON (managers.id = employees.id) WHERE (employees.kind IN ('GeniusManager'))"
    UberManager.dataset.sql.must_equal "SELECT employees.id, employees.name, employees.kind, managers.num_staff, uber_managers.special FROM employees INNER JOIN managers ON (managers.id = employees.id) INNER JOIN uber_managers ON (uber_managers.id = managers.id)"
    DumbManager.dataset.sql.must_equal "SELECT employees.id, employees.name, employees.kind, managers.num_staff FROM employees INNER JOIN managers ON (managers.id = employees.id) WHERE (employees.kind IN ('DumbManager'))"
    Executive.dataset.sql.must_equal "SELECT employees.id, employees.name, employees.kind, managers.num_staff, executives.num_managers FROM employees INNER JOIN managers ON (managers.id = employees.id) INNER JOIN executives ON (executives.id = managers.id)"
    Staff.dataset.sql.must_equal "SELECT employees.id, employees.name, employees.kind, staff.manager_id FROM employees INNER JOIN staff ON (staff.id = employees.id)"
    Cook.dataset.sql.must_equal "SELECT employees.id, employees.name, employees.kind, staff.manager_id, cooks.speciality FROM employees INNER JOIN staff ON (staff.id = employees.id) INNER JOIN cooks ON (cooks.id = staff.id)"
  end

  specify "should implicity determine dataset" do
    should_datasets
  end

  specify "should explicity determine dataset" do
    table_map = {
        :Employee => :employees,
        :Unmanaged => :employees,
        :Manager => :managers,
        :SmartManager => :managers,
        :GeniusManager => :managers,
        :UberManager => :uber_managers,
        :DumbManager => :managers,
        :Executive => :executives,
        :Staff => :staff,
        :Cook => :cooks
    }
    Employee.plugin :class_table_inheritance, :key=>:kind, :table_map=>table_map, :subclass_load => :eager
    remove_subclasses
    class ::Unmanaged < Employee; end
    class ::Manager < Employee
      one_to_many :staff_members, :class=>:Staff
    end
    class ::SmartManager < Manager; end
    class ::GeniusManager < SmartManager; end
    class ::UberManager < SmartManager; end
    class ::DumbManager < Manager; end
    class ::Executive < Manager; end
    class ::Staff < Employee
      many_to_one :manager
    end
    class ::Cook < Staff; end

    should_datasets
  end

  it "should allow setting and overriding subclass loading options" do
    Employee.dataset.uses_eager_load?.must_equal true
    Employee.dataset.eager.uses_eager_load?.must_equal true
    Employee.dataset.lazy.uses_eager_load?.must_equal false

    Employee.plugin :class_table_inheritance, :subclass_load => :lazy
    Employee.dataset.uses_eager_load?.must_equal false
    Employee.dataset.lazy.uses_eager_load?.must_equal false
    Employee.dataset.eager.uses_eager_load?.must_equal true

    Employee.plugin :class_table_inheritance, :subclass_load => :eager_only
    Employee.dataset.uses_eager_load?.must_equal true
    Employee.dataset.eager.uses_eager_load?.must_equal true
    proc{Employee.dataset.lazy.uses_eager_load?}.must_raise(Sequel::Error)

    Employee.plugin :class_table_inheritance, :subclass_load => :lazy_only
    Employee.dataset.uses_eager_load?.must_equal false
    Employee.dataset.lazy.uses_eager_load?.must_equal false
    proc{Employee.dataset.eager.uses_eager_load?}.must_raise(Sequel::Error)
  end

  it "should initialize subclass datasets" do
    Employee.cti_subclass_datasets.collect { |k, ds| [k, ds.sql] }.must_equal [
        [Manager, "SELECT managers.id, managers.num_staff FROM managers"],
        [UberManager, "SELECT managers.id, managers.num_staff, uber_managers.special FROM managers INNER JOIN uber_managers ON (uber_managers.id = managers.id)"],
        [Executive, "SELECT managers.id, managers.num_staff, executives.num_managers FROM managers INNER JOIN executives ON (executives.id = managers.id)"],
        [Staff, "SELECT staff.id, staff.manager_id FROM staff"],
        [Cook, "SELECT staff.id, staff.manager_id, cooks.speciality FROM staff INNER JOIN cooks ON (cooks.id = staff.id)"]
    ]
    Manager.cti_subclass_datasets.collect { |k, ds| [k, ds.sql] }.must_equal [
        [UberManager, "SELECT uber_managers.id, uber_managers.special FROM uber_managers"],
        [Executive, "SELECT executives.id, executives.num_managers FROM executives"]
        ]
    Staff.cti_subclass_datasets.collect { |k, ds| [k, ds.sql] }.must_equal [
        [Cook, "SELECT cooks.id, cooks.speciality FROM cooks"]
    ]
    [Unmanaged, SmartManager, GeniusManager, UberManager, DumbManager, Executive].each do |klass|
      klass.cti_subclass_datasets.must_be_empty
    end
  end

  it "should eager load subclasses with first" do
    Employee.dataset._fetch = [ { :id=>1, :kind => 'Executive', :name=>'Tim'} ]
    Employee.cti_subclass_datasets[Executive]._fetch = [
        { :id=>1, :num_managers => 4 },
    ]
    rec = Employee.first
    rec.class.must_equal Executive
    rec.values.must_equal(:id=>1, :kind => 'Executive', :name=>'Tim', :num_managers => 4)
    @db.sqls.must_equal [
        "SELECT * FROM employees LIMIT 1",
        "SELECT managers.id, managers.num_staff, executives.num_managers FROM managers INNER JOIN executives ON (executives.id = managers.id) WHERE (managers.id = 1) LIMIT 1",
    ]
  end

  it "should eager load subclasses with all" do
    Employee.dataset._fetch = [
        { :id=>1, :kind=>'Unmanaged', :name=>'Mr Unmanagable'},
        { :id=>2, :kind=>'SmartManager', :name=>'Joe'},
        { :id=>3, :kind=>'GeniusManager', :name=>'Steve'},
        { :id=>4, :kind=>'UberManager', :name=>'Jesus'},
        { :id=>5, :kind=>'DumbManager', :name=>'Erkle'},
        { :id=>6, :kind=>'Executive', :name=>'Tim'},
        { :id=>7, :kind=>'Staff', :name=>'Kim'},
    ]
    Employee.cti_subclass_datasets[Manager]._fetch = [
        { :id=>2, :num_staff=>1 },
        { :id=>3, :num_staff=>2 },
        { :id=>5, :num_staff=>3 },
    ]
    Employee.cti_subclass_datasets[UberManager]._fetch = [
        { :id=>4, :num_staff=>4, :special=>'Very Special' },
        { :id=>4, :num_staff=>4, :special=>'Very Special Duplicate' }
    ]
    Employee.cti_subclass_datasets[Executive]._fetch = [
        { :id=>6, :num_managers=>5 },
    ]
    Employee.cti_subclass_datasets[Staff]._fetch = [
        { :id=>7, :manager_id=>2 },
    ]
    list = Employee.all
    list.collect{|x| [x.class, x.values] }.must_equal [
        [Unmanaged,     {:id=>1, :kind=>"Unmanaged", :name=>"Mr Unmanagable"}],
        [SmartManager,  {:id=>2, :kind=>"SmartManager", :name=>"Joe", :num_staff=>1} ],
        [GeniusManager, {:id=>3, :kind=>"GeniusManager", :name=>"Steve", :num_staff=>2}],
        [UberManager,   {:id=>4, :kind=>"UberManager", :name=>"Jesus", :num_staff=>4, :special=>'Very Special'}],
        [UberManager,   {:id=>4, :kind=>"UberManager", :name=>"Jesus", :num_staff=>4, :special=>'Very Special Duplicate'}],
        [DumbManager,   {:id=>5, :kind=>"DumbManager", :name=>"Erkle", :num_staff=>3}],
        [Executive,     {:id=>6, :kind=>"Executive", :name=>"Tim", :num_managers=>5}],
        [Staff,         {:id=>7, :kind=>"Staff", :name=>"Kim", :manager_id => 2}]
    ]
    @db.sqls.must_equal [
        "SELECT * FROM employees",
        "SELECT managers.id, managers.num_staff FROM managers WHERE (managers.id IN (2, 3, 5))",
        "SELECT managers.id, managers.num_staff, uber_managers.special FROM managers INNER JOIN uber_managers ON (uber_managers.id = managers.id) WHERE (managers.id IN (4))",
        "SELECT managers.id, managers.num_staff, executives.num_managers FROM managers INNER JOIN executives ON (executives.id = managers.id) WHERE (managers.id IN (6))",
        "SELECT staff.id, staff.manager_id FROM staff WHERE (staff.id IN (7))"
    ]
  end

  it "should eager load many_to_one relationships correctly" do
    Manager.dataset._fetch = {:id=>3, :name=>'E', :kind=>'Executive'}
    Manager.cti_subclass_datasets[Executive]._fetch = { :id=>3, :num_managers=>3}
    ldr = Staff.load(:manager_id=>3)
    res = ldr.manager
    res.must_equal Executive.load(:id=>3, :name=>'E', :kind=>'Executive', :num_managers=>3)
    @db.sqls.must_equal [
        'SELECT employees.id, employees.name, employees.kind, managers.num_staff FROM employees INNER JOIN managers ON (managers.id = employees.id) WHERE (managers.id = 3) LIMIT 1',
        'SELECT executives.id, executives.num_managers FROM executives WHERE (executives.id = 3) LIMIT 1'
    ]
  end

  it "should eager load one_to_many relationships correctly" do
    Staff.dataset._fetch = {:id=>1, :name=>'S', :kind=>'Cook', :manager_id=>3}
    Staff.cti_subclass_datasets[Cook]._fetch = { :id=>1, :speciality=>'Burgers'}
    Executive.load(:id=>3).staff_members.must_equal [Cook.load(:id=>1, :name=>'S', :kind=>'Cook', :manager_id=>3, :speciality=>'Burgers')]
    @db.sqls.must_equal [
        'SELECT employees.id, employees.name, employees.kind, staff.manager_id FROM employees INNER JOIN staff ON (staff.id = employees.id) WHERE (staff.manager_id = 3)',
        'SELECT cooks.id, cooks.speciality FROM cooks WHERE (cooks.id IN (1))'
    ]
  end
end