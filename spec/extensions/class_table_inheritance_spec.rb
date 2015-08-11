require File.join(File.dirname(File.expand_path(__FILE__)), "spec_helper")

describe "class_table_inheritance plugin" do
  before do
    @db = Sequel.mock(:autoid=>proc{|sql| 1})
    def @db.supports_schema_parsing?() true end
    def @db.schema(table, opts={})
      {:employees=>[[:id, {:primary_key=>true, :type=>:integer}], [:name, {:type=>:string}], [:kind, {:type=>:string}]],
       :managers=>[[:id, {:type=>:integer}], [:num_staff, {:type=>:integer}]],
       :executives=>[[:id, {:type=>:integer}], [:num_managers, {:type=>:integer}]],
       :staff=>[[:id, {:type=>:integer}], [:manager_id, {:type=>:integer}]],
       }[table.is_a?(Sequel::Dataset) ? table.first_source_table : table]
    end
    @db.extend_datasets do
      def columns
        {[:employees]=>[:id, :name, :kind],
         [:managers]=>[:id, :num_staff],
         [:executives]=>[:id, :num_managers],
         [:staff]=>[:id, :manager_id],
         [:employees, :managers]=>[:id, :name, :kind, :num_staff],
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
      plugin :class_table_inheritance, :key=>:kind, :table_map=>{:Staff=>:staff}
    end 
    class ::Manager < Employee
      one_to_many :staff_members, :class=>:Staff
    end 
    class ::Executive < Manager
    end 
    class ::Ceo < Executive
    end 
    class ::Staff < Employee
      many_to_one :manager
    end 
    @ds = Employee.dataset
    @db.sqls
  end
  after do
    Object.send(:remove_const, :Ceo)
    Object.send(:remove_const, :Executive)
    Object.send(:remove_const, :Manager)
    Object.send(:remove_const, :Staff)
    Object.send(:remove_const, :Employee)
  end

  it "should have simple_table = nil for all subclasses" do
    Manager.simple_table.must_equal nil
    Executive.simple_table.must_equal nil
    Ceo.simple_table.must_equal nil
    Staff.simple_table.must_equal nil
  end
  
  it "should have working row_proc if using set_dataset in subclass to remove columns" do
    Manager.set_dataset(Manager.dataset.select(*(Manager.columns - [:blah])))
    Manager.dataset._fetch = {:id=>1, :kind=>'Ceo'}
    Manager[1].must_equal Ceo.load(:id=>1, :kind=>'Ceo')
  end

  it "should use a joined dataset in subclasses" do
    Employee.dataset.sql.must_equal 'SELECT * FROM employees'
    Manager.dataset.sql.must_equal 'SELECT employees.id, employees.name, employees.kind, managers.num_staff FROM employees INNER JOIN managers ON (managers.id = employees.id)'
    Executive.dataset.sql.must_equal 'SELECT employees.id, employees.name, employees.kind, managers.num_staff, executives.num_managers FROM employees INNER JOIN managers ON (managers.id = employees.id) INNER JOIN executives ON (executives.id = managers.id)'
    Ceo.dataset.sql.must_equal 'SELECT employees.id, employees.name, employees.kind, managers.num_staff, executives.num_managers FROM employees INNER JOIN managers ON (managers.id = employees.id) INNER JOIN executives ON (executives.id = managers.id) WHERE (employees.kind IN (\'Ceo\'))'
    Staff.dataset.sql.must_equal 'SELECT employees.id, employees.name, employees.kind, staff.manager_id FROM employees INNER JOIN staff ON (staff.id = employees.id)'
  end
  
  it "should return rows with the correct class based on the polymorphic_key value" do
    @ds._fetch = [{:kind=>'Employee'}, {:kind=>'Manager'}, {:kind=>'Executive'}, {:kind=>'Ceo'}, {:kind=>'Staff'}]
    Employee.all.collect{|x| x.class}.must_equal [Employee, Manager, Executive, Ceo, Staff]
  end 
  
  it "should return rows with the correct class based on the polymorphic_key value for subclasses" do
    Manager.dataset._fetch = [{:kind=>'Manager'}, {:kind=>'Executive'}, {:kind=>'Ceo'}]
    Manager.all.collect{|x| x.class}.must_equal [Manager, Executive, Ceo]
  end
  
  it "should have refresh return all columns in subclass after loading from superclass" do
    Employee.dataset._fetch = [{:id=>1, :name=>'A', :kind=>'Ceo'}]
    Ceo.instance_dataset._fetch = [{:id=>1, :name=>'A', :kind=>'Ceo', :num_staff=>3, :num_managers=>2}]
    a = Employee.first
    a.class.must_equal Ceo
    a.values.must_equal(:id=>1, :name=>'A', :kind=>'Ceo')
    a.refresh.values.must_equal(:id=>1, :name=>'A', :kind=>'Ceo', :num_staff=>3, :num_managers=>2)
    @db.sqls.must_equal ["SELECT * FROM employees LIMIT 1",
      "SELECT employees.id, employees.name, employees.kind, managers.num_staff, executives.num_managers FROM employees INNER JOIN managers ON (managers.id = employees.id) INNER JOIN executives ON (executives.id = managers.id) WHERE ((employees.kind IN ('Ceo')) AND (executives.id = 1)) LIMIT 1"]
  end
  
  it "should return rows with the current class if cti_key is nil" do
    Employee.plugin(:class_table_inheritance)
    Employee.dataset._fetch = [{:kind=>'Employee'}, {:kind=>'Manager'}, {:kind=>'Executive'}, {:kind=>'Ceo'}, {:kind=>'Staff'}]
    Employee.all.collect{|x| x.class}.must_equal [Employee, Employee, Employee, Employee, Employee]
  end
  
  it "should return rows with the current class if cti_key is nil in subclasses" do
    Employee.plugin(:class_table_inheritance)
    Object.send(:remove_const, :Executive)
    Object.send(:remove_const, :Manager)
    class ::Manager < Employee; end 
    class ::Executive < Manager; end 
    Manager.dataset._fetch = [{:kind=>'Manager'}, {:kind=>'Executive'}]
    Manager.all.collect{|x| x.class}.must_equal [Manager, Manager]
  end
  
  it "should handle a model map with integer values" do
    Employee.plugin(:class_table_inheritance, :key=>:kind, :model_map=>{0=>:Employee, 1=>:Manager, 2=>:Executive, 3=>:Ceo})
    Object.send(:remove_const, :Ceo)
    Object.send(:remove_const, :Executive)
    Object.send(:remove_const, :Manager)
    class ::Manager < Employee; end 
    class ::Executive < Manager; end 
    class ::Ceo < Executive; end 
    Employee.dataset._fetch = [{:kind=>nil},{:kind=>0},{:kind=>1}, {:kind=>2}, {:kind=>3}]
    Employee.all.collect{|x| x.class}.must_equal [Employee, Employee, Manager, Executive, Ceo]
    Manager.dataset._fetch = [{:kind=>nil},{:kind=>0},{:kind=>1}, {:kind=>2}, {:kind=>3}]
    Manager.all.collect{|x| x.class}.must_equal [Manager, Employee, Manager, Executive, Ceo]
  end
  
  it "should fallback to the main class if the given class does not exist" do
    @ds._fetch = [{:kind=>'Employee'}, {:kind=>'Manager'}, {:kind=>'Blah'}, {:kind=>'Staff'}]
    Employee.all.collect{|x| x.class}.must_equal [Employee, Manager, Employee, Staff]
  end
  
  it "should fallback to the main class if the given class does not exist in subclasses" do
    Manager.dataset._fetch = [{:kind=>'Manager'}, {:kind=>'Executive'}, {:kind=>'Ceo'}, {:kind=>'Blah'}]
    Manager.all.collect{|x| x.class}.must_equal [Manager, Executive, Ceo, Manager]
  end

  it "should sets the model class name for the key when creating new parent class records" do
    Employee.create
    @db.sqls.must_equal ["INSERT INTO employees (kind) VALUES ('Employee')"]
  end
  
  it "should sets the model class name for the key when creating new subclass records" do
    Ceo.create
    @db.sqls.must_equal ["INSERT INTO employees (kind) VALUES ('Ceo')",
      "INSERT INTO managers (id) VALUES (1)",
      "INSERT INTO executives (id) VALUES (1)"]
  end

  it "should ignore existing cti_key value when creating new records" do
    Employee.create(:kind=>'Manager')
    @db.sqls.must_equal ["INSERT INTO employees (kind) VALUES ('Employee')"]
  end
    
  it "should ignore existing cti_key value in subclasses" do
    Manager.create(:kind=>'Executive')
    @db.sqls.must_equal ["INSERT INTO employees (kind) VALUES ('Manager')",
      "INSERT INTO managers (id) VALUES (1)"]
  end

  it "should handle validations on the type column field" do
    o = Employee.new
    def o.validate
      errors.add(:kind, 'not present') unless kind
    end
    o.valid?.must_equal true
  end

  it "should set the type column field even when not validating" do
    Employee.new.save(:validate=>false)
    @db.sqls.must_equal ["INSERT INTO employees (kind) VALUES ('Employee')"]
  end

  it "should allow specifying a map of names to tables to override implicit mapping" do
    Manager.dataset.sql.must_equal 'SELECT employees.id, employees.name, employees.kind, managers.num_staff FROM employees INNER JOIN managers ON (managers.id = employees.id)'
    Staff.dataset.sql.must_equal 'SELECT employees.id, employees.name, employees.kind, staff.manager_id FROM employees INNER JOIN staff ON (staff.id = employees.id)'
  end

  it "should lazily load attributes for columns in subclass tables" do
    Manager.instance_dataset._fetch = Manager.dataset._fetch = {:id=>1, :name=>'J', :kind=>'Ceo', :num_staff=>2}
    m = Manager[1]
    @db.sqls.must_equal ['SELECT employees.id, employees.name, employees.kind, managers.num_staff FROM employees INNER JOIN managers ON (managers.id = employees.id) WHERE (managers.id = 1) LIMIT 1']
    @db.fetch = {:num_managers=>3}
    m.must_be_kind_of Ceo
    m.num_managers.must_equal 3
    @db.sqls.must_equal ['SELECT executives.num_managers FROM employees INNER JOIN managers ON (managers.id = employees.id) INNER JOIN executives ON (executives.id = managers.id) WHERE (executives.id = 1) LIMIT 1']
    m.values.must_equal(:id=>1, :name=>'J', :kind=>'Ceo', :num_staff=>2, :num_managers=>3)
  end

  it "should lazily load columns in middle classes correctly when loaded from parent class" do
    Employee.dataset._fetch = {:id=>1, :kind=>'Ceo'}
    Manager.dataset._fetch = {:num_staff=>2}
    e = Employee[1]
    e.must_be_kind_of(Ceo)
    @db.sqls.must_equal ["SELECT * FROM employees WHERE (id = 1) LIMIT 1"]
    e.num_staff.must_equal 2
    @db.sqls.must_equal ["SELECT managers.num_staff FROM employees INNER JOIN managers ON (managers.id = employees.id) WHERE (managers.id = 1) LIMIT 1"]
  end

  it "should eagerly load lazily columns in subclasses when loaded from parent class" do
    Employee.dataset._fetch = {:id=>1, :kind=>'Ceo'}
    Manager.dataset._fetch = {:id=>1, :num_staff=>2}
    @db.fetch = {:id=>1, :num_managers=>3}
    e = Employee.all.first
    e.must_be_kind_of(Ceo)
    @db.sqls.must_equal ["SELECT * FROM employees"]
    e.num_staff.must_equal 2
    @db.sqls.must_equal ["SELECT managers.id, managers.num_staff FROM employees INNER JOIN managers ON (managers.id = employees.id) WHERE (managers.id IN (1))"]
    e.num_managers.must_equal 3
    @db.sqls.must_equal ['SELECT executives.id, executives.num_managers FROM employees INNER JOIN managers ON (managers.id = employees.id) INNER JOIN executives ON (executives.id = managers.id) WHERE (executives.id IN (1))']
  end
  
  it "should include schema for columns for tables for ancestor classes" do
    Employee.db_schema.must_equal(:id=>{:primary_key=>true, :type=>:integer}, :name=>{:type=>:string}, :kind=>{:type=>:string})
    Manager.db_schema.must_equal(:id=>{:primary_key=>true, :type=>:integer}, :name=>{:type=>:string}, :kind=>{:type=>:string}, :num_staff=>{:type=>:integer})
    Executive.db_schema.must_equal(:id=>{:primary_key=>true, :type=>:integer}, :name=>{:type=>:string}, :kind=>{:type=>:string}, :num_staff=>{:type=>:integer}, :num_managers=>{:type=>:integer})
    Staff.db_schema.must_equal(:id=>{:primary_key=>true, :type=>:integer}, :name=>{:type=>:string}, :kind=>{:type=>:string}, :manager_id=>{:type=>:integer})
  end

  it "should use the correct primary key (which should have the same name in all subclasses)" do
    [Employee, Manager, Executive, Ceo, Staff].each{|c| c.primary_key.must_equal :id}
  end

  it "should have table_name return the table name of the most specific table" do
    Employee.table_name.must_equal :employees
    Manager.table_name.must_equal :managers
    Executive.table_name.must_equal :executives
    Ceo.table_name.must_equal :executives
    Staff.table_name.must_equal :staff
  end

  it "should delete the correct rows from all tables when deleting" do
    Ceo.load(:id=>1).delete
    @db.sqls.must_equal ["DELETE FROM executives WHERE (id = 1)", "DELETE FROM managers WHERE (id = 1)", "DELETE FROM employees WHERE (id = 1)"]
  end

  it "should not allow deletion of frozen object" do
    o = Ceo.load(:id=>1)
    o.freeze
    proc{o.delete}.must_raise(Sequel::Error)
    @db.sqls.must_equal []
  end

  it "should insert the correct rows into all tables when inserting" do
    Ceo.create(:num_managers=>3, :num_staff=>2, :name=>'E')
    sqls = @db.sqls
    sqls.length.must_equal 3
    sqls[0].must_match(/INSERT INTO employees \((name|kind), (name|kind)\) VALUES \('(E|Ceo)', '(E|Ceo)'\)/)
    sqls[1].must_match(/INSERT INTO managers \((num_staff|id), (num_staff|id)\) VALUES \([12], [12]\)/)
    sqls[2].must_match(/INSERT INTO executives \((num_managers|id), (num_managers|id)\) VALUES \([13], [13]\)/)
    end
    
  it "should insert the correct rows into all tables with a given primary key" do
    e = Ceo.new(:num_managers=>3, :num_staff=>2, :name=>'E')
    e.id = 2
    e.save
    sqls = @db.sqls
    sqls.length.must_equal 3
    sqls[0].must_match(/INSERT INTO employees \((name|kind|id), (name|kind|id), (name|kind|id)\) VALUES \(('E'|'Ceo'|2), ('E'|'Ceo'|2), ('E'|'Ceo'|2)\)/)
    sqls[1].must_match(/INSERT INTO managers \((num_staff|id), (num_staff|id)\) VALUES \(2, 2\)/)
    sqls[2].must_match(/INSERT INTO executives \((num_managers|id), (num_managers|id)\) VALUES \([23], [23]\)/)
  end

  it "should update the correct rows in all tables when updating" do
    Ceo.load(:id=>2).update(:num_managers=>3, :num_staff=>2, :name=>'E')
    @db.sqls.must_equal ["UPDATE employees SET name = 'E' WHERE (id = 2)", "UPDATE managers SET num_staff = 2 WHERE (id = 2)", "UPDATE executives SET num_managers = 3 WHERE (id = 2)"]
  end

  it "should handle many_to_one relationships correctly" do
    Manager.dataset._fetch = {:id=>3, :name=>'E', :kind=>'Ceo', :num_managers=>3}
    Staff.load(:manager_id=>3).manager.must_equal Ceo.load(:id=>3, :name=>'E', :kind=>'Ceo', :num_managers=>3)
    @db.sqls.must_equal ['SELECT employees.id, employees.name, employees.kind, managers.num_staff FROM employees INNER JOIN managers ON (managers.id = employees.id) WHERE (managers.id = 3) LIMIT 1']
  end
  
  it "should handle one_to_many relationships correctly" do
    Staff.dataset._fetch = {:id=>1, :name=>'S', :kind=>'Staff', :manager_id=>3}
    Ceo.load(:id=>3).staff_members.must_equal [Staff.load(:id=>1, :name=>'S', :kind=>'Staff', :manager_id=>3)]
    @db.sqls.must_equal ['SELECT employees.id, employees.name, employees.kind, staff.manager_id FROM employees INNER JOIN staff ON (staff.id = employees.id) WHERE (staff.manager_id = 3)']
  end
end

describe "class_table_inheritance plugin without sti_key" do
  before do
    @db = Sequel.mock(:autoid=>proc{|sql| 1})
    def @db.supports_schema_parsing?() true end
    def @db.schema(table, opts={})
      {:employees=>[[:id, {:primary_key=>true, :type=>:integer}], [:name, {:type=>:string}]],
       :managers=>[[:id, {:type=>:integer}], [:num_staff, {:type=>:integer}]],
       :executives=>[[:id, {:type=>:integer}], [:num_managers, {:type=>:integer}]],
       :staff=>[[:id, {:type=>:integer}], [:manager_id, {:type=>:integer}]],
       }[table.is_a?(Sequel::Dataset) ? table.first_source_table : table]
    end
    @db.extend_datasets do
      def columns
        {[:employees]=>[:id, :name],
         [:managers]=>[:id, :num_staff],
         [:executives]=>[:id, :num_managers],
         [:staff]=>[:id, :manager_id],
         [:employees, :managers]=>[:id, :name, :num_staff],
         [:employees, :managers, :executives]=>[:id, :name, :num_staff, :num_managers],
         [:employees, :staff]=>[:id, :name, :manager_id],
        }[opts[:from] + (opts[:join] || []).map{|x| x.table}]
      end
    end
    class ::Employee < Sequel::Model(@db)
      def _save_refresh; @values[:id] = 1 end
      def self.columns
        dataset.columns
      end
      plugin :class_table_inheritance, :table_map=>{:Staff=>:staff}
    end 
    class ::Manager < Employee
      one_to_many :staff_members, :class=>:Staff
    end 
    class ::Executive < Manager
    end 
    class ::Staff < Employee
      many_to_one :manager
    end 
    @ds = Employee.dataset
    @db.sqls
  end
  after do
    Object.send(:remove_const, :Executive)
    Object.send(:remove_const, :Manager)
    Object.send(:remove_const, :Staff)
    Object.send(:remove_const, :Employee)
  end

  it "should have simple_table = nil for all subclasses" do
    Manager.simple_table.must_equal nil
    Executive.simple_table.must_equal nil
    Staff.simple_table.must_equal nil
  end
  
  it "should have working row_proc if using set_dataset in subclass to remove columns" do
    Manager.set_dataset(Manager.dataset.select(*(Manager.columns - [:blah])))
    Manager.dataset._fetch = {:id=>1}
    Manager[1].must_equal Manager.load(:id=>1)
  end

  it "should use a joined dataset in subclasses" do
    Employee.dataset.sql.must_equal 'SELECT * FROM employees'
    Manager.dataset.sql.must_equal 'SELECT employees.id, employees.name, managers.num_staff FROM employees INNER JOIN managers ON (managers.id = employees.id)'
    Executive.dataset.sql.must_equal 'SELECT employees.id, employees.name, managers.num_staff, executives.num_managers FROM employees INNER JOIN managers ON (managers.id = employees.id) INNER JOIN executives ON (executives.id = managers.id)'
    Staff.dataset.sql.must_equal 'SELECT employees.id, employees.name, staff.manager_id FROM employees INNER JOIN staff ON (staff.id = employees.id)'
  end
  
  it "should return rows with the current class if cti_key is nil" do
    Employee.plugin(:class_table_inheritance)
    Employee.dataset._fetch = [{}]
    Employee.first.class.must_equal Employee
  end
  
  
  it "should include schema for columns for tables for ancestor classes" do
    Employee.db_schema.must_equal(:id=>{:primary_key=>true, :type=>:integer}, :name=>{:type=>:string})
    Manager.db_schema.must_equal(:id=>{:primary_key=>true, :type=>:integer}, :name=>{:type=>:string}, :num_staff=>{:type=>:integer})
    Executive.db_schema.must_equal(:id=>{:primary_key=>true, :type=>:integer}, :name=>{:type=>:string}, :num_staff=>{:type=>:integer}, :num_managers=>{:type=>:integer})
    Staff.db_schema.must_equal(:id=>{:primary_key=>true, :type=>:integer}, :name=>{:type=>:string}, :manager_id=>{:type=>:integer})
  end

  it "should use the correct primary key (which should have the same name in all subclasses)" do
    [Employee, Manager, Executive, Staff].each{|c| c.primary_key.must_equal :id}
  end

  it "should have table_name return the table name of the most specific table" do
    Employee.table_name.must_equal :employees
    Manager.table_name.must_equal :managers
    Executive.table_name.must_equal :executives
    Staff.table_name.must_equal :staff
  end

  it "should delete the correct rows from all tables when deleting" do
    Executive.load(:id=>1).delete
    @db.sqls.must_equal ["DELETE FROM executives WHERE (id = 1)", "DELETE FROM managers WHERE (id = 1)", "DELETE FROM employees WHERE (id = 1)"]
  end

  it "should not allow deletion of frozen object" do
    o = Executive.load(:id=>1)
    o.freeze
    proc{o.delete}.must_raise(Sequel::Error)
    @db.sqls.must_equal []
  end

  it "should insert the correct rows into all tables when inserting" do
    Executive.create(:num_managers=>3, :num_staff=>2, :name=>'E')
    sqls = @db.sqls
    sqls.length.must_equal 3
    sqls[0].must_match(/INSERT INTO employees \(name\) VALUES \('E'\)/)
    sqls[1].must_match(/INSERT INTO managers \((num_staff|id), (num_staff|id)\) VALUES \([12], [12]\)/)
    sqls[2].must_match(/INSERT INTO executives \((num_managers|id), (num_managers|id)\) VALUES \([13], [13]\)/)
    end
    
  it "should insert the correct rows into all tables with a given primary key" do
    e = Executive.new(:num_managers=>3, :num_staff=>2, :name=>'E')
    e.id = 2
    e.save
    sqls = @db.sqls
    sqls.length.must_equal 3
    sqls[0].must_match(/INSERT INTO employees \((name|id), (name|id)\) VALUES \(('E'|2), ('E'|2)\)/)
    sqls[1].must_match(/INSERT INTO managers \((num_staff|id), (num_staff|id)\) VALUES \(2, 2\)/)
    sqls[2].must_match(/INSERT INTO executives \((num_managers|id), (num_managers|id)\) VALUES \([23], [23]\)/)
  end

  it "should update the correct rows in all tables when updating" do
    Executive.load(:id=>2).update(:num_managers=>3, :num_staff=>2, :name=>'E')
    @db.sqls.must_equal ["UPDATE employees SET name = 'E' WHERE (id = 2)", "UPDATE managers SET num_staff = 2 WHERE (id = 2)", "UPDATE executives SET num_managers = 3 WHERE (id = 2)"]
  end

  it "should handle many_to_one relationships correctly" do
    Manager.dataset._fetch = {:id=>3, :name=>'E',  :num_staff=>3}
    Staff.load(:manager_id=>3).manager.must_equal Manager.load(:id=>3, :name=>'E', :num_staff=>3)
    @db.sqls.must_equal ['SELECT employees.id, employees.name, managers.num_staff FROM employees INNER JOIN managers ON (managers.id = employees.id) WHERE (managers.id = 3) LIMIT 1']
  end
  
  it "should handle one_to_many relationships correctly" do
    Staff.dataset._fetch = {:id=>1, :name=>'S', :manager_id=>3}
    Executive.load(:id=>3).staff_members.must_equal [Staff.load(:id=>1, :name=>'S', :manager_id=>3)]
    @db.sqls.must_equal ['SELECT employees.id, employees.name, staff.manager_id FROM employees INNER JOIN staff ON (staff.id = employees.id) WHERE (staff.manager_id = 3)']
  end
end
