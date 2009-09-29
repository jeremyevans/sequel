require File.join(File.dirname(__FILE__), "spec_helper")

describe "class_table_inheritance plugin" do
  before do
    @db = db = MODEL_DB.clone
    def db.schema(table, opts={})
      {:employees=>[[:id, {:primary_key=>true, :type=>:integer}], [:name, {:type=>:string}], [:kind, {:type=>:string}]],
       :managers=>[[:id, {:type=>:integer}], [:num_staff, {:type=>:integer}]],
       :executives=>[[:id, {:type=>:integer}], [:num_managers, {:type=>:integer}]],
       :staff=>[[:id, {:type=>:integer}], [:manager_id, {:type=>:integer}]],
       }[table]
     end
    def db.dataset(*args)
      ds = super(*args)
      def ds.columns
        {[:employees]=>[:id, :name, :kind],
         [:managers]=>[:id, :num_staff],
         [:executives]=>[:id, :num_managers],
         [:staff]=>[:id, :manager_id],
         [:employees, :managers]=>[:id, :name, :kind, :num_staff],
         [:employees, :managers, :executives]=>[:id, :name, :kind, :num_staff, :num_managers],
         [:employees, :staff]=>[:id, :name, :kind, :manager_id],
        }[opts[:from] + (opts[:join] || []).map{|x| x.table}]
      end
      def ds.insert(*args)
        db << insert_sql(*args)
        1
      end
      ds
    end
    class ::Employee < Sequel::Model(db)
      def _refresh(x); @values[:id] = 1 end
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
    class ::Staff < Employee
      many_to_one :manager
    end 
    @ds = Employee.dataset
    @db.reset
  end
  after do
    Object.send(:remove_const, :Executive)
    Object.send(:remove_const, :Manager)
    Object.send(:remove_const, :Staff)
    Object.send(:remove_const, :Employee)
  end

  specify "should have simple_table = nil" do
    Employee.simple_table.should == nil
    Manager.simple_table.should == nil
    Executive.simple_table.should == nil
    Staff.simple_table.should == nil
  end
  
  specify "should use a joined dataset in subclasses" do
    Employee.dataset.sql.should == 'SELECT * FROM employees'
    Manager.dataset.sql.should == 'SELECT * FROM employees INNER JOIN managers USING (id)'
    Executive.dataset.sql.should == 'SELECT * FROM employees INNER JOIN managers USING (id) INNER JOIN executives USING (id)'
    Staff.dataset.sql.should == 'SELECT * FROM employees INNER JOIN staff USING (id)'
  end
  
  it "should return rows with the correct class based on the polymorphic_key value" do
    def @ds.fetch_rows(sql)
      yield({:kind=>'Employee'})
      yield({:kind=>'Manager'})
      yield({:kind=>'Executive'})
      yield({:kind=>'Staff'})
    end 
    Employee.all.collect{|x| x.class}.should == [Employee, Manager, Executive, Staff]
  end 
  
  it "should return rows with the correct class based on the polymorphic_key value for subclasses" do
    ds = Manager.dataset
    def ds.fetch_rows(sql)
      yield({:kind=>'Manager'})
      yield({:kind=>'Executive'})
    end 
    Manager.all.collect{|x| x.class}.should == [Manager, Executive]
  end
  
  it "should return rows with the current class if cti_key is nil" do
    Employee.plugin(:class_table_inheritance)
    def @ds.fetch_rows(sql)
      yield({:kind=>'Employee'})
      yield({:kind=>'Manager'})
      yield({:kind=>'Executive'})
      yield({:kind=>'Staff'})
    end 
    Employee.all.collect{|x| x.class}.should == [Employee, Employee, Employee, Employee]
  end
  
  it "should return rows with the current class if cti_key is nil in subclasses" do
    Employee.plugin(:class_table_inheritance)
    Object.send(:remove_const, :Executive)
    Object.send(:remove_const, :Manager)
    class ::Manager < Employee
    end 
    class ::Executive < Manager
    end 
    ds = Manager.dataset
    def ds.fetch_rows(sql)
      yield({:kind=>'Manager'})
      yield({:kind=>'Executive'})
    end 
    Manager.all.collect{|x| x.class}.should == [Manager, Manager]
  end
  
  it "should fallback to the main class if the given class does not exist" do
    def @ds.fetch_rows(sql)
      yield({:kind=>'Employee'})
      yield({:kind=>'Manager'})
      yield({:kind=>'Blah'})
      yield({:kind=>'Staff'})
    end 
    Employee.all.collect{|x| x.class}.should == [Employee, Manager, Employee, Staff]
  end
  
  it "should fallback to the main class if the given class does not exist in subclasses" do
    ds = Manager.dataset
    def ds.fetch_rows(sql)
      yield({:kind=>'Manager'})
      yield({:kind=>'Executive'})
      yield({:kind=>'Blah'})
    end 
    Manager.all.collect{|x| x.class}.should == [Manager, Executive, Manager]
  end

  it "should add a before_create hook that sets the model class name for the key" do
    Employee.create
    @db.sqls.should == ["INSERT INTO employees (kind) VALUES ('Employee')"]
  end
  
  it "should add a before_create hook that sets the model class name for the key in subclasses" do
    Executive.create
    @db.sqls.should == ["INSERT INTO employees (kind) VALUES ('Executive')",
      "INSERT INTO managers (id) VALUES (1)",
      "INSERT INTO executives (id) VALUES (1)"]
  end

  it "should ignore existing cti_key value" do
    Employee.create(:kind=>'Manager')
    @db.sqls.should == ["INSERT INTO employees (kind) VALUES ('Employee')"]
  end
    
  it "should ignore existing cti_key value in subclasses" do
    Manager.create(:kind=>'Executive')
    @db.sqls.should == ["INSERT INTO employees (kind) VALUES ('Manager')",
      "INSERT INTO managers (id) VALUES (1)"]
  end

  it "should raise an error if attempting to create an anonymous subclass" do
    proc{Class.new(Manager)}.should raise_error(Sequel::Error)
  end

  it "should allow specifying a map of names to tables to override implicit mapping" do
    Manager.dataset.sql.should == 'SELECT * FROM employees INNER JOIN managers USING (id)'
    Staff.dataset.sql.should == 'SELECT * FROM employees INNER JOIN staff USING (id)'
  end

  it "should lazily load attributes for columns in subclass tables" do
    ds = Manager.dataset
    def ds.fetch_rows(sql)
      @db << sql
      yield({:id=>1, :name=>'J', :kind=>'Executive', :num_staff=>2})
    end
    m = Manager[1]
    @db.sqls.should == ['SELECT * FROM employees INNER JOIN managers USING (id) WHERE (id = 1) LIMIT 1']
    @db.reset
    ds = Executive.dataset
    def ds.fetch_rows(sql)
      @db << sql
      yield({:num_managers=>3})
    end
    m.num_managers.should == 3
    @db.sqls.should == ['SELECT num_managers FROM employees INNER JOIN managers USING (id) INNER JOIN executives USING (id) WHERE (id = 1) LIMIT 1']
    m.values.should == {:id=>1, :name=>'J', :kind=>'Executive', :num_staff=>2, :num_managers=>3}
  end

  it "should include schema for columns for tables for ancestor classes" do
    Employee.db_schema.should == {:id=>{:primary_key=>true, :type=>:integer}, :name=>{:type=>:string}, :kind=>{:type=>:string}}
    Manager.db_schema.should == {:id=>{:primary_key=>true, :type=>:integer}, :name=>{:type=>:string}, :kind=>{:type=>:string}, :num_staff=>{:type=>:integer}}
    Executive.db_schema.should == {:id=>{:primary_key=>true, :type=>:integer}, :name=>{:type=>:string}, :kind=>{:type=>:string}, :num_staff=>{:type=>:integer}, :num_managers=>{:type=>:integer}}
    Staff.db_schema.should == {:id=>{:primary_key=>true, :type=>:integer}, :name=>{:type=>:string}, :kind=>{:type=>:string}, :manager_id=>{:type=>:integer}}
  end

  it "should use the correct primary key (which should have the same name in all subclasses)" do
    [Employee, Manager, Executive, Staff].each{|c| c.primary_key.should == :id}
  end

  it "should have table_name return the table name of the most specific table" do
    Employee.table_name.should == :employees
    Manager.table_name.should == :managers
    Executive.table_name.should == :executives
    Staff.table_name.should == :staff
  end

  it "should delete the correct rows from all tables when deleting" do
    Executive.load(:id=>1).delete
    @db.sqls.should == ["DELETE FROM executives WHERE (id = 1)", "DELETE FROM managers WHERE (id = 1)", "DELETE FROM employees WHERE (id = 1)"]
  end

  it "should insert the correct rows into all tables when inserting" do
    Executive.create(:num_managers=>3, :num_staff=>2, :name=>'E')
    @db.sqls.length.should == 3
    @db.sqls[0].should =~ /INSERT INTO employees \((name|kind), (name|kind)\) VALUES \('(E|Executive)', '(E|Executive)'\)/
    @db.sqls[1].should =~ /INSERT INTO managers \((num_staff|id), (num_staff|id)\) VALUES \([12], [12]\)/
    @db.sqls[2].should =~ /INSERT INTO executives \((num_managers|id), (num_managers|id)\) VALUES \([13], [13]\)/
    end
    
  it "should insert the correct rows into all tables with a given primary key" do
    e = Executive.new(:num_managers=>3, :num_staff=>2, :name=>'E')
    e.id = 2
    e.save
    @db.sqls.length.should == 3
    @db.sqls[0].should =~ /INSERT INTO employees \((name|kind|id), (name|kind|id), (name|kind|id)\) VALUES \(('E'|'Executive'|2), ('E'|'Executive'|2), ('E'|'Executive'|2)\)/
    @db.sqls[1].should =~ /INSERT INTO managers \((num_staff|id), (num_staff|id)\) VALUES \(2, 2\)/
    @db.sqls[2].should =~ /INSERT INTO executives \((num_managers|id), (num_managers|id)\) VALUES \([23], [23]\)/
  end

  it "should update the correct rows in all tables when updating" do
    Executive.load(:id=>2).update(:num_managers=>3, :num_staff=>2, :name=>'E')
    @db.sqls.should == ["UPDATE employees SET name = 'E' WHERE (id = 2)", "UPDATE managers SET num_staff = 2 WHERE (id = 2)", "UPDATE executives SET num_managers = 3 WHERE (id = 2)"]
  end

  it "should handle many_to_one relationships correctly" do
    ds = Manager.dataset
    def ds.fetch_rows(sql)
      @db << sql
      yield({:id=>3, :name=>'E', :kind=>'Executive', :num_managers=>3})
    end
    Staff.load(:manager_id=>3).manager.should == Executive.load(:id=>3, :name=>'E', :kind=>'Executive', :num_managers=>3)
    @db.sqls.should == ['SELECT * FROM employees INNER JOIN managers USING (id) WHERE (managers.id = 3) LIMIT 1']
  end
  
  it "should handle one_to_many relationships correctly" do
    ds = Staff.dataset
    def ds.fetch_rows(sql)
      @db << sql
      yield({:id=>1, :name=>'S', :kind=>'Staff', :manager_id=>3})
    end
    Executive.load(:id=>3).staff_members.should == [Staff.load(:id=>1, :name=>'S', :kind=>'Staff', :manager_id=>3)]
    @db.sqls.should == ['SELECT * FROM employees INNER JOIN staff USING (id) WHERE (staff.manager_id = 3)']
  end
end
