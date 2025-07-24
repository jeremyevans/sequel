# frozen_string_literal: true

require_relative 'spec_helper'

describe "Sequel::Plugins::ClassTableInheritanceConstraintValidations" do
  before do
    @db = Sequel.mock
    def @db.schema(table, opts = {})
      {
        parents: [
          [:id, {primary_key: true, type: :integer}],
          [:column_a, {type: :string}]
        ],
        children: [
          [:id, {primary_key: false, type: :integer}], # foreign key to parents
          [:column_b, {type: :string}]
        ],
        grandchildren: [
          [:id, {primary_key: false, type: :integer}], # foreign key to children
          [:column_c, {type: :string}]
        ],
        siblings: [
          [:id, {primary_key: false, type: :integer}], # foreign key to parents
          [:column_d, {type: :string}]
        ]
      }[table]
    end
    def @db.supports_schema_parsing?
      true
    end

    db_constraint_validations = {}
    db_constraint_validations["parents"] = [{
      table: "parents",
      constraint_name: "foo",
      validation_type: "includes_str_array",
      column: "column_a",
      argument: "arg1,arg2",
      message: nil,
      allow_nil: true
    }]
    db_constraint_validations["children"] = [{
      table: "children",
      constraint_name: "bar",
      validation_type: "includes_str_array",
      column: "column_b",
      argument: "param1,param2",
      message: nil,
      allow_nil: true
    }]
    db_constraint_validations["siblings"] = [{
      table: "siblings",
      constraint_name: "qux",
      validation_type: "includes_str_array",
      column: "column_d",
      argument: "baz,quux",
      message: nil,
      allow_nil: true
    }]
    db_constraint_validations["grandchildren"] = [{
      table: "grandchildren",
      constraint_name: "baz",
      validation_type: "includes_str_array",
      column: "column_c",
      argument: "foo,bar",
      message: nil,
      allow_nil: true
    }]

    @db.instance_variable_set :@constraint_validations, db_constraint_validations

    class ::Parent < Sequel::Model(@db[:parents])
      plugin :class_table_inheritance, :key=>:kind, :table_map=>{:Child=>:children, :Grandchild=>:grandchildren, :Sibling=>:siblings}
      plugin :constraint_validations
      plugin :class_table_inheritance_constraint_validations
    end
    class ::Child < Parent; end
    class ::Grandchild < Child; end
    class ::Sibling < Parent; end
  end

  after do
    [:Sibling, :Grandchild, :Child, :Parent].each{|s| Object.send(:remove_const, s) if Object.const_defined?(s)}
  end

  it "includes constraint_validations from ancestor tables in class_table_inheritance table hierarchy" do
    # child has children and parent constraints
    Child.constraint_validations.must_include([
     :validates_includes,
      ["arg1", "arg2"],
      :column_a,
      {:allow_nil=>true, :from=>:values}
    ])
    Child.constraint_validations.must_include([
     :validates_includes,
      ["param1", "param2"],
      :column_b,
      {:allow_nil=>true, :from=>:values}
    ])

    # granchild has grandchildren, children and parent constraints
    Grandchild.constraint_validations.must_include([
     :validates_includes,
      ["arg1", "arg2"],
      :column_a,
      {:allow_nil=>true, :from=>:values}
    ])

    Grandchild.constraint_validations.must_include([
     :validates_includes,
      ["param1", "param2"],
      :column_b,
      {:allow_nil=>true, :from=>:values}
    ])

    Grandchild.constraint_validations.must_include([
     :validates_includes,
      ["foo", "bar"],
      :column_c,
      {:allow_nil=>true, :from=>:values}
    ])
  end

  it "does not include constraint_validations from singble tables in class_table_inheritance table hierarchy" do
    # siblling does *not* have constraints from childen or grandchildren
    Sibling.constraint_validations.wont_include([
     :validates_includes,
      ["param1", "param2"],
      :column_b,
      {:allow_nil=>true, :from=>:values}
    ])

    Sibling.constraint_validations.wont_include([
     :validates_includes,
      ["foo", "bar"],
      :column_c,
      {:allow_nil=>true, :from=>:values}
    ])
  end

  it "raises if attempting to load plugin into model without plugins it depends on" do
    c = Class.new(Sequel::Model)
    proc{c.plugin :class_table_inheritance_constraint_validations}.must_raise Sequel::Error

    c = Class.new(Sequel::Model)
    c.plugin :constraint_validations
    proc{c.plugin :class_table_inheritance_constraint_validations}.must_raise Sequel::Error

    c = Class.new(Sequel::Model(:parents))
    c.plugin :class_table_inheritance, :key=>:kind, :table_map=>{:Child=>:children, :Grandchild=>:grandchildren, :Sibling=>:siblings}
    proc{c.plugin :class_table_inheritance_constraint_validations}.must_raise Sequel::Error
  end
end
