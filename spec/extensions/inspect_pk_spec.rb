require_relative "spec_helper"

describe "inspect_pk plugin" do
  def inspect(vals={}, &block)
    Class.new(Sequel::Model) do
      def self.name; 'M' end
      plugin :inspect_pk
      set_dataset DB.dataset
      class_exec(&block) if block
      columns(*vals.keys)
      unrestrict_primary_key
    end.new(vals).inspect
  end

  it "should not include primary key value if model does not have primary key" do
    inspect(id: 1){no_primary_key}.must_equal "#<M @values=#{{:id=>1}.inspect}>"
  end

  it "should not include primary key value if model has scalar primary key and instance does not have primary key value" do
    inspect{set_primary_key :id}.must_equal "#<M @values={}>"
  end

  it "should not include primary key value if model instance has composite primary key and instance does not have values for all primary key components" do
    [{id1: 1, id2: nil}, {id1: nil, id2: 2}, {id1: nil, id2: nil}].each do |vals|
      inspect(vals){set_primary_key [:id1, :id2]}.must_equal "#<M @values=#{vals.inspect}>"
    end
  end

  it "should include primary value for scalar primary key if present" do
    inspect(id: 1){set_primary_key :id}.must_equal "#<M[1] @values=#{{id: 1}.inspect}>"
  end

  it "should include primary value for composite primary key if all fields present" do
    vals = {id1: 1, id2: 2}
    inspect(vals){set_primary_key [:id1, :id2]}.must_equal "#<M[[1, 2]] @values=#{vals.inspect}>"
  end

  it "should use inspect value of primary key" do
    inspect(id: "1"){}.must_equal "#<M[\"1\"] @values=#{{id: "1"}.inspect}>"
  end

  it "should use inspect_pk method to get inspect pk value" do
    inspect{private; def inspect_pk; "2" end}.must_equal "#<M[\"2\"] @values={}>"
  end
end
