require_relative "spec_helper"

Sequel.extension :sqlite_json_ops

describe "Sequel::SQLite::JSONOp" do
  before do
    @db = Sequel.connect('mock://sqlite')
    @db.extend_datasets{def quote_identifiers?; false end}
    @j = Sequel.sqlite_json_op(:j)
    @l = proc{|o| @db.literal(o)}
  end

  it "#[]/#get should use the ->> operator" do
    @l[@j[1]].must_equal "(j ->> 1)"
    @l[@j.get('a')].must_equal "(j ->> 'a')"
  end

  it "#[]/#get should return a JSONOp" do
    @l[@j[1][2]].must_equal "((j ->> 1) ->> 2)"
    @l[@j.get('a').get('b')].must_equal "((j ->> 'a') ->> 'b')"
  end

  it "#array_length should use the json_array_length function" do
    @l[@j.array_length].must_equal "json_array_length(j)"
    @l[@j.array_length("$[1]")].must_equal "json_array_length(j, '$[1]')"
  end

  it "#array_length should return a numeric expression" do
    @l[@j.array_length + 1].must_equal "(json_array_length(j) + 1)"
  end

  it "#each should use the json_each function" do
    @l[@j.each].must_equal "json_each(j)"
    @l[@j.each("$[1]")].must_equal "json_each(j, '$[1]')"
  end

  it "#extract should use the json_extract function" do
    @l[@j.extract].must_equal "json_extract(j)"
    @l[@j.extract("$[1]")].must_equal "json_extract(j, '$[1]')"
  end

  it "#get_json should use the -> operator" do
    @l[@j.get_json(1)].must_equal "(j -> 1)"
  end

  it "#get_json should return a JSONOp" do
    @l[@j.get_json('a').get_json('b')].must_equal "((j -> 'a') -> 'b')"
  end

  it "#insert should use the json_insert function" do
    @l[@j.insert('$.a', 1)].must_equal "json_insert(j, '$.a', 1)"
    @l[@j.insert('$.a', 1, '$.b', 2)].must_equal "json_insert(j, '$.a', 1, '$.b', 2)"
  end

  it "#insert should return JSONOp" do
    @l[@j.insert('$.a', 1).insert('$.b', 2)].must_equal "json_insert(json_insert(j, '$.a', 1), '$.b', 2)"
  end

  it "#json/#minify should use the json function" do
    @l[@j.json].must_equal "json(j)"
    @l[@j.minify].must_equal "json(j)"
  end

  it "#json/#minify should return JSONOp" do
    @l[@j.json.minify].must_equal "json(json(j))"
    @l[@j.minify.json].must_equal "json(json(j))"
  end

  it "#patch should use the json_patch function" do
    @l[@j.patch('{"a": 1}')].must_equal "json_patch(j, '{\"a\": 1}')"
  end

  it "#patch should return JSONOp" do
    @l[@j.patch('{"a": 1}').patch('{"b": 2}')].must_equal "json_patch(json_patch(j, '{\"a\": 1}'), '{\"b\": 2}')"
  end

  it "#remove should use the json_remove function" do
    @l[@j.remove('$.a')].must_equal "json_remove(j, '$.a')"
    @l[@j.remove('$.a', '$[1]')].must_equal "json_remove(j, '$.a', '$[1]')"
  end

  it "#remove should return JSONOp" do
    @l[@j.remove('$.a').remove('$[1]')].must_equal "json_remove(json_remove(j, '$.a'), '$[1]')"
  end

  it "#replace should use the json_replace function" do
    @l[@j.replace('$.a', 1)].must_equal "json_replace(j, '$.a', 1)"
    @l[@j.replace('$.a', 1, '$.b', 2)].must_equal "json_replace(j, '$.a', 1, '$.b', 2)"
  end

  it "#replace should return JSONOp" do
    @l[@j.replace('$.a', 1).replace('$.b', 2)].must_equal "json_replace(json_replace(j, '$.a', 1), '$.b', 2)"
  end

  it "#set should use the json_set function" do
    @l[@j.set('$.a', 1)].must_equal "json_set(j, '$.a', 1)"
    @l[@j.set('$.a', 1, '$.b', 2)].must_equal "json_set(j, '$.a', 1, '$.b', 2)"
  end

  it "#set should return JSONOp" do
    @l[@j.set('$.a', 1).set('$.b', 2)].must_equal "json_set(json_set(j, '$.a', 1), '$.b', 2)"
  end

  it "#tree should use the json_tree function" do
    @l[@j.tree].must_equal "json_tree(j)"
    @l[@j.tree("$[1]")].must_equal "json_tree(j, '$[1]')"
  end

  it "#type/#typeof should use the json_type function" do
    @l[@j.type].must_equal "json_type(j)"
    @l[@j.typeof].must_equal "json_type(j)"
    @l[@j.type("$[1]")].must_equal "json_type(j, '$[1]')"
    @l[@j.typeof("$[1]")].must_equal "json_type(j, '$[1]')"
  end

  it "#type/#typeof should return a string expression" do
    @l[@j.type + '1'].must_equal "(json_type(j) || '1')"
    @l[@j.typeof('$.a') + '1'].must_equal "(json_type(j, '$.a') || '1')"
  end

  it "#valid should use the json_valid function" do
    @l[@j.valid].must_equal "json_valid(j)"
  end

  it "#valid should return a boolean expression" do
    @l[@j.valid & 1].must_equal "(json_valid(j) AND 1)"
  end

  it "Sequel.sqlite_json_op should wrap object in a JSONOp" do
    @l[Sequel.sqlite_json_op(:j).valid].must_equal "json_valid(j)"
    @l[Sequel.sqlite_json_op(Sequel.join([:j, :k])).valid].must_equal "json_valid((j || k))"
  end

  it "Sequel.sqlite_json_op return a JSONOp as-is" do
    v = Sequel.sqlite_json_op(:j)
    Sequel.sqlite_json_op(v).must_be_same_as v
  end

  it "SQL::GenericExpression#sqlite_json_op should wrap receiver in JSON op" do
    @l[Sequel.function(:j, :k).sqlite_json_op.valid].must_equal "json_valid(j(k))"
  end

  it "SQL::LiteralString#sqlite_json_op should wrap receiver in JSON op" do
    @l[Sequel.lit('j || k').sqlite_json_op.valid].must_equal "json_valid(j || k)"
  end
end
