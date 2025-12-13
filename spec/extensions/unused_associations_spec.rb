require_relative "spec_helper"

describe "unused_associations plugin" do
  require 'rbconfig'
  require 'json'

  ua_file = "spec/tua-#{$$}.json"
  uac_file = "spec/tua-coverage-#{$$}.json"

  after do
    File.delete(ua_file) if File.file?(ua_file)
    File.delete(uac_file) if File.file?(uac_file)
  end

  def check(code, env={})
    ruby = RbConfig.ruby
    runner = File.expand_path('../files/unused_associations/run_tua.rb', File.dirname(__FILE__))
    input_read, input_write = IO.pipe
    output_read, output_write = IO.pipe
    Process.spawn(env, ruby, runner, :in=>input_read, :out=>output_write)
    input_write.write(code)
    input_write.close
    output_write.close
    result = output_read.read
    input_read.close
    output_read.close
    res = Sequel.parse_json(result)
    raise res if res.is_a?(String)
    res
  end

  it "should correctly determine which associations are unused or partially used" do
    ua, uao = check("TUA::O.a1")
    ua.must_equal [["TUA", "a2s"], ["TUA", "a3"], ["TUA", "a4s"], ["TUA", "a5"], ["TUA", "a6s"], ["TUA::SC", "a7"]]
    uao.must_equal [["TUA", "a1", {"read_only"=>true, "no_dataset_method"=>true}]]

    ua, uao = check(<<-RUBY)
      obj = TUA::O
      obj.a1
      obj.a1_dataset
      obj.a1 = nil

      obj.a2s_dataset
      obj.remove_all_a2s
      obj.a3 = nil
      obj.add_a4(obj)
      obj.remove_a6(obj)
      
      obj = TUA::SC::O
      obj.a7
      obj.a7_dataset
    RUBY
    ua.must_equal [["TUA", "a5"]]
    uao.must_equal [
      ["TUA", "a2s", {"no_association_method"=>true, "adder"=>nil, "remover"=>nil}],
      ["TUA", "a3", {"no_dataset_method"=>true, "no_association_method"=>true}],
      ["TUA", "a4s", {"no_dataset_method"=>true, "no_association_method"=>true, "remover"=>nil, "clearer"=>nil}],
      ["TUA", "a6s", {"no_association_method"=>true, "adder"=>nil, "clearer"=>nil}],
      ["TUA::SC", "a7", {"read_only"=>true}]]
  end
  
  it "should use association reflection access to determine which associations are used" do
    ua, uao = check("TUA.association_reflection(:a1); TUA::O.a2s", 'A1_IS_USED'=>'1', 'A5_IS_USED'=>'1')
    ua.must_equal [["TUA", "a3"], ["TUA", "a4s"], ["TUA", "a6s"], ["TUA::SC", "a7"]]
    uao.must_equal [["TUA", "a2s", {"read_only"=>true, "no_dataset_method"=>true}]]
  end
  
  it "should not report associations as unused if they have an :is_used option" do
    ua, uao = check("TUA.association_reflection(:a1); TUA::O.a2s")
    ua.must_equal [["TUA", "a3"], ["TUA", "a4s"], ["TUA", "a5"], ["TUA", "a6s"], ["TUA::SC", "a7"]]
    uao.must_equal [
      ["TUA", "a1", {"read_only"=>true, "no_dataset_method"=>true, "no_association_method"=>true}],
      ["TUA", "a2s", {"read_only"=>true, "no_dataset_method"=>true}]]
  end
  
  it "should work with :file and :coverage_file plugin options" do
    ua, uao = check("TUA::O.a1", 'PLUGIN_OPTS'=>Sequel.object_to_json(:coverage_file=>uac_file, :file=>ua_file))
    ua.must_equal [["TUA", "a2s"], ["TUA", "a3"], ["TUA", "a4s"], ["TUA", "a5"], ["TUA", "a6s"], ["TUA::SC", "a7"]]
    uao.must_equal [["TUA", "a1", {"read_only"=>true, "no_dataset_method"=>true}]]
    Sequel.parse_json(File.binread(ua_file)).must_be_kind_of(Hash)
    File.file?(uac_file).must_equal false
  end

  it "should work without arguments when using :file and :coverage_file plugin options" do
    ua, uao = check("TUA::O.a1", 'PLUGIN_OPTS'=>Sequel.object_to_json(:coverage_file=>uac_file, :file=>ua_file), 'NO_COVERAGE_RESULT'=>'1', 'NO_COVERAGE_DATA'=>'1', 'NO_DATA'=>'1')
    ua.must_equal [["TUA", "a2s"], ["TUA", "a3"], ["TUA", "a4s"], ["TUA", "a5"], ["TUA", "a6s"], ["TUA::SC", "a7"]]
    uao.must_equal [["TUA", "a1", {"read_only"=>true, "no_dataset_method"=>true}]]
    Sequel.parse_json(File.binread(ua_file)).must_be_kind_of(Hash)
    File.file?(uac_file).must_equal false
  end

  it "should be able to combine information from multiple coverage runs" do
    ua, uao = check("TUA::O.a1", 'KEEP_COVERAGE'=>'1', 'PLUGIN_OPTS'=>Sequel.object_to_json(:coverage_file=>uac_file, :file=>ua_file))
    ua.must_equal [["TUA", "a2s"], ["TUA", "a3"], ["TUA", "a4s"], ["TUA", "a5"], ["TUA", "a6s"], ["TUA::SC", "a7"]]
    uao.must_equal [["TUA", "a1", {"read_only"=>true, "no_dataset_method"=>true}]]
    Sequel.parse_json(File.binread(ua_file)).must_be_kind_of(Hash)
    Sequel.parse_json(File.binread(uac_file)).must_be_kind_of(Hash)

    ua, uao = check("TUA::O.a2s", 'KEEP_COVERAGE'=>'1', 'PLUGIN_OPTS'=>Sequel.object_to_json(:coverage_file=>uac_file, :file=>ua_file))
    ua.must_equal [["TUA", "a3"], ["TUA", "a4s"], ["TUA", "a5"], ["TUA", "a6s"], ["TUA::SC", "a7"]]
    uao.must_equal [
      ["TUA", "a1", {"read_only"=>true, "no_dataset_method"=>true}],
      ["TUA", "a2s", {"read_only"=>true, "no_dataset_method"=>true}]]
  end

  it "should respect association_reflection information from multiple coverage runs" do
    check("", 'KEEP_COVERAGE'=>'1', 'PLUGIN_OPTS'=>Sequel.object_to_json(:coverage_file=>uac_file, :file=>ua_file))
    ua, uao = check("TUA.association_reflection(:a1); TUA::O.a2s", 'KEEP_COVERAGE'=>'1', 'PLUGIN_OPTS'=>Sequel.object_to_json(:coverage_file=>uac_file, :file=>ua_file))

    ua.must_equal [["TUA", "a3"], ["TUA", "a4s"], ["TUA", "a5"], ["TUA", "a6s"], ["TUA::SC", "a7"]]
    uao.must_equal [
      ["TUA", "a1", {"read_only"=>true, "no_dataset_method"=>true, "no_association_method"=>true}],
      ["TUA", "a2s", {"read_only"=>true, "no_dataset_method"=>true}]]
  end

  it "should not define unused associations when using :modify_associations and :file plugin options" do
    check(<<-RUBY, 'PLUGIN_OPTS'=>Sequel.object_to_json(:file=>ua_file), 'NO_DATA'=>'1')
      obj = TUA::O
      obj.a1
      obj.a1_dataset
      obj.a1 = nil

      obj.a2s_dataset
      obj.remove_all_a2s
      obj.a3 = nil
      obj.add_a4(obj)
      obj.remove_a6(obj)
    RUBY

    assocs, meths = check(<<-RUBY, 'PLUGIN_OPTS'=>Sequel.object_to_json(:modify_associations=>true, :file=>ua_file), 'NO_COVERAGE_RESULT'=>'1')
      print Sequel.object_to_json([TUA.associations.sort, TUA.instance_methods])
      exit
    RUBY
    assocs.must_equal %w'a1 a2s a3 a4s a6s'
    %w'a1 a1_dataset a1= a2s_dataset remove_all_a2s a3= add_a4 a6s_dataset remove_a6'.each do |meth|
      meths.must_include meth
    end
    %w'a2s add_a2 remove_a2 a3 a3_dataset a4s a4s_dataset remove_a4 remove_all_a4s a5 a5_dataset a5= a6s add_a6 remove_all_a6s'.each do |meth|
      meths.wont_include meth
    end
  end

  it "should not define unused associations when using :modify_associations and :unused_associations_data options" do
    check(<<-RUBY, 'PLUGIN_OPTS'=>Sequel.object_to_json(:coverage_file=>uac_file, :file=>ua_file), 'NO_COVERAGE_DATA'=>'1')
      obj = TUA::O
      obj.a1
      obj.a1_dataset
      obj.a1 = nil

      obj.a2s_dataset
      obj.remove_all_a2s
      obj.a3 = nil
      obj.add_a4(obj)
      obj.remove_a6(obj)
    RUBY

    assocs, meths = check(<<-RUBY, 'PLUGIN_OPTS'=>Sequel.object_to_json(:modify_associations=>true, :unused_associations_data=>Sequel.parse_json(File.binread(ua_file))))
      print Sequel.object_to_json([TUA.associations.sort, TUA.instance_methods])
      exit
    RUBY
    assocs.must_equal %w'a1 a2s a3 a4s a6s'
    %w'a1 a1_dataset a1= a2s_dataset remove_all_a2s a3= add_a4 a6s_dataset remove_a6'.each do |meth|
      meths.must_include meth
    end
    %w'a2s add_a2 remove_a2 a3 a3_dataset a4s a4s_dataset remove_a4 remove_all_a4s a5 a5_dataset a5= a6s add_a6 remove_all_a6s'.each do |meth|
      meths.wont_include meth
    end
  end

  it "should respect :is_used association option when modifying associations" do
    check(<<-RUBY, 'PLUGIN_OPTS'=>Sequel.object_to_json(:file=>ua_file), 'NO_DATA'=>'1')
      obj = TUA::O
      obj.a1
      obj.a1_dataset
      obj.a1 = nil

      obj.a2s_dataset
      obj.remove_all_a2s
      obj.a3 = nil
      obj.add_a4(obj)
      obj.remove_a6(obj)
    RUBY

    assocs, meths = check(<<-RUBY, 'PLUGIN_OPTS'=>Sequel.object_to_json(:modify_associations=>true, :file=>ua_file), 'NO_COVERAGE_RESULT'=>'1', 'A5_IS_USED'=>'1', 'A6S_IS_USED'=>'1')
      print Sequel.object_to_json([TUA.associations.sort, TUA.instance_methods])
      exit
    RUBY
    assocs.must_equal %w'a1 a2s a3 a4s a5 a6s'
    %w'a1 a1_dataset a1= a2s_dataset remove_all_a2s a3= add_a4 a5 a5_dataset a5= a6s a6s_dataset add_a6 remove_a6 remove_all_a6s'.each do |meth|
      meths.must_include meth
    end
    %w'a2s add_a2 remove_a2 a3 a3_dataset a4s a4s_dataset remove_a4 remove_all_a4s'.each do |meth|
      meths.wont_include meth
    end
  end

  it "should skip associations that use :methods_module" do
    ua, uao = check("TUA::O.a1", 'A4S_METHODS_MODULE'=>'1')
    ua.must_equal [["TUA", "a2s"], ["TUA", "a3"], ["TUA", "a5"], ["TUA", "a6s"], ["TUA::SC", "a7"]]
    uao.must_equal [["TUA", "a1", {"read_only"=>true, "no_dataset_method"=>true}]]
  end

  it "should ignore association modification methods for read_only associations" do
    ua, uao = check("TUA::O.a4s", 'A4S_READ_ONLY'=>'1')
    ua.must_equal [["TUA", "a1"], ["TUA", "a2s"], ["TUA", "a3"], ["TUA", "a5"], ["TUA", "a6s"], ["TUA::SC", "a7"]]
    uao.must_equal [["TUA", "a4s", {"no_dataset_method"=>true}]]
  end

  it "should ignore missing association modification methods" do
    ua, uao = check("nil", 'A4S_NO_METHODS'=>'1')
    ua.must_equal [["TUA", "a1"], ["TUA", "a2s"], ["TUA", "a3"], ["TUA", "a5"], ["TUA", "a6s"], ["TUA::SC", "a7"]]
    uao.must_equal []
  end

end if RUBY_VERSION >= '2.5' && RUBY_ENGINE == 'ruby'
