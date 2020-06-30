require_relative "spec_helper"

describe Sequel::Inflections do
  before do
    @plurals, @singulars, @uncountables = Sequel.inflections.plurals.dup, Sequel.inflections.singulars.dup, Sequel.inflections.uncountables.dup
  end
  after do
    Sequel.inflections.plurals.replace(@plurals)
    Sequel.inflections.singulars.replace(@singulars)
    Sequel.inflections.uncountables.replace(@uncountables)
  end

  it "should be possible to clear the list of singulars, plurals, and uncountables" do
    Sequel.inflections.clear(:plurals)
    Sequel.inflections.plurals.must_equal []
    Sequel.inflections.plural('blah', 'blahs')
    Sequel.inflections.clear
    Sequel.inflections.plurals.must_equal []
    Sequel.inflections.singulars.must_equal []
    Sequel.inflections.uncountables.must_equal []
  end

  it "should be yielded and returned by Sequel.inflections" do
    Sequel.inflections{|i| i.must_equal Sequel::Inflections}.must_equal Sequel::Inflections
  end
end

describe Sequel::Inflections do
  include Sequel::Inflections

  it "#camelize should transform the word to CamelCase" do
    camelize("post").must_equal "Post"
    camelize("egg_and_hams").must_equal "EggAndHams"
    camelize("foo/bar").must_equal "Foo::Bar"
    camelize("foo/").must_equal "Foo::"
    camelize("foo//bar").must_equal "Foo::/bar"
    camelize("foo///bar").must_equal "Foo::/::Bar"

    s = "x".dup
    def s.camelize; "P" end
    camelize(s).must_equal "P"
  end

  it "#constantize should eval the string to get a constant" do
    constantize("String").must_equal String
    constantize("Sequel::Inflections").must_equal Sequel::Inflections
    proc{constantize("BKSDDF")}.must_raise NameError
    proc{constantize("++A++")}.must_raise NameError

    s = "x".dup
    def s.constantize; "P" end
    constantize(s).must_equal "P"
  end
  
  it "#demodulize should remove any preceding modules" do
    demodulize("String::Inflections::Blah").must_equal "Blah"
    demodulize("String::Inflections").must_equal "Inflections"
    demodulize("String").must_equal "String"

    s = "x".dup
    def s.demodulize; "P" end
    demodulize(s).must_equal "P"
  end
  
  it "#pluralize should transform words from singular to plural" do
    pluralize("sheep").must_equal "sheep"
    pluralize("post").must_equal "posts"
    pluralize("octopus").must_equal"octopuses"
    pluralize("the blue mailman").must_equal "the blue mailmen"
    pluralize("CamelOctopus").must_equal "CamelOctopuses"

    s = "x".dup
    def s.pluralize; "P" end
    pluralize(s).must_equal "P"
  end
  
  it "#singularize should transform words from plural to singular" do
    singularize("sheep").must_equal "sheep"
    singularize("posts").must_equal "post"
    singularize("octopuses").must_equal "octopus"
    singularize("the blue mailmen").must_equal "the blue mailman"
    singularize("CamelOctopuses").must_equal "CamelOctopus"

    s = "x".dup
    def s.singularize; "P" end
    singularize(s).must_equal "P"
  end
  
  it "#underscore should convert class name to underscored string" do
    underscore("Message").must_equal "message"
    underscore("Admin::Post").must_equal "admin/post"

    s = "x".dup
    def s.underscore; "P" end
    underscore(s).must_equal "P"
  end
end
