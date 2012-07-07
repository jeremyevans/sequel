require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper')

describe String do
  it "#camelize and #camelcase should transform the word to CamelCase" do
    "egg_and_hams".camelize.should == "EggAndHams"
    "egg_and_hams".camelize(false).should == "eggAndHams"
    "post".camelize.should == "Post"
    "post".camelcase.should == "Post"
  end

  it "#constantize should eval the string to get a constant" do
    "String".constantize.should == String
    "String::Inflections".constantize.should == String::Inflections
    proc{"BKSDDF".constantize}.should raise_error
    proc{"++A++".constantize}.should raise_error
  end

  it "#dasherize should transform underscores to dashes" do
    "egg_and_hams".dasherize.should == "egg-and-hams"
    "post".dasherize.should == "post"
  end

  it "#demodulize should remove any preceding modules" do
    "String::Inflections::Blah".demodulize.should == "Blah"
    "String::Inflections".demodulize.should == "Inflections"
    "String".demodulize.should == "String"
  end

  it "#humanize should remove _i, transform underscore to spaces, and capitalize" do
    "egg_and_hams".humanize.should == "Egg and hams"
    "post".humanize.should == "Post"
    "post_id".humanize.should == "Post"
  end

  it "#titleize and #titlecase should underscore, humanize, and capitalize all words" do
    "egg-and: hams".titleize.should == "Egg And: Hams"
    "post".titleize.should == "Post"
    "post".titlecase.should == "Post"
  end

  it "#underscore should add underscores between CamelCased words, change :: to / and - to _, and downcase" do
    "EggAndHams".underscore.should == "egg_and_hams"
    "EGGAndHams".underscore.should == "egg_and_hams"
    "Egg::And::Hams".underscore.should == "egg/and/hams"
    "post".underscore.should == "post"
    "post-id".underscore.should == "post_id"
  end

  it "#pluralize should transform words from singular to plural" do
    "post".pluralize.should == "posts"
    "octopus".pluralize.should =="octopuses"
    "the blue mailman".pluralize.should == "the blue mailmen"
    "CamelOctopus".pluralize.should == "CamelOctopuses"
  end

  it "#singularize should transform words from plural to singular" do
    "posts".singularize.should == "post"
    "octopuses".singularize.should == "octopus"
    "the blue mailmen".singularize.should == "the blue mailman"
    "CamelOctopuses".singularize.should == "CamelOctopus"
  end

  it "#tableize should transform class names to table names" do
    "RawScaledScorer".tableize.should == "raw_scaled_scorers"
    "egg_and_ham".tableize.should == "egg_and_hams"
    "fancyCategory".tableize.should == "fancy_categories"
  end

  it "#classify should tranform table names to class names" do
    "egg_and_hams".classify.should == "EggAndHam"
    "post".classify.should == "Post"
  end

  it "#foreign_key should create a foreign key name from a class name" do
    "Message".foreign_key.should == "message_id"
    "Message".foreign_key(false).should == "messageid"
    "Admin::Post".foreign_key.should == "post_id"
  end
end

describe String::Inflections do
  before do
    @plurals, @singulars, @uncountables = String.inflections.plurals.dup, String.inflections.singulars.dup, String.inflections.uncountables.dup
  end
  after do
    String.inflections.plurals.replace(@plurals)
    String.inflections.singulars.replace(@singulars)
    String.inflections.uncountables.replace(@uncountables)
  end

  it "should be possible to clear the list of singulars, plurals, and uncountables" do
    String.inflections.clear(:plurals)
    String.inflections.plurals.should == []
    String.inflections.plural('blah', 'blahs')
    String.inflections.clear
    String.inflections.plurals.should == []
    String.inflections.singulars.should == []
    String.inflections.uncountables.should == []
  end

  it "should be able to specify new inflection rules" do
    String.inflections do |i|
      i.plural(/xx$/i, 'xxx')
      i.singular(/ttt$/i, 'tt')
      i.irregular('yy', 'yyy')
      i.uncountable(%w'zz')
    end
    'roxx'.pluralize.should == 'roxxx'
    'rottt'.singularize.should == 'rott'
    'yy'.pluralize.should == 'yyy'
    'yyy'.singularize.should == 'yy'
    'zz'.pluralize.should == 'zz'
    'zz'.singularize.should == 'zz'
  end

  it "should be yielded and returned by String.inflections" do
    String.inflections{|i| i.should == String::Inflections}.should == String::Inflections
  end
end

describe 'Default inflections' do
  it "should support the default inflection rules" do
    {
      :test=>:tests,
      :ax=>:axes,
      :testis=>:testes,
      :octopus=>:octopuses,
      :virus=>:viruses,
      :alias=>:aliases,
      :status=>:statuses,
      :bus=>:buses,
      :buffalo=>:buffaloes,
      :tomato=>:tomatoes,
      :datum=>:data,
      :bacterium=>:bacteria,
      :analysis=>:analyses,
      :basis=>:bases,
      :diagnosis=>:diagnoses,
      :parenthesis=>:parentheses,
      :prognosis=>:prognoses,
      :synopsis=>:synopses,
      :thesis=>:theses,
      :wife=>:wives,
      :giraffe=>:giraffes,
      :self=>:selves,
      :dwarf=>:dwarves,
      :hive=>:hives,
      :fly=>:flies,
      :buy=>:buys,
      :soliloquy=>:soliloquies,
      :day=>:days,
      :attorney=>:attorneys,
      :boy=>:boys,
      :hoax=>:hoaxes,
      :lunch=>:lunches,
      :princess=>:princesses,
      :matrix=>:matrices,
      :vertex=>:vertices,
      :index=>:indices,
      :mouse=>:mice,
      :louse=>:lice,
      :quiz=>:quizzes,
      :motive=>:motives,
      :movie=>:movies,
      :series=>:series,
      :crisis=>:crises,
      :person=>:people,
      :man=>:men,
      :woman=>:women,
      :child=>:children,
      :sex=>:sexes,
      :move=>:moves
    }.each do |k, v|
      k.to_s.pluralize.should == v.to_s
      v.to_s.singularize.should == k.to_s
    end
    [:equipment, :information, :rice, :money, :species, :series, :fish, :sheep, :news].each do |a|
      a.to_s.pluralize.should == a.to_s.singularize
    end
  end
end
