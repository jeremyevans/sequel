# Add inflection methods to String, which allows the easy transformation of
# words from singular to plural,class names to table names, modularized class
# names to ones without, and class names to foreign keys.

class String
  # This module acts as a singleton returned/yielded by String.inflections,
  # which is used to override or specify additional inflection rules. Examples:
  #
  #   String.inflections do |inflect|
  #     inflect.plural /^(ox)$/i, '\1\2en'
  #     inflect.singular /^(ox)en/i, '\1'
  #
  #     inflect.irregular 'octopus', 'octopi'
  #
  #     inflect.uncountable "equipment"
  #   end
  #
  # New rules are added at the top. So in the example above, the irregular rule for octopus will now be the first of the
  # pluralization and singularization rules that is runs. This guarantees that your rules run before any of the rules that may
  # already have been loaded.
  module Inflections
    @plurals, @singulars, @uncountables = [], [], []

    class << self
      attr_reader :plurals, :singulars, :uncountables
    end

    # Clears the loaded inflections within a given scope (default is :all). Give the scope as a symbol of the inflection type,
    # the options are: :plurals, :singulars, :uncountables
    #
    # Examples:
    #   clear :all
    #   clear :plurals
    def self.clear(scope = :all)
      Sequel::Inflections.clear(scope)
      case scope
      when :all
        @plurals, @singulars, @uncountables = [], [], []
      else
        instance_variable_set("@#{scope}", [])
      end
    end

    # Specifies a new irregular that applies to both pluralization and singularization at the same time. This can only be used
    # for strings, not regular expressions. You simply pass the irregular in singular and plural form.
    #
    # Examples:
    #   irregular 'octopus', 'octopi'
    #   irregular 'person', 'people'
    def self.irregular(singular, plural)
      Sequel::Inflections.irregular(singular, plural)
      plural(Regexp.new("(#{singular[0,1]})#{singular[1..-1]}$", "i"), '\1' + plural[1..-1])
      singular(Regexp.new("(#{plural[0,1]})#{plural[1..-1]}$", "i"), '\1' + singular[1..-1])
    end

    # Specifies a new pluralization rule and its replacement. The rule can either be a string or a regular expression.
    # The replacement should always be a string that may include references to the matched data from the rule.
    #
    # Example:
    #   plural(/(x|ch|ss|sh)$/i, '\1es')
    def self.plural(rule, replacement)
      Sequel::Inflections.plural(rule, replacement)
      @plurals.insert(0, [rule, replacement])
    end

    # Specifies a new singularization rule and its replacement. The rule can either be a string or a regular expression.
    # The replacement should always be a string that may include references to the matched data from the rule.
    #
    # Example:
    #   singular(/([^aeiouy]|qu)ies$/i, '\1y') 
    def self.singular(rule, replacement)
      Sequel::Inflections.singular(rule, replacement)
      @singulars.insert(0, [rule, replacement])
    end

    # Add uncountable words that shouldn't be attempted inflected.
    #
    # Examples:
    #   uncountable "money"
    #   uncountable "money", "information"
    #   uncountable %w( money information rice )
    def self.uncountable(*words)
      Sequel::Inflections.uncountable(*words)
      (@uncountables << words).flatten!
    end

    # Setup the default inflections
    plural(/$/, 's')
    plural(/s$/i, 's')
    plural(/(ax|test)is$/i, '\1es')
    plural(/(octop|vir)us$/i, '\1i')
    plural(/(alias|status)$/i, '\1es')
    plural(/(bu)s$/i, '\1ses')
    plural(/(buffal|tomat)o$/i, '\1oes')
    plural(/([ti])um$/i, '\1a')
    plural(/sis$/i, 'ses')
    plural(/(?:([^f])fe|([lr])f)$/i, '\1\2ves')
    plural(/(hive)$/i, '\1s')
    plural(/([^aeiouy]|qu)y$/i, '\1ies')
    plural(/(x|ch|ss|sh)$/i, '\1es')
    plural(/(matr|vert|ind)ix|ex$/i, '\1ices')
    plural(/([m|l])ouse$/i, '\1ice')
    plural(/^(ox)$/i, '\1en')
    plural(/(quiz)$/i, '\1zes')

    singular(/s$/i, '')
    singular(/(n)ews$/i, '\1ews')
    singular(/([ti])a$/i, '\1um')
    singular(/((a)naly|(b)a|(d)iagno|(p)arenthe|(p)rogno|(s)ynop|(t)he)ses$/i, '\1\2sis')
    singular(/(^analy)ses$/i, '\1sis')
    singular(/([^f])ves$/i, '\1fe')
    singular(/(hive)s$/i, '\1')
    singular(/(tive)s$/i, '\1')
    singular(/([lr])ves$/i, '\1f')
    singular(/([^aeiouy]|qu)ies$/i, '\1y')
    singular(/(s)eries$/i, '\1eries')
    singular(/(m)ovies$/i, '\1ovie')
    singular(/(x|ch|ss|sh)es$/i, '\1')
    singular(/([m|l])ice$/i, '\1ouse')
    singular(/(bus)es$/i, '\1')
    singular(/(o)es$/i, '\1')
    singular(/(shoe)s$/i, '\1')
    singular(/(cris|ax|test)es$/i, '\1is')
    singular(/(octop|vir)i$/i, '\1us')
    singular(/(alias|status)es$/i, '\1')
    singular(/^(ox)en/i, '\1')
    singular(/(vert|ind)ices$/i, '\1ex')
    singular(/(matr)ices$/i, '\1ix')
    singular(/(quiz)zes$/i, '\1')

    irregular('person', 'people')
    irregular('man', 'men')
    irregular('child', 'children')
    irregular('sex', 'sexes')
    irregular('move', 'moves')

    uncountable(%w(equipment information rice money species series fish sheep))
  end

  # Yield the Inflections module if a block is given, and return
  # the Inflections module.
  def self.inflections
    yield Inflections if block_given?
    Inflections
  end

  # By default, camelize converts the string to UpperCamelCase. If the argument to camelize
  # is set to :lower then camelize produces lowerCamelCase.
  #
  # camelize will also convert '/' to '::' which is useful for converting paths to namespaces
  #
  # Examples
  #   "active_record".camelize #=> "ActiveRecord"
  #   "active_record".camelize(:lower) #=> "activeRecord"
  #   "active_record/errors".camelize #=> "ActiveRecord::Errors"
  #   "active_record/errors".camelize(:lower) #=> "activeRecord::Errors"
  def camelize(first_letter_in_uppercase = :upper)
    s = gsub(/\/(.?)/){|x| "::#{x[-1..-1].upcase unless x == '/'}"}.gsub(/(^|_)(.)/){|x| x[-1..-1].upcase}
    s[0...1] = s[0...1].downcase unless first_letter_in_uppercase == :upper
    s
  end
  alias_method :camelcase, :camelize

  # Singularizes and camelizes the string.  Also strips out all characters preceding
  # and including a period (".").
  #
  # Examples
  #   "egg_and_hams".classify #=> "EggAndHam"
  #   "post".classify #=> "Post"
  #   "schema.post".classify #=> "Post"
  def classify
    sub(/.*\./, '').singularize.camelize
  end

  # Constantize tries to find a declared constant with the name specified
  # in the string. It raises a NameError when the name is not in CamelCase
  # or is not initialized.
  #
  # Examples
  #   "Module".constantize #=> Module
  #   "Class".constantize #=> Class
  def constantize
    raise(NameError, "#{inspect} is not a valid constant name!") unless m = /\A(?:::)?([A-Z]\w*(?:::[A-Z]\w*)*)\z/.match(self)
    Object.module_eval("::#{m[1]}", __FILE__, __LINE__)
  end

  # Replaces underscores with dashes in the string.
  #
  # Example
  #   "puni_puni".dasherize #=> "puni-puni"
  def dasherize
    gsub(/_/, '-')
  end

  # Removes the module part from the expression in the string
  #
  # Examples
  #   "ActiveRecord::CoreExtensions::String::Inflections".demodulize #=> "Inflections"
  #   "Inflections".demodulize #=> "Inflections"
  def demodulize
    gsub(/^.*::/, '')
  end

  # Creates a foreign key name from a class name.
  # +use_underscore+ sets whether the method should put '_' between the name and 'id'.
  #
  # Examples
  #   "Message".foreign_key #=> "message_id"
  #   "Message".foreign_key(false) #=> "messageid"
  #   "Admin::Post".foreign_key #=> "post_id"
  def foreign_key(use_underscore = true)
    "#{demodulize.underscore}#{'_' if use_underscore}id"
  end

  # Capitalizes the first word and turns underscores into spaces and strips _id.
  # Like titleize, this is meant for creating pretty output.
  #
  # Examples
  #   "employee_salary" #=> "Employee salary"
  #   "author_id" #=> "Author"
  def humanize
    gsub(/_id$/, "").gsub(/_/, " ").capitalize
  end

  # Returns the plural form of the word in the string.
  #
  # Examples
  #   "post".pluralize #=> "posts"
  #   "octopus".pluralize #=> "octopi"
  #   "sheep".pluralize #=> "sheep"
  #   "words".pluralize #=> "words"
  #   "the blue mailman".pluralize #=> "the blue mailmen"
  #   "CamelOctopus".pluralize #=> "CamelOctopi"
  def pluralize
    result = dup
    Inflections.plurals.each{|(rule, replacement)| break if result.gsub!(rule, replacement)} unless Inflections.uncountables.include?(downcase)
    result
  end

  # The reverse of pluralize, returns the singular form of a word in a string.
  #
  # Examples
  #   "posts".singularize #=> "post"
  #   "octopi".singularize #=> "octopus"
  #   "sheep".singluarize #=> "sheep"
  #   "word".singluarize #=> "word"
  #   "the blue mailmen".singularize #=> "the blue mailman"
  #   "CamelOctopi".singularize #=> "CamelOctopus"
  def singularize
    result = dup
    Inflections.singulars.each{|(rule, replacement)| break if result.gsub!(rule, replacement)} unless Inflections.uncountables.include?(downcase)
    result
  end

  # Underscores and pluralizes the string.
  #
  # Examples
  #   "RawScaledScorer".tableize #=> "raw_scaled_scorers"
  #   "egg_and_ham".tableize #=> "egg_and_hams"
  #   "fancyCategory".tableize #=> "fancy_categories"
  def tableize
    underscore.pluralize
  end

  # Capitalizes all the words and replaces some characters in the string to create
  # a nicer looking title. Titleize is meant for creating pretty output.
  #
  # titleize is also aliased as as titlecase
  #
  # Examples
  #   "man from the boondocks".titleize #=> "Man From The Boondocks"
  #   "x-men: the last stand".titleize #=> "X Men: The Last Stand"
  def titleize
    underscore.humanize.gsub(/\b([a-z])/){|x| x[-1..-1].upcase}
  end
  alias_method :titlecase, :titleize

  # The reverse of camelize. Makes an underscored form from the expression in the string.
  # Also changes '::' to '/' to convert namespaces to paths.
  #
  # Examples
  #   "ActiveRecord".underscore #=> "active_record"
  #   "ActiveRecord::Errors".underscore #=> active_record/errors
  def underscore
    gsub(/::/, '/').gsub(/([A-Z]+)([A-Z][a-z])/,'\1_\2').
      gsub(/([a-z\d])([A-Z])/,'\1_\2').tr("-", "_").downcase
  end
end
