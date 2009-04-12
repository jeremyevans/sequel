module Sequel
  # Yield the Inflections module if a block is given, and return
  # the Inflections module.
  def self.inflections
    yield Inflections if block_given?
    Inflections
  end

  # This module acts as a singleton returned/yielded by Sequel.inflections,
  # which is used to override or specify additional inflection rules
  # for Sequel. Examples:
  #
  #   Sequel.inflections do |inflect|
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
    CAMELIZE_CONVERT_REGEXP = /(^|_)(.)/.freeze
    CAMELIZE_MODULE_REGEXP = /\/(.?)/.freeze
    DASH = '-'.freeze
    DEMODULIZE_CONVERT_REGEXP = /^.*::/.freeze
    EMPTY_STRING= ''.freeze
    SLASH = '/'.freeze
    VALID_CONSTANT_NAME_REGEXP = /\A(?:::)?([A-Z]\w*(?:::[A-Z]\w*)*)\z/.freeze
    UNDERSCORE = '_'.freeze
    UNDERSCORE_CONVERT_REGEXP1 = /([A-Z]+)([A-Z][a-z])/.freeze
    UNDERSCORE_CONVERT_REGEXP2 = /([a-z\d])([A-Z])/.freeze
    UNDERSCORE_CONVERT_REPLACE = '\1_\2'.freeze
    UNDERSCORE_MODULE_REGEXP = /::/.freeze

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
      plural(Regexp.new("(#{singular[0,1]})#{singular[1..-1]}$", "i"), '\1' + plural[1..-1])
      singular(Regexp.new("(#{plural[0,1]})#{plural[1..-1]}$", "i"), '\1' + singular[1..-1])
    end

    # Specifies a new pluralization rule and its replacement. The rule can either be a string or a regular expression.
    # The replacement should always be a string that may include references to the matched data from the rule.
    #
    # Example:
    #   plural(/(x|ch|ss|sh)$/i, '\1es')
    def self.plural(rule, replacement)
      @plurals.insert(0, [rule, replacement])
    end

    # Specifies a new singularization rule and its replacement. The rule can either be a string or a regular expression.
    # The replacement should always be a string that may include references to the matched data from the rule.
    #
    # Example:
    #   singular(/([^aeiouy]|qu)ies$/i, '\1y') 
    def self.singular(rule, replacement)
      @singulars.insert(0, [rule, replacement])
    end

    # Add uncountable words that shouldn't be attempted inflected.
    #
    # Examples:
    #   uncountable "money"
    #   uncountable "money", "information"
    #   uncountable %w( money information rice )
    def self.uncountable(*words)
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

    private

    # Convert the given string to CamelCase.  Will also convert '/' to '::' which is useful for converting paths to namespaces.
    def camelize(s)
      s = s.to_s
      return s.camelize if s.respond_to?(:camelize)
      s = s.gsub(CAMELIZE_MODULE_REGEXP){|x| "::#{x[-1..-1].upcase unless x == SLASH}"}.gsub(CAMELIZE_CONVERT_REGEXP){|x| x[-1..-1].upcase}
      s
    end
  
    # Tries to find a declared constant with the name specified
    # in the string. It raises a NameError when the name is not in CamelCase
    # or is not initialized.
    def constantize(s)
      s = s.to_s
      return s.constantize if s.respond_to?(:constantize)
      raise(NameError, "#{inspect} is not a valid constant name!") unless m = VALID_CONSTANT_NAME_REGEXP.match(s.to_s)
      Object.module_eval("::#{m[1]}", __FILE__, __LINE__)
    end
  
    # Removes the module part from the expression in the string
    def demodulize(s)
      s = s.to_s
      return s.demodulize if s.respond_to?(:demodulize)
      s.gsub(DEMODULIZE_CONVERT_REGEXP, EMPTY_STRING)
    end
  
    # Returns the plural form of the word in the string.
    def pluralize(s)
      s = s.to_s
      return s.pluralize if s.respond_to?(:pluralize)
      result = s.dup
      Inflections.plurals.each{|(rule, replacement)| break if result.gsub!(rule, replacement)} unless Inflections.uncountables.include?(s.downcase)
      result
    end
  
    # The reverse of pluralize, returns the singular form of a word in a string.
    def singularize(s)
      s = s.to_s
      return s.singularize if s.respond_to?(:singularize)
      result = s.dup
      Inflections.singulars.each{|(rule, replacement)| break if result.gsub!(rule, replacement)} unless Inflections.uncountables.include?(s.downcase)
      result
    end
  
    # The reverse of camelize. Makes an underscored form from the expression in the string.
    # Also changes '::' to '/' to convert namespaces to paths.
    def underscore(s)
      s = s.to_s
      return s.underscore if s.respond_to?(:underscore)
      s.gsub(UNDERSCORE_MODULE_REGEXP, SLASH).gsub(UNDERSCORE_CONVERT_REGEXP1, UNDERSCORE_CONVERT_REPLACE).
        gsub(UNDERSCORE_CONVERT_REGEXP2, UNDERSCORE_CONVERT_REPLACE).tr(DASH, UNDERSCORE).downcase
    end
  end
end
