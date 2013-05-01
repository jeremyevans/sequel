def Sequel.core_extensions?
  true
end

class Array
  def ~
    Sequel::Deprecation.deprecate('Array#~', 'Please use Sequel.~ instead, or Sequel.extension(:core_extensions) to continue using it')
    Sequel.~(self)
  end

  def case(*args)
    Sequel::Deprecation.deprecate('Array#case', 'Please use Sequel.case instead, or Sequel.extension(:core_extensions) to continue using it')
    ::Sequel::SQL::CaseExpression.new(self, *args)
  end

  def sql_value_list
    Sequel::Deprecation.deprecate('Array#sql_value_list/Array#sql_array', 'Please use Sequel.value_list instead, or Sequel.extension(:core_extensions) to continue using it')
    ::Sequel::SQL::ValueList.new(self)
  end
  alias sql_array sql_value_list

  def sql_expr
    Sequel::Deprecation.deprecate('Array#sql_expr', 'Please use Sequel.expr instead, or Sequel.extension(:core_extensions) to continue using it')
    Sequel.expr(self)
  end

  def sql_negate
    Sequel::Deprecation.deprecate('Array#sql_negate', 'Please use Sequel.negate instead, or Sequel.extension(:core_extensions) to continue using it')
    Sequel.negate(self)
  end

  def sql_or
    Sequel::Deprecation.deprecate('Array#sql_or', 'Please use Sequel.or instead, or Sequel.extension(:core_extensions) to continue using it')
    Sequel.or(self)
  end

  def sql_string_join(joiner=nil)
    Sequel::Deprecation.deprecate('Array#sql_string_join', 'Please use Sequel.join instead, or Sequel.extension(:core_extensions) to continue using it')
    Sequel.join(self, joiner)
  end
end

class Hash
  def &(ce)
    Sequel::Deprecation.deprecate('Hash#&', 'Please use Sequel.& instead, or Sequel.extension(:core_extensions) to continue using it')
    ::Sequel::SQL::BooleanExpression.new(:AND, self, ce)
  end

  def |(ce)
    Sequel::Deprecation.deprecate('Hash#|', 'Please use Sequel.| instead, or Sequel.extension(:core_extensions) to continue using it')
    ::Sequel::SQL::BooleanExpression.new(:OR, self, ce)
  end

  def ~
    Sequel::Deprecation.deprecate('Hash#~', 'Please use Sequel.~ instead, or Sequel.extension(:core_extensions) to continue using it')
    ::Sequel::SQL::BooleanExpression.from_value_pairs(self, :OR, true)
  end

  def case(*args)
    Sequel::Deprecation.deprecate('Hash#case', 'Please use Sequel.case instead, or Sequel.extension(:core_extensions) to continue using it')
    ::Sequel::SQL::CaseExpression.new(to_a, *args)
  end

  def sql_expr
    Sequel::Deprecation.deprecate('Hash#sql_expr', 'Please use Sequel.expr instead, or Sequel.extension(:core_extensions) to continue using it')
    ::Sequel::SQL::BooleanExpression.from_value_pairs(self)
  end

  def sql_negate
    Sequel::Deprecation.deprecate('Hash#sql_negate', 'Please use Sequel.negate instead, or Sequel.extension(:core_extensions) to continue using it')
    ::Sequel::SQL::BooleanExpression.from_value_pairs(self, :AND, true)
  end

  def sql_or
    Sequel::Deprecation.deprecate('Hash#sql_or', 'Please use Sequel.or instead, or Sequel.extension(:core_extensions) to continue using it')
    ::Sequel::SQL::BooleanExpression.from_value_pairs(self, :OR)
  end
end

class String
  include Sequel::Deprecation.deprecated_module(Sequel::SQL::AliasMethods){|meth| ["String#as", 'Please use Sequel.as instead, or Sequel.extension(:core_extensions) to continue using it']}
  include Sequel::Deprecation.deprecated_module(Sequel::SQL::CastMethods){|meth| ["String##{meth}", "Please use Sequel.#{meth} instead, or Sequel.extension(:core_extensions) to continue using it"]}

  def lit(*args)
    Sequel::Deprecation.deprecate('String#lit', 'Please use Sequel.lit instead, or Sequel.extension(:core_extensions) to continue using it')
    args.empty? ? Sequel::LiteralString.new(self) : Sequel::SQL::PlaceholderLiteralString.new(self, args)
  end
  
  def to_sequel_blob
    Sequel::Deprecation.deprecate('String#to_sequel_blob', 'Please use Sequel.blob instead, or Sequel.extension(:core_extensions) to continue using it')
    ::Sequel::SQL::Blob.new(self)
  end
end

class Symbol
  include Sequel::Deprecation.deprecated_module(Sequel::SQL::AliasMethods){|meth| ["Symbol#as", 'Please use Sequel.as instead, or Sequel.extension(:core_extensions) to continue using it']}
  include Sequel::Deprecation.deprecated_module(Sequel::SQL::CastMethods){|meth| ["Symbol##{meth}", "Please use Sequel.#{meth} instead, or Sequel.extension(:core_extensions) to continue using it"]}
  include Sequel::Deprecation.deprecated_module(Sequel::SQL::OrderMethods){|meth| ["Symbol##{meth}", "Please use Sequel.#{meth} instead, or Sequel.extension(:core_extensions) to continue using it"]}
  include Sequel::Deprecation.deprecated_module(Sequel::SQL::BooleanMethods){|meth| ["Symbol##{meth}", "Please use Sequel.#{meth} instead, or Sequel.extension(:core_extensions) to continue using it"]}
  include Sequel::Deprecation.deprecated_module(Sequel::SQL::NumericMethods){|meth| ["Symbol##{meth}", "Please use Sequel.#{meth} instead, or Sequel.extension(:core_extensions) to continue using it"]}
  include(Sequel::Deprecation.deprecated_module(Sequel::SQL::QualifyingMethods){|meth| ["Symbol##{meth}", "Please use Sequel.#{meth} instead, or Sequel.extension(:core_extensions) to continue using it"]}.module_eval do
    def *(ce=(arg=false;nil))
      if arg == false
        Sequel::Deprecation.deprecate('Symbol#*', "Please use Sequel.expr(symbol).* instead, or Sequel.extension(:core_extensions) to continue using it")
        Sequel::SQL::ColumnAll.new(self)
      else
        super(ce)
      end
    end
    self
  end)
  include Sequel::Deprecation.deprecated_module(Sequel::SQL::StringMethods){|meth| ["Symbol##{meth}", "Please use Sequel.#{meth} instead, or Sequel.extension(:core_extensions) to continue using it"]}
  include Sequel::Deprecation.deprecated_module(Sequel::SQL::SubscriptMethods){["Symbol#sql_subscript", "Please use Sequel.subscript instead, or Sequel.extension(:core_extensions) to continue using it"]}
  include Sequel::Deprecation.deprecated_module(Sequel::SQL::ComplexExpressionMethods){|meth| ["Symbol##{meth}", "Please use Sequel.expr(symbol).#{meth} instead, or Sequel.extension(:core_extensions) to continue using it"]}

  if RUBY_VERSION < '1.9.0'
    include Sequel::Deprecation.deprecated_module(Sequel::SQL::InequalityMethods){|meth| ["Symbol##{meth}", "Please use Sequel.expr(symbol).#{meth} instead, or Sequel.extension(:ruby18_symbol_extensions) to continue using it"]}
  end

  def identifier
    Sequel::Deprecation.deprecate('Symbol#identifier', 'Please use Sequel.identifier instead, or Sequel.extension(:core_extensions) to continue using it')
    Sequel::SQL::Identifier.new(self)
  end

  def sql_function(*args)
    Sequel::Deprecation.deprecate('Symbol#sql_function', 'Please use Sequel.function instead, or Sequel.extension(:core_extensions) to continue using it')
    Sequel::SQL::Function.new(self, *args)
  end
  if RUBY_VERSION < '1.9.0'
    def [](*args)
      Sequel::Deprecation.deprecate('Symbol#[]', 'Please use Sequel.function instead, or Sequel.extension(:ruby18_symbol_extensions) to continue using it')
      Sequel::SQL::Function.new(self, *args)
    end
  end
end
