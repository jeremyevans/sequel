# The query_literals extension changes Sequel's default behavior of
# the select, order and group methods so that if the first argument
# is a regular string, it is treated as a literal string, with the
# rest of the arguments (if any) treated as placeholder values. This
# allows you to write code such as:
#
#   DB[:table].select('a, b, ?', 2).group('a, b').order('c')
#
# The default Sequel behavior would literalize that as:
#
#   SELECT 'a, b, ?', 2 FROM table GROUP BY 'a, b' ORDER BY 'c'
#
# Using this extension changes the literalization to:
#
#   SELECT a, b, 2, FROM table GROUP BY a, b ORDER BY c
#
# This extension makes select, group, and order methods operate
# like filter methods, which support the same interface.
#
# There are very few places where Sequel's default behavior is
# desirable in this area, but for backwards compatibility, the
# defaults won't be changed until the next major release.
#
# Loading this extension does nothing by default except make the
# Sequel::QueryLiterals module available. 
#
# To load the extension:
#
#   Sequel.extension :query_literals
#
# Then you can extend specific datasets with this module:
#
#   ds = DB[:table]
#   ds.extend(Sequel::QueryLiterals)
#
# Or you can extend all of a database's datasets with it, which
# is probably the desired behavior if you are using this extension:
#
#   DB.extend_datasets(Sequel::QueryLiterals)

module Sequel
  # The QueryLiterals module can be used to make select, group, and
  # order methods operate similar to the filter methods if the first
  # argument is a plain string, treating it like a literal string,
  # with any remaining arguments treated as placeholder values.
  #
  # This adds such support to the following methods: select, select_append,
  # select_group, select_more, group, group_and_count, order, order_append,
  # and order_more.
  #
  # Note that if you pass a block to these methods, it will use the default
  # implementation without the special literal handling.
  module QueryLiterals
    %w'select select_append select_group select_more group group_and_count order order_append order_more'.each do |m|
      class_eval(<<-END, __FILE__, __LINE__ + 1)
        def #{m}(*args)
          if !block_given? && (l = query_literal(args))
            super(l)
          else
            super
          end
        end
      END
    end

    private

    # If the first argument is a plain string, return a literal string
    # if there are no additional args or a placeholder literal string with
    # the remaining args.  Otherwise, return nil.
    def query_literal(args)
      case (s = args[0])
      when LiteralString, SQL::Blob
        nil
      when String
        if args.length == 1
          LiteralString.new(s)
        else
          SQL::PlaceholderLiteralString.new(s, args[1..-1])
        end
      end
    end
  end
end
