# This adds a <tt>Sequel::Dataset#to_dot</tt> method.  The +to_dot+ method
# returns a string that can be processed by graphviz's +dot+ program in
# order to get a visualization of the dataset.  Basically, it shows a version
# of the dataset's abstract syntax tree.

module Sequel
  class Dataset
    # The option keys that should be included in the dot output.
    TO_DOT_OPTIONS = [:with, :distinct, :select, :from, :join, :where, :group, :having, :compounds, :order, :limit, :offset, :lock].freeze

    # Return a string that can be processed by the +dot+ program (included
    # with graphviz) in order to see a visualization of the dataset's
    # abstract syntax tree.
    def to_dot
      i = 0
      dot = ["digraph G {", "#{i} [label=\"self\"];"]
      _to_dot(dot, "", i, self, i)
      dot << "}"
      dot.join("\n")
    end

    private

    # Internal recursive version that handles all object types understood
    # by Sequel.  Arguments:
    # * dot :: An array of strings representing the lines in the returned
    #          output.  This function just pushes strings onto this array.
    # * l :: The transition label from the parent node of the AST to the
    #        current node.
    # * c :: An integer representing the parent node of the AST.
    # * e :: The current node of the AST.
    # * i :: The integer representing the last created node of the AST.
    #
    # The basic algorithm is that the +i+ is incremented to get the current
    # node's integer.  Then the transition from the parent node to the
    # current node is added to the +dot+ array.  Finally, the current node
    # is added to the +dot+ array, and if it is a compound node with children,
    # its children are then added by recursively calling this method. The
    # return value is the integer representing the last created node.
    def _to_dot(dot, l, c, e, i)
      i += 1
      dot << "#{c} -> #{i} [label=\"#{l}\"];" if l
      c = i
      case e
      when LiteralString
        dot << "#{i} [label=\"#{e.inspect.gsub('"', '\\"')}.lit\"];"
        i
      when Symbol, Numeric, String, Class, TrueClass, FalseClass, NilClass
        dot << "#{i} [label=\"#{e.inspect.gsub('"', '\\"')}\"];"
        i
      when Array
        dot << "#{i} [label=\"Array\"];"
        e.each_with_index do |v, j|
          i = _to_dot(dot, j, c, v, i)
        end
      when Hash
        dot << "#{i} [label=\"Hash\"];"
        e.each do |k, v|
          i = _to_dot(dot, k, c, v, i)
        end
      when SQL::ComplexExpression 
        dot << "#{i} [label=\"ComplexExpression: #{e.op}\"];"
        e.args.each_with_index do |v, j|
          i = _to_dot(dot, j, c, v, i)
        end
      when SQL::Identifier
        dot << "#{i} [label=\"Identifier\"];"
        i = _to_dot(dot, :value, c, e.value, i)
      when SQL::QualifiedIdentifier
        dot << "#{i} [label=\"QualifiedIdentifier\"];"
        i = _to_dot(dot, :table, c, e.table, i)
        i = _to_dot(dot, :column, c, e.column, i)
      when SQL::OrderedExpression
        dot << "#{i} [label=\"OrderedExpression: #{e.descending ? :DESC : :ASC}#{" NULLS #{e.nulls.to_s.upcase}" if e.nulls}\"];"
        i = _to_dot(dot, :expression, c, e.expression, i)
      when SQL::AliasedExpression
        dot << "#{i} [label=\"AliasedExpression\"];"
        i = _to_dot(dot, :expression, c, e.expression, i)
        i = _to_dot(dot, :alias, c, e.aliaz, i)
      when SQL::CaseExpression
        dot << "#{i} [label=\"CaseExpression\"];"
        i = _to_dot(dot, :expression, c, e.expression, i) if e.expression
        i = _to_dot(dot, :conditions, c, e.conditions, i)
        i = _to_dot(dot, :default, c, e.default, i)
      when SQL::Cast
        dot << "#{i} [label=\"Cast\"];"
        i = _to_dot(dot, :expr, c, e.expr, i)
        i = _to_dot(dot, :type, c, e.type, i)
      when SQL::Function
        dot << "#{i} [label=\"Function: #{e.f}\"];"
        e.args.each_with_index do |v, j|
          i = _to_dot(dot, j, c, v, i)
        end
      when SQL::Subscript 
        dot << "#{i} [label=\"Subscript: #{e.f}\"];"
        i = _to_dot(dot, :f, c, e.f, i)
        i = _to_dot(dot, :sub, c, e.sub, i)
      when SQL::WindowFunction
        dot << "#{i} [label=\"WindowFunction\"];"
        i = _to_dot(dot, :function, c, e.function, i)
        i = _to_dot(dot, :window, c, e.window, i)
      when SQL::Window
        dot << "#{i} [label=\"Window\"];"
        i = _to_dot(dot, :opts, c, e.opts, i)
      when SQL::PlaceholderLiteralString
        str = e.str
        str = "(#{str})" if e.parens
        dot << "#{i} [label=\"PlaceholderLiteralString: #{str.inspect.gsub('"', '\\"')}\"];"
        i = _to_dot(dot, :args, c, e.args, i)
      when SQL::JoinClause
        str = "#{e.join_type.to_s.upcase} JOIN"
        if e.is_a?(SQL::JoinOnClause)
          str << " ON"
        elsif e.is_a?(SQL::JoinUsingClause)
          str << " USING"
        end
        dot << "#{i} [label=\"#{str}\"];"
        i = _to_dot(dot, :table, c, e.table, i)
        i = _to_dot(dot, :alias, c, e.table_alias, i) if e.table_alias
        if e.is_a?(SQL::JoinOnClause)
          i = _to_dot(dot, :on, c, e.on, i)
        elsif e.is_a?(SQL::JoinUsingClause)
          i = _to_dot(dot, :using, c, e.using, i)
        end
      when Dataset
        dot << "#{i} [label=\"Dataset\"];"
        TO_DOT_OPTIONS.each do |k|
          next unless e.opts[k]
          i = _to_dot(dot, k, c, e.opts[k], i)
        end
      else
        dot << "#{i} [label=\"Unhandled: #{e.inspect.gsub('"', "''")}\"];"
      end
      i
    end
  end
end
