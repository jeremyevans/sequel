# frozen-string-literal: true
#
# The provenance dataset extension tracks the locations of all
# dataset clones that resulted in the current dataset, and includes
# the information as a comment in the dataset's SQL.  This makes it
# possible to see how a query was built, which can aid debugging.
# Example:
#
#   DB[:table].
#     select(:a).
#     where{b > 10}.
#     order(:c).
#     limit(10)
#   # SQL:
#   # SELECT a FROM table WHERE (b > 10) ORDER BY c LIMIT 10 --
#   #  -- Dataset Provenance
#   #  -- Keys:[:from] Source:(eval at bin/sequel:257):2:in `<main>'
#   #  -- Keys:[:select] Source:(eval at bin/sequel:257):3:in `<main>'
#   #  -- Keys:[:where] Source:(eval at bin/sequel:257):4:in `<main>'
#   #  -- Keys:[:order] Source:(eval at bin/sequel:257):5:in `<main>'
#   #  -- Keys:[:limit] Source:(eval at bin/sequel:257):6:in `<main>'
#
# With the above example, the source is fairly obvious and not helpful,
# but in real applications, where datasets can be built from multiple
# files, seeing where each dataset clone was made can be helpful.
#
# The Source listed will skip locations in the Ruby standard library
# as well as Sequel itself.  Other locations can be skipped by
# providing a Database :provenance_caller_ignore Regexp option:
#
#   DB.opts[:provenance_caller_ignore] = /\/gems\/library_name-/
#
# Related module: Sequel::Dataset::Provenance

#
module Sequel
  class Dataset
    module Provenance
      SEQUEL_LIB_PATH = (File.expand_path('../../..', __FILE__) + '/').freeze
      RUBY_STDLIB = RbConfig::CONFIG["rubylibdir"]
      INTERNAL = '<internal'

      if TRUE_FREEZE
        # Include provenance information when cloning datasets.
        def clone(opts = nil || (return self))
          super(provenance_opts(opts))
        end
      else
        # :nocov:
        def clone(opts = OPTS) # :nodoc:
          super(provenance_opts(opts))
        end
        # :nocov:
      end

      %w'select insert update delete'.each do |type|
        # Include the provenance information as a comment when preparing dataset SQL
        define_method(:"#{type}_sql") do |*a|
          sql = super(*a)

          if provenance = @opts[:provenance]
            comment = provenance.map do |hash|
              " -- Keys:#{hash[:keys].inspect} Source:#{hash[:source]}".to_s.gsub(/\s+/, ' ')
            end
            comment << ""
            comment.unshift " -- Dataset Provenance"
            comment.unshift " -- "
            comment = comment.join("\n")

            if sql.frozen?
              sql += comment
              sql.freeze
            elsif @opts[:append_sql] || @opts[:placeholder_literalizer]
              sql << comment
            else
              sql += comment
            end
          end

          sql
        end
      end

      private

      # Return a copy of opts with provenance information added.
      def provenance_opts(opts)
        provenance = {source: provenance_source, keys: opts.keys.freeze}.freeze
        opts = opts.dup
        opts[:provenance] = ((@opts[:provenance] || EMPTY_ARRAY).dup << provenance).freeze
        opts
      end

      # Return the caller line for the provenance change. This skips
      # Sequel itself and the standard library.  Additional locations
      # can be skipped using the :provenance_caller_ignore Dataset option.
      def provenance_source
        ignore = db.opts[:provenance_caller_ignore]
        caller.find do |line|
          !(line.start_with?(SEQUEL_LIB_PATH, RUBY_STDLIB, INTERNAL) ||
            (ignore && line =~ ignore))
        end
      end
    end

    register_extension(:provenance, Provenance)
  end
end
