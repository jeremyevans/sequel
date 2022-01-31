# frozen-string-literal: true
#
# The sql_comments extension adds Dataset#comment to the datasets,
# allowing you to set SQL comments in the resulting query.  These
# comments are appended to the end of the SQL query:
#
#   ds = DB[:table].comment("Some Comment").all
#   # SELECT * FROM table -- Some Comment
#   #
#
# As you can see, this uses single line SQL comments (--) suffixed
# by a newline.  This plugin transforms all consecutive whitespace
# in the comment to a single string:
#
#   ds = DB[:table].comment("Some\r\nComment     Here").all
#   # SELECT * FROM table -- Some Comment Here
#   #
#
# The reason for the prefixing and suffixing by newlines is to
# work correctly when used in subqueries:
#
#   ds = DB[:table].comment("Some\r\nComment     Here")
#   ds.where(id: ds).all
#   # SELECT * FROM table WHERE (id IN (SELECT * FROM table -- Some Comment Here
#   # )) -- Some Comment Here
#   #
#
# In addition to working on SELECT queries, it also works when
# inserting, updating, and deleting.
#
# Due to the use of single line SQL comments and converting all
# whitespace to spaces, this should correctly handle even
# malicious input.  However, it would be unwise to rely on that,
# you should ensure that the argument given
# to Dataset#comment is not derived from user input.
#
# You can load this extension into specific datasets:
#
#   ds = DB[:table]
#   ds = ds.extension(:sql_comments)
#
# Or you can load it into all of a database's datasets, which
# is probably the desired behavior if you are using this extension:
#
#   DB.extension(:sql_comments)
#
# Loading the sql_comments extension into the database also adds 
# support for block-level comment support via Database#with_comments.
# You call #with_comments with a hash.  Queries inside the hash will
# include a comment based on the hash (assuming they are inside the
# same thread):
#
#   DB.with_comments(model: Album, action: :all) do
#     DB[:albums].all
#     # SELECT * FROM albums -- model:Album,action:all
#   end
#
# You can nest calls to #with_comments, which will combine the
# entries from both calls:
#
#   DB.with_comments(application: App, path: :scrubbed_path) do
#     DB.with_comments(model: Album, action: :all) do
#       ds = DB[:albums].all
#       # SELECT * FROM albums
#       # -- application:App,path:scrubbed_path,model:Album,action:all
#     end
#   end
#
# You can override comment entries specified in earlier blocks, or
# remove entries specified earlier using a nil value:
#
#   DB.with_comments(application: App, path: :scrubbed_path) do
#     DB.with_comments(application: Foo, path: nil) do
#       ds = DB[:albums].all
#       # SELECT * FROM albums # -- application:Foo
#     end
#   end
#
# You can combine block-level comments with dataset-specific
# comments:
#
#   DB.with_comments(model: Album, action: :all) do
#     DB[:table].comment("Some Comment").all
#     # SELECT * FROM albums -- model:Album,action:all -- Some Comment
#   end
#
# Note that Microsoft Access does not support inline comments,
# and attempting to use comments on it will result in SQL syntax
# errors.
#
# Related modules: Sequel::SQLComments, Sequel::Database::SQLComments

#
module Sequel
  module SQLComments
    # Return a modified copy of the dataset that will use the given comment.
    # To uncomment a commented dataset, pass nil as the argument.
    def comment(comment)
      clone(:comment=>(format_sql_comment(comment) if comment))
    end

    %w'select insert update delete'.each do |type|
      define_method(:"#{type}_sql") do |*a|
        sql = super(*a)
        if comment = _sql_comment
          # This assumes that the comment stored in the dataset has
          # already been formatted. If not, this could result in SQL
          # injection.
          #
          # Additionally, due to the use of an SQL comment, if any
          # SQL is appened to the query after the comment is added,
          # it will become part of the comment unless it is preceded
          # by a newline.
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

    # The comment to include in the SQL query, if any.
    def _sql_comment
      @opts[:comment]
    end

    # Format the comment.  For maximum compatibility, this uses a
    # single line SQL comment, and converts all consecutive whitespace
    # in the comment to a single space.
    def format_sql_comment(comment)
      " -- #{comment.to_s.gsub(/\s+/, ' ')}\n"
    end
  end

  module Database::SQLComments
    def self.extended(db)
      db.instance_variable_set(:@comment_hashes, {})
      db.extend_datasets DatasetSQLComments
    end

    # A map of threads to comment hashes, used for correctly setting
    # comments for all queries inside #with_comments blocks.
    attr_reader :comment_hashes

    # Store the comment hash and use it to create comments inside the block
    def with_comments(comment_hash)
      hashes = @comment_hashes
      t = Sequel.current
      new_hash = if hash = Sequel.synchronize{hashes[t]}
        hash.merge(comment_hash)
      else
        comment_hash.dup
      end
      yield Sequel.synchronize{hashes[t] = new_hash}
    ensure
      if hash
        Sequel.synchronize{hashes[t] = hash}
      else
        t && Sequel.synchronize{hashes.delete(t)}
      end
    end

    module DatasetSQLComments
      include Sequel::SQLComments

      private

      # Include comments added via Database#with_comments in the output SQL.
      def _sql_comment
        specific_comment = super
        return specific_comment if @opts[:append_sql]

        t = Sequel.current
        hashes = db.comment_hashes
        block_comment = if comment_hash = Sequel.synchronize{hashes[t]}
          comment_array = comment_hash.map{|k,v| "#{k}:#{v}" unless v.nil?}
          comment_array.compact!
          comment_array.join(",")
        end

        if block_comment
          if specific_comment
            format_sql_comment(block_comment + specific_comment)
          else
            format_sql_comment(block_comment)
          end
        else
          specific_comment
        end
      end
    end
  end

  Dataset.register_extension(:sql_comments, SQLComments)
  Database.register_extension(:sql_comments, Database::SQLComments)
end
