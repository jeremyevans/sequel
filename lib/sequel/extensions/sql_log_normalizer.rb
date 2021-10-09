# frozen-string-literal: true
#
# The sql_log_normalizer extension normalizes the SQL that is logged,
# removing the literal strings and numbers in the SQL, and removing the
# logging of any bound variables:
#
#   ds = DB[:table].first(a: 1, b: 'something')
#   # Without sql_log_normalizer extension
#   # SELECT * FROM "table" WHERE (("a" = 1) AND ("b" = 'something')) LIMIT 1
#
#   # With sql_log_normalizer_extension
#   # SELECT * FROM "table" WHERE (("a" = ?) AND ("b" = ?)) LIMIT ?
#
# The normalization is done by scanning the SQL string being executed
# for literal strings and numbers, and replacing them with question
# marks.  While this should work for all or almost all production queries,
# there are pathlogical queries that will not be handled correctly, such as
# the use of apostrophes in identifiers:
#
#   DB[:"asf'bar"].where(a: 1, b: 'something').first
#   # Logged as:
#   # SELECT * FROM "asf?something')) LIMIT ?
#
# The expected use case for this extension is when you want to normalize
# logs to group similar queries, or when you want to protect sensitive
# data from being stored in the logs.
#
# Related module: Sequel::SQLLogNormalizer

#
module Sequel
  module SQLLogNormalizer
    def self.extended(db)
      type = case db.literal("'")
      when "''''"
        :standard
      when "'\\''"
        :backslash
      when "N''''"
        :n_standard
      else
        raise Error, "SQL log normalization is not supported on this database (' literalized as #{db.literal("'").inspect})"
      end
      db.instance_variable_set(:@sql_string_escape_type, type)
    end

    # Normalize the SQL before calling super.
    def log_connection_yield(sql, conn, args=nil)
      unless skip_logging?
        sql = normalize_logged_sql(sql)
        args = nil
      end
      super
    end

    # Replace literal strings and numbers in SQL with question mark placeholders.
    def normalize_logged_sql(sql)
      sql = sql.dup
      sql.force_encoding('BINARY')
      start_index = 0
      check_n = @sql_string_escape_type == :n_standard
      outside_string = true

      if @sql_string_escape_type == :backslash
        search_char = /[\\']/
        escape_char_offset = 0
        escape_char_value = 92 # backslash
      else
        search_char = "'"
        escape_char_offset = 1
        escape_char_value = 39 # apostrophe
      end

      # The approach used here goes against Sequel's philosophy of never attempting
      # to parse SQL.  However, parsing the SQL is basically the only way to implement
      # this support with Sequel's design, and it's better to be pragmatic and accept
      # this than not be able to support this.

      # Replace literal strings
      while outside_string && (index = start_index = sql.index("'", start_index))
        if check_n && index != 0 && sql.getbyte(index-1) == 78 # N' start
          start_index -= 1
        end
        index += 1
        outside_string = false

        while (index = sql.index(search_char, index)) && (sql.getbyte(index + escape_char_offset) == escape_char_value)
          # skip escaped characters inside string literal
          index += 2
        end

        if index
          # Found end of string
          sql[start_index..index] = '?'
          start_index += 1
          outside_string = true
        end
      end

      # Replace integer and decimal floating point numbers
      sql.gsub!(/\b-?\d+(?:\.\d+)?\b/, '?')

      sql
    end
  end

  Database.register_extension(:sql_log_normalizer, SQLLogNormalizer)
end
