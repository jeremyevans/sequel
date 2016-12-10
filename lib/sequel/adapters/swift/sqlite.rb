# frozen-string-literal: true

require 'swift/db/sqlite3'
Sequel.require 'adapters/shared/sqlite'

module Sequel
  module Swift
    # Database and Dataset instance methods for SQLite specific
    # support via Swift.
    module SQLite
      # Database instance methods for SQLite databases accessed via Swift.
      module DatabaseMethods
        include Sequel::SQLite::DatabaseMethods

        DATABASE_ERROR_REGEXPS = {
          /\AUNIQUE constraint failed: / => UniqueConstraintViolation,
          /\AFOREIGN KEY constraint failed/ => ForeignKeyConstraintViolation,
          /\ACHECK constraint failed/ => CheckConstraintViolation,
          /\A(SQLITE ERROR 19 \(CONSTRAINT\) : )?constraint failed/ => ConstraintViolation,
          /may not be NULL\z|NOT NULL constraint failed: .+/ => NotNullConstraintViolation,
          /\ASQLITE ERROR \d+ \(\) : CHECK constraint failed: / => CheckConstraintViolation
        }.freeze
        def database_error_regexps
          DATABASE_ERROR_REGEXPS
        end

        # Set the correct pragmas on the connection.
        def connect(opts)
          c = super
          connection_pragmas.each{|s| log_connection_yield(s, c){c.execute(s)}}
          c
        end
      end
      
      # Dataset class for SQLite datasets accessed via Swift.
      class Dataset < Swift::Dataset
        include Sequel::SQLite::DatasetMethods
        
        private
        
        # Use Swift's escape method for quoting.
        def literal_string_append(sql, s)
          sql << APOS << db.synchronize(@opts[:server]){|c| c.escape(s)} << APOS
        end
      end
    end
  end
end
