#
# FoundationDB SQL Layer Sequel Adapter
# Copyright (c) 2013-2014 FoundationDB, LLC
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#

Sequel::JDBC.load_driver('com.foundationdb.sql.jdbc.Driver')
# TODO when merged into Sequel, switch to Sequel.require 'adapters/shared/fdbsql'
require 'sequel/adapters/shared/fdbsql'

module Sequel
  Fdbsql::CONVERTED_EXCEPTIONS << NativeException

  module JDBC
    Sequel.synchronize do
      DATABASE_SETUP[:fdbsql] = proc do |db|
        db.extend(Sequel::JDBC::Fdbsql::DatabaseMethods)
        db.dataset_class = Sequel::JDBC::Fdbsql::Dataset
        com.foundationdb.sql.jdbc.Driver
      end
    end

    module Fdbsql
      # Methods to add to Database instances that access Fdbsql via
      # JDBC.
      module DatabaseMethods
        include Sequel::Fdbsql::DatabaseMethods

        # Add the primary_keys and primary_key_sequences instance variables,
        # so we can get the correct return values for inserted rows.
        def self.extended(db)
          super
          db.send(:adapter_initialize)
        end
      end

      class Dataset < JDBC::Dataset
        include Sequel::Fdbsql::DatasetMethods
      end
    end

  end
end
