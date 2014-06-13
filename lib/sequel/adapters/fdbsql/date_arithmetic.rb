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

require 'sequel/extensions/date_arithmetic'

# Because of the way date arithmetic is written in Sequel, we need to create a
# different date arithmetic extension that replaces the other
module Sequel
  module Fdbsql
    module DateArithmeticDatasetMethods
      include Sequel::SQL::DateAdd::DatasetMethods
      # chop off the s at the end of each of the units
      FDBSQL_DURATION_UNITS = DURATION_UNITS.zip(DURATION_UNITS.map{|s| s.to_s.chop.freeze}).freeze
      # Append the SQL fragment for the DateAdd expression to the SQL query.
      def date_add_sql_append(sql, da)
        h = da.interval
        expr = da.expr
        if db.database_type == :fdbsql
          expr = Sequel.cast(expr, Time)
          each_valid_interval_unit(h, FDBSQL_DURATION_UNITS) do |value, sql_unit|
            expr = Sequel.+(expr, Sequel.lit(["INTERVAL ", " "], value, Sequel.lit(sql_unit)))
          end
          literal_append(sql, expr)
        else
          super
        end
      end

      private

      # Yield the value in the interval for each of the units
      # present in the interval, along with the SQL fragment
      # representing the unit name.  Returns false if any
      # values were yielded, true otherwise
      def each_valid_interval_unit(interval, units)
        cast = true
        units.each do |unit, sql_unit|
          if (value = interval[unit]) && value != 0
            cast = false
            yield value, sql_unit
          end
        end
        cast
      end
    end
  end

  Dataset.register_extension(:date_arithmetic, Sequel::Fdbsql::DateArithmeticDatasetMethods)
end
