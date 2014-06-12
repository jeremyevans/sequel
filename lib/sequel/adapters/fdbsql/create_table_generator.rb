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


module Sequel
  module Fdbsql

    # Right now, against FDBSQL 1.9.5, FDBSQL throws an error if you try to create
    # a primary key that doesn't explicitly state NOT NULL. Until FDBSQL is changed
    # to conform with other DBs, the custom definition here is required.
    class CreateTableGenerator < Sequel::Schema::Generator


      # Adds an autoincrementing primary key column or a primary key constraint to the DDL.
      # To just create a constraint, the first argument should be an array of column symbols
      # specifying the primary key columns. To create an autoincrementing primary key
      # column, a single symbol can be used. In both cases, an options hash can be used
      # as the second argument.
      #
      # If you want to create a primary key column that is not autoincrementing, you
      # should not use this method.  Instead, you should use the regular +column+ method
      # with a <tt>:primary_key=>true</tt> option.
      #
      # If an array of column symbols is used, you can specify the :name option
      # to name the constraint.
      #
      # Examples:
      #   primary_key(:id)
      #   primary_key([:street_number, :house_number], :name=>:some constraint_name)
      def primary_key(name, *args)
        return composite_primary_key(name, *args) if name.is_a?(Array)
        @primary_key = @db.serial_primary_key_options.merge({:name => name})

        if opts = args.pop
          opts = {:type => opts} unless opts.is_a?(Hash)
          if type = args.pop
            opts.merge!(:type => type)
          end
          opts[:null] = false
          @primary_key.merge!(opts)
        end
        @primary_key
      end

      private


      # Add a composite primary key constraint
      def composite_primary_key(primary_columns, *args)
        opts = args.pop || {}
        constraints << {:type => :primary_key, :columns => primary_columns}.merge(opts)
        primary_columns.each do |pc|
          columns.each do |c|
            if (c[:name] == pc)
              c[:null] = false
            end
          end
        end
      end

    end
  end
end
