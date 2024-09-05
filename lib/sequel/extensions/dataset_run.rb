# frozen-string-literal: true
#
# The dataset_run extension is designed for cases where you want
# to use dataset methods to build a query, but want to run that
# query without returning a result.  The most common need would
# be to easily use placeholders in an SQL string, which Database#run
# does not support directly.
#
# You can load this extension into specific datasets:
#
#   ds = DB["GRANT SELECT ON ? TO ?", :table, :user]
#   ds = ds.extension(:dataset_run)
#   ds.run
#
# Or you can load it into all of a database's datasets, which
# is probably the desired behavior if you are using this extension:
#
#   DB.extension(:dataset_run)
#   DB["GRANT SELECT ON ? TO ?", :table, :user].run
#
# Related module: Sequel::DatasetRun

#
module Sequel
  module DatasetRun
    # Run the dataset's SQL on the database.  Returns NULL.  This is
    # useful when you want to run SQL without returning a result.
    #
    #   DB["GRANT SELECT ON ? TO ?", :table, :user].run
    #   # GRANT SELECT ON "table" TO "user"
    def run
      if server = @opts[:server]
        db.run(sql, :server=>server)
      else
        db.run(sql)
      end
    end
  end

  Dataset.register_extension(:dataset_run, DatasetRun)
end
