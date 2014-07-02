## FoundationDB SQL Layer Sequel Adapter

The [FoundationDB SQL Layer](https://github.com/FoundationDB/sql-layer) is a
full SQL implementation built on the [FoundationDB](https://foundationdb.com)
storage substrate. It provides high performance, multi-node scalability,
fault-tolerance and true multi-key ACID transactions.

This project provides connection adapter integration for [Sequel](http://sequel.jeremyevans.net/).

### Supported SQL Layer Versions

Version 1.9.6 is the minimum recommended release for use with this adapter.

All previous releases are unsupported.

### Supported Sequel Versions

This project currently supports Sequel v4.11.0

### Quick Start

> Important:
>
> The [SQL Layer](https://foundationdb.com/layers/sql/) installed and running
> before attempting to use this adapter.
>

1. Add the following line to `Gemfile`:
    - Unreleased development version:
        - `gem 'sequel-fdbsql-adapter', github: 'FoundationDB/sql-layer-adapter-sequel`
2. Install the new gem
    - `$ bundle install`
3. Connect
    ```
    require 'sequel'
    require 'sequel-fdbsql-adapter'

    DB = Sequel.connect('fdbsql://user@localhost:15432/database_name')
    ```

### Adapter specific connection options
The following additional options are supported for Sequel.connect

* :hostaddr - Server address (avoids hostname lookup, overrides host)
* :connect_timeout - Maximum time (in seconds) to wait for connection to succeed (default 20)
* :sslmode - Set to 'disable', 'allow', 'prefer', 'require' to choose how to treat SSL


### Contributing

1. Fork
2. Branch
3. Commit
4. Pull Request

If you would like to contribute a feature or fix, thanks! Please make
sure any changes come with new tests to ensure acceptance. Please read
the `RUNNING_UNIT_TESTS.md` file for more details.

### Contact

* GitHub: http://github.com/FoundationDB/sql-layer-adapter-sequel
* Community: http://community.foundationdb.com
* IRC: #FoundationDB on irc.freenode.net

### License

The MIT License (MIT)

Copyright (c) 2013-2014 FoundationDB, LLC

It is free software and may be redistributed under the terms specified in the LICENSE file.
