## Running Tests

### FDBSQL tests

1. Start FoundationDB SQL Layer
2. `bundle install`
3. `SEQUEL_INTEGRATION_URL='fdbsql://user@localhost:15432/sequel_testing' bundle exec rake spec`


### Sequel Integration Tests

1. Start FoundationDB SQL Layer
2. `bundle install`
3. `SEQUEL_INTEGRATION_URL='fdbsql://user@localhost:15432/sequel_testing' bundle exec rake spec_integration`

### Sequel Integration Tests W/JDBC

1. Start FoundationDB SQL Layer
2. `bundle install`
3. Download the foundationdb sql jdbc adapter: `fdb-sql-layer-jdbc-2.0-0-jdbc41.jar` from: http://search.maven.org/#search|ga|1|foundationdb%20jdbc
4. `CLASSPATH='/path/to/fdb-sql-layer-jdbc-2.0-0-jdbc41.jar' SEQUEL_INTEGRATION_URL='fdbsql://localhost:15432/sequel_testing' bundle exec rake spec_integration`


### Expected Failures

Using the forked version of Sequel for testing, there are no expected failures. Sometimes, if the sql-layer is under load you may get a test failure due to past_version; rerunning the test should work.
