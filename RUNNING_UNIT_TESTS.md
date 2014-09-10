## Running Tests

### FDBSQL tests

1. Start FoundationDB SQL Layer
2. `bundle install`
3. `SEQUEL_INTEGRATION_URL='fdbsql://user@localhost:15432/sequel_testing' bundle exec rake spec`


### Sequel Integration Tests

1. Start FoundationDB SQL Layer
2. `bundle install`
3. `SEQUEL_INTEGRATION_URL='fdbsql://user@localhost:15432/sequel_testing' bundle exec rake spec_integration`


### Expected Failures

Using the forked version of Sequel for testing, there are no expected failures. Sometimes, if the sql-layer is under load you may get a test failure due to past_version; rerunning the test should work.
