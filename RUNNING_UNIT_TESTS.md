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

`DROP TABLE IF EXISTS` causes a warning with the FoundationDB SQL Layer, but a notice with postgres,
this causes the tests to print out `WARN:  Cannot find the table 'sequel_testing'.'artists'`


There are 66 failing tests in the spec_integration, hopefully by the time SQL Layer 1.9.6 comes out
they will all be fixed.
