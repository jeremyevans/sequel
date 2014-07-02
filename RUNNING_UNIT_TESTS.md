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

There are 42 failing tests in the spec_integration. Some of these will be fixed as part of the 1.9.6 release of the sql-layer, others will be removed by a patch to the specs.
