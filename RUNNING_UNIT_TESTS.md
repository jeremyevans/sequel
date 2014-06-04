## Running Tests

### Sequel Integration Tests

1. Start FoundationDB SQL Layer
2. `bundle install`
4. `SEQUEL_INTEGRATION_URL='fdbsql://user@localhost:15432/sequel_testing' bundle exec rake spec_integration`

### Expected Failures

There are no expected failures. Please report *any* issues encoutnered.

