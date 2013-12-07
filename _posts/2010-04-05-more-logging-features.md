---
 layout: post
 title: More Logging Features
---

As I mentioned in <a href="/2010/04/02/sequel-3100-released.html">my last blog post</a>, one of the main features planned for Sequel 3.11.0 is an overhaul of the logging support.  I'm happy to report that this work has already been completed and is now in <a href="http://github.com/jeremyevans/sequel/tree/">Sequel's master branch</a>.

Before, Sequel logged all queries before they were sent to the database to all of the Sequel::Database's loggers at info level (via the log_info method).  This is how logging in Sequel has been done since before I took over maintenance.  However, some Sequel users have been asking for more flexibility when logging.  After listening to the requests and giving it a lot of thought, I've modified Sequel's logging to use a new Database method called log_yield, and modified all of the adapters to use it.  log_yield has the following advantages over log_info:

* Queries that raise errors are now logged at error level instead of info level.
* Queries that don't raise errors now log the duration of the query (how long it took to execute).
* You can use the log_warn_duration Database attribute to set a duration (Float/Integer of seconds) above which Sequel will log successful queries at warn level instead of info level.

Since all decent loggers can be set to only log some levels and not others, this allows you to only log queries that raise errors, or only log queries that raise errors and queries take more than a specified amount of time.  Both of these new capabilities mean that it is now feasible to use logging in Sequel in production, and get useful information, assuming you can take the modest performance hit.

Nope that there are a couple of minor backwards compatibility issues:

* Successful queries are now logged with the duration preceeding the query.  So if you using logging output in your specs, you may want to <a href="http://github.com/jeremyevans/sequel/commit/62a1ebf69d81aff1c11cef629564c05ebd2cd9f6#L0R13">turn logging durations off</a>.
* Loggers must now respond to error as well as info, and warn if you are using log_warn_duration.
* Logging now happens after a query is issued instead of before.  This means that if a query blocks or crashes the interpreter, it will not be logged.  This is an unfortunate side effect of the implementation.  If you do run into such a situation, it's fairly easy to override log_yield for your Database object to log the query at debug level and then call super.

I hope these new logging features are useful.  Please let me know if you'll be using them in your app in the comments.
