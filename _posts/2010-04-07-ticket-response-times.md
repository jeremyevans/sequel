---
 layout: post
 title: Ticket Response Times
---

One of the things I brag about in my presentations on Sequel is the response times on tickets filed on <a href="http://code.google.com/p/ruby-sequel/issues/list">Sequel's bug tracker</a>.  Here are the stats for all tickets filed since January 1st, 2009:

    Time till first response
    ------------------------
    avg: 0 days, 03:39:28
    min: 0 days, 00:03:56
    median: 0 days, 01:13:38
    p75: 0 days, 05:14:40
    p90: 0 days, 10:15:58
    max: 1 days, 03:09:48
    
    Time till closed
    ----------------
    avg: 0 days, 21:04:06
    min: 0 days, 00:11:37
    median: 0 days, 05:40:33
    p75: 1 days, 00:40:06
    p90: 3 days, 00:18:11
    max: 5 days, 05:42:19
    
As you can see, over half of the tickets receive a response within an hour and fifteen minutes, and over 50% are resolved within 5 hours.  Even the worst case scenarios aren't bad, with about a day for a response and 5 days for resolution.  Considering I do occassionally take vacations, I'm very happy with these numbers.

I'd be interested in comparing Sequel's ticket response time to "the other guys", but not interested enough to do the work myself.  Any takers?
