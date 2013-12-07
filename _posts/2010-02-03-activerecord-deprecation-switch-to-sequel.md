---
 layout: post
 title: ActiveRecord Deprecation?  Switch to Sequel!
---

You may not have heard the news, but the <a href="http://m.onkey.org/2010/1/22/active-record-query-interface">ActiveRecord developers plan on deprecating a significant portion of the ActiveRecord API in favor of the new Arel API</a> (which is similar in concept to Sequel's API).  The old API will be deprecated in Rails 3.1 and removed in 3.2.  This will break pretty much every ActiveRecord based application.

In the past, people would often ask me if there is any reason to switch to Sequel if they have an existing project using ActiveRecord.  My response has been, unless you plan on making significant modifications, there's not really a reason to switch working ActiveRecord code over to Sequel.  

With the deprecation of a significant portion of the ActiveRecord API, most projects using ActiveRecord are going to need significant modifications to work on Rails 3.2.  Since I think Sequel offers significant advantages over ActiveRecord (even after the ARel integration), I'm recommending that if you plan on upgrading your application beyond Rails 3, you should consider switching to Sequel instead of upgrading your ActiveRecord code to the new API.

Obviously, that makes sense only if the ActiveRecord -> Sequel modifications are not significantly more difficult than the ActiveRecord -> ARel modifications compared to the benefits that Sequel brings over ARel.  In a series of future blog posts, I will be discussing techniques that make the ActiveRecord -> Sequel transition easier as well as the advantages that Sequel offers over the new ARel based ActiveRecord.
