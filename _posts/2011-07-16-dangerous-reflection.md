---
 layout: post
 title: Dangerous Reflection
---

Reflection in ruby is a powerful thing.  Like many powerful things, it can be dangerous if used incorrectly.  <a href="http://groups.google.com/group/rubyonrails-security/browse_thread/thread/bb9dcf7701f8f1b7">I recently discovered vulnerabilities in ActiveRecord's mass assignment code</a> that relate to the use of ruby's reflection methods, and since I think that many ruby users are unaware of the issues, it would be helpful to explain the problem and detail the solution that Sequel has used for over 3 years to work around it.

Let's work backward and start with the effect of the vulnerability, which is that a user can create arbitrary symbols.  Symbols in ruby are not garbage collected, so arbitrary symbol creation can result in denial of service through memory exhaustion.  In innocuous usage, such lost memory is inconsequential, but you don't have to be a very good hacker to exploit this vulnerability.  With this vulnerability, a hacker can create two arbitrary symbols per request, and for most vulnerable web applications, you can get ruby to leak memory at a rate of about twice the bandwidth.  So if an attacker has a 100Mb/s connection to your server, he can cause about 200Mb/s memory leakage.  While the exploitation of this is fairly trivial in many applications, it does cost the attacker significantly, since they have to be transmitting half of the memory lost.  So this is not an end-of-the-world scenario, but it's not difficult to avoid either.

The root cause of the problem is the use of ruby's reflection methods with strings.  Reflection methods such as <code>respond_to?</code>, <code>public_method_defined?</code>, <code>instance_variable_defined?</code>, <code>const_defined?</code>, and <code>class_variable_defined?</code> are not safe to use with user-defined strings.  This is because all of them convert the strings into symbols internally.  This hides the symbol creation from the user, making the methods appear safe to use with user-defined input, when they are not.

At the interpreter level, it is easy to see why it works this way. (<em>Overly simplified explanation</em>) In MRI, all constants, methods, instance variables, and class variables are stored in internal hash tables (<code>struct st_table</code>s).  These hash tables map ruby <code>ID</code> keys to ruby <code>VALUE</code>s (<code>ID</code>s are like ruby symbols, and <code>VALUE</code>s are like ruby objects).  So if you call a method such as <code>respond_to?</code> with a string, ruby is going to need to convert it to a symbol/<code>ID</code> in order to lookup the value in the internal hash table.

So what is the solution?  The only solution I'm aware of is fairly slow, as it requires getting an array of method/instance variable/class variable/constant names, converting it to strings, and then checking if the string value you have is included in the array.  So instead of

    respond_to?('user_string')

you need to do:

    public_methods.map{|x| x.to_s}.include?('user_string')

or check the string against a separate whitelist (like ActiveRecord's attr_accessible).

A simple replacement of <code>respond_to?</code> with <code>public_methods.map</code> is a huge performance hit unless you can cache the values of that map call.  When Sequel started caching that map call, <a href="https://github.com/jeremyevans/sequel/commit/133d7b77d39704f847160df0b06f1ee261ada386">it sped up mass assignment about 10x</a>.

Avoiding this vulnerability was actually <a href="https://github.com/jeremyevans/sequel/commit/f79b53e635fd81dd28d9ab30c1ab4ae95e820f0c">one of my early commits to Sequel</a>, on April 10, 2008.  You may be wondering why I didn't report this to the ActiveRecord developers then.  Truth is, I didn't really think about it.  I had just started as maintainer of Sequel and was very busy getting familiar with the codebase and fixing bugs in it.  I didn't think about other projects and how they handled such a situation.  I forgot about the issue completely until a few days ago when I received <a href="https://github.com/jeremyevans/sequel/pull/369">a pull request that would have reintroduced the vulnerability</a>.  I ended up merging that pull request and committing <a href="https://github.com/jeremyevans/sequel/commit/90adf2e76656a0a459efb2c8e47c498e04c18fd3">a separate patch that fixed the vulnerability</a>, using code pretty much identical to that above.  That's when I thought to check if ActiveRecord was vulnerable, and it turns out it was.  That's when I notified the ActiveRecord developers. 

So there you have it.  Bottom line: don't call ruby's reflection methods with user-defined strings or you open yourself up to denial of service.
