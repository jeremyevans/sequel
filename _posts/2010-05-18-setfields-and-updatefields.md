---
 layout: post
 title: set_fields and update_fields
---

Since June 2008, Sequel has had 4 main methods to use to set a model object's attributes: set, set_all, set_except, and set_only.  <a href="http://github.com/jeremyevans/sequel/commit/7f36a64e603e7e678a05e357ad1b2f4f2acbbf86">All use the same basic framework</a>, iterating over the hash provided, and checking each entry to see if it was allowed.  If it was allowed, the setter method was called, otherwise, depending on the value of strict_param_setting, it was ignored or a Sequel::Error was raised.

This system is flexible, allowing all of the power of ActiveRecord's attr_accessible and attr_protected, but being able to modify the allowed/restricted fields on a per-call basis, which is great for any system where different roles can change different fields.  With ActiveRecord, that type of system is hard to enforce without modifying the hash before passing it to attributes= or a related method.

One case where the current system has issues is how it handles a hash with entries that aren't in the fields that you want to allow.  A common case for this is when submitting a web form.  Let's say you are opting for simplicity and don't want to use a nested hash for the objects parameters.  So the parameters come in like:

    {"first_name"=>"Jeremy", "last_name"=>"Evans", "submit"=>"Create Programmer", "agree"=>"t"}

In this case, you only care about first_name and last_name, the other two parameters are for fields on the form that don't relate to the model object you are dealing with.  Let's say you handle this in your Sequel code using:

    Programmer.new(params)

Sequel will raise an error, because submit= and agree= are not instance methods in the Programmer class.  So you look at Sequel's documentation and you find set_only.  Because of the name, it certainly sounds like it will only set the fields you give it, so you try:

    p = Programmer.new
    p.set_only(params, [:first_name, :last_name])

It turns out that this gives you exactly the same error.  This is because set_only really means: set the values of the object using the hash, and only allow the fields I explicitly list.  One way around it is to turn off strict_param_setting for your model:

    Programmer.strict_param_setting = false

However, then other possible errors will be silently ignored.  In this case, we expect there will other unrelated parameters given, but we don't care about them.  This is where the new <a href="http://github.com/jeremyevans/sequel/commit/9b28b3bd5ad59d1b026a1cb494849641d9967ea4">set_fields method</a> makes sense to use:

    p = Programmer.new
    p.set_fields(params, [:first_name, :last_name])

Unlike set_only, which iterates over the params and checks each param to see if it is allowed or not, set_fields iterates over the array, and just calls the setter method on the object with the value for the matching key in params.  I've actually created methods like this in some of my own apps, and have gotten a request for a method like this from multiple Sequel users, so I think it's a good fit for Sequel.  Like the other set_* methods, set_fields has a matching update_fields method, which just calls set_fields followed by save_changes.

There are a couple of minor differences between set_fields and set_only that you should be aware of:

1. set_only will raise a Sequel::Error if the method does not exist, set_fields will raise a NoMethodError.
2. If an entry in the array is not present in the hash, set_only will skip that field (since it iterates over the hash), while set_fields will call the setter method with nil (or whatever the [] method on the hash returns).
