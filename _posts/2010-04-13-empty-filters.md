---
 layout: post
 title: Empty Filters
---

Historically, Sequel's behavior for the following has violated many people's principle of least surprise:

    ds.filter     # raises Sequel::Error
    ds.filter({}) # raises Sequel::Error
    ds.filter([]) # raises Sequel::Error
    ds.filter('') # invalid SQL: WHERE ()

Now who in their right mind would do that?  Well, using the direct syntax above, probably nobody, but consider the following cases:

    attrs = {}
    attrs[:year] = year if year
    attrs[:state] = state if state 
    ds.filter(attrs)
    # same as ds.filter({}) if
    # year and state are both nil.

    attrs = []
    attrs.push([:year, 40..60]) if middle_aged?
    attrs.push([:year, 55..75]) if nearing_retirement?
    ds.filter(attrs)
    # same as ds.filter([]) if
    # middle_aged? and nearing_retirement? are both
    # false.

    attrs = []
    attrs.push(:name.like('a%')) if name_starts_with_a?
    attrs.push(:name.like('%z')) if name_ends_with_z?
    ds.filter(*attrs)
    # same as ds.filter if name_starts_with_a?
    # and name_ends_with_z? are both false.

    attrs = []
    attrs.push('number > 10') if gt_10?
    attrs.push('number < 30') if lt_30?
    ds.filter(attrs.join(' AND '))
    # same as ds.filter('') if gt_10? and lt_30?
    # are both false.
    
<a href="http://github.com/jeremyevans/sequel/commit/27154a5f53b29bf2075da680989d70c3cf0ac89c">Sequel now handles all of the above behaviors by just returning a clone of the receiving dataset.</a>  This is just another of the many good suggestions that are submitted via the Google Group. <a href="http://groups.google.com/group/sequel-talk/browse_thread/thread/fdeb350400572fd8">This particular suggestion came from Shawn.</a>
