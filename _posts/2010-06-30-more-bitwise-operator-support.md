---
 layout: post
 title: More Bitwise Operator Support
---

Previously, Sequel treated bitwise operators fairly literally on all databases.  So the bitwise operator support worked well for databases that natively supported the bitwise operators, and not so well for databases that did not.  With a <a href="http://github.com/jeremyevans/sequel/commit/4afc7ef6bbfc66b4839497f3c2ebef952243cbbf">recent patch</a>, Sequel now has much wider bitwise operator support on databases.

For example, on the H2 database, Sequel will now use the BITAND, BITOR, and BITXOR methods for the &, |, and ^ operators:

    DB[:a].select(:a.sql_number & :b)
    # SELECT BITAND(a, b) FROM a


On both H2 and Microsoft SQL server, the lack of the bitwise shift operators (<< and >>) is compensated by emulating them with multiply and divide operators on powers of 2:

    DB[:a].select(:a.sql_number << 2)
    # SELECT (a * POWER(2, 2)) FROM a

On MySQL, the bitwise complement operator now operates similarly to other databases.  Before, it returned an unsigned integer, now it returns a signed integer.  Sequel handles this by casting the result:

    DB[:a].select(~:a.sql_number)
    # SELECT CAST(~a AS SIGNED INTEGER) FROM a

There was also <a href="http://github.com/jeremyevans/sequel/commit/1acde327810aaac09186b2eaf24f2f06c82f199e">a separate patch for PostgreSQL bitwise operator support</a>, which changed the ruby ^ operator (bitwise xor) to use the PostgreSQL # operator (bitwise xor) instead of the PostgreSQL ^ operator (power).  Sequel aims to translate ruby syntax into SQL, and in doing so should use the most appropriate translation, which is not always the most direct syntatic translation.

If you were previously using the ^ operator on PostgreSQL and wanting the power operator, you should switch to using the power function.  If you were previously using the ~ operator on MySQL and wanting an unsigned integer, you'll need to cast it to 'unsigned integer' manually.

