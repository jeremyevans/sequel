---
 layout: post
 title: Transaction Isolation
---

With a recent commit, Sequel can now <a href="http://github.com/jeremyevans/sequel/commit/6e967a2be8176bcffcdcbec93ef07c365eb8eab9">set the transaction isolation level used for database transactions</a>.  Setting the transaction isolation level is a fairly advanced feature that I don't expect many people to need, but in some cases it is necessary to get the desired behavior.

The API for setting the transaction isolation level is simple.  You use the :isolation option to Database#transaction with either :uncommitted (for READ UNCOMMITTED), :committed (for READ COMMITTED), :repeatable (for REPEATABLE READ), or :serializable (for SERIALIZABLE).  A simple example is:

    DB.transaction(:isolation=>:serializable) do
      ...
    end

You can also set a default isolation level used for transactions via:

    DB.transaction_isolation_level = :repeatable

Sequel currently supports setting the transaction isolation level on PostgreSQL, MySQL, and Microsoft SQL Server.  Support for other databases may be added in the future.
