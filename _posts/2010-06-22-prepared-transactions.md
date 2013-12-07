---
 layout: post
 title: Prepared Transactions
---

With a recent commit, <a href="http://github.com/jeremyevans/sequel/commit/dc1ac38ce98bed13bab983a12dc9fce1728564ad">Sequel now supports prepared transactions/two-phase commit on PostgreSQL, MySQL, and H2.</a>

Prepared transactions/two-phase commit is a database feature that allows you to "prepare" a database transaction for later commit/rollback, instead of commiting right away.  It's generally used to implement distributed transactions, where multiple databases are involved and you want all of them to commit or none of them.  In this scenario, you don't want to commit on each database, because if you commit on the first database, and there is an error committing to the second database (with those changes rolled back), the databases become out of sync.  Instead, you prepare the transactions on all databases before committing them.  If all databases were able to prepare the transactions successfully, it is very likely that committing the prepared transactions will work correctly.  So using the same scenario, you would prepare the transaction on the first database, and if the second database raised an error when preparing the transaction, you would rollback the prepared transaction on the first database, so both databases would show no changes.

I've had prepared transactions on my todo list for a while, and had already done the necessary research about database support.  The reason it wasn't implemented earlier, other than time, was that I hadn't decided on an API.  I thought of a few different possibilities.  The common thread was that Database#transaction would take a :prepare option to use a prepared transaction.  The difference was in what the value of the :prepare option would be:

1. Originally, I was just going to have the :prepare option use true, and have the transaction method return an object with commit and rollback methods if :prepare=>true.  However, that would require Sequel generating the transaction identifiers manually, and in most cases, the transaction identifiers in a distributed transaction are going to come from an external source (the transaction manager).

2. My second thought was to have the :prepare option take a proc that was called after the transaction had been prepared, where raising an exception in the block would rollback the transaction (using a rescue block) and not raising an exception would commit the transaction (using an ensure block).  I liked that this method couldn't leak prepared transactions, but ultimately rejected it because one of the features of prepared transactions is that they can be committed at some later point by a different connection using just the transaction identifier.  Let's say after preparing the transaction, your ruby process crashes.  You want to be able to commit or rollback the prepared transaction after restarting it.

3. My final decision was to have the :prepare option take a transaction identifier string.  This string would be provided by the user, and used for the prepare transaction queries.  To commit or rollback prepared transactions, two public Database instance methods were added, commit_prepared_transaction and rollback_prepared_transaction, both of which take the transaction id string provided in the :prepare option.

So the general usage with Sequel is:

    DB.transaction(:prepare=>'some_transaction_id_string') do
      ...
    end
     
    DB.commit_prepared_transaction('some_transaction_id_string')
    # or
    DB.rollback_prepared_transaction('some_transaction_id_string')

I hope some Sequel users find this new feature useful.  If you try it out, please let me know in the comments how it turned out.
