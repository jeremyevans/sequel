---
 layout: post
 title: A Real World Example
---

Today's blog post is going to explain the Sequel code behind <a href="http://github.com/jeremyevans/giftsmas/blob/master/models/event.rb#L27">a crosstab report from Giftsmas</a>.  First I'll give you some background.  Giftsmas is a web application that keeps track of gift giving.  Basically, each gift can have multiple senders and multiple receivers, and the cross tab report shows receivers as columns and senders as rows, with each entry in the matrix representing the number of gifts that sender gave that receiver.  The basic strategy for producing such a report in SQL is to get all valid columns (Giftsmas does this in two queries), and then run a final query with a case statement for each column to conditionally sum.

    # Explanatory comments added
    def gifts_crosstab
      # Get ids for all receivers of gifts for
      # this event.
      person_ids = model.db[:gifts].
        join(:gift_receivers, :gift_id=>:id).
        filter(:event_id=>id).
        distinct.
        select_order_map(:person_id)

      # Get an ordered mapping of ids to names.
      person_names = model.db[:people].
        filter(:id=>person_ids).
        order(:name).
        map{|x| [x[:id], x[:name]]}

      # Extract just the name out of the previous
      # query, as a symbol.
      person_name_values = person_names.
        map{|x| x.last.to_sym}

      # The main query.  The final map at the
      # end means that this will return an array
      # of arrays, with each array having 
      # a sender name as the first element,
      # with the rest of the elements being, the
      # number of gifts per receiver in the same
      # order as person_name_values.
      rows = model.db[:gifts].
        filter(:event_id=>id).
        join(:gift_receivers, :gift_id=>:id).
        join(:gift_senders, :gift_id=>:gifts__id).
        join(:people.as(:sender), :id=>:person_id).
        select(:sender__name.as(:sender_name), 
          # Select 1 column for each receiver
          # with the number of gifts given by the
          # sender, aliased to the receiver's name.
          *person_names.sort.
          map{|k,v| :sum.sql_function({k=>1}.
          case(0, :gift_receivers__person_id)).as(v)}).
        group_by(:sender__name).
        order(:sender_name).
        map{|r| [r[:sender_name]] +
          person_name_values.map{|x| r[x]\}\}
      [person_name_values, rows]
    end


Here's the resulting SQL (which depends on the receivers in the event):

    # Get all ids of receivers
    SELECT DISTINCT "person_id"
    FROM "gifts"
    INNER JOIN "gift_receivers"
     ON ("gift_receivers"."gift_id" = "gifts"."id")
    WHERE ("event_id" = 2)
    ORDER BY "person_id"

    # Get receiver information for all receivers from previous query
    SELECT * FROM "people"
    WHERE ("id" IN (2, 4, 6, 8, 9, 10, 11, 12, 13, 16, 22))

    # Main query to get cross tab information
    SELECT "sender"."name" AS "sender_name",
     sum((CASE "gift_receivers"."person_id" WHEN 2 THEN 1 ELSE 0 END)) AS "Receiver1",
     sum((CASE "gift_receivers"."person_id" WHEN 4 THEN 1 ELSE 0 END)) AS "Receiver2",
     sum((CASE "gift_receivers"."person_id" WHEN 6 THEN 1 ELSE 0 END)) AS "Receiver3",
     sum((CASE "gift_receivers"."person_id" WHEN 8 THEN 1 ELSE 0 END)) AS "Receiver4",
     sum((CASE "gift_receivers"."person_id" WHEN 9 THEN 1 ELSE 0 END)) AS "Receiver5",
     sum((CASE "gift_receivers"."person_id" WHEN 10 THEN 1 ELSE 0 END)) AS "Receiver6",
     sum((CASE "gift_receivers"."person_id" WHEN 11 THEN 1 ELSE 0 END)) AS "Receiver7",
     sum((CASE "gift_receivers"."person_id" WHEN 12 THEN 1 ELSE 0 END)) AS "Receiver8",
     sum((CASE "gift_receivers"."person_id" WHEN 13 THEN 1 ELSE 0 END)) AS "Receiver9",
     sum((CASE "gift_receivers"."person_id" WHEN 16 THEN 1 ELSE 0 END)) AS "Receiver10",
     sum((CASE "gift_receivers"."person_id" WHEN 22 THEN 1 ELSE 0 END)) AS "Receiver11"
    FROM "gifts"
    INNER JOIN "gift_receivers" ON ("gift_receivers"."gift_id" = "gifts"."id")
    INNER JOIN "gift_senders" ON ("gift_senders"."gift_id" = "gifts"."id")
    INNER JOIN "people" AS "sender" ON ("sender"."id" = "gift_senders"."person_id")
    WHERE ("event_id" = 2)
    GROUP BY "sender"."name"
    ORDER BY "sender_name"

Think about building that final query by manipulating SQL strings.  It's certainly possible, but it's probably more work.  I think this is a good example of how Sequel's DSL makes building complex queries easier.
