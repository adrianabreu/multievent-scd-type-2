---
marp: true
theme: gaia
class:
  - lead
---
# Modelling multi event SCD Type 2

---

# Disclaimer (I)
We will be using streaming processing on top of Delta, if you're not familiar with Delta, [I made an introduction with some examples](https://adrianabreu.github.io/how-delta-works). And if you don't know about [stream processing]((https://docs.delta.io/latest/delta-streaming.html)), it's something like just reading the newly inserted data.

--- 

# Disclaimer (II)

Sample code will be in Scala, but is just Spark, so do not worry.

---
### What is an Slow Change Dimension Type 2?

A data modelling technique where we store all the changes that happen to an etnity as rows. When an attribute change we invalidate the current row and generate a new one using the changes. 

There are different flavours, I prefer the one that uses timestamps and a boolean field, so we can query the current status by plain columns like `where is_current = true` or check for the changes of the table that happen at a certain point of time
`where start_at > '2024-04-10' and end_at < '2024-04-10'`

---

Let's start simple. Just a model for people updating the bank savings.
```json
{
    "user": "Adrian",
    "savings": 300,
    "has_deposit": false,
    "event_ts": "2024-04-15"
}
{
    "user": "Pujol",
    "savings": 5000,
    "has_deposit": true,
    "event_ts": "2024-04-15"
}
{
    "user": "Pujol",
    "savings": 5500,
    "has_deposit": true,
    "event_ts": "2024-04-16"
}
```
---

Each event becomes a row in the target table, and the last one for Pujol has the _end_at as null, while the previous one has the following start_at as end_at.

| user | savings | has_desposit | __START_AT | __END_AT |
| ----- | ---- | ---- | ---------- | ------- |
| Adrian | 300 | false | 2024-04-15 |  | 
| Pujol | 5000 | true | 2024-04-15 | 2024-04-16 | 
| Pujol | 5500 | true | 2024-04-16 |  | 

---
### Wait, again? Aren't there enough examples?

Well, sure, but most of them are just like the one above, and don't show "the hard parts". As every change contains all the information. For example on [delta live tables documentation](https://docs.databricks.com/en/delta-live-tables/cdc.html)

| userId | name | city | __START_AT | __END_AT |
| ----- | ---- | ---- | ---------- | ------- |
| 123 | Isabel | Monterrey | 1 | 5 | 
| 123 | Isabel | Chihuahua | 5 | 6 | 
| 124 | Raul | Oaxaca | 1 | null | 
| 125 | Mercedes | Tijuana | 2 | 5 | 
| 125 | Mercedes | Mexicali | 5 | null | 
| 126 | Lily | Cancun | 2 | null |

---

So ok, we know how to generate an scd type 2. Now let's go into the hard parts

---

### 1st Problem: Partial Updates

---

 The bank above decided to split the current behavior into two independent microservices. One for the savings part, which will send an event when the client increases or decreases its savings, and another one for the deposit, which will tell us when the client has a deposit. 

```json
{
    "event": "savings",
    "user": "Adrian",
    "savings": 300,
    "event_ts": "2024-04-15"
}
{
    "event": "deposit",
    "user": "Adrian",
    "has_deposit": false,
    "event_ts": "2024-04-17"
}
```
---

Now every row doesn't reflect the status of the table client. But it doesn't make sense to analyze those events idendepently.


| user | event | savings | has_desposit | __START_AT | __END_AT |
| ----- | ---- | ---- | ---- | ---------- | ------- |
| Adrian |savings | 300 |  | 2024-04-15 | 2024-04-17 | 
| Adrian | deposit | |false | 2024-04-17 |  | 
 

--- 

How can we solve this? By pulling the missing information!

We know that an event updates specific columns, so we need to look into the previous rows for a nonnull value of the missing column.

```sql
when(
    col("event").isin("savings"), 
    lag(e=col("has_desposit"), offset=1, defaultValue=null, ignoreNulls = true)
    .over(Window.partitionBy(col("user")).orderBy(col("event_ts").asc))
).otherwise(col("has_desposit"))
```

---

Now our table looks quite complete!

| user | event | savings | has_desposit | __START_AT | __END_AT |
| ----- | ---- | ---- | ---- | ---------- | ------- |
| Adrian | savings | 300 |  | 2024-04-15 | 2024-04-17 | 
| Adrian | deposit | 300 | false | 2024-04-17 |  | 


---

### 2nd Problem: Incremental Processing


---

The transformations stated above were really simple to deal with when we just read the whole data and transformed it as a whole
But what can you do when you have only a few new rows?

---

Solution... Read the current table, and union the data! 

```scala
    val modifiableData = targetDf
      .filter(col("is_current") === true)
      .join(dedupDf.select(col(id)).distinct, id)

    val allData = modifiableData
      .unionByName(dedupDf, allowMissingColumns = true)

    allData
      .transform(findCurrentRows)
      .transform(pullPartialInformation)
```

This is superwell explained on: https://docs.delta.io/2.0.2/delta-update.html#slowly-changing-data-scd-type-2-operation-into-delta-tables

---

### 3rd Problem: Out of Order

What happens when I got a piece of missing event in between? 

| user | event | savings | has_desposit | __START_AT | __END_AT |
| ----- | ---- | ---- | ---- | ---------- | ------- |
| Adrian | savings | 300 |  | 2024-04-15 | 2024-04-17 | 
| Adrian | deposit | 300 | false | 2024-04-17 |  | 


---

I received a delayed event from my savings microservice.

```json
{
    "event": "savings",
    "user": "Adrian",
    "savings": 400,
    "event_ts": "2024-04-16"
}
```

---

The best solution is just... To reprocess your whole id! That will ensure consistencies with some out out order events that may appear spetially when you have a table represent by several different topics.

```scala
    val modifiableData = targetDf
      .join(dedupDf.select(col(id)).distinct, id)

    val allData = modifiableData
      .unionByName(dedupDf, allowMissingColumns = true)

    allData
      .transform(findCurrentRows)
      .transform(pullPartialInformation)
```
---

| user | event | savings | has_desposit | __START_AT | __END_AT |
| ----- | ---- | ---- | ---- | ---------- | ------- |
| Adrian | savings | 300 |  | 2024-04-15 | 2024-04-16 | 
| Adrian | savings | 400 |  | 2024-04-16 | 2024-04-17 | 
| Adrian | deposit | 400 | false | 2024-04-17 |  | 


