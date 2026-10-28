# Assignment #3: Report

**Name:**
**Student ID:**
**Email:**

---

## Seed and commands

Seed used for `datagen.py`:

The commands you ran, in order:

```bash

```

docker compose up -d

python3 datagen.py 801240291

docker cp datagen.py spark-master:/opt/spark/work-dir/shared/

docker exec spark-master python3 /opt/spark/work-dir/shared/datagen.py 801240291 /opt/spark/work-dir/shared/input

docker cp main.py spark-master:/opt/spark/work-dir/main.py

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py \
  /opt/spark/work-dir/shared/input \
  /opt/spark/work-dir/shared/output




---

## Part 1: load and clean

How you implemented rule 1 (dropoff after pickup) and rule 2 (fare present and numeric):

I removed trips where dropoff time was not after pickup time.

I used try_cast to convert fares to doubles and removed all invalid or missing fares.

Why `fare` is read as a string, and what would have happened with a `DOUBLE` column:

fares as strings invalidates values until cleaning. Reading them as doubles could maybe convert the bad values to null.


The five counts:

+----------+--------+--------+-------+---------+
|rows_total|bad_time|bad_fare|removed|rows_kept|
+----------+--------+--------+-------+---------+
|100000    |1464    |1514    |2978   |97022    |
+----------+--------+--------+-------+---------+

Two or three bad rows, as they appear in `trips.csv`:

```

T000037,D158,11,15,2026-09-29 13:39:07,2026-09-29 13:59:29,4.52,,card
T000041,D139,3,14,2026-09-24 08:17:37,2026-09-24 08:30:10,4.46,N/A,card
T000074,D203,9,16,2026-09-16 03:23:32,2026-09-16 03:14:42,2.24,9.73,app



```

---

## Part 2a: trips and revenue per zone per day

First ten rows, and one sentence on what they show:

```

2026-09-01  1        Uptown Center    316    5927.61
2026-09-01  2        First Ward       246    4977.30
2026-09-01  3        Fourth Ward      187    3492.37
2026-09-01  4        South End        313    6297.59
2026-09-01  5        Dilworth         173    3411.63
2026-09-01  6        Plaza Midwood    109    2241.97
2026-09-01  7        NoDa             187    3693.96
2026-09-01  8        University City  200    3909.17
2026-09-01  9        Ballantyne       131    2542.37
2026-09-01  10       SouthPark        188    3787.12

```

## Part 2b: each driver's busiest day

First ten rows, and one sentence on what they show:

```

D001       2026-09-07  23
D002       2026-09-30  22
D003       2026-09-01  22
D004       2026-09-03  21
D005       2026-09-03  24
D006       2026-09-11  23
D007       2026-09-01  21
D008       2026-09-06  19
D009       2026-09-06  20
D010       2026-09-20  22


```

## Part 3: activity per borough (Spark SQL)

Your SQL query and its result:

```sql

```

SELECT
    z.borough,
    COUNT(*) AS trips,
    ROUND(SUM(t.fare), 2) AS revenue,
    ROUND(AVG(t.distance_km), 2) AS avg_distance_km
FROM trips t
JOIN zones z ON t.pickup_zone = z.zone_id
GROUP BY z.borough
ORDER BY revenue DESC


```

borough  trips   revenue     avg_distance_km
Central  40248   792597.54   5.34
South    18556   368335.10   5.38
North    14408   287369.87   5.41
East     11965   239247.28   5.44
West     11845   232711.90   5.32


```



---

## The two plans

`explain()` output of part 2a (DataFrame API):

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [day#35 ASC NULLS FIRST, zone_id#9 ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(day#35 ASC NULLS FIRST, zone_id#9 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=1527]
      +- HashAggregate(keys=[day#35, zone_id#9, zone_name#10], functions=[count(1), sum(fare#34)])
         +- Exchange hashpartitioning(day#35, zone_id#9, zone_name#10, 200), ENSURE_REQUIREMENTS, [plan_id=1524]
            +- HashAggregate(keys=[day#35, zone_id#9, zone_name#10], functions=[partial_count(1), partial_sum(fare#34)])
               +- Project [fare#34, day#35, zone_id#9, zone_name#10]
                  +- BroadcastHashJoin [pickup_zone#2], [zone_id#9], Inner, BuildRight, false, false
                     :- Project [pickup_zone#2, try_cast(fare#7 as double) AS fare#34, cast(pickup_ts#4 as date) AS day#35]
                     :  +- Filter (((((isnotnull(fare#7) AND isnotnull(pickup_ts#4)) AND isnotnull(dropoff_ts#5)) AND (dropoff_ts#5 > pickup_ts#4)) AND isnotnull(try_cast(fare#7 as double))) AND isnotnull(pickup_zone#2))
                     :     +- FileScan csv [pickup_zone#2,pickup_ts#4,dropoff_ts#5,fare#7] Batched: false, DataFilters: [isnotnull(fare#7), isnotnull(pickup_ts#4), isnotnull(dropoff_ts#5), (dropoff_ts#5 > pickup_ts#4)..., Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/trips.csv], PartitionFilters: [], PushedFilters: [IsNotNull(fare), IsNotNull(pickup_ts), IsNotNull(dropoff_ts), IsNotNull(pickup_zone)], ReadSchema: struct<pickup_zone:int,pickup_ts:timestamp,dropoff_ts:timestamp,fare:string>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1519]
                        +- Filter isnotnull(zone_id#9)
                           +- FileScan csv [zone_id#9,zone_name#10] Batched: false, DataFilters: [isnotnull(zone_id#9)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/zones.csv], PartitionFilters: [], PushedFilters: [IsNotNull(zone_id)], ReadSchema: struct<zone_id:int,zone_name:string>



```

`explain()` output of part 3 (Spark SQL):

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Sort [revenue#193 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(revenue#193 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=1577]
      +- HashAggregate(keys=[borough#11], functions=[count(1), sum(fare#34), avg(distance_km#6)])
         +- Exchange hashpartitioning(borough#11, 200), ENSURE_REQUIREMENTS, [plan_id=1574]
            +- HashAggregate(keys=[borough#11], functions=[partial_count(1), partial_sum(fare#34), partial_avg(distance_km#6)])
               +- Project [distance_km#6, fare#34, borough#11]
                  +- BroadcastHashJoin [pickup_zone#2], [zone_id#9], Inner, BuildRight, false, false
                     :- Project [pickup_zone#2, distance_km#6, try_cast(fare#7 as double) AS fare#34]
                     :  +- Filter (((((isnotnull(fare#7) AND isnotnull(pickup_ts#4)) AND isnotnull(dropoff_ts#5)) AND (dropoff_ts#5 > pickup_ts#4)) AND isnotnull(try_cast(fare#7 as double))) AND isnotnull(pickup_zone#2))
                     :     +- FileScan csv [pickup_zone#2,pickup_ts#4,dropoff_ts#5,distance_km#6,fare#7] Batched: false, DataFilters: [isnotnull(fare#7), isnotnull(pickup_ts#4), isnotnull(dropoff_ts#5), (dropoff_ts#5 > pickup_ts#4)..., Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/trips.csv], PartitionFilters: [], PushedFilters: [IsNotNull(fare), IsNotNull(pickup_ts), IsNotNull(dropoff_ts), IsNotNull(pickup_zone)], ReadSchema: struct<pickup_zone:int,pickup_ts:timestamp,dropoff_ts:timestamp,distance_km:double,fare:string>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1569]
                        +- Filter isnotnull(zone_id#9)
                           +- FileScan csv [zone_id#9,borough#11] Batched: false, DataFilters: [isnotnull(zone_id#9)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/zones.csv], PartitionFilters: [], PushedFilters: [IsNotNull(zone_id)], ReadSchema: struct<zone_id:int,borough:string>


```

Your comparison: where the file scans are, which operator is the join and of which kind,
where the exchanges are and why, and what the two plans have in common although they were
written in different languages.



---

## At scale

With 200 million trips instead of 100,000: what would you keep as it is, and what would you
change first? One paragraph.

I would switch to Parquet split date and keep the date.


---

## Problems and fixes

What went wrong and how you fixed it, with the actual error messages. If nothing went wrong,
say so.
Everything went smoothe

