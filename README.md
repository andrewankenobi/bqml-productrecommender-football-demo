# BigQueryML - Product Recommendation for football games

This outsources demo showcases how to create a product recommender system to be used to recommend products to fans during football games.

## Preparation

### Baseline Data
The foundational data is available [in this Google Drive folder](https://drive.google.com/drive/folders/1V5q155BDNohhlCbMxIp5YbDMJOThU5kG?usp=sharing); three files are available: 
- events-raw.csv
- matches-raw.csv
- customers.csv

### Import the baseline data into BigQuery
- Create a BigQuery project, e.g. `bq-ml-football`
- Create a `raw` dataset into the BigQuery project and then: 
  - Import the content of `events-raw.csv` into a table called `events-raw` (native or external/federated)
  - Import the content of `matches-raw.csv` into a table called `match-raw` (native or external/federated)
- Create a `actionable` dataset into the BigQuery project import the contect of `customers.csv` into a table called `customers` (native or external/federated)

### Derive curated data
- Create a `curated` dataset into the BigQuery project
- Create a view called `events-baseline` defined as follows:
```
SELECT
  a.id,
  a.home,
  a.away,
  a.attendance,
  a.date,
  a.year,
  a.venue,
  a.home_score,
  a.away_score,
  CASE
    WHEN INSTR(REPLACE(b.time,"'",''), "45+") >0 THEN 45
    WHEN INSTR(REPLACE(b.time,"'",''), "90+") >0 THEN 90
  ELSE
  CAST(REPLACE(b.time,"'",'') AS INT64)
END
  AS time,
  b.time AS time_orig,
  b.event,
  CASE
    WHEN LOWER(b.event) LIKE "%outcome: goal%" THEN "goal"
    WHEN LOWER(b.event) LIKE "%yellow card%" THEN "booking"
    WHEN LOWER(b.event) LIKE "%red card%" THEN "ejection"
    WHEN LOWER(b.event) LIKE "%kick off%" THEN "kickoff"
    WHEN LOWER(b.event) LIKE "%halftime%" THEN "halftime"
    WHEN LOWER(b.event) LIKE "%foul%" THEN "foul"
    WHEN LOWER(b.event) LIKE "%end regular time%" THEN "fulltime"
    WHEN LOWER(b.event) LIKE "%penalty scored%" THEN "penaltyscored"
    WHEN LOWER(b.event) LIKE "%penalty missed%" THEN "penaltymissed"
    WHEN LOWER(b.event) LIKE "%|%" THEN "sub"
  ELSE
  "unmatched"
END
  AS event_type
FROM
  `g-football.raw.match-raw` a
JOIN
  `g-football.raw.events-raw` b
ON
  a.id=b.id
WHERE
  b.time IS NOT null
```
-Create a view called `features-events` defined as follows

```
SELECT
 DISTINCT id, home, away,
 DIV(time,5) time_bucket,
 COUNTIF(event_type="goal") ev_goals,
 COUNTIF(event_type="booking") ev_booking,
 COUNTIF(event_type="ejection") ev_ejection,
 COUNTIF(event_type="sub") ev_sub,
 COUNTIF(event_type="penaltyscored") ev_penaltyscored,
 COUNTIF(event_type="penaltymissed") ev_penaltymissed,
 COUNTIF(event_type="foul") ev_foul,
 COUNTIF(event_type<>"unmatched") tot_ev,
FROM
 `bq-ml-football.curated.events-baseline`
GROUP BY
 id,home,away,
 DIV(time,5)
```

-Create a view called `features-events-cumulative` defined as follows

```
SELECT
 id, time_bucket, home, away,
 sum(ev_goals) over (partition by id order by time_bucket) as tot_ev_goals,
 sum(ev_booking) over (partition by id order by time_bucket) as tot_ev_booking,
 sum(ev_ejection) over (partition by id order by time_bucket) as tot_ev_ejection,
 sum(ev_sub) over (partition by id order by time_bucket) as tot_ev_sub,
 sum(ev_penaltyscored) over (partition by id order by time_bucket) as tot_ev_penaltyscored,
 sum(ev_penaltymissed) over (partition by id order by time_bucket) as tot_ev_penaltymissed,
 sum(ev_foul) over (partition by id order by time_bucket) as tot_ev_foul,
 sum(tot_ev) over (partition by id order by time_bucket) as tot_ev
FROM
 `bq-ml-football.curated.features-events`
```
