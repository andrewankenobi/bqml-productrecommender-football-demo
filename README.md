# BigQueryML - Product Recommendation for football games

This outsources demo showcases how to create a product recommender system to be used to recommend products to fans during football games.

## Data Preparation

### Baseline Data
The foundational data is available [in this Google Drive folder](https://drive.google.com/drive/folders/1V5q155BDNohhlCbMxIp5YbDMJOThU5kG?usp=sharing); three files are available: 
- `events-raw.csv`
- `matches-raw.csv`
- `customers.csv`

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

### Generate product buying activity
In the `actionable` dataset, persist the result of the query below in a table called `customers-activity`:
```
SELECT
  *
FROM ( /*40% chance buys a product type 1 at the beginning if they have a fav team, and customergroup is 1*/
  SELECT
    a.PlayerID,
    b.id,
    b.time_bucket,
    CASE
      WHEN ROUND(RAND()-0.1) =1 THEN 1
    ELSE
    NULL
  END
    AS product_type
  FROM
    `bq-ml-football.actionable.customers`a,
    `bq-ml-football.curated.features-events-cumulative` b
  WHERE
    a.FavTeam IS NOT NULL
    AND a.CustomerGroup =1
    AND time_bucket =0
    AND (b.home=a.FavTeam
      OR b.away=a.FavTeam)
  UNION ALL
    /*35% chance buys a product type 2 if customergroup is 2,3,4, time bucket between 4 and 17, and there's a penalty either missed or scored*/
  SELECT
    a.PlayerID,
    b.id,
    b.time_bucket,
    CASE
      WHEN ROUND(RAND()-0.15) =1 THEN 2
    ELSE
    NULL
  END
    AS product_type
  FROM
    `bq-ml-football.actionable.customers`a,
    `bq-ml-football.curated.features-events` b
  WHERE
    CustomerGroup IN (2,
      3,
      4)
    AND time_bucket BETWEEN 4
    AND 17
    AND (b.ev_penaltymissed >0
      OR b.ev_penaltyscored>0)
  UNION ALL
    /*40% chance buys a product type 3 if they have a fav team, bet between timebuckets 8 and 14, more than 2 subs and group in 0,1,5*/
  SELECT
    a.PlayerID,
    b.id,
    b.time_bucket,
    CASE
      WHEN ROUND(RAND()-0.1) =1 THEN 3
    ELSE
    NULL
  END
    AS product_type
  FROM
    `bq-ml-football.actionable.customers`a,
    `bq-ml-football.curated.features-events` b
  WHERE
    a.FavTeam IS NOT NULL
    AND a.CustomerGroup IN (0,
      1,
      5)
    AND time_bucket BETWEEN 8
    AND 14
    AND (b.ev_sub >1)
    AND (b.home=a.FavTeam
      OR b.away=a.FavTeam) )
WHERE
  product_type IS NOT NULL
UNION ALL
SELECT
  *
FROM ( /* random products from non-fans*/
  SELECT
    a.PlayerID,
    b.id,
    b.time_bucket,
    CAST (ROUND((RAND()-0.499))* ROUND(1 + RAND() * (2)) AS INT64) AS product_type
  FROM
    `bq-ml-football.actionable.customers`a,
    `bq-ml-football.curated.features-events` b
  WHERE
    a.FavTeam IS NULL )
WHERE
  product_type >0
```

## Model Preparation

### Create the model training data
In the `curated` dataset, persist the result of the query below in a table called `model-training`:

```
SELECT
  * EXCEPT (id)
FROM (
  SELECT
    a.*,
    b.product_type,
    CASE
      WHEN a.home = d.FavTeam THEN 1
    ELSE
    0
  END
    AS favTeamHome,
    CASE
      WHEN a.away = d.FavTeam THEN 1
    ELSE
    0
  END
    AS favTeamAway,
    d.CustomerGroup,
    d.CustomerYears
  FROM
    `bq-ml-football.curated.features-events`a
  JOIN
    `bq-ml-football.actionable.customers-activity` b
  ON
    a.id=b.id
    AND a.time_bucket=b.time_bucket
  JOIN
    `bq-ml-football.actionable.customers` d
  ON
    b.PlayerID=d.PlayerID)
```


### Create a CustomerGroup Classifier model
In the `actionable` dataset, training a KMEANS model called `CustomerGroupClassifier` defined as follows (execution time below 2mins):
```
CREATE OR REPLACE MODEL
  `bq-ml-football.actionable.CustomerGroupClassifier` OPTIONS(MODEL_TYPE='KMEANS',
    num_clusters = 6 ) AS
SELECT
  * EXCEPT(CustomerGroup)
FROM
  `bq-ml-football.curated.model-training`;
```

### Create a Product-Predictor recommender model
In the `actionable` dataset, training a BOOSTED_TREE_CLASSIFIER model called `product-predictor` defined as follows (execution time ~10 mins):
```
CREATE OR REPLACE MODEL `bq-ml-football.actionable.product-predictor`
OPTIONS(MODEL_TYPE='BOOSTED_TREE_CLASSIFIER',
       AUTO_CLASS_WEIGHTS = TRUE,
       BOOSTER_TYPE = 'GBTREE',
       DATA_SPLIT_METHOD = 'AUTO_SPLIT',
       NUM_PARALLEL_TREE = 1,
       MAX_ITERATIONS = 50,
       TREE_METHOD = 'AUTO',
       EARLY_STOP = FALSE,
       SUBSAMPLE = 0.85,
       INPUT_LABEL_COLS = ['product_type'])
AS SELECT * FROM `bq-ml-football.curated.model-training`;
```
The resulting recommender model should perform roughly like this:
![Model Evaluation](https://github.com/AndreUanKenobi/bqml-sportsbetting-demo/blob/main/productpredictorstats.png?raw=true)


## Demo execution

### Classify customers in groups
Test the *CustomerGroup Classifier* model by running the following query:

```
SELECT
*
FROM
ML.PREDICT(MODEL `bq-ml-football.actionable.CustomerGroupClassifier`,
  (
  SELECT
    * except(CustomerGroup)
  FROM
    `bq-ml-football.curated.model-training`))
```

**ATTENTION: for simplicity, the query above uses the model training data in input**



### Recommend Products
Test the *Product-Predictor* model by running the following query:

```
SELECT
*
FROM
ML.PREDICT(MODEL `bq-ml-football.actionable.product-predictor`,
  (
  SELECT
    * except(product_type)
  FROM
    `bq-ml-football.curated.model-training`))
```

**ATTENTION: for simplicity, the query above uses the model training data in input**
