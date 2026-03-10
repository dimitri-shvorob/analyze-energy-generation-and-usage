import datetime
from pathlib import Path

import polars as pl

PATH = Path(r"C:\Users\dimit\Documents\GitHub")

# Octopus uses 39.0-39.2 in different months -> cannot replicate exactly, but 39.1 is a reasonable average
GAS_KWH_PER_CUBIC_METER = 1.02264*39.1 /3.6

# Solis - generation, cumulative totals for 5-min intervals
ds = (
    pl.read_parquet(
        PATH / "get-solis-cloud-generation-data" / "data" / "*.parquet"
    )
    .with_columns(time=pl.col("timeStr").str.to_datetime())
    .with_columns(date=pl.col("time").dt.truncate("1d").dt.date())
    .drop("timeStr")
)

# convert to generation-over-30-min values
dfs = []
for x in ds.partition_by(by="date", as_dict=True).values():
    date = x["date"][0]
    g = pl.DataFrame({"date": date, "mins_since_midnight": pl.arange(0.0, 1441.0, eager = True).cast(pl.Float64)})
    # initial and ending row
    r = pl.DataFrame({"mins_since_midnight": [0.0, 1440.0], "eToday": [0.0, x["eToday"].max()]})
    y = (
        x.select("time", "eToday")
        .with_columns(mins_since_midnight = 60*pl.col("time").dt.hour().cast(pl.Float64) + pl.col("time").dt.minute().cast(pl.Float64))
        .drop("time")
        )
    z = (
        g.join(pl.concat([r, y], how = "diagonal"), on="mins_since_midnight", how="left")
        .sort("mins_since_midnight")
        .with_columns(cum_value = pl.col("eToday").interpolate(method="linear"))
        .filter(pl.col("mins_since_midnight") % 30 == 0)
        .with_columns(electric_generation = pl.col("cum_value").shift(-1) - pl.col("cum_value"))
        .with_columns(start_time = pl.col("date").cast(pl.Datetime) + pl.duration(minutes=pl.col("mins_since_midnight")))
        .filter(pl.col("mins_since_midnight") < 1440)
        .select("date", "start_time", "electric_generation")
    )
    dfs.append(z)
dt = pl.concat(dfs)
# check for duplicates
de = dt.filter(pl.col("start_time").is_duplicated())
assert de.is_empty(), "Duplicates found in Solis data"

# count of records per day
dg = de.group_by("date").agg(pl.count("start_time").alias("count")).filter(pl.col("count") != 48)
assert dg.is_empty(), "Incomplete data found in Solis data"

# dates present in solis data
dd = dt.select(pl.col("date").unique()).to_series().to_list()

# Octopus - convert gas usage to kWh
do = (
    pl.read_parquet(PATH / "get-octopus-energy-usage-data" / "data" / "*.parquet")
    .with_columns(start_time=pl.col("start_time").dt.replace_time_zone(time_zone=None))
    .unique(subset=["start_time", "type"])
    .with_columns(value_kwh = pl.when(pl.col("type") != "gas").then(pl.col("value")).otherwise(GAS_KWH_PER_CUBIC_METER * pl.col("value")))
    .pivot("type", index="start_time", values="value_kwh", aggregate_function="min")
    .with_columns(date=pl.col("start_time").dt.truncate("1d").dt.date())
)
# check for duplicates
de = do.filter(pl.col("start_time").is_duplicated())
assert de.is_empty(), "Duplicates found in Octopus data"

# count of records per day
dg = do.group_by("date").agg(pl.count("start_time").alias("count")).filter(pl.col("count") != 48)
assert dg.is_empty(), "Incomplete data found in Octopus data"

# combine
# note "magic date"
df = (
      do.join(dt, on=["date", "start_time"], how="full", coalesce=True)
      .sort("start_time")
      .with_columns(is_generation_data_available = pl.col("date").is_in(dd).cast(pl.Int32))
      .with_columns(electric_generation = pl.when(pl.col("date") < datetime.date(2025, 3, 7))
          .then(0)
          .otherwise(pl.col("electric_generation")))
      .with_columns(electric_export = pl.when(pl.col("date") < datetime.date(2025, 3, 7))
          .then(0)
          .otherwise(pl.col("electric_export")))
      .with_columns(electric_consumption = pl.col("electric_import") + pl.col("electric_generation") - pl.col("electric_export"))
      .with_columns(
        month = pl.col("date").dt.truncate("1mo"),
        month_short = pl.col("date").dt.month(),
        year = pl.col("date").dt.year()
      )
)

df.write_parquet(
    PATH / "analyze-energy-generation-and-usage" / "data combined - quantities.parquet"
)
df.write_csv(
    PATH / "analyze-energy-generation-and-usage" / "data combined - quantities.csv"
)
