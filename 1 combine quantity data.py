import datetime
from pathlib import Path

import polars as pl

PATH = Path(r"C:\Users\dimit\Documents\GitHub")

# Octopus uses 39.0-39.2 in different months -> cannot replicate exactly, but 39.1 is a reasonable average
GAS_KWH_PER_CUBIC_METER = 1.02264*39.1 /3.6

# Solis - generation for 30-min intervals
ds = (
    pl.read_parquet(
        PATH / "get-solis-cloud-generation-data" / "data" / "*.parquet",
        include_file_paths="file",
    )
    .with_columns(
        file=pl.col("file")
        .str.split(by="\\", inclusive=False)
        .list.last()
        .str.replace(".parquet", "")
    )
    .with_columns(time=pl.col("timeStr").str.to_datetime())
    .with_columns(date=pl.col("time").dt.truncate("1d").dt.date())
    .with_columns(start_time=pl.col("time").dt.truncate("30m"))
    .drop("timeStr")
)
dfs = []
for x in ds.partition_by(by="date", maintain_order=True, as_dict=True).values():
    g = (
        x.with_columns(value_change=pl.col("eToday").diff())
        .group_by("date", "start_time")
        .agg(electric_generation=pl.col("value_change").sum())
        .sort("start_time")
    )
    dfs.append(g)

dt = pl.concat(dfs)

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

df = (
      do.join(dt, on=["date", "start_time"], how="full", coalesce=True)
      .sort("start_time")
      .with_columns(is_generation_data_available = pl.col("date").is_in(dd).cast(pl.Int32))
      .with_columns(electric_generation = pl.when(pl.col("date") < datetime.date(2025, 3, 7))
          .then(0)
          .otherwise(pl.when(pl.col("is_generation_data_available") == 1).then(pl.col("electric_generation").fill_null(0)).otherwise(None))
      )
      .with_columns(electric_export = pl.when(pl.col("date") < datetime.date(2025, 3, 7))
          .then(0)
          .otherwise(pl.when(pl.col("is_generation_data_available") == 1).then(pl.col("electric_export").fill_null(0)).otherwise(None))
      )
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
