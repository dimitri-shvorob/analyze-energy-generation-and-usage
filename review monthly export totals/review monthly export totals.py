from pathlib import Path

import polars as pl

PATH = Path(r"C:\Users\dimit\Documents\GitHub")

# monthly totals from app
x = pl.read_csv(PATH / "analyze-energy-generation-and-usage" / "review monthly export totals" / "data app.csv", try_parse_dates=True)

# monthly totals from API
y = (
    pl.read_parquet(PATH / "get-octopus-energy-usage-data" / "data" / "*.parquet")
    .filter(pl.col("type") == "electric_export")
    .with_columns(start_time=pl.col("start_time").dt.replace_time_zone(time_zone=None).dt.date())
    .with_columns(month = pl.col("start_time").dt.truncate("1mo"))
    .group_by("month").agg(pl.col("value").sum().alias("value_api"))
)

z = (
      x.join(y, on = "month", how = "outer", coalesce = True)
      .with_columns(value_api_shortfall = pl.col("value") - pl.col("value_api"))
      .sort("month")
)
z.write_csv(PATH / "analyze-energy-generation-and-usage" / "review monthly export totals" / "data combined.csv")


