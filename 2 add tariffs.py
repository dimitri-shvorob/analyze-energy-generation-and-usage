import datetime
from pathlib import Path

import polars as pl

from classes import TariffHistory

PATH = Path(r"C:\Users\dimit\Documents\GitHub\analyze-energy-generation-and-usage")

start_date = datetime.date(2020, 1, 1)
end_date = datetime.date(2026, 12, 31)

# tariffs - actual values from Jan 2025
# electricity
the = TariffHistory("electricity import")
for t in [
    ("2020-01-01", 0.3765, 0.2482),
    ("2025-01-01", 0.3765, 0.2482),
    ("2025-04-01", 0.4269, 0.2522),
    ("2025-07-01", 0.4076, 0.2393),
    ("2025-10-01", 0.4295, 0.2441),
    ("2026-01-01", 0.4360, 0.2571),
]:
    the.add_tariff(t[0], t[1], t[2])
the.validate_over_date_range(start_date, end_date)


# electricity export
thx = TariffHistory("electricity export")
for t in [("2020-01-01", 0, 0.15), ("2026-04-01", 0, 0.12)]:
    thx.add_tariff(t[0], t[1], t[2])
the.validate_over_date_range(start_date, end_date)

# gas
thg = TariffHistory("gas")
for t in [("2020-01-01", 0.2852, 0.061),
          ("2025-01-01", 0.2852, 0.061),
          ("2025-07-01", 0.2783, 0.0612),
          ("2025-10-01", 0.3201, 0.0608),
          ("2026-01-01", 0.3265, 0.0574),
          ]:
    thg.add_tariff(t[0], t[1], t[2])
thg.validate_over_date_range(start_date, end_date)


df = pl.read_parquet(PATH / "data combined - quantities.parquet")

dfs = []
for x in df.partition_by(by="date", maintain_order=True, as_dict=True).values():
    date = x["date"][0]
    tg = thg.get_tariff_for_date(date)
    te = the.get_tariff_for_date(date)
    tx = thx.get_tariff_for_date(date)
    if len(x) != 48:
        print(f"{len(x)} rows for {date}")
    df = x.with_columns(
        gas_daily_charge=1.05 * tg["daily_charge"],
        gas_fixed_charge = 1.05 * tg["daily_charge"] / 48,
        gas_charge_per_kwh = 1.05 * tg["charge_per_kwh"],
        electric_import_daily_charge=1.05 * te["daily_charge"],
        electric_import_fixed_charge=1.05 * te["daily_charge"] / 48,
        electric_import_charge_per_kwh =1.05 * te["charge_per_kwh"],
        electric_export_credit_per_kwh= tx["charge_per_kwh"],
    )
    dfs.append(df)

df = pl.concat(dfs)
df.write_parquet(PATH / "data combined - quantities and tariffs.parquet")
df.write_csv(PATH / "data combined - quantities and tariffs.csv")
