import copy
import datetime
from pathlib import Path

import polars as pl

from classes import Battery

PATH = Path(r"C:\Users\dimit\Documents\GitHub\analyze-energy-generation-and-usage")

df = pl.read_parquet(PATH / "data combined - quantities and tariffs.parquet")

BATTERY_TOPUP_COST_PER_KWH = 0.07

DATES_BAD_OCTOPUS_DATA = [datetime.date(2025, 3, 30)]

rr = []
for b in [Battery(0, 0), Battery(5, 0)]:
    for x in df.sort("date", "start_time").partition_by(by="date", maintain_order=True, as_dict=True).values():
        date = x["date"][0]
        print(date)
        is_hypo_calc_feasible = x["is_generation_data_available"][0] == 1 & (date not in DATES_BAD_OCTOPUS_DATA)
        if is_hypo_calc_feasible:
            battery_starting_charge = b.charge
            battery_topup = b.capacity - b.charge
            _ = b.transform_net_supply(battery_topup)
        for r in x.rows(named=True):
            row = copy.deepcopy(r)
            row["scenario"] = f"{b.capacity} kWh battery"
            row["value_gbp_gas"] = None if row["gas"] is None else (row["gas_fixed_charge"] + row["gas"] * row["gas_charge_per_kwh"])
            row["value_gbp_electric_import"] = None if row["electric_import"] is None else (row["electric_import_fixed_charge"] + row["electric_import"] * row["electric_import_charge_per_kwh"])
            row["value_gbp_electric_export"] = None if row["electric_export"] is None else (row["electric_export"] * row["electric_export_credit_per_kwh"])
            row["value_gbp_electric_import_less_export"] = None if row["value_gbp_electric_import"] is None or row["value_gbp_electric_export"] is None else (row["value_gbp_electric_import"] - row["value_gbp_electric_export"])
            # hypothetical calcs - battery
            if is_hypo_calc_feasible:
                if row["start_time"].hour == 0 and row["start_time"].minute == 0:
                    row["hypo_electric_import_battery_topup"] = battery_topup
                    row["hypo_value_gbp_electric_import"] = battery_topup * BATTERY_TOPUP_COST_PER_KWH
                else:
                    row["hypo_electric_import_battery_topup"] = 0
                    row["hypo_value_gbp_electric_import"] = 0
                #
                row["hypo_electric_generation_less_consumption_before_battery"] = row["electric_generation"] - row["electric_consumption"]
                row["hypo_battery_charge_before"] = b.charge
                row["hypo_electric_generation_less_consumption_after_battery"] = b.transform_net_supply(
                    row["hypo_electric_generation_less_consumption_before_battery"]
                )
                row["hypo_battery_charge_after"] = b.charge
                if row["hypo_electric_generation_less_consumption_after_battery"] > 0:
                    row["hypo_electric_export"] = row["hypo_electric_generation_less_consumption_after_battery"]
                    row["hypo_electric_import"] = 0
                else:
                    row["hypo_electric_export"] = 0
                    row["hypo_electric_import"] = -row["hypo_electric_generation_less_consumption_after_battery"]
                row["hypo_value_gbp_electric_import"] =  row["hypo_value_gbp_electric_import"] + (
                    row["electric_import_fixed_charge"]
                    + row["hypo_electric_import"]
                    * row["electric_import_charge_per_kwh"]
                )
                row["hypo_value_gbp_electric_export"] = (
                    row["hypo_electric_export"] * row["electric_export_credit_per_kwh"]
                )
                row["hypo_value_gbp_electric_import_less_export"] = row["hypo_value_gbp_electric_import"] - row["hypo_value_gbp_electric_export"]
            else:
                row["hypo_electric_import_battery_topup"] = None
                row["hypo_battery_charge_before"] = None
                row["hypo_battery_charge_after"] = None
                row["hypo_electric_generation_less_consumption_after_battery"] = None
                row["hypo_electric_generation_less_consumption_before_battery"] = None
                row["hypo_electric_export"] = None
                row["hypo_electric_import"] = None
                row["hypo_value_gbp_electric_import"] = None
                row["hypo_value_gbp_electric_export"] = None
                row["hypo_value_gbp_electric_import_less_export"] = None
            rr.append(row)

y = pl.DataFrame(rr, infer_schema_length = len(rr)).sort("start_time")
y.write_csv(PATH / "data combined - final.csv")
y.write_parquet(PATH / "data combined - final.parquet")
