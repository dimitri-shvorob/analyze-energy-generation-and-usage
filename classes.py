import datetime

import polars as pl


class TariffHistory:
    def __init__(self, label: str) -> None:
        self.label = label
        self.data = {}

    def add_tariff(
        self,
        effective_date_iso: str,
        daily_charge: float,
        charge_per_kwh: float,
    ) -> None:
        effective_date = datetime.date.fromisoformat(effective_date_iso)
        self.data[effective_date] = {
            "daily_charge": daily_charge,
            "charge_per_kwh": charge_per_kwh,
        }

    def get_tariff_for_date(self, date: datetime.date) -> dict:
        selected_effective_date = datetime.date(1900, 1, 1)
        for effective_date in self.data:
            if effective_date <= date and effective_date >= selected_effective_date:
                selected_effective_date = effective_date
        if selected_effective_date == datetime.date(1900, 1, 1):
            raise ValueError(f"No {self.label} tariff found for {date}.")
        return self.data[selected_effective_date]

    def validate_over_date_range(
        self, start_date: datetime.date, end_date: datetime.date
    ) -> None:
        if start_date > end_date:
            raise ValueError("Start date must be before end date.")
        for date in pl.date_range(start_date, end_date, eager = True):
            self.get_tariff_for_date(date)


class Battery:
    def __init__(self, capacity: float, charge: float) -> None:
        if capacity < 0:
            raise ValueError("Capacity must be non-negative.")
        if charge < 0:
            raise ValueError("Charge must be non-negative.")
        if charge > capacity:
            raise ValueError("Charge cannot exceed capacity.")
        self.capacity = capacity
        self.charge = charge

    def transform_net_supply(self, net_supply: float) -> float:
        potential_charge = self.charge + net_supply
        if net_supply > 0:  # charging
            if potential_charge > self.capacity:  # charging over capacity
                self.charge = self.capacity
                return potential_charge - self.capacity
            else:
                self.charge = potential_charge  # charging under capacity - no excess
                return 0
        elif net_supply < 0:  # discharging
            if potential_charge < 0:  # need more than stored
                self.charge = 0
                return potential_charge
            else:
                self.charge = potential_charge
                return 0
        else:
            return 0
