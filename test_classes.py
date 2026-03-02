from classes import Battery


def test_battery_charging_small() -> None:
    b = Battery(10, 5)
    s = b.transform_surplus(4)
    assert b.charge == 9
    assert s == 0


def test_battery_charging_large() -> None:
    b = Battery(10, 5)
    s = b.transform_surplus(6)
    assert b.charge == 10
    assert s == 1


def test_battery_discharging_small() -> None:
    b = Battery(10, 5)
    s = b.transform_surplus(-3)
    assert b.charge == 2
    assert s == 0


def test_battery_discharging_large() -> None:
    b = Battery(10, 5)
    s = b.transform_surplus(-6)
    assert b.charge == 0
    assert s == -1


def test_zero_capacity_battery_charging() -> None:
    b = Battery(0, 0)
    s = b.transform_surplus(4)
    assert b.charge == 0
    assert s == 4


def test_zero_capacity_battery_discharging() -> None:
    b = Battery(0, 0)
    s = b.transform_surplus(-4)
    assert b.charge == 0
    assert s == -4
