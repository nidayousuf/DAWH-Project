"""Data oversampling layer: synthesises a new batch of records for ALL 7 source files.

Method (per refresh = one quarter of new business):
  * Seasonal bootstrap - new rows are sampled from the SAME calendar month one year earlier,
    so monthly seasonality, channel mix, medicine mix and pharmacy mix are preserved.
  * Trend - volumes are scaled by the observed year-on-year growth of the history
    (sales 2024 -> 2025 = +9.1%) with +/-3% random noise per month.
  * Time shift - every date/timestamp moves forward by whole years, then dependent fields are
    recomputed (days-to-expiry, amounts, stock balances).
  * Identity - new primary keys continue each source's sequence; foreign keys between the
    files (batch -> shipment -> lot -> sale) are offset consistently so lineage still joins.
  * SCD events - refreshes change a few master-data prices / classes so Type 2 history
    appears in Dim_Medicine.
  * Data-quality noise - about 0.2% of sales/IoT rows are deliberately corrupted to prove the
    validation layer rejects them.

Usage:
    python -m generator.generate --refresh 1     # Jan-Mar 2026 -> data/landing + data/stream
    python -m generator.generate --refresh 2     # Apr-Jun 2026
"""
import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from common import config

SEED = config.SEED_DIR
STREAM_DIR = config.DATA_DIR / "stream"
REPORT_DIR = config.DATA_DIR / "generator_reports"
for d in (STREAM_DIR, REPORT_DIR):
    d.mkdir(parents=True, exist_ok=True)

FILES = {
    "master": "01_master_data", "manufacturing": "02_manufacturing_data",
    "delivery": "03_manufacturer_delivery_data", "warehouse": "04_warehouse_data",
    "sales": "05_pharmacy_sales_data", "demand": "06_hospital_demand_data",
    "iot": "07_iot_streaming_data",
}

# Each refresh = one quarter. Base year supplies the seasonal template.
REFRESHES = {
    1: dict(start="2026-01-01", months=3, base_year=2025, shift_years=1),
    2: dict(start="2026-04-01", months=3, base_year=2025, shift_years=1),
    3: dict(start="2026-07-01", months=3, base_year=2025, shift_years=1),
    4: dict(start="2026-10-01", months=3, base_year=2025, shift_years=1),
}

# Master-data changes. Presc.Class and Pack.Size are SCD Type 2 attributes of DimMedicine,
# so these events create new versions; the price changes flow into the sales amounts.
SCD_EVENTS = {
    1: [dict(ids=["MED037"], set={"PrescriptionClass": "OTC"},
             note="Omeprazole Rx-to-OTC switch (Presc.Class, SCD Type 2)"),
        dict(ids=["MED007", "MED008", "MED009"], wholesale_pct=6.0, retail_pct=6.0,
             note="Cold-chain price rise +6% (affects sales amounts)")],
    2: [dict(ids=["MED055"], set={"PackSize": "24"}, wholesale_pct=20.0, retail_pct=20.0,
             note="Paracetamol pack size 20 -> 24 with +20% price (Pack.Size, SCD Type 2)")],
}

GROWTH = 1.091          # sales YoY 2024->2025 (180k / 165k rows)
NOISE = 0.03
CORRUPT_RATE = 0.002


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _load(name: str, prefer_latest=True) -> pd.DataFrame:
    return pd.read_csv(SEED / f"{name}.csv", dtype=str, keep_default_na=False)


def _shift_date_col(df, col, years, fmt):
    s = pd.to_datetime(df[col], format=fmt, errors="coerce")
    shifted = s + pd.DateOffset(years=years)
    out = shifted.dt.strftime(fmt)
    if fmt == "%m/%d/%Y":   # source uses M/D/YYYY without leading zeros
        out = shifted.dt.month.astype("Int64").astype(str) + "/" + \
              shifted.dt.day.astype("Int64").astype(str) + "/" + shifted.dt.year.astype("Int64").astype(str)
    df[col] = out.where(s.notna(), df[col])


def _shift_ts_col(df, col, years):
    s = pd.to_datetime(df[col], errors="coerce", utc=True)
    df[col] = (s + pd.DateOffset(years=years)).dt.strftime("%Y-%m-%dT%H:%M:%SZ").where(s.notna(), df[col])


def _offset_id(series: pd.Series, offset: int, new_prefix=None) -> pd.Series:
    """'WHLOT-0000658' + 5000 -> 'WHLOT-0005658' (keeps format and zero padding)."""
    def f(v):
        m = re.match(r"^(.*?)(\d+)$", v)
        if not m:
            return v
        pre, num = m.groups()
        if new_prefix:
            pre = new_prefix(pre)
        return f"{pre}{int(num) + offset:0{len(num)}d}"
    return series.map(f)


def _seq_ids(prefix: str, start: int, n: int, width: int) -> list:
    return [f"{prefix}{i:0{width}d}" for i in range(start, start + n)]


def _max_num(series: pd.Series) -> int:
    return int(series.str.extract(r"(\d+)$")[0].astype(float).max())


def _month_list(start, months):
    first = pd.Timestamp(start)
    return [first + pd.DateOffset(months=i) for i in range(months)]


class State:
    def __init__(self):
        p = config.GENERATOR_STATE
        self.d = json.loads(p.read_text()) if p.exists() else {"done": [], "offsets": {}}

    def save(self):
        config.GENERATOR_STATE.write_text(json.dumps(self.d, indent=2))


def generate(refresh: int, seed: int = 42, out_dir: Path = None):
    cfg = REFRESHES[refresh]
    rng = np.random.default_rng(seed + refresh)
    out_dir = out_dir or config.LANDING_DIR
    months = _month_list(cfg["start"], cfg["months"])
    shift = cfg["shift_years"]
    tag = f"refresh_{refresh}"
    period = f"{months[0]:%Y-%m}_to_{months[-1]:%Y-%m}"
    state = State()
    stats = {"refresh": refresh, "label": tag, "period": period,
             "generated_at": datetime.now(timezone.utc).isoformat(), "files": {}}
    # ID offsets: make every refresh's foreign keys unique but consistent across files
    off = 100000 * refresh

    # ---------------- 01 master (full snapshot + SCD events) ----------------
    master = _load(FILES["master"])
    if state.d.get("master"):                       # continue from the last refresh's master
        master = pd.DataFrame(state.d["master"])[master.columns]
    changed = []
    for ev in SCD_EVENTS.get(refresh, []):
        mask = master.MedicineID.isin(ev["ids"])
        if "wholesale_pct" in ev:
            for col, pct in (("WholesalePriceUSD", ev["wholesale_pct"]),
                             ("RecommendedRetailPriceUSD", ev["retail_pct"])):
                master.loc[mask, col] = (_num(master.loc[mask, col]) * (1 + pct / 100)).round(2).astype(str)
        for col, val in ev.get("set", {}).items():
            master.loc[mask, col] = val
        master.loc[mask, "EffectiveFromDate"] = cfg["start"]
        changed.append({"medicines": ev["ids"], "change": ev["note"]})
    state.d["master"] = master.to_dict("records")
    price = master.set_index("MedicineID")[["WholesalePriceUSD", "RecommendedRetailPriceUSD"]].apply(_num)

    def month_mask(df, col, fmt):
        d = pd.to_datetime(df[col], format=fmt, errors="coerce")
        return d, [(d.dt.year == cfg["base_year"]) & (d.dt.month == m.month) for m in months]

    def sample_months(df, col, fmt, scale):
        """seasonal bootstrap: n_new(month) = n_base(month) x growth x noise"""
        _, masks = month_mask(df, col, fmt)
        parts = []
        for mk in masks:
            base = df[mk]
            if base.empty:
                continue
            n = int(round(len(base) * scale * (1 + rng.uniform(-NOISE, NOISE))))
            idx = rng.choice(base.index.values, size=n, replace=n > len(base))
            parts.append(df.loc[idx])
        return pd.concat(parts, ignore_index=True)

    # ---------------- 02 manufacturing ----------------
    mfg = _load(FILES["manufacturing"])
    new_mfg = sample_months(mfg, "OrderDate", "%Y-%m-%d", GROWTH)
    old_batch = new_mfg.ProductionBatchID.copy()
    old_mo = new_mfg.ManufacturingOrderID.copy()
    new_mfg["ManufacturingOrderID"] = _seq_ids(f"MO-{months[0].year}-", 1 + state.d["offsets"].get("mo", 0),
                                               len(new_mfg), 5)
    state.d["offsets"]["mo"] = state.d["offsets"].get("mo", 0) + len(new_mfg)
    yy = str(months[0].year)[2:]
    new_mfg["ProductionBatchID"] = _offset_id(old_batch, off,
                                              lambda p: re.sub(r"^B\d\d", f"B{yy}", p))
    batch_map = dict(zip(old_batch, new_mfg.ProductionBatchID))
    mo_map = dict(zip(old_mo, new_mfg.ManufacturingOrderID))
    keep = ~new_mfg.ProductionBatchID.duplicated()
    new_mfg = new_mfg[keep].reset_index(drop=True)
    for c in ["OrderDate", "PlannedStartDate", "PlannedCompletionDate", "ActualStartDate",
              "ActualCompletionDate", "QualitySampleDate", "BatchReleaseDate", "ExpiryDate"]:
        _shift_date_col(new_mfg, c, shift, "%Y-%m-%d")
    _shift_ts_col(new_mfg, "RecordCreatedTimestampUTC", shift)

    # ---------------- 03 manufacturer deliveries (follow the new batches) ----------------
    dlv = _load(FILES["delivery"])
    new_dlv = dlv[dlv.ProductionBatchID.isin(batch_map.keys())].copy()
    new_dlv["ProductionBatchID"] = new_dlv.ProductionBatchID.map(batch_map)
    new_dlv["ManufacturingOrderID"] = new_dlv.ManufacturingOrderID.map(mo_map).fillna(
        new_dlv.ManufacturingOrderID)
    new_dlv["ShipmentID"] = _offset_id(new_dlv.ShipmentID, off)
    new_dlv["SSCC18"] = _offset_id(new_dlv.SSCC18, off * 10)
    new_dlv["SealNumber"] = _offset_id(new_dlv.SealNumber, off)
    for c in ["DispatchDate", "PromisedDeliveryDate", "ActualDeliveryDate"]:
        _shift_date_col(new_dlv, c, shift, "%Y-%m-%d")

    # ---------------- 04 warehouse daily lot snapshots ----------------
    wh = _load(FILES["warehouse"])
    new_wh = sample_months(wh, "SnapshotDate", "%Y-%m-%d", 1.0)
    new_wh = new_wh.drop_duplicates()
    start_inv = _max_num(wh.InventorySnapshotID) + 1 + state.d["offsets"].get("inv", 0)
    new_wh["InventorySnapshotID"] = _seq_ids("INV-", start_inv, len(new_wh), 9)
    state.d["offsets"]["inv"] = state.d["offsets"].get("inv", 0) + len(new_wh)
    new_wh["WarehouseLotID"] = _offset_id(new_wh.WarehouseLotID, off)
    new_wh["InboundShipmentID"] = _offset_id(new_wh.InboundShipmentID, off)
    new_wh["ProductionBatchID"] = _offset_id(new_wh.ProductionBatchID, off,
                                             lambda p: re.sub(r"^B\d\d", f"B{yy}", p))
    for c in ["SnapshotDate", "BatchExpiryDate"]:
        _shift_date_col(new_wh, c, shift, "%Y-%m-%d")

    # ---------------- 05 pharmacy sales ----------------
    sales = _load(FILES["sales"])
    new_s = sample_months(sales, "SalesDate", "%m/%d/%Y", GROWTH)
    n = len(new_s)
    start_sale = _max_num(sales.SalesTransactionID) + 1 + state.d["offsets"].get("sale", 0)
    new_s["SalesTransactionID"] = _seq_ids("SALE-", start_sale, n, 9)
    state.d["offsets"]["sale"] = state.d["offsets"].get("sale", 0) + n
    rx = new_s.PrescriptionID != ""
    start_rx = _max_num(sales.PrescriptionID[sales.PrescriptionID != ""]) + 1 + state.d["offsets"].get("rx", 0)
    new_s.loc[rx, "PrescriptionID"] = _seq_ids("RX-", start_rx, int(rx.sum()), 9)
    state.d["offsets"]["rx"] = state.d["offsets"].get("rx", 0) + int(rx.sum())
    for c in ["PharmacyOrderID", "PharmacyDeliveryID", "WarehouseLotID"]:
        new_s[c] = _offset_id(new_s[c], off)
    new_s["ProductionBatchID"] = _offset_id(new_s.ProductionBatchID, off,
                                            lambda p: re.sub(r"^B\d\d", f"B{yy}", p))
    for c in ["SalesDate", "PharmacyDeliveryDate", "BatchExpiryDate"]:
        _shift_date_col(new_s, c, shift, "%m/%d/%Y")
    _shift_ts_col(new_s, "SalesTimestampUTC", shift)
    # recompute money with the CURRENT master prices (so SCD price changes flow into sales)
    qty = _num(new_s.QuantitySoldPacks)
    retail = new_s.MedicineID.map(price.RecommendedRetailPriceUSD)
    wholesale = new_s.MedicineID.map(price.WholesalePriceUSD)
    disc_rate = (_num(new_s.DiscountAmountUSD) / _num(new_s.GrossSalesAmountUSD)).fillna(0)
    tax_rate = (_num(new_s.SalesTaxUSD) / _num(new_s.NetSalesAmountUSD)).fillna(0)
    gross = (qty * retail).round(2)
    disc = (gross * disc_rate).round(2)
    net = (gross - disc).round(2)
    cogs = (qty * wholesale).round(2)
    new_s["UnitRetailPriceUSD"] = retail.round(2).astype(str)
    new_s["GrossSalesAmountUSD"] = gross.astype(str)
    new_s["DiscountAmountUSD"] = disc.astype(str)
    new_s["NetSalesAmountUSD"] = net.astype(str)
    new_s["SalesTaxUSD"] = (net * tax_rate).round(2).astype(str)
    new_s["CostOfGoodsSoldUSD"] = cogs.astype(str)
    new_s["GrossMarginUSD"] = (net - cogs).round(2).astype(str)
    dte = (pd.to_datetime(new_s.BatchExpiryDate, format="%m/%d/%Y")
           - pd.to_datetime(new_s.SalesDate, format="%m/%d/%Y")).dt.days
    new_s["DaysToExpiryAtSale"] = dte.astype(str)
    new_s = new_s.sort_values("SalesTimestampUTC").reset_index(drop=True)
    new_s["SalesTransactionID"] = _seq_ids("SALE-", start_sale, n, 9)
    # deliberate data-quality defects (validation must catch them)
    k = max(5, int(n * CORRUPT_RATE))
    bad = rng.choice(new_s.index.values, size=k, replace=False)
    kinds = ["neg_qty", "null_sku", "bad_date", "zero_price", "dup_id"]
    defects = {}
    for i, idx in enumerate(bad):
        kind = kinds[i % len(kinds)]
        defects[kind] = defects.get(kind, 0) + 1
        if kind == "neg_qty":
            new_s.at[idx, "QuantitySoldPacks"] = "-1"
        elif kind == "null_sku":
            new_s.at[idx, "MedicineID"] = ""
        elif kind == "bad_date":
            new_s.at[idx, "SalesDate"] = "13/45/2026"
        elif kind == "zero_price":
            new_s.at[idx, "UnitRetailPriceUSD"] = "0"
        elif kind == "dup_id":
            new_s.at[idx, "SalesTransactionID"] = new_s.at[max(idx - 1, 0), "SalesTransactionID"]

    # ---------------- 06 hospital demand (full grid: 12 hospitals x 63 medicines x month) ----
    dem = _load(FILES["demand"])
    dmon = pd.to_datetime(dem.DemandMonth)
    prev_close = None
    last_hist = dem[dmon == dmon.max()].set_index(["HospitalID", "MedicineID"]).ClosingStockPacks
    if state.d.get("demand_closing"):
        last_hist = pd.Series({tuple(k.split("|")): v for k, v in state.d["demand_closing"].items()})
    prev_close = _num(last_hist)
    parts = []
    start_hd = _max_num(dem.HospitalDemandRecordID) + 1 + state.d["offsets"].get("hdem", 0)
    flow = ["EstimatedDiseaseCases", "DiseaseRelatedAdmissions", "OutpatientEncounters",
            "ForecastDemandPacks", "RequestedQuantityPacks", "ApprovedQuantityPacks",
            "ReceivedQuantityPacks", "EmergencyRequestQuantityPacks", "DispensedQuantityPacks",
            "ConsumedQuantityPacks"]
    for m in months:
        base = dem[(dmon.dt.year == cfg["base_year"]) & (dmon.dt.month == m.month)].copy()
        f = GROWTH * (1 + rng.uniform(-NOISE, NOISE, size=len(base)))
        for c in flow:
            base[c] = (np.round(_num(base[c]).values * f)).astype(int).astype(str)
        key = list(zip(base.HospitalID, base.MedicineID))
        opening = pd.Series([prev_close.get(k, np.nan) for k in key], index=base.index)
        opening = opening.fillna(_num(base.OpeningStockPacks))
        closing = (opening + _num(base.ReceivedQuantityPacks) - _num(base.ConsumedQuantityPacks)
                   - _num(base.WastageQuantityPacks)).clip(lower=0)
        base["OpeningStockPacks"] = opening.astype(int).astype(str)
        base["ClosingStockPacks"] = closing.astype(int).astype(str)
        base["DemandMonth"] = m.strftime("%Y-%m-%d")
        prev_close = pd.Series(closing.values, index=pd.MultiIndex.from_tuples(key))
        parts.append(base)
    new_d = pd.concat(parts, ignore_index=True)
    new_d["HospitalDemandRecordID"] = _seq_ids("HDEM-", start_hd, len(new_d), 8)
    state.d["offsets"]["hdem"] = state.d["offsets"].get("hdem", 0) + len(new_d)
    state.d["demand_closing"] = {f"{h}|{md}": float(v) for (h, md), v in prev_close.items()}

    # ---------------- 07 IoT sensor stream ----------------
    iot = _load(FILES["iot"])
    ets = pd.to_datetime(iot.EventTimestampUTC, utc=True)
    parts = []
    for m in months:
        base = iot[(ets.dt.year == cfg["base_year"]) & (ets.dt.month == m.month)]
        parts.append(base)
    new_i = pd.concat(parts, ignore_index=True).copy()
    _shift_ts_col(new_i, "EventTimestampUTC", shift)
    _shift_ts_col(new_i, "IngestionTimestampUTC", shift)
    new_i["TemperatureC"] = (_num(new_i.TemperatureC) + rng.normal(0, 0.15, len(new_i))).round(2).astype(str)
    start_io = _max_num(iot.SensorReadingID) + 1 + state.d["offsets"].get("iot", 0)
    new_i["SensorReadingID"] = _seq_ids("IOTR-", start_io, len(new_i), 10)
    state.d["offsets"]["iot"] = state.d["offsets"].get("iot", 0) + len(new_i)
    new_i["ShipmentID"] = new_i.ShipmentID.where(new_i.ShipmentID == "", _offset_id(new_i.ShipmentID, off))
    kb = max(5, int(len(new_i) * CORRUPT_RATE))
    bad_i = rng.choice(new_i.index.values, size=kb, replace=False)
    for j, idx in enumerate(bad_i):
        if j % 2 == 0:
            new_i.at[idx, "TemperatureC"] = "ERR"
        else:
            new_i.at[idx, "DeviceID"] = ""

    # ---------------- write files ----------------
    outputs = {"master": master, "manufacturing": new_mfg, "delivery": new_dlv,
               "warehouse": new_wh, "sales": new_s, "demand": new_d}
    for key, df in outputs.items():
        path = out_dir / f"{FILES[key]}__{tag}__{period}.csv"
        tmp = path.with_suffix(".tmp")
        df.to_csv(tmp, index=False)
        tmp.rename(path)   # atomic: the watcher never sees a half-written file
        stats["files"][key] = _file_stats(df, path, key)
    ipath = STREAM_DIR / f"{FILES['iot']}__{tag}__{period}.csv"
    new_i.to_csv(ipath, index=False)
    stats["files"]["iot"] = _file_stats(new_i, ipath, "iot")
    stats["sales_defects_injected"] = defects
    stats["iot_defects_injected"] = int(kb)
    stats["scd_changes"] = changed
    stats["assumptions"] = {"growth_factor": GROWTH, "noise_pct": NOISE * 100,
                            "corrupt_rate_pct": CORRUPT_RATE * 100,
                            "seasonal_base_year": cfg["base_year"]}
    state.d["done"].append(refresh)
    state.save()
    (REPORT_DIR / f"{tag}.json").write_text(json.dumps(stats, indent=2, default=str))
    return stats


DATE_COLS = {"master": None, "manufacturing": "OrderDate", "delivery": "DispatchDate",
             "warehouse": "SnapshotDate", "sales": "SalesDate", "demand": "DemandMonth",
             "iot": "EventTimestampUTC"}


def _file_stats(df, path, key):
    col = DATE_COLS[key]
    out = {"file": path.name, "rows": len(df), "columns": df.shape[1],
           "size_mb": round(path.stat().st_size / 1e6, 2)}
    if col:
        d = pd.to_datetime(df[col], errors="coerce", format="mixed")
        out.update(min_date=str(d.min())[:10], max_date=str(d.max())[:10])
    if key == "sales":
        out["net_sales_usd"] = round(float(_num(df.NetSalesAmountUSD).sum()), 2)
        out["qty_sold"] = int(_num(df.QuantitySoldPacks).clip(lower=0).sum())
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--refresh", type=int, required=True, choices=sorted(REFRESHES))
    ap.add_argument("--seed", type=int, default=42)
    a = ap.parse_args()
    s = generate(a.refresh, a.seed)
    for k, v in s["files"].items():
        print(f"{k:14s} {v['rows']:>8,} rows  {v.get('min_date', '')} .. {v.get('max_date', '')}  {v['file']}")
    print("SCD changes:", s["scd_changes"])
    print("Defects injected:", s["sales_defects_injected"], "iot:", s["iot_defects_injected"])
