"""Datetime hashing and zone labels (owner scicanonicalhash.datetimes).

Decisions (2026-10-22): datetime.datetime, pd.Timestamp and np.datetime64
holding the same value hash the same; NaT has one spelling; the zone label is
part of the hash, because sciduckdb restores it on load.
"""

import datetime
import zoneinfo

import pytest

from scicanonicalhash import canonical_hash, utc_ns, zone_from_label, zone_label
from scicanonicalhash.datetimes import is_missing_datetime

np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")

BERLIN = zoneinfo.ZoneInfo("Europe/Berlin")
NEW_YORK = zoneinfo.ZoneInfo("America/New_York")


class TestHashParity:
    def test_naive_same_value_three_types(self):
        dt = datetime.datetime(2026, 1, 1, 10, 30, 15, 123456)
        h = canonical_hash(dt)
        assert canonical_hash(pd.Timestamp(dt)) == h
        assert canonical_hash(np.datetime64("2026-01-01T10:30:15.123456")) == h
        assert canonical_hash(np.datetime64("2026-01-01T10:30:15.123456000", "ns")) == h

    def test_datetime64_unit_does_not_matter(self):
        h = canonical_hash(datetime.datetime(2026, 1, 1))
        for unit in ("Y", "M", "D", "h", "m", "s", "ms", "us", "ns"):
            assert canonical_hash(np.datetime64("2026-01-01", unit)) == h, unit

    def test_nanoseconds_count(self):
        a = pd.Timestamp("2026-01-01 00:00:00.000000001")
        b = pd.Timestamp("2026-01-01 00:00:00.000000002")
        assert canonical_hash(a) != canonical_hash(b)
        assert canonical_hash(a) == canonical_hash(np.datetime64("2026-01-01T00:00:00.000000001"))

    def test_before_epoch(self):
        dt = datetime.datetime(1900, 6, 1, 12)
        assert canonical_hash(dt) == canonical_hash(pd.Timestamp(dt))
        assert canonical_hash(dt) == canonical_hash(np.datetime64("1900-06-01T12"))

    def test_nat_one_spelling(self):
        h = canonical_hash(pd.NaT)
        assert canonical_hash(np.datetime64("NaT")) == h
        assert canonical_hash(np.datetime64("NaT", "ns")) == h
        assert canonical_hash(datetime.datetime(1970, 1, 1)) != h

    def test_aware_types_agree(self):
        dt = datetime.datetime(2026, 7, 1, 12, tzinfo=BERLIN)
        assert canonical_hash(pd.Timestamp(dt)) == canonical_hash(dt)
        assert canonical_hash(pd.Timestamp("2026-07-01 12:00", tz="Europe/Berlin")) == canonical_hash(dt)

    def test_zone_is_part_of_the_hash(self):
        """Same instant, two zones: two values (each loads back in its own
        zone, so they are different data)."""
        berlin = pd.Timestamp("2026-07-01 12:00", tz="Europe/Berlin")
        assert canonical_hash(berlin) != canonical_hash(berlin.tz_convert("America/New_York"))

    def test_aware_differs_from_naive_wall_clock(self):
        naive = datetime.datetime(2026, 7, 1, 12)
        aware_utc = naive.replace(tzinfo=datetime.timezone.utc)
        assert canonical_hash(naive) != canonical_hash(aware_utc)

    def test_containers_and_object_arrays(self):
        vals = [pd.Timestamp("2026-01-01"), pd.NaT, pd.Timestamp("2026-01-02", tz="UTC")]
        assert canonical_hash(vals) == canonical_hash(list(vals))
        assert canonical_hash(np.array(vals, dtype=object)) == canonical_hash(
            np.array(list(vals), dtype=object)
        )

    def test_tz_aware_dataframe_column_hashes(self):
        df = pd.DataFrame({"t": pd.to_datetime(["2026-01-01", None]).tz_localize("Europe/Berlin")})
        assert canonical_hash(df) == canonical_hash(df.copy())

    def test_naive_datetime64_array_bytes_unchanged(self):
        """Arrays keep their raw-bytes form (pinned by test_hash_byte_identity)."""
        arr = np.array(["2024-01-01", "NaT"], dtype="datetime64[ns]")
        assert canonical_hash(arr) == canonical_hash(arr.copy())


class TestZoneLabel:
    def test_naive_and_missing(self):
        assert zone_label(datetime.datetime(2026, 1, 1)) is None
        assert zone_label(pd.NaT) is None
        assert zone_label(np.datetime64("2026-01-01")) is None

    def test_named_zones_keep_their_name(self):
        assert zone_label(datetime.datetime(2026, 1, 1, tzinfo=BERLIN)) == "Europe/Berlin"
        assert zone_label(pd.Timestamp("2026-01-01", tz="America/New_York")) == "America/New_York"

    def test_utc_spellings(self):
        assert zone_label(datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)) == "UTC"
        assert zone_label(pd.Timestamp("2026-01-01", tz="UTC")) == "UTC"
        assert zone_label(datetime.datetime(2026, 1, 1, tzinfo=zoneinfo.ZoneInfo("UTC"))) == "UTC"

    def test_fixed_offsets(self):
        plus2 = datetime.timezone(datetime.timedelta(hours=2))
        minus530 = datetime.timezone(-datetime.timedelta(hours=5, minutes=30))
        assert zone_label(datetime.datetime(2026, 1, 1, tzinfo=plus2)) == "+02:00"
        assert zone_label(datetime.datetime(2026, 1, 1, tzinfo=minus530)) == "-05:30"
        assert zone_label(pd.Timestamp("2026-01-01T00:00+02:00")) == "+02:00"

    @pytest.mark.parametrize("label", ["UTC", "+02:00", "-05:30", "Europe/Berlin"])
    def test_label_round_trip(self, label):
        """zone_from_label inverts zone_label: the restored value's label is
        the stored one, so a loaded value hashes like the saved one."""
        tz = zone_from_label(label)
        ts = pd.Timestamp("2026-07-01 12:00", tz="UTC").tz_convert(tz)
        assert zone_label(ts) == label

    def test_unknown_zone_names_the_label(self):
        with pytest.raises(ValueError, match="Not/AZone"):
            zone_from_label("Not/AZone")


class TestInstant:
    def test_utc_ns_aware(self):
        berlin = pd.Timestamp("2026-07-01 14:00", tz="Europe/Berlin")  # UTC+2 in July
        assert utc_ns(berlin) == pd.Timestamp("2026-07-01 12:00").value

    def test_utc_ns_naive_is_wall_clock(self):
        assert utc_ns(datetime.datetime(2026, 7, 1, 12)) == pd.Timestamp("2026-07-01 12:00").value

    def test_is_missing(self):
        assert is_missing_datetime(pd.NaT)
        assert is_missing_datetime(np.datetime64("NaT"))
        assert not is_missing_datetime(pd.Timestamp("2026-01-01"))
