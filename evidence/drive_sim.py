"""Deterministic reproduction of #39026.

Drives ImpulseSeqGenDoFn.process() directly and reads the *output watermark the
SDK reports to the runner* (the ManualWatermarkEstimator value) as simulated
wall-clock time advances. That reported watermark is exactly what Dataflow
surfaces as "data freshness" = now - watermark.

For each simulated `now` we run process() from a fresh estimator with
time.time() pinned to `now`, and record the resulting watermark hold. No
real-time sleeping, no runner flakiness -- the value is a pure function of the
code under test, so master vs fix is an apples-to-apples comparison.
"""
import argparse
from unittest import mock

import apache_beam.transforms.periodicsequence as ps
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.io.restriction_trackers import OffsetRestrictionTracker
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.runners.sdf_utils import RestrictionTrackerView
from apache_beam.runners.sdf_utils import ThreadsafeRestrictionTracker
from apache_beam.transforms.periodicsequence import ImpulseSeqGenDoFn


def reported_watermark(now, start, interval):
  dofn = ImpulseSeqGenDoFn()
  tracker = ThreadsafeRestrictionTracker(
      OffsetRestrictionTracker(OffsetRange(0, 10**9)))
  view = RestrictionTrackerView(tracker)
  est = ManualWatermarkEstimator(None)
  with mock.patch.object(ps.time, 'time', return_value=now):
    list(
        dofn.process((start, start + 10**9, interval),
                     restriction_tracker=view,
                     watermark_estimator=est))
  wm = est.current_watermark()
  return float(wm) if wm is not None else None


def run(csv_path):
  start = 0.0
  interval = 60.0  # side input fires every 60s
  with open(csv_path, 'w') as f:
    f.write('now,watermark,freshness\n')
    now = start
    while now <= 5 * interval:
      wm = reported_watermark(now, start, interval)
      freshness = now - wm
      f.write('%.1f,%.1f,%.1f\n' % (now, wm, freshness))
      now += 2.0


if __name__ == '__main__':
  ap = argparse.ArgumentParser()
  ap.add_argument('--csv', required=True)
  args = ap.parse_args()
  run(args.csv)
  print('wrote', args.csv)
