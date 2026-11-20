"""Offline acceptance controls for the standalone LOAD runner."""
import unittest
from urllib.parse import parse_qs, urlsplit

import standalone_load as load


class StandaloneLoadControls(unittest.TestCase):
    def test_interleaved_pairs_and_matched_concern(self):
        self.assertEqual(list(load.trial_order(3)), ["gomap", "mongodb", "mongodb", "gomap", "gomap", "mongodb"])
        uri = urlsplit(load.acknowledged_uri("mongodb://127.0.0.1:123/?directConnection=true", "journaled"))
        self.assertEqual(uri.path, "/ycsb")
        self.assertEqual(parse_qs(uri.query), {"w": ["1"], "journal": ["true"],
                                               "directConnection": ["true"], "retryWrites": ["false"]})
        uri = urlsplit(load.acknowledged_uri("mongodb://127.0.0.1:123", "w1"))
        self.assertEqual(parse_qs(uri.query), {"w": ["1"], "directConnection": ["true"], "retryWrites": ["false"]})

    def test_cow_publication_must_be_positive_and_journal_ack_must_cover_inserts(self):
        before = {load.PUBLICATIONS: "30"}
        counts = ("Requests", "LogicalWrites", "JournalRequests", "JournalAcknowledgements",
                  "VisibleRequests", "VisibleAcknowledgements", *load.FAILURE_COUNTS)
        before.update({load.WRITE_CONCERN + k: "12" for k in counts})
        after = dict(before)
        after[load.PUBLICATIONS] = "32"
        after.update({load.WRITE_CONCERN + k: "17" for k in counts[:4]})
        self.assertEqual(load.cow_evidence(before, after, 5, "journaled")["publications"], 2)
        with self.assertRaises(ValueError):
            load.cow_evidence(before, after, 5, "w1")
        w1_after = dict(after)
        for suffix in ("Requests", "Acknowledgements"):
            w1_after[load.WRITE_CONCERN + "Visible" + suffix] = "17"
            w1_after[load.WRITE_CONCERN + "Journal" + suffix] = "12"
        self.assertEqual(load.cow_evidence(before, w1_after, 5, "w1")["publications"], 2)
        for key, bad in (
            (load.PUBLICATIONS, None),
            (load.PUBLICATIONS, "30"),
            (load.PUBLICATIONS, "29"),
            (load.PUBLICATIONS, "not-a-counter"),
            (load.WRITE_CONCERN + "JournalAcknowledgements", "16"),
            (load.WRITE_CONCERN + "SyncFailures", "13"),
            (load.WRITE_CONCERN + "VisibleRequests", "13"),
        ):
            rejected = dict(after)
            if bad is None:
                del rejected[key]
            else:
                rejected[key] = bad
            with self.subTest(key=key, bad=bad), self.assertRaises(ValueError):
                load.cow_evidence(before, rejected, 5, "journaled")
