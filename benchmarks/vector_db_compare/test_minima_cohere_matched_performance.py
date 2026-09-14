import unittest

import minima_cohere_matched_performance as subject


class MatchedPerformanceTest(unittest.TestCase):
    def test_distribution_uses_nearest_rank_percentiles(self):
        self.assertEqual(subject.distribution([4, 1, 3, 2]), {
            "count": 4, "mean_ns": 2.5, "p50_ns": 2, "p95_ns": 4, "p99_ns": 4, "max_ns": 4,
        })

    def test_mean_recall(self):
        self.assertEqual(subject.mean_recall([["a", "b"], ["x", "z"]], [["a", "c"], ["x", "y"]]), {
            "mean_recall_at_10": .5, "per_query": [.5, .5],
        })


if __name__ == "__main__":
    unittest.main()
