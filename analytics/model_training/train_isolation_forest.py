"""
Offline IsolationForest trainer for the analytics layer (claude.md
Phase 5). A deliberate one-off/periodic manual script, not a
long-running service - run it once ~50+ cycles have accumulated in
cycle_metrics, and re-run it manually whenever you want to retrain
against more data. Not an online/incremental learner - a simpler,
more honest story than pretending this is a productionized MLOps
pipeline.

Usage (from the repo root, with PYTHONPATH set to the repo root - the
Docker image already sets this; run locally with
`python -m analytics.model_training.train_isolation_forest` if not):

    docker compose run --rm analytics python analytics/model_training/train_isolation_forest.py
    python analytics/model_training/train_isolation_forest.py --min-cycles 50

After training, restart the analytics service (docker compose restart
analytics) to pick up the new model - v1 is restart-to-reload, not
hot-reload (see analytics/analytics_consumer.py's load_model()).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime, timezone

import joblib
from sklearn.ensemble import IsolationForest

from analytics.scoring import feature_vector
from database.models import fetch_recent_cycle_features, get_connection, init_analytics_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] train_isolation_forest: %(message)s",
)
logger = logging.getLogger(__name__)

MODEL_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "models")
MANIFEST_PATH = os.path.join(MODEL_DIR, "MODEL_MANIFEST.json")

DEFAULT_MIN_CYCLES = 50
DEFAULT_TRAIN_LIMIT = 2000


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--min-cycles",
        type=int,
        default=DEFAULT_MIN_CYCLES,
        help="Refuse to train below this many available cycles (default: %(default)s).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_TRAIN_LIMIT,
        help="Max number of most-recent cycles to train on (default: %(default)s).",
    )
    args = parser.parse_args()

    conn = get_connection()
    init_analytics_db(conn)

    rows = fetch_recent_cycle_features(conn, args.limit)
    if len(rows) < args.min_cycles:
        logger.error(
            "Only %d completed cycles available in cycle_metrics, need at least %d - "
            "let main.py's simulator and the analytics service run longer before training.",
            len(rows),
            args.min_cycles,
        )
        raise SystemExit(1)

    vectors = [feature_vector(row) for row in rows]
    logger.info(
        "Training IsolationForest on %d cycles (feature vector length=%d)...",
        len(vectors),
        len(vectors[0]),
    )

    model = IsolationForest(n_estimators=100, contamination="auto", random_state=42)
    model.fit(vectors)

    os.makedirs(MODEL_DIR, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    model_filename = f"isolation_forest_{stamp}.joblib"
    joblib.dump(model, os.path.join(MODEL_DIR, model_filename))

    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(
            {
                "active_model_filename": model_filename,
                "trained_on_cycles": len(vectors),
                "trained_at": stamp,
            },
            f,
            indent=2,
        )

    logger.info(
        "Saved model %s and updated %s. Restart the analytics service to load it.",
        model_filename,
        MANIFEST_PATH,
    )


if __name__ == "__main__":
    main()
