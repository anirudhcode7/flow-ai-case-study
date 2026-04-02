"""
FlowAI Phase 1 — Master data generation entry point.

Orchestrates all generators in dependency order:
  1. EMR (requires Synthea output)
  2. RCM (requires EMR patient CSV)
  3. Referral (requires EMR patient CSV)

Usage:
  python ingestion/generate/run_generate.py
  python ingestion/generate/run_generate.py --dry-run
"""

import argparse
import logging
import os
import sys
import time

# Ensure project root is on sys.path when run as a script
_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_HERE))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from ingestion.generate import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _check_synthea_exists() -> bool:
    """Return True if Synthea output directory has at least patients.csv."""
    patients_csv = os.path.join(config.SYNTHEA_OUTPUT, "patients.csv")
    return os.path.isfile(patients_csv)


def main():
    parser = argparse.ArgumentParser(
        description="FlowAI synthetic data generator (Phase 1)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip all LLM calls; use Faker fallback for every field.",
    )
    args = parser.parse_args()

    if args.dry_run:
        config.DRY_RUN = True
        print("DRY-RUN MODE: All LLM calls disabled; Faker fallback will be used.")

    print("=" * 60)
    print("FlowAI Phase 1 — Synthetic Data Generation")
    print("=" * 60)

    # In dry-run mode, skip the Synthea check — emr_from_synthea will use Faker fallback
    if not args.dry_run and not _check_synthea_exists():
        print(
            "\nERROR: Synthea output not found at:\n"
            f"  {config.SYNTHEA_OUTPUT}\n\n"
            "Please run Synthea first:\n"
            "  java -jar synthea-with-dependencies.jar -s 42 -p 600 \\\n"
            "    --exporter.csv.export=true --exporter.fhir.export=false \\\n"
            "    --exporter.ccda.export=false California\n"
            "Then copy output/csv/* to data/raw/synthea_output/\n"
            "\nTip: use --dry-run to generate fake EMR data without Synthea.\n"
        )
        sys.exit(1)

    total_start = time.time()

    # ── Step 1: EMR ───────────────────────────────────────────────────────────
    print("\n[1/3] Generating EMR tables...")
    t0 = time.time()
    try:
        from ingestion.generate.generators import emr_from_synthea
        emr_results = emr_from_synthea.run()
        print(f"  EMR done in {time.time() - t0:.1f}s — "
              f"{sum(len(v) for v in emr_results.values())} total rows")
    except Exception as exc:
        logger.exception("EMR generation failed: %s", exc)
        sys.exit(1)

    # ── Step 2: RCM ───────────────────────────────────────────────────────────
    print("\n[2/3] Generating RCM tables...")
    t0 = time.time()
    try:
        from ingestion.generate.generators import rcm
        rcm_results = rcm.run()
        print(f"  RCM done in {time.time() - t0:.1f}s — "
              f"{sum(len(v) for v in rcm_results.values())} total rows")
    except Exception as exc:
        logger.exception("RCM generation failed: %s", exc)
        sys.exit(1)

    # ── Step 3: Referral ──────────────────────────────────────────────────────
    print("\n[3/3] Generating Referral tables...")
    t0 = time.time()
    try:
        from ingestion.generate.generators import referral
        referral_results = referral.run()
        print(f"  Referral done in {time.time() - t0:.1f}s — "
              f"{sum(len(v) for v in referral_results.values())} total rows")
    except Exception as exc:
        logger.exception("Referral generation failed: %s", exc)
        sys.exit(1)

    # ── Summary ───────────────────────────────────────────────────────────────
    all_results = {**emr_results, **rcm_results, **referral_results}
    elapsed = time.time() - total_start

    print("\n" + "=" * 60)
    print(f"Generation complete in {elapsed:.1f}s")
    print("-" * 60)
    for table, df in all_results.items():
        print(f"  {table:<40} {len(df):>6} rows")
    print("=" * 60)
    print(f"\nOutput written to:")
    print(f"  EMR:      {config.EMR_OUTPUT}")
    print(f"  RCM:      {config.RCM_OUTPUT}")
    print(f"  Referral: {config.REFERRAL_OUTPUT}")
    print("\nNext step: python ingestion/bronze/load_bronze.py")


if __name__ == "__main__":
    main()
