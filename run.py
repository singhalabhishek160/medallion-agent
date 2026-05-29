"""
Main pipeline runner - executes bronze → silver → gold → agents.

Usage:
    python run.py              # Full pipeline + agents
    python run.py pipeline     # Only bronze → silver → gold
    python run.py agents       # Only run AI agents
    python run.py bronze       # Only bronze
    python run.py silver       # Only silver
    python run.py gold         # Only gold
"""
import sys
import time


def run_bronze():
    print("\n" + "=" * 60)
    print("STAGE: BRONZE (Raw Ingestion)")
    print("=" * 60)
    from pipeline.bronze import ingest
    ingest()


def run_silver():
    print("\n" + "=" * 60)
    print("STAGE: SILVER (Clean & Transform)")
    print("=" * 60)
    from pipeline.silver import transform
    transform()


def run_gold():
    print("\n" + "=" * 60)
    print("STAGE: GOLD (Aggregations)")
    print("=" * 60)
    from pipeline.gold import build
    build()


def run_agents():
    print("\n" + "=" * 60)
    print("STAGE: AI AGENTS")
    print("=" * 60)
    
    print("\n--- Agent 1: Data Quality ---")
    from agents.data_quality_agent import run_agent as run_dq
    run_dq()
    
    print("\n--- Agent 2: Semantic Classification ---")
    from agents.semantic_agent import run_agent as run_sc
    run_sc(max_tickets=60)  # Limit for demo (free tier rate limits)


def main():
    start = time.time()
    stage = sys.argv[1] if len(sys.argv) > 1 else "all"
    
    print("=" * 60)
    print("MEDALLION PIPELINE (PySpark + PostgreSQL)")
    print(f"Mode: {stage}")
    print("=" * 60)
    
    if stage in ("all", "pipeline", "bronze"):
        run_bronze()
    if stage in ("all", "pipeline", "silver"):
        run_silver()
    if stage in ("all", "pipeline", "gold"):
        run_gold()
    if stage in ("all", "agents"):
        run_agents()
    
    elapsed = time.time() - start
    print(f"\n{'=' * 60}")
    print(f"COMPLETE! Total time: {elapsed:.1f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
