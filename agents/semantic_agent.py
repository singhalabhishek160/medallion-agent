"""
Semantic Classification Agent - Uses Meta Llama 3.3 70B (via Groq) to enrich tickets.

This agent:
1. Takes free-text/ambiguous columns from silver layer
2. Enriches them via LLM — urgency classification, entity extraction, location normalization
3. Output lands in silver.ticket_enrichments as new structured, queryable columns
4. Handles scale concerns: batching, caching (skip already-enriched), cost tracking

Uses Groq (Llama 3.3 70B) — free tier available at console.groq.com.
Scale strategy:
- Batch size of 20 tickets per LLM call (balance between context size and API calls)
- Skip already-enriched tickets (idempotent, incremental)
- Rate limiting with configurable delay between batches
- Token usage tracking for cost awareness
"""
import os
import json
import time
import psycopg2
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# Groq uses OpenAI-compatible API
client = OpenAI(
    api_key=os.getenv("GROQ_API_KEY"),
    base_url="https://api.groq.com/openai/v1",
)
MODEL = "llama-3.3-70b-versatile"

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB", "medallion"),
    "user": os.getenv("POSTGRES_USER", "pipeline"),
    "password": os.getenv("POSTGRES_PASSWORD", "pipeline123"),
}


def setup_enrichment_table():
    """Create the enrichment table if not exists."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS silver.ticket_enrichments (
            ticket_id TEXT PRIMARY KEY,
            urgency_signal TEXT,
            affected_system TEXT,
            location_detail TEXT,
            root_cause_category TEXT,
            _enriched_at TIMESTAMP DEFAULT NOW()
        )
    """)
    # Add root_cause_category column if missing (for upgrades)
    cur.execute("""
        DO $$ BEGIN
            ALTER TABLE silver.ticket_enrichments ADD COLUMN root_cause_category TEXT;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END $$;
    """)
    conn.commit()
    cur.close()
    conn.close()


def get_tickets_to_classify(limit=100):
    """Get tickets that haven't been enriched yet (incremental/cached)."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(f"""
        SELECT t.ticket_id, t.description, t.category, t.building, t.priority
        FROM silver.tickets t
        LEFT JOIN silver.ticket_enrichments e ON t.ticket_id = e.ticket_id
        WHERE e.ticket_id IS NULL
          AND t.description IS NOT NULL AND t.description != ''
        ORDER BY t.ticket_id
        LIMIT {limit}
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{"ticket_id": r[0], "description": r[1], "category": r[2],
             "building": r[3], "priority": r[4]} for r in rows]


def classify_batch(tickets):
    """Send a batch of tickets to LLM for semantic extraction."""
    system_prompt = """You are a facilities management expert. For each support ticket, extract structured information from the free-text description:

1. **urgency_signal**: Based on language cues (not just priority field). One of: "critical", "high", "medium", "low"
   - "critical": safety risk, system down, production impact mentioned
   - "high": multiple people affected, escalation language, repeated issue  
   - "medium": single system, inconvenience, standard request
   - "low": cosmetic, future concern, informational

2. **affected_system**: The specific equipment/system affected. Extract from description.
   Examples: "HVAC compressor", "fire alarm panel", "elevator motor", "main water line", "LED lighting", "door access control"

3. **location_detail**: Specific location BEYOND the building name. Extract floor, wing, room.
   Examples: "floor 3 east wing", "server room 201", "parking level B2", "main lobby"

4. **root_cause_category**: Infer likely root cause category:
   "equipment_age", "overload", "weather", "human_error", "design_flaw", "wear_and_tear", "external_vendor", "unknown"

Return ONLY a valid JSON array. Each object: {"id": "TKT-XXXX", "urgency_signal": "...", "affected_system": "...", "location_detail": "...", "root_cause_category": "..."}
If a field can't be determined from the text, use "unknown". Do NOT include any explanation, just the JSON array."""

    # Format tickets — truncate descriptions for token efficiency
    ticket_list = [
        {"id": t["ticket_id"], "desc": (t["description"] or "")[:200],
         "cat": t["category"], "bldg": t["building"]}
        for t in tickets
    ]

    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Classify these {len(ticket_list)} tickets:\n{json.dumps(ticket_list)}"},
        ],
        temperature=0.1,
        max_tokens=2500,
    )

    result_text = response.choices[0].message.content
    tokens = response.usage.total_tokens

    # Parse JSON from response
    try:
        # Handle markdown code blocks
        if "```" in result_text:
            parts = result_text.split("```")
            for part in parts[1:]:
                cleaned = part.strip()
                if cleaned.startswith("json"):
                    cleaned = cleaned[4:].strip()
                if cleaned.startswith("["):
                    result_text = cleaned
                    break
        # Find JSON array boundaries
        start = result_text.find("[")
        end = result_text.rfind("]") + 1
        if start != -1 and end > start:
            result_text = result_text[start:end]
        results = json.loads(result_text)
    except (json.JSONDecodeError, ValueError):
        print(f"    [!] Failed to parse LLM JSON response, skipping batch")
        return [], tokens

    return results, tokens


def save_enrichments(enrichments):
    """Write enrichments to database (upsert)."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    saved = 0

    for e in enrichments:
        ticket_id = e.get("id") or e.get("ticket_id")
        if not ticket_id:
            continue
        cur.execute("""
            INSERT INTO silver.ticket_enrichments
                (ticket_id, urgency_signal, affected_system, location_detail, root_cause_category)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ticket_id) DO UPDATE SET
                urgency_signal = EXCLUDED.urgency_signal,
                affected_system = EXCLUDED.affected_system,
                location_detail = EXCLUDED.location_detail,
                root_cause_category = EXCLUDED.root_cause_category,
                _enriched_at = NOW()
        """, (
            ticket_id,
            e.get("urgency_signal", "unknown"),
            e.get("affected_system", "unknown"),
            e.get("location_detail", "unknown"),
            e.get("root_cause_category", "unknown"),
        ))
        saved += 1

    conn.commit()
    cur.close()
    conn.close()
    return saved


def run_agent(max_tickets=100, batch_size=20):
    """
    Run the Semantic Classification Agent.
    
    Scale considerations:
    - max_tickets: Controls how many to process per run (incremental)
    - batch_size: 20 tickets/call balances token usage vs API calls
    - Caching: Only processes tickets not yet in silver.ticket_enrichments
    - Rate limiting: 2s delay between batches (Groq allows 30/min)
    - Cost tracking: Reports token usage per batch and total
    """
    print("=" * 60)
    print("SEMANTIC CLASSIFICATION AGENT")
    print("=" * 60)

    setup_enrichment_table()

    tickets = get_tickets_to_classify(limit=max_tickets)
    print(f"\n[Semantic Agent] Found {len(tickets)} un-enriched tickets")
    print(f"[Semantic Agent] Batch size: {batch_size} | Max: {max_tickets}")
    print(f"[Semantic Agent] Model: {MODEL} (Groq free tier)")

    if not tickets:
        print("[Semantic Agent] All tickets already enriched! (cached)")
        return

    total_tokens = 0
    total_classified = 0
    total_batches = (len(tickets) + batch_size - 1) // batch_size

    # Process in batches
    for i in range(0, len(tickets), batch_size):
        batch = tickets[i:i + batch_size]
        batch_num = (i // batch_size) + 1

        print(f"\n  [Batch {batch_num}/{total_batches}] Classifying {len(batch)} tickets...")

        # Rate limiting: 2s between batches (Groq allows 30 req/min)
        if batch_num > 1:
            time.sleep(2)

        try:
            results, tokens = classify_batch(batch)
        except Exception as e:
            if "rate_limit" in str(e).lower() or "429" in str(e):
                print(f"    [!] Rate limited. Waiting 30s...")
                time.sleep(30)
                try:
                    results, tokens = classify_batch(batch)
                except Exception:
                    print(f"    [!] Still failing. Stopping.")
                    break
            else:
                print(f"    [!] Error: {e}")
                continue

        total_tokens += tokens

        if results:
            saved = save_enrichments(results)
            total_classified += saved
            print(f"    Enriched {saved} tickets ({tokens} tokens)")
        else:
            print(f"    No results parsed ({tokens} tokens)")

    # Summary
    print(f"\n{'=' * 60}")
    print(f"[Semantic Agent] COMPLETE!")
    print(f"  Tickets enriched: {total_classified}/{len(tickets)}")
    print(f"  Total tokens: {total_tokens}")
    print(f"  Estimated cost: FREE (Groq free tier)")
    print(f"  Scale note: Re-run to process more — already-enriched tickets are skipped")

    # Show sample enrichments
    if total_classified > 0:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            SELECT ticket_id, urgency_signal, affected_system, location_detail, root_cause_category
            FROM silver.ticket_enrichments ORDER BY _enriched_at DESC LIMIT 5
        """)
        print(f"\n  Sample Enrichments (latest 5):")
        print(f"  {'ticket_id':<12} {'urgency':<10} {'system':<25} {'location':<20} {'root_cause'}")
        print(f"  {'-'*12} {'-'*10} {'-'*25} {'-'*20} {'-'*15}")
        for row in cur.fetchall():
            print(f"  {row[0]:<12} {row[1]:<10} {(row[2] or '')[:25]:<25} {(row[3] or '')[:20]:<20} {row[4]}")
        cur.close()
        conn.close()


if __name__ == "__main__":
    run_agent()
