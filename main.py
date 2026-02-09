from ingestion import stream_records
from normalize import normalize_record
from analyzer import Analyzer
from classifier import classify
from storage_manager import StorageManager
import json

print("=" * 70)
print("ADAPTIVE INGESTION & HYBRID BACKEND PLACEMENT")
print("=" * 70)

# Initialize components
analyzer = Analyzer()
storage = StorageManager()

# Load previous metadata if exists
try:
    with open("metadata.json") as f:
        metadata = json.load(f)
        print(f"✓ Loaded previous metadata with {len(metadata)} field decisions")
except:
    metadata = {}
    print("• No previous metadata found")

# Connect to databases (Phase 4: Commit & Routing)
print("\n--- Connecting to Backends ---")
if not storage.connect():
    print("✗ Database connection failed. Exiting.")
    exit(1)

# Initialize SQL schema from metadata before processing
if metadata:
    storage.initialize_schema(metadata)

print("\n--- Processing Records ---")
stats_counter = {'total': 0, 'sql_stored': 0, 'mongo_stored': 0}

try:
    for i, record in enumerate(stream_records(batch_size=10, delay=1)):
        stats_counter['total'] += 1
        
        # Phase 1: Normalize keys
        clean = normalize_record(record)
        
        # Phase 2: Analyze field patterns
        analyzer.update(clean)
        stats = analyzer.get_stats()
        
        # Phase 3: Classify fields - use existing metadata for known fields
        current_decisions = classify(stats)
        
        # Merge: keep existing metadata decisions, add new fields only
        for field, decision in current_decisions.items():
            if field not in metadata:
                metadata[field] = decision
        
        # Save updated metadata
        with open("metadata.json", "w") as f:
            json.dump(metadata, f, indent=2, default=lambda x: list(x) if isinstance(x, set) else x)
        
        # Phase 4: Route and store using stable metadata
        sql_id, mongo_id = storage.store_record(clean, metadata)
        
        if sql_id:
            stats_counter['sql_stored'] += 1
        if mongo_id:
            stats_counter['mongo_stored'] += 1
        
        # Progress update
        if (i + 1) % 10 == 0:
            counts = storage.get_stats()
            print(f"Processed: {i+1} | SQL: {counts['sql']} | MongoDB: {counts['mongo']}")
        
        # Detailed output for first few records
        if i < 3:
            sql_fields = [f for f, d in metadata.items() if d == 'sql']
            mongo_fields = [f for f, d in metadata.items() if d == 'mongo']
            print(f"\nRecord #{i+1}:")
            print(f"  SQL fields: {sql_fields}")
            print(f"  Mongo fields: {mongo_fields}")
            print(f"  IDs: SQL={sql_id}, Mongo={mongo_id}")
        
        # Stop after 50 records for testing
        if i >= 49:
            print(f"\n✓ Processed {i+1} records, stopping.")
            break

except KeyboardInterrupt:
    print("\n\n⚠ Interrupted by user")

finally:
    # Final statistics
    final_counts = storage.get_stats()
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Records processed:     {stats_counter['total']}")
    print(f"SQL records stored:    {final_counts['sql']}")
    print(f"MongoDB docs stored:   {final_counts['mongo']}")
    print(f"Metadata saved:        metadata.json")
    print("=" * 70)
    
    storage.close()
    print("\n✓ Pipeline completed")
