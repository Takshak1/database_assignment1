from ingestion import stream_records
from normalize import normalize_record
from analyzer import Analyzer
from classifier import classify_with_placement_heuristics, get_placement_summary
from storage_manager import StorageManager
import json

print("=" * 80)
print("           ADAPTIVE INGESTION & HYBRID BACKEND PLACEMENT")
print("=" * 80)

# Initialize components
analyzer = Analyzer()
storage = StorageManager()

# Load previous metadata if exists
try:
    with open("metadata.json") as f:
        metadata = json.load(f)
        print(f"Loaded previous metadata with {len(metadata)} field decisions")
except:
    metadata = {}
    print("No previous metadata found")

# Connect to databases (Phase 4: Commit & Routing)
print("\n" + "-" * 40)
print("CONNECTING TO BACKENDS")
print("-" * 40)
if not storage.connect():
    print("Database connection failed. Exiting.")
    exit(1)

# Initialize SQL schema from metadata before processing
if metadata:
    storage.initialize_schema(metadata)

print("\n" + "-" * 40)
print("PROCESSING RECORDS")
print("-" * 40)
stats_counter = {'total': 0, 'sql_stored': 0, 'mongo_stored': 0}

try:
    for i, record in enumerate(stream_records(batch_size=10, delay=1)):
        stats_counter['total'] += 1
        
        # Phase 1: Normalize keys
        clean = normalize_record(record)
        
        # Phase 2: Analyze field patterns
        analyzer.update(clean)
        stats = analyzer.get_stats()
        
        # Phase 3: Enhanced placement heuristics with semantic analysis
        if len(stats) > 0:  # Only classify if we have stats
            current_decisions, placement_reasons = classify_with_placement_heuristics(stats)
            
            # Store placement reasons for analysis
            if i < 10 and 'detailed_placement' not in locals():
                detailed_placement = placement_reasons
        else:
            current_decisions = {}
            placement_reasons = {}
        
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
            print(f"Processed {i+1} records, stopping.")
            break

except KeyboardInterrupt:
    print("\nInterrupted by user")

finally:
    # Final statistics
    final_counts = storage.get_stats()
    print("\n" + "=" * 80)
    print("                              FINAL SUMMARY")
    print("=" * 80)
    print(f"Records processed:     {stats_counter['total']:>8}")
    print(f"SQL records stored:    {final_counts['sql']:>8}")
    print(f"MongoDB docs stored:   {final_counts['mongo']:>8}")
    print(f"Metadata saved:        {'metadata.json':>8}")
    print("=" * 80)
    
    # Normalization report
    norm_report = analyzer.get_normalization_report()
    print("\n" + "=" * 80)
    print("                        NORMALIZATION STRATEGY")
    print("=" * 80)
    print(f"Total raw fields processed: {norm_report['total_raw_fields']:>8}")
    print(f"Canonical fields created:   {norm_report['canonical_fields']:>8}")
    print(f"Aliases resolved:           {norm_report['aliases_resolved']:>8}")
    print(f"Type conflicts detected:    {norm_report['type_conflicts']:>8}")
    
    if norm_report["alias_mappings"]:
        print(f"\nFIELD NORMALIZATION MAPPINGS:")
        for canonical, aliases in norm_report["alias_mappings"].items():
            aliases_str = ", ".join(aliases)
            print(f"  '{canonical}' <- {{{aliases_str}}}")
    
    if norm_report["conflicts"]:
        print(f"\nTYPE CONFLICTS (routed to MongoDB):")
        for canonical, conflict_info in norm_report["conflicts"].items():
            types_str = ", ".join(conflict_info["conflicting_types"])
            raw_names_str = ", ".join(conflict_info["raw_names"])
            print(f"  '{canonical}' has mixed types [{types_str}] from fields: {raw_names_str}")
    
    if not norm_report["alias_mappings"] and not norm_report["conflicts"]:
        print("\nNo normalization conflicts - all field names were unique and consistent")
    
    # Field uniqueness analysis
    uniqueness_analysis = analyzer.analyze_field_uniqueness()
    print("\n" + "=" * 80)
    print("                      FIELD UNIQUENESS ANALYSIS")
    print("=" * 80)
    
    if uniqueness_analysis["unique_fields"]:
        print(f"\nUNIQUE FIELDS ({len(uniqueness_analysis['unique_fields'])}):")
        for field_info in uniqueness_analysis["unique_fields"]:
            print(f"  {field_info['field']}: {field_info['uniqueness_ratio']:.1%} unique "
                  f"({field_info['unique_values']}/{field_info['total_occurrences']} values)")
    
    if uniqueness_analysis["semi_unique_fields"]:
        print(f"\nSEMI-UNIQUE FIELDS ({len(uniqueness_analysis['semi_unique_fields'])}):")
        for field_info in uniqueness_analysis["semi_unique_fields"]:
            print(f"  {field_info['field']}: {field_info['uniqueness_ratio']:.1%} unique "
                  f"({field_info['unique_values']}/{field_info['total_occurrences']} values)")
    
    if uniqueness_analysis["common_fields"]:
        print(f"\nCOMMON FIELDS ({len(uniqueness_analysis['common_fields'])}):")
        for field_info in uniqueness_analysis["common_fields"][:5]:  # Show top 5
            print(f"  {field_info['field']}: {field_info['uniqueness_ratio']:.1%} unique "
                  f"({field_info['unique_values']}/{field_info['total_occurrences']} values)")
        if len(uniqueness_analysis["common_fields"]) > 5:
            print(f"    ... and {len(uniqueness_analysis['common_fields']) - 5} more")
    
    print("=" * 70)
    
    # Enhanced placement heuristics analysis
    if 'detailed_placement' in locals():
        placement_summary = get_placement_summary(detailed_placement)
        print("\n" + "=" * 80)
        print("                   PLACEMENT HEURISTICS ANALYSIS")
        print("=" * 80)
        
        print(f"\nPLACEMENT OVERVIEW:")
        print(f"  Total fields analyzed:      {placement_summary['total_fields']:>8}")
        print(f"  SQL assignments:            {placement_summary['sql_decisions']:>8}")
        print(f"  MongoDB assignments:        {placement_summary['mongo_decisions']:>8}")
        
        print(f"\nCOMPOSITE SCORE DISTRIBUTION:")
        print(f"  High scores (>=0.8):        {placement_summary['score_distribution']['high']:>8}")
        print(f"  Medium scores (0.5-0.8):    {placement_summary['score_distribution']['medium']:>8}")
        print(f"  Low scores (<0.5):          {placement_summary['score_distribution']['low']:>8}")
        
        if placement_summary["high_confidence_sql"]:
            print(f"\nHIGH-CONFIDENCE SQL PLACEMENTS:")
            for item in placement_summary["high_confidence_sql"][:8]:  # Show top 8
                signals = detailed_placement[item['field']]['signals']
                print(f"  {item['field']}: {item['semantic_type']} "
                      f"(freq={signals['freq']:.2f}, stability={signals['stability']:.2f}, "
                      f"score={signals['composite_score']:.2f})")
        
        print(f"\nPLACEMENT REASONING BREAKDOWN:")
        for reason, fields in placement_summary['placement_breakdown'].items():
            if len(fields) <= 5:
                fields_str = ", ".join(fields)
            else:
                fields_str = ", ".join(fields[:5]) + f" + {len(fields)-5} more"
            print(f"  {reason}: {fields_str}")
        
        if placement_summary['semantic_distribution']:
            print(f"\nSEMANTIC TYPE DISTRIBUTION:")
            for sem_type, counts in placement_summary['semantic_distribution'].items():
                total = counts['sql'] + counts['mongo']
                print(f"  {sem_type}: {total} fields -> SQL: {counts['sql']}, MongoDB: {counts['mongo']}")
    
    print("=" * 80)
    
    # Type Drift Analysis
    drift_summary = analyzer.get_drift_summary()
    if drift_summary['total_fields_tracked'] > 0:
        print("\n" + "=" * 80)
        print("                    MIXED DATA HANDLING (TYPE DRIFT)")
        print("=" * 80)
        
        print(f"\nDRIFT OVERVIEW:")
        print(f"  Fields tracked for drift:   {drift_summary['total_fields_tracked']:>8}")
        print(f"  Quarantined fields:         {drift_summary['quarantined_fields']:>8}")
        print(f"  High drift fields:          {len(drift_summary['high_drift_fields']):>8}")
        print(f"  Stable fields:              {len(drift_summary['stable_fields']):>8}")
        
        if drift_summary['high_drift_fields']:
            print(f"\nHIGH DRIFT FIELDS (quarantined to MongoDB):")
            for field_info in drift_summary['high_drift_fields'][:5]:
                field = field_info['field']
                drift_score = field_info['drift_score']
                type_shares = field_info['type_shares']
                patterns = field_info['flip_patterns']
                
                types_str = ', '.join([f"{t}({s:.0%})" for t, s in type_shares.items()])
                print(f"  {field}: drift_score={drift_score:.2f}, types=[{types_str}]")
                
                if patterns:
                    print(f"    Patterns: {', '.join(patterns)}")
        
        if drift_summary['quarantine_list']:
            print(f"\nQUARANTINED FIELDS: {', '.join(drift_summary['quarantine_list'])}")
            print("    (These fields routed to MongoDB to prevent SQL schema conflicts)")
        
        if drift_summary['drift_patterns']:
            print(f"\nDETECTED FLIP PATTERNS:")
            for pattern, fields in drift_summary['drift_patterns'].items():
                fields_str = ', '.join(fields[:5])  # Show first 5
                if len(fields) > 5:
                    fields_str += f" + {len(fields)-5} more"
                print(f"  {pattern}: {fields_str}")
    
    print("=" * 80)
    
    # Demonstrate bi-temporal join capabilities
    if final_counts['sql'] > 0 and final_counts['mongo'] > 0:
        storage.demonstrate_bi_temporal_join()
    else:
        print("\nNo records processed - bi-temporal join demo requires data in both backends")
    
    storage.close()
    print("\nPipeline completed successfully")
