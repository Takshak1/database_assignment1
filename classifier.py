import re
from datetime import datetime

def detect_value_types(field_name, values_sample):
    """
    Advanced semantic analysis to detect value types and patterns
    
    Args:
        field_name (str): Name of the field
        values_sample (set): Sample of unique values from the field
    
    Returns:
        dict: Analysis results with semantic type information
    """
    analysis = {
        "semantic_type": "unknown",
        "sql_preference": 0.5,  # 0.0 = MongoDB preferred, 1.0 = SQL preferred
        "patterns": [],
        "indexable": False,
        "relational": False
    }
    
    # Convert to list for easier processing
    sample_values = list(values_sample)[:20]  # Analyze up to 20 samples
    
    # Email detection
    email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    email_matches = sum(1 for v in sample_values if email_pattern.match(str(v)))
    
    # IP address detection
    ip_pattern = re.compile(r'^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$')
    ip_matches = sum(1 for v in sample_values if ip_pattern.match(str(v)))
    
    # URL detection
    url_pattern = re.compile(r'^https?://[^\s/$.?#].[^\s]*$', re.IGNORECASE)
    url_matches = sum(1 for v in sample_values if url_pattern.match(str(v)))
    
    # UUID detection
    uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
    uuid_matches = sum(1 for v in sample_values if uuid_pattern.match(str(v)))
    
    # Timestamp detection
    timestamp_keywords = ['time', 'date', 'created', 'updated', 'stamp', 'at', 'when']
    is_timestamp_field = any(keyword in field_name.lower() for keyword in timestamp_keywords)
    
    # Numeric ID detection
    id_keywords = ['id', 'key', 'ref', 'pk', 'fk']
    is_id_field = any(keyword in field_name.lower() for keyword in id_keywords)
    
    # Geographic detection
    geo_keywords = ['lat', 'lon', 'gps', 'coord', 'city', 'country', 'zip', 'postal']
    is_geo_field = any(keyword in field_name.lower() for keyword in geo_keywords)
    
    # Determine semantic type and SQL preference
    total_samples = len(sample_values)
    if total_samples == 0:
        return analysis
    
    # Email addresses - SQL preferred for indexing
    if email_matches / total_samples > 0.8:
        analysis.update({
            "semantic_type": "email",
            "sql_preference": 0.9,
            "patterns": ["email_format"],
            "indexable": True,
            "relational": True
        })
    
    # IP addresses - SQL preferred for network analysis
    elif ip_matches / total_samples > 0.8:
        analysis.update({
            "semantic_type": "ip_address",
            "sql_preference": 0.9,
            "patterns": ["ipv4_format"],
            "indexable": True,
            "relational": True
        })
    
    # URLs - MongoDB preferred for flexibility
    elif url_matches / total_samples > 0.8:
        analysis.update({
            "semantic_type": "url",
            "sql_preference": 0.2,
            "patterns": ["url_format"],
            "indexable": False,
            "relational": False
        })
    
    # UUIDs - SQL preferred for primary keys
    elif uuid_matches / total_samples > 0.8:
        analysis.update({
            "semantic_type": "uuid",
            "sql_preference": 0.95,
            "patterns": ["uuid_format"],
            "indexable": True,
            "relational": True
        })
    
    # Timestamp fields - SQL preferred for temporal queries
    elif is_timestamp_field:
        analysis.update({
            "semantic_type": "timestamp",
            "sql_preference": 0.85,
            "patterns": ["temporal_data"],
            "indexable": True,
            "relational": True
        })
    
    # ID fields - SQL preferred for relationships
    elif is_id_field:
        analysis.update({
            "semantic_type": "identifier",
            "sql_preference": 0.9,
            "patterns": ["identifier"],
            "indexable": True,
            "relational": True
        })
    
    # Geographic fields - Mixed preference based on complexity
    elif is_geo_field:
        analysis.update({
            "semantic_type": "geographic",
            "sql_preference": 0.7,
            "patterns": ["geographic_data"],
            "indexable": True,
            "relational": True
        })
    
    # Numeric patterns
    numeric_count = sum(1 for v in sample_values if str(v).replace('.', '').replace('-', '').isdigit())
    if numeric_count / total_samples > 0.9:
        analysis.update({
            "semantic_type": "numeric",
            "sql_preference": 0.8,
            "patterns": ["numeric_data"],
            "indexable": True,
            "relational": True
        })
    
    return analysis

def classify(stats):
    """
    Enhanced classification with multiple thresholds and semantic analysis
    stats: output from Analyzer.get_stats()
    returns: dict {field_name: 'sql' or 'mongo'}
    """
    decisions = {}
    classification_reasons = {}
    
    # Classification thresholds
    THRESHOLDS = {
        "very_high_freq": 0.9,     # 90%+ occurrence
        "high_freq": 0.7,          # 70%+ occurrence  
        "medium_freq": 0.5,        # 50%+ occurrence
        "low_freq": 0.3,           # 30%+ occurrence
        "very_unique": 0.95,       # 95%+ unique values
        "unique": 0.8,             # 80%+ unique values
        "semi_unique": 0.6,        # 60%+ unique values
        "common": 0.3              # 30%+ unique values
    }
    
    for field, s in stats.items():
        freq = s["freq"]
        uniqueness = s.get("uniqueness_ratio", 0)
        types_count = len(s["types"])
        is_nested = s["nested"]
        unique_values = s.get("unique", set())
        
        # Perform semantic analysis
        semantic_analysis = detect_value_types(field, unique_values)
        
        # Decision logic with multiple thresholds
        decision = "mongo"  # Default
        reason = "default"
        
        # Rule 1: Type conflicts always go to MongoDB (normalization conflict handling)
        if s.get("has_type_conflict", False):
            decision = "mongo"
            reason = "type_conflict_from_normalization"
            
        # Rule 2: Nested structures always go to MongoDB
        elif is_nested:
            decision = "mongo"
            reason = "nested_structure"
        
        # Rule 2: Semantic type strongly suggests SQL
        elif semantic_analysis["sql_preference"] >= 0.9:
            decision = "sql"
            reason = f"semantic_{semantic_analysis['semantic_type']}"
        
        # Rule 3: Primary key candidates (very unique + high frequency + single type)
        elif (uniqueness >= THRESHOLDS["very_unique"] and 
              freq >= THRESHOLDS["high_freq"] and 
              types_count == 1):
            decision = "sql"
            reason = "primary_key_candidate"
        
        # Rule 4: Foreign key candidates (unique + medium frequency + single type + relational semantic)
        elif (uniqueness >= THRESHOLDS["unique"] and 
              freq >= THRESHOLDS["medium_freq"] and 
              types_count == 1 and 
              semantic_analysis["relational"]):
            decision = "sql"
            reason = "foreign_key_candidate"
        
        # Rule 5: Indexed lookup fields (semi-unique + high frequency + single type)
        elif (uniqueness >= THRESHOLDS["semi_unique"] and 
              freq >= THRESHOLDS["very_high_freq"] and 
              types_count == 1):
            decision = "sql"
            reason = "indexed_lookup"
        
        # Rule 6: Category fields with good indexability (low uniqueness + high frequency + single type)
        elif (uniqueness <= THRESHOLDS["common"] and 
              freq >= THRESHOLDS["high_freq"] and 
              types_count == 1 and 
              semantic_analysis["indexable"]):
            decision = "sql"
            reason = "category_indexed"
        
        # Rule 7: Structured data with medium consistency
        elif (freq >= THRESHOLDS["medium_freq"] and 
              types_count == 1 and 
              not is_nested and 
              semantic_analysis["sql_preference"] >= 0.6):
            decision = "sql"
            reason = "structured_consistent"
        
        # Rule 8: Everything else goes to MongoDB (flexible schema)
        else:
            decision = "mongo"
            reason = "flexible_schema"
        
        decisions[field] = decision
        classification_reasons[field] = {
            "decision": decision,
            "reason": reason,
            "semantic_type": semantic_analysis["semantic_type"],
            "sql_preference": semantic_analysis["sql_preference"],
            "patterns": semantic_analysis["patterns"],
            "freq": freq,
            "uniqueness": uniqueness,
            "types_count": types_count
        }
    
    return decisions, classification_reasons

def get_classification_summary(classification_reasons):
    """
    Generate a summary of classification decisions and reasoning
    
    Args:
        classification_reasons (dict): Output from enhanced classify function
    
    Returns:
        dict: Summary statistics and reasoning breakdown
    """
    summary = {
        "total_fields": len(classification_reasons),
        "sql_fields": 0,
        "mongo_fields": 0,
        "by_reason": {},
        "by_semantic_type": {},
        "high_confidence_sql": [],
        "semantic_patterns": []
    }
    
    for field, info in classification_reasons.items():
        # Count by decision
        if info["decision"] == "sql":
            summary["sql_fields"] += 1
        else:
            summary["mongo_fields"] += 1
        
        # Count by reason
        reason = info["reason"]
        if reason not in summary["by_reason"]:
            summary["by_reason"][reason] = []
        summary["by_reason"][reason].append(field)
        
        # Count by semantic type
        semantic_type = info["semantic_type"]
        if semantic_type not in summary["by_semantic_type"]:
            summary["by_semantic_type"][semantic_type] = []
        summary["by_semantic_type"][semantic_type].append(field)
        
        # High confidence SQL decisions
        if info["decision"] == "sql" and info["sql_preference"] >= 0.8:
            summary["high_confidence_sql"].append({
                "field": field,
                "semantic_type": semantic_type,
                "confidence": info["sql_preference"],
                "reason": reason
            })
        
        # Collect unique patterns
        for pattern in info["patterns"]:
            if pattern not in summary["semantic_patterns"]:
                summary["semantic_patterns"].append(pattern)
    
    return summary


def classify_with_placement_heuristics(stats):
    """
    Advanced placement heuristics with multiple signals and composite scoring
    
    Signals considered:
    - freq: occurrences / total_records  
    - types_count: number of distinct types observed
    - uniqueness_ratio: unique_values / occurrences
    - stability: consistency across batches (0-1)
    - semantics: detected_kind with weights
    - length: avg/max length for strings
    
    Returns: (decisions, detailed_reasons)
    """
    decisions = {}
    placement_reasons = {}
    
    # Configurable thresholds
    THRESHOLDS = {
        'sql_freq_min': 0.60,
        'sql_stability_min': 0.80,
        'semi_unique_min': 0.70,
        'semi_unique_freq_min': 0.50,
        'composite_score_threshold': 0.65,
        'long_text_threshold': 120
    }
    
    for canonical, s in stats.items():
        # Extract signals
        freq = s['freq']
        types_count = s['types_count']
        uniqueness_ratio = s['uniqueness_ratio']
        stability = s['stability']
        nested = s['nested']
        has_type_conflict = s['has_type_conflict']
        semantic_info = s['semantic_info']
        composite_score = s['composite_score']
        
        # Enhanced: Extract drift information
        should_quarantine = s.get('should_quarantine', False)
        quarantine_reason = s.get('quarantine_reason', 'none')
        drift_analysis = s.get('drift_analysis', {})
        drift_score = drift_analysis.get('drift_score', 0.0)
        
        detected_kind = semantic_info['detected_kind']
        semantic_weight = semantic_info['semantic_weight']
        is_long_text = semantic_info['is_long_text']
        
        # Decision logic with detailed reasoning
        decision = "mongo"  # Default
        reason = "default"
        confidence = 0.5
        
        # Rule -1: Drift-based quarantine → MongoDB (highest priority)
        if should_quarantine:
            decision = "mongo"
            reason = f"drift_quarantine_{quarantine_reason}"
            confidence = max(0.1, 0.9 - drift_score)  # Confidence reduces with drift
            
        # Rule 0: Type conflicts from normalization → MongoDB
        elif has_type_conflict:
            decision = "mongo"
            reason = "type_conflict_from_normalization"
            confidence = 0.9 - (drift_score * 0.2)  # Reduce confidence if also has drift
            
        # Rule 1: Forced MongoDB cases
        elif nested:
            decision = "mongo" 
            reason = "nested_structure"
            confidence = 1.0
        elif is_long_text:
            decision = "mongo"
            reason = "long_text"
            confidence = 0.85
        elif detected_kind == 'json-like':
            decision = "mongo"
            reason = "json_like_structure"
            confidence = 0.9
            
        # Rule 2: SQL strong candidates
        elif (not nested and 
              freq >= THRESHOLDS['sql_freq_min'] and 
              types_count == 1 and 
              stability >= THRESHOLDS['sql_stability_min'] and
              detected_kind in {'timestamp', 'ip', 'email', 'uuid', 'username'}):
            decision = "sql"
            reason = "sql_strong_candidate"
            confidence = 0.9
            
        # Rule 3: SQL categorical with low cardinality
        elif (not nested and
              freq >= THRESHOLDS['sql_freq_min'] and
              types_count == 1 and
              stability >= THRESHOLDS['sql_stability_min'] and
              detected_kind == 'categorical'):
            decision = "sql"
            reason = "categorical_low_cardinality"
            confidence = 0.8
            
        # Rule 4: Semi-unique to SQL
        elif (uniqueness_ratio >= THRESHOLDS['semi_unique_min'] and
              freq >= THRESHOLDS['semi_unique_freq_min'] and
              types_count == 1):
            decision = "sql"
            reason = "semi_unique_field"
            confidence = 0.75
            
        # Rule 5: Composite score threshold
        elif composite_score >= THRESHOLDS['composite_score_threshold']:
            decision = "sql"
            reason = "composite_score_threshold"
            confidence = min(0.9, composite_score)
            
        # Rule 6: Default to MongoDB
        else:
            decision = "mongo"
            reason = "flexible_schema_default"
            confidence = 0.6
        
        decisions[canonical] = decision
        placement_reasons[canonical] = {
            'decision': decision,
            'reason': reason,
            'confidence': confidence,
            'signals': {
                'freq': freq,
                'uniqueness_ratio': uniqueness_ratio,
                'stability': stability,
                'semantic_type': detected_kind,
                'composite_score': composite_score,
                'types_count': types_count,
                'semantic_weight': semantic_weight,
                'drift_score': drift_score,
                'quarantine_reason': quarantine_reason if should_quarantine else None
            }
        }
    
    return decisions, placement_reasons


def get_placement_summary(placement_reasons):
    """
    Generate placement heuristics summary
    """
    summary = {
        'total_fields': len(placement_reasons),
        'sql_decisions': 0,
        'mongo_decisions': 0,
        'high_confidence_sql': [],
        'placement_breakdown': {},
        'semantic_distribution': {},
        'score_distribution': {'high': 0, 'medium': 0, 'low': 0}
    }
    
    for field, info in placement_reasons.items():
        decision = info['decision']
        reason = info['reason']
        confidence = info['confidence']
        signals = info['signals']
        
        # Count decisions
        if decision == 'sql':
            summary['sql_decisions'] += 1
        else:
            summary['mongo_decisions'] += 1
            
        # Group by reason
        if reason not in summary['placement_breakdown']:
            summary['placement_breakdown'][reason] = []
        summary['placement_breakdown'][reason].append(field)
        
        # Track semantic types
        semantic_type = signals['semantic_type']
        if semantic_type not in summary['semantic_distribution']:
            summary['semantic_distribution'][semantic_type] = {'sql': 0, 'mongo': 0}
        summary['semantic_distribution'][semantic_type][decision] += 1
        
        # Score distribution
        score = signals['composite_score']
        if score >= 0.8:
            summary['score_distribution']['high'] += 1
        elif score >= 0.5:
            summary['score_distribution']['medium'] += 1
        else:
            summary['score_distribution']['low'] += 1
            
        # High confidence SQL
        if decision == 'sql' and confidence >= 0.8:
            summary['high_confidence_sql'].append({
                'field': field,
                'confidence': confidence,
                'reason': reason,
                'score': score,
                'semantic_type': semantic_type
            })
    
    return summary

