from collections import defaultdict
import re
from drift_detector import TypeDriftDetector

def normalize_field_name(field_name):
    """
    Normalize field names to canonical form: lowercase, snake_case, no special chars.
    Examples: 'IP' -> 'ip', 'IpAddress' -> 'ip_address', 'ip-address' -> 'ip_address'
    """
    # Convert to string if not already
    field_name = str(field_name)
    
    # Replace hyphens and spaces with underscores
    normalized = re.sub(r'[-\s]+', '_', field_name)
    
    # Insert underscores before uppercase letters (camelCase to snake_case)
    normalized = re.sub(r'([a-z])([A-Z])', r'\1_\2', normalized)
    
    # Convert to lowercase
    normalized = normalized.lower()
    
    # Remove any remaining special characters except underscores
    normalized = re.sub(r'[^a-z0-9_]', '', normalized)
    
    # Clean up multiple underscores
    normalized = re.sub(r'_+', '_', normalized)
    
    # Remove leading/trailing underscores
    normalized = normalized.strip('_')
    
    return normalized or 'field'  # fallback if empty

def detect_semantic_type(field_name, values_sample):
    """
    Detect semantic type and assign weights for placement decisions
    
    Returns:
        dict: {
            'detected_kind': str,
            'semantic_weight': float,
            'avg_length': float,
            'max_length': int,
            'is_long_text': bool
        }
    """
    sample_values = [str(v) for v in list(values_sample)[:50]]  # Sample up to 50
    if not sample_values:
        return {
            'detected_kind': 'unknown',
            'semantic_weight': 0.0,
            'avg_length': 0.0,
            'max_length': 0,
            'is_long_text': False
        }
    
    # Length analysis
    lengths = [len(v) for v in sample_values]
    avg_length = sum(lengths) / len(lengths)
    max_length = max(lengths)
    is_long_text = avg_length >= 120
    
    # Pattern matching
    ip_pattern = re.compile(r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$')
    email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    uuid_pattern = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
    timestamp_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}')
    
    # Count matches
    total = len(sample_values)
    ip_matches = sum(1 for v in sample_values if ip_pattern.match(v))
    email_matches = sum(1 for v in sample_values if email_pattern.match(v))
    uuid_matches = sum(1 for v in sample_values if uuid_pattern.match(v))
    timestamp_matches = sum(1 for v in sample_values if timestamp_pattern.match(v))
    numeric_matches = sum(1 for v in sample_values if v.replace('.', '').replace('-', '').isdigit())
    
    # Determine semantic type
    if ip_matches / total >= 0.8:
        return {
            'detected_kind': 'ip',
            'semantic_weight': 0.15,
            'avg_length': avg_length,
            'max_length': max_length,
            'is_long_text': is_long_text
        }
    elif email_matches / total >= 0.8:
        return {
            'detected_kind': 'email',
            'semantic_weight': 0.15,
            'avg_length': avg_length,
            'max_length': max_length,
            'is_long_text': is_long_text
        }
    elif uuid_matches / total >= 0.8:
        return {
            'detected_kind': 'uuid',
            'semantic_weight': 0.15,
            'avg_length': avg_length,
            'max_length': max_length,
            'is_long_text': is_long_text
        }
    elif timestamp_matches / total >= 0.8:
        return {
            'detected_kind': 'timestamp',
            'semantic_weight': 0.15,
            'avg_length': avg_length,
            'max_length': max_length,
            'is_long_text': is_long_text
        }
    elif 'username' in field_name.lower() or 'user_name' in field_name.lower():
        return {
            'detected_kind': 'username',
            'semantic_weight': 0.15,
            'avg_length': avg_length,
            'max_length': max_length,
            'is_long_text': is_long_text
        }
    elif numeric_matches / total >= 0.9:
        # Check if categorical (low cardinality) or continuous
        unique_count = len(set(sample_values))
        if unique_count <= 20:  # Low cardinality = categorical
            return {
                'detected_kind': 'categorical',
                'semantic_weight': 0.10,
                'avg_length': avg_length,
                'max_length': max_length,
                'is_long_text': is_long_text
            }
        else:
            return {
                'detected_kind': 'continuous',
                'semantic_weight': 0.05,
                'avg_length': avg_length,
                'max_length': max_length,
                'is_long_text': is_long_text
            }
    elif is_long_text:
        return {
            'detected_kind': 'long_text',
            'semantic_weight': -0.10,
            'avg_length': avg_length,
            'max_length': max_length,
            'is_long_text': is_long_text
        }
    else:
        return {
            'detected_kind': 'unknown',
            'semantic_weight': 0.0,
            'avg_length': avg_length,
            'max_length': max_length,
            'is_long_text': is_long_text
        }

class Analyzer:
    def __init__(self):
        self.total = 0
        self.batch_size = 10  # Track stability per batch
        self.current_batch = 0
        self.stats = defaultdict(lambda: {
            "count": 0,
            "types": set(),
            "unique": set(),
            "nested": False,
            "raw_names": set(),
            "batch_history": [],  # Track per-batch presence and types
            "values_sample": set()  # For semantic analysis
        })
        # Normalization tracking
        self.canonical_to_aliases = defaultdict(set)  # canonical -> {original_names}
        self.raw_to_canonical = {}  # original_name -> canonical_name
        self.column_registry = set()  # canonical names that have been "claimed"
        self.normalization_conflicts = defaultdict(list)  # canonical -> [(raw_name, type_conflicts)]
        
        # Enhanced drift detection
        self.drift_detector = TypeDriftDetector(window_size=50, drift_threshold=0.20)
        self.batch_types_tracking = defaultdict(lambda: defaultdict(set))  # batch -> field -> types

    def update(self, record):
        self.total += 1
        
        # Track batches for stability analysis
        if self.total % self.batch_size == 1:
            self.current_batch += 1
            # Process drift detection for completed batch
            if self.current_batch > 1:
                self._process_batch_drift_detection()
            
            # Initialize batch tracking for all existing fields
            for canonical in self.stats:
                self.stats[canonical]["batch_history"].append({
                    "batch": self.current_batch,
                    "present": False,
                    "types": set()
                })

        for raw_field, value in record.items():
            # Normalize the field name
            canonical = normalize_field_name(raw_field)
            
            # Track the mapping
            self.raw_to_canonical[raw_field] = canonical
            self.canonical_to_aliases[canonical].add(raw_field)
            
            # Use canonical name for stats
            s = self.stats[canonical]
            s["count"] += 1
            s["raw_names"].add(raw_field)
            
            # Type tracking for conflict detection
            value_type = type(value).__name__
            s["types"].add(value_type)
            s["unique"].add(str(value))
            
            # Keep sample values for semantic analysis (limit to 100)
            if len(s["values_sample"]) < 100:
                s["values_sample"].add(str(value))

            if isinstance(value, (dict, list)):
                s["nested"] = True
            
            # Update current batch tracking
            if s["batch_history"] and s["batch_history"][-1]["batch"] == self.current_batch:
                s["batch_history"][-1]["present"] = True
                s["batch_history"][-1]["types"].add(value_type)
            else:
                s["batch_history"].append({
                    "batch": self.current_batch,
                    "present": True,
                    "types": {value_type}
                })
            
            # Track types per batch for drift detection
            self.batch_types_tracking[self.current_batch][canonical].add(value_type)
            
            # Detect type conflicts for this canonical name
            if len(s["types"]) > 1:
                # Multiple types detected for same canonical name
                type_list = list(s["types"])
                if canonical not in [conf[0] for conf in self.normalization_conflicts[canonical]]:
                    self.normalization_conflicts[canonical].append((raw_field, type_list))

    def calculate_stability(self, canonical):
        """
        Calculate stability score (0-1) based on consistent presence and type across batches
        """
        s = self.stats[canonical]
        if not s["batch_history"] or len(s["batch_history"]) < 2:
            return 1.0  # Not enough data, assume stable
        
        # Check presence consistency
        total_batches = len(s["batch_history"])
        present_batches = sum(1 for batch in s["batch_history"] if batch["present"])
        presence_ratio = present_batches / total_batches if total_batches > 0 else 0
        
        # Check type consistency across batches where field was present
        present_batch_types = [batch["types"] for batch in s["batch_history"] if batch["present"]]
        if not present_batch_types:
            return 0.0
        
        # Calculate type stability (how often the same type set appears)
        type_consistency = 0.0
        if present_batch_types:
            most_common_types = max(present_batch_types, key=lambda x: len(x))
            consistent_batches = sum(1 for types in present_batch_types if types == most_common_types)
            type_consistency = consistent_batches / len(present_batch_types)
        
        # Combine presence and type stability
        stability = 0.6 * presence_ratio + 0.4 * type_consistency
        return min(1.0, max(0.0, stability))
    
    def _process_batch_drift_detection(self):
        """
        Process completed batch for type drift detection
        """
        prev_batch = self.current_batch - 1
        if prev_batch in self.batch_types_tracking:
            for field, types in self.batch_types_tracking[prev_batch].items():
                # Update drift detector with batch types
                self.drift_detector.update_field_types(field, types)
            
            # Clean up old batch data to save memory
            del self.batch_types_tracking[prev_batch]

    def get_stats(self):
        result = {}

        for canonical, s in self.stats.items():
            uniqueness_ratio = len(s["unique"]) / s["count"] if s["count"] > 0 else 0
            is_unique_field = uniqueness_ratio >= 0.95  # 95% or higher uniqueness
            
            # Check for type conflicts
            has_type_conflict = len(s["types"]) > 1
            
            # Calculate stability
            stability = self.calculate_stability(canonical)
            
            # Semantic analysis
            semantic_info = detect_semantic_type(canonical, s["values_sample"])
            
            # Drift analysis
            drift_analysis = self.drift_detector.calculate_drift_score(canonical)
            quarantine_check = self.drift_detector.should_quarantine_field(canonical)
            
            # Calculate composite placement score
            freq = s["count"] / self.total
            types_count = len(s["types"])
            
            # Uniqueness weight
            if uniqueness_ratio >= 0.95 and freq >= 0.5:
                uniqueness_weight = 0.20
            elif 0.70 <= uniqueness_ratio < 0.95:
                uniqueness_weight = 0.15
            else:
                uniqueness_weight = 0.0
            
            # Type consistency weight (1 - abs(types_count - 1)) normalizes to 1.0 for single type
            type_weight = 1.0 - min(abs(types_count - 1), 1.0)
            
            # Composite score (reduced by drift)
            drift_penalty = drift_analysis['drift_score'] * 0.3  # Reduce score by up to 30% for high drift
            score = (0.30 * freq + 
                    0.20 * stability + 
                    0.20 * type_weight + 
                    0.15 * (semantic_info['semantic_weight'] + 0.10) +  # Normalize to positive
                    0.15 * uniqueness_weight) - drift_penalty
            
            result[canonical] = {
                "freq": freq,
                "types": s["types"],
                "unique_count": len(s["unique"]),
                "uniqueness_ratio": uniqueness_ratio,
                "is_unique_field": is_unique_field,
                "nested": s["nested"],
                "raw_names": s["raw_names"],
                "has_type_conflict": has_type_conflict,
                "canonical_name": canonical,
                "stability": stability,
                "semantic_info": semantic_info,
                "composite_score": max(0.0, score),  # Ensure non-negative
                "types_count": types_count,
                # Drift information
                "drift_analysis": drift_analysis,
                "should_quarantine": quarantine_check['should_quarantine'],
                "quarantine_reason": quarantine_check['reason'],
                "drift_report": self.drift_detector.generate_drift_report(canonical)
            }

        return result
    
    def get_normalization_report(self):
        """
        Generate normalization report showing aliases and conflicts.
        Returns dict with normalization statistics and conflicts.
        """
        report = {
            "total_raw_fields": len(self.raw_to_canonical),
            "canonical_fields": len(self.stats),
            "aliases_resolved": 0,
            "type_conflicts": len(self.normalization_conflicts),
            "alias_mappings": {},
            "conflicts": {}
        }
        
        # Count aliases (canonical names with multiple raw names)
        for canonical, raw_names in self.canonical_to_aliases.items():
            if len(raw_names) > 1:
                report["aliases_resolved"] += 1
                report["alias_mappings"][canonical] = list(raw_names)
        
        # Report type conflicts
        for canonical, conflicts in self.normalization_conflicts.items():
            report["conflicts"][canonical] = {
                "raw_names": list(self.canonical_to_aliases[canonical]),
                "conflicting_types": list(self.stats[canonical]["types"]),
                "should_route_to_mongo": True
            }
        
        return report
    
    def get_unique_fields(self, threshold=0.95):
        """
        Returns fields that have a uniqueness ratio above the threshold.
        
        Args:
            threshold (float): Uniqueness ratio threshold (default: 0.95)
            
        Returns:
            dict: Fields with high uniqueness ratios
        """
        unique_fields = {}
        
        for field, stats in self.stats.items():
            if stats["count"] > 0:
                uniqueness_ratio = len(stats["unique"]) / stats["count"]
                if uniqueness_ratio >= threshold:
                    unique_fields[field] = {
                        "uniqueness_ratio": uniqueness_ratio,
                        "unique_count": len(stats["unique"]),
                        "total_count": stats["count"],
                        "types": stats["types"]
                    }
        
        return unique_fields
    
    def analyze_field_uniqueness(self):
        """
        Provides detailed uniqueness analysis for all fields.
        
        Returns:
            dict: Comprehensive uniqueness analysis
        """
        analysis = {
            "total_records": self.total,
            "field_analysis": {},
            "unique_fields": [],
            "semi_unique_fields": [],
            "common_fields": []
        }
        
        for field, stats in self.stats.items():
            if stats["count"] > 0:
                uniqueness_ratio = len(stats["unique"]) / stats["count"]
                
                field_info = {
                    "field": field,
                    "uniqueness_ratio": round(uniqueness_ratio, 4),
                    "unique_values": len(stats["unique"]),
                    "total_occurrences": stats["count"],
                    "frequency": stats["count"] / self.total,
                    "data_types": list(stats["types"]),
                    "is_nested": stats["nested"]
                }
                
                analysis["field_analysis"][field] = field_info
                
                # Categorize fields by uniqueness
                if uniqueness_ratio >= 0.95:
                    analysis["unique_fields"].append(field_info)
                elif uniqueness_ratio >= 0.7:
                    analysis["semi_unique_fields"].append(field_info)
                else:
                    analysis["common_fields"].append(field_info)
        
        # Sort by uniqueness ratio (descending)
        analysis["unique_fields"].sort(key=lambda x: x["uniqueness_ratio"], reverse=True)
        analysis["semi_unique_fields"].sort(key=lambda x: x["uniqueness_ratio"], reverse=True)
        analysis["common_fields"].sort(key=lambda x: x["uniqueness_ratio"], reverse=True)
        
        return analysis
    
    def get_drift_summary(self):
        """
        Get comprehensive drift detection summary
        """
        return self.drift_detector.get_drift_summary()
