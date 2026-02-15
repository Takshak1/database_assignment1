from collections import defaultdict
import re
from drift_detector import TypeDriftDetector

def detect_type_ambiguity(field_name, values_sample):
    """
    Detect type ambiguities for the same field across different records.
    Focus on cases where same field name has different data types.
    
    Returns:
        dict: {
            'has_type_ambiguity': bool,
            'types_detected': list,
            'ambiguity_score': float (0-1, where 1 = high ambiguity)
        }
    """
    if not values_sample:
        return {
            'has_type_ambiguity': False,
            'types_detected': [],
            'ambiguity_score': 0.0
        }
    
    # Detect all types present
    types_found = set()
    for value in values_sample:
        try:
            # Try to determine actual type
            if isinstance(value, str):
                # Check if string represents other types
                if value.isdigit():
                    types_found.add('potential_int')
                elif value.replace('.', '').replace('-', '').isdigit():
                    types_found.add('potential_float')
                elif value.lower() in ['true', 'false']:
                    types_found.add('potential_bool')
                else:
                    types_found.add('str')
            else:
                types_found.add(type(value).__name__)
        except:
            types_found.add('unknown')
    
    has_ambiguity = len(types_found) > 1
    ambiguity_score = min(1.0, (len(types_found) - 1) / 3.0)  # Normalize to 0-1
    
    return {
        'has_type_ambiguity': has_ambiguity,
        'types_detected': list(types_found),
        'ambiguity_score': ambiguity_score
    }

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
            "batch_history": [],  # Track per-batch presence and types
            "values_sample": set()  # For semantic analysis
        })
        # Type ambiguity tracking - focus on same field having different types
        self.type_conflicts = defaultdict(list)  # field_name -> [(value, type, batch)]
        
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
            for field_name in self.stats:
                self.stats[field_name]["batch_history"].append({
                    "batch": self.current_batch,
                    "present": False,
                    "types": set()
                })

        for field_name, value in record.items():
            # Use original field name - no normalization
            s = self.stats[field_name]
            s["count"] += 1
            
            # Type tracking for ambiguity detection
            value_type = type(value).__name__
            old_types = s["types"].copy()
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
            self.batch_types_tracking[self.current_batch][field_name].add(value_type)
            
            # Detect type ambiguities - same field with different types
            if len(s["types"]) > 1 and len(old_types) < len(s["types"]):
                # New type detected for existing field
                self.type_conflicts[field_name].append((str(value), value_type, self.current_batch))

    def calculate_stability(self, field_name):
        """
        Calculate stability score (0-1) based on consistent presence and type across batches
        """
        s = self.stats[field_name]
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

        for field_name, s in self.stats.items():
            uniqueness_ratio = len(s["unique"]) / s["count"] if s["count"] > 0 else 0
            is_unique_field = uniqueness_ratio >= 0.95  # 95% or higher uniqueness
            
            # Check for type ambiguities - same field with multiple types
            has_type_ambiguity = len(s["types"]) > 1
            
            # Calculate stability
            stability = self.calculate_stability(field_name)
            
            # Semantic analysis
            semantic_info = detect_semantic_type(field_name, s["values_sample"])
            
            # Type ambiguity analysis
            ambiguity_info = detect_type_ambiguity(field_name, s["values_sample"])
            
            # Drift analysis
            drift_analysis = self.drift_detector.calculate_drift_score(field_name)
            quarantine_check = self.drift_detector.should_quarantine_field(field_name)
            
            # Calculate composite placement score with type ambiguity penalty
            freq = s["count"] / self.total
            types_count = len(s["types"])
            
            # Uniqueness weight
            if uniqueness_ratio >= 0.95 and freq >= 0.5:
                uniqueness_weight = 0.20
            elif 0.70 <= uniqueness_ratio < 0.95:
                uniqueness_weight = 0.15
            else:
                uniqueness_weight = 0.0
            
            # Type consistency weight - penalize multiple types (type ambiguities)
            type_weight = 1.0 - min(abs(types_count - 1), 1.0)
            
            # Composite score (reduced by drift)
            drift_penalty = drift_analysis['drift_score'] * 0.3  # Reduce score by up to 30% for high drift
            score = (0.30 * freq + 
                    0.20 * stability + 
                    0.20 * type_weight + 
                    0.15 * (semantic_info['semantic_weight'] + 0.10) +  # Normalize to positive
                    0.15 * uniqueness_weight) - drift_penalty
            
            result[field_name] = {
                "freq": freq,
                "types": s["types"],
                "unique_count": len(s["unique"]),
                "uniqueness_ratio": uniqueness_ratio,
                "is_unique_field": is_unique_field,
                "nested": s["nested"],
                "field_name": field_name,
                "has_type_ambiguity": has_type_ambiguity,
                "stability": stability,
                "semantic_info": semantic_info,
                "ambiguity_info": ambiguity_info,
                "composite_score": max(0.0, score),  # Ensure non-negative
                "types_count": types_count,
                # Drift information
                "drift_analysis": drift_analysis,
                "should_quarantine": quarantine_check['should_quarantine'],
                "quarantine_reason": quarantine_check['reason'],
                "drift_report": self.drift_detector.generate_drift_report(field_name)
            }

        return result
    
    def get_normalization_report(self):
        """
        Generate type ambiguity report showing fields with multiple types.
        Returns dict with type ambiguity statistics and conflicts.
        """
        report = {
            "total_fields": len(self.stats),
            "fields_with_type_ambiguity": len(self.type_conflicts),
            "ambiguous_fields": {},
            "clean_fields": {}
        }
        
        # Report fields with type ambiguities
        for field_name, conflicts in self.type_conflicts.items():
            field_types = list(self.stats[field_name]["types"])
            report["ambiguous_fields"][field_name] = {
                "types_detected": field_types,
                "type_conflicts": conflicts,
                "should_route_to_mongo": True,
                "reason": "type_ambiguity_detected"
            }
        
        # Report clean fields (no type ambiguities)
        for field_name, stats in self.stats.items():
            if len(stats["types"]) == 1:
                report["clean_fields"][field_name] = {
                    "type": list(stats["types"])[0],
                    "count": stats["count"],
                    "suitable_for_mysql": True
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
