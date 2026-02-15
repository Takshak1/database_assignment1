"""
drift_detector.py - Advanced Type Drift Detection and Mixed Data Handling
"""
from collections import defaultdict, Counter

class TypeDriftDetector:
    """
    Detects and handles type drift in streaming data with quarantine mechanisms
    """
    
    def __init__(self, window_size=50, drift_threshold=0.20):
        self.window_size = window_size  # Size of sliding window for drift detection
        self.drift_threshold = drift_threshold  # Minimum drift score to trigger action
        
        # Track type history per field in sliding windows
        self.field_windows = defaultdict(list)  # field -> list of type_distributions
        self.quarantined_fields = set()  # Fields banned from SQL due to drift
        self.drift_events = []  # Log of drift detection events
        
        # Track flip patterns (e.g., str->int->str)
        self.type_sequences = defaultdict(list)  # field -> [type1, type2, type3, ...]
        
    def update_field_types(self, field, batch_types):
        """
        Update type tracking for a field with types from current batch
        
        Args:
            field: Field name
            batch_types: Set of types observed in this batch for this field
        """
        # Convert set to sorted list for consistency
        types_list = sorted(list(batch_types))
        
        # Update type sequence for flip detection
        if types_list != self.type_sequences[field][-1:]:  # Different from last
            self.type_sequences[field].extend(types_list)
            # Keep only last 10 type changes for pattern detection
            if len(self.type_sequences[field]) > 10:
                self.type_sequences[field] = self.type_sequences[field][-10:]
        
        # Calculate type distribution for this batch
        if len(batch_types) > 1:
            # Multiple types in same batch = immediate high drift
            type_dist = {t: 1.0/len(batch_types) for t in batch_types}
        else:
            # Single type in batch
            type_dist = {list(batch_types)[0]: 1.0} if batch_types else {}
        
        # Add to sliding window
        self.field_windows[field].append({
            'types': batch_types,
            'distribution': type_dist,
            'batch_id': len(self.field_windows[field])
        })
        
        # Maintain sliding window size
        if len(self.field_windows[field]) > self.window_size:
            self.field_windows[field] = self.field_windows[field][-self.window_size:]
    
    def calculate_drift_score(self, field):
        """
        Calculate type drift score for a field
        
        Returns:
            dict: {
                'drift_score': float (0-1),
                'dominant_type': str,
                'type_shares': dict,
                'has_drift': bool,
                'flip_patterns': list
            }
        """
        if field not in self.field_windows or len(self.field_windows[field]) < 2:
            return {
                'drift_score': 0.0,
                'dominant_type': 'unknown',
                'type_shares': {},
                'has_drift': False,
                'flip_patterns': []
            }
        
        # Aggregate type counts across all windows
        all_types = Counter()
        total_windows = len(self.field_windows[field])
        
        for window in self.field_windows[field]:
            for type_name in window['types']:
                all_types[type_name] += 1
        
        # Calculate type shares
        type_shares = {t: count/total_windows for t, count in all_types.items()}
        
        # Calculate drift score = 1 - max(type_share)
        max_share = max(type_shares.values()) if type_shares else 1.0
        drift_score = 1.0 - max_share
        
        # Find dominant type
        dominant_type = max(type_shares, key=type_shares.get) if type_shares else 'unknown'
        
        # Detect flip patterns
        flip_patterns = self.detect_flip_patterns(field)
        
        # Determine if drift is significant
        has_drift = drift_score >= self.drift_threshold
        
        return {
            'drift_score': drift_score,
            'dominant_type': dominant_type,
            'type_shares': type_shares,
            'has_drift': has_drift,
            'flip_patterns': flip_patterns,
            'window_count': total_windows
        }
    
    def detect_flip_patterns(self, field):
        """
        Detect common flip patterns like string→number→string
        
        Returns:
            list: Detected patterns
        """
        if field not in self.type_sequences or len(self.type_sequences[field]) < 3:
            return []
        
        patterns = []
        sequence = self.type_sequences[field]
        
        # Look for specific patterns
        for i in range(len(sequence) - 2):
            triple = (sequence[i], sequence[i+1], sequence[i+2])
            
            # String → Number → String
            if (triple[0] in ['str', 'string'] and 
                triple[1] in ['int', 'float', 'number'] and 
                triple[2] in ['str', 'string']):
                patterns.append('str→num→str')
            
            # Number → String → Number  
            elif (triple[0] in ['int', 'float', 'number'] and 
                  triple[1] in ['str', 'string'] and 
                  triple[2] in ['int', 'float', 'number']):
                patterns.append('num→str→num')
            
            # Any A→B→A pattern
            elif triple[0] == triple[2] and triple[0] != triple[1]:
                patterns.append(f'{triple[0]}→{triple[1]}→{triple[0]}')
        
        return list(set(patterns))  # Remove duplicates
    
    def should_quarantine_field(self, field):
        """
        Determine if a field should be quarantined from SQL due to drift
        
        Returns:
            dict: {
                'should_quarantine': bool,
                'reason': str,
                'drift_analysis': dict
            }
        """
        drift_analysis = self.calculate_drift_score(field)
        
        # Already quarantined
        if field in self.quarantined_fields:
            return {
                'should_quarantine': True,
                'reason': 'already_quarantined',
                'drift_analysis': drift_analysis
            }
        
        # High drift score
        if drift_analysis['has_drift']:
            reason = f"high_drift_score_{drift_analysis['drift_score']:.2f}"
            if drift_analysis['flip_patterns']:
                reason += f"_with_patterns_{'+'.join(drift_analysis['flip_patterns'])}"
            
            return {
                'should_quarantine': True,
                'reason': reason,
                'drift_analysis': drift_analysis
            }
        
        # Multiple flip patterns detected
        if len(drift_analysis['flip_patterns']) >= 2:
            return {
                'should_quarantine': True,
                'reason': f"multiple_flip_patterns_{'+'.join(drift_analysis['flip_patterns'])}",
                'drift_analysis': drift_analysis
            }
        
        return {
            'should_quarantine': False,
            'reason': 'stable',
            'drift_analysis': drift_analysis
        }
    
    def quarantine_field(self, field, reason="manual"):
        """
        Quarantine a field from SQL placement
        """
        if field not in self.quarantined_fields:
            self.quarantined_fields.add(field)
            self.drift_events.append({
                'field': field,
                'action': 'quarantined',
                'reason': reason,
                'timestamp': len(self.drift_events)
            })
            return True
        return False
    
    def get_drift_summary(self):
        """
        Generate comprehensive drift analysis summary
        """
        summary = {
            'total_fields_tracked': len(self.field_windows),
            'quarantined_fields': len(self.quarantined_fields),
            'drift_events': len(self.drift_events),
            'high_drift_fields': [],
            'stable_fields': [],
            'drift_patterns': {},
            'quarantine_list': list(self.quarantined_fields),
            'recent_events': self.drift_events[-5:]  # Last 5 events
        }
        
        # Analyze all tracked fields
        for field in self.field_windows:
            drift_analysis = self.calculate_drift_score(field)
            
            field_info = {
                'field': field,
                'drift_score': drift_analysis['drift_score'],
                'dominant_type': drift_analysis['dominant_type'],
                'type_shares': drift_analysis['type_shares'],
                'flip_patterns': drift_analysis['flip_patterns'],
                'is_quarantined': field in self.quarantined_fields
            }
            
            if drift_analysis['has_drift']:
                summary['high_drift_fields'].append(field_info)
            else:
                summary['stable_fields'].append(field_info)
            
            # Collect patterns
            for pattern in drift_analysis['flip_patterns']:
                if pattern not in summary['drift_patterns']:
                    summary['drift_patterns'][pattern] = []
                summary['drift_patterns'][pattern].append(field)
        
        # Sort by drift score
        summary['high_drift_fields'].sort(key=lambda x: x['drift_score'], reverse=True)
        summary['stable_fields'].sort(key=lambda x: x['drift_score'], reverse=True)
        
        return summary
    
    def generate_drift_report(self, field):
        """
        Generate detailed drift report for a specific field
        
        Returns:
            str: Human-readable drift analysis
        """
        quarantine_check = self.should_quarantine_field(field)
        drift_analysis = quarantine_check['drift_analysis']
        
        if not drift_analysis['type_shares']:
            return f"Mixed data: '{field}' - no type data available"
        
        # Format type shares
        type_shares_str = ', '.join([
            f"{t} {share:.0%}" for t, share in drift_analysis['type_shares'].items()
        ])
        
        # Base message
        report = f"Mixed data: '{field}' showed type drift ({type_shares_str})"
        
        # Add routing decision
        if quarantine_check['should_quarantine']:
            report += "; routed to Mongo"
        else:
            report += "; stable enough for SQL"
        
        # Add confidence
        confidence = max(0.1, 1.0 - drift_analysis['drift_score'])
        report += f". Confidence={confidence:.2f}"
        
        # Add patterns if detected
        if drift_analysis['flip_patterns']:
            patterns_str = ', '.join(drift_analysis['flip_patterns'])
            report += f" (patterns: {patterns_str})"
        
        return report