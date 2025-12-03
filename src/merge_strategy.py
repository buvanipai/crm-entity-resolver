"""
Merge straegy with data provenance tracking.

Implements lossless merging where all source data is preserved,
allowing for audit trails and the ability to undo merges.
"""

from typing import List, Dict, Optional, Any
from datetime import datetime
from dataclasses import dataclass, asdict
import json

@dataclass
class SorceMetadata:
    """
    Tracks where a piece of data came from.
    """
    
    source: str
    timestamp: str
    original_record_id: str
    confidence: float = 1.0
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    
@dataclass
class MergedEntity:
    """
    Result of merging multiple entity records.
    """
    
    canonical_id: str
    canonical_name: str
    all_names: List[Dict[str, Any]]  # All name variations with sources
    all_emails: List[Dict[str, Any]]
    all_phones: List[Dict[str, Any]]
    all_companies: List[Dict[str, Any]]
    all_titles: List[Dict[str, Any]]
    other_fields: Dict[str, List[Dict[str, Any]]]
    source_records: List[Dict[str, Any]]  # Complete original records
    merge_timestamp: str
    conflicts: List[Dict[str, Any]]  # Conflicting data points
    
    def to_dict(self) -> Dict:
        return asdict(self)
    

class MergeStrategy:
    """
    Handles merging of entity records while preserving data provenance.
    Key: Never lose original data; always track sources.
    """
    
    def __init__(self):
        self.merge_history: List[MergedEntity] = []
        
    def merge_entities(
        self, 
        entities: List[Dict],
        primary_id: Optional[str] = None
        ) -> MergedEntity:
        """
        Merges multiple entity records into a single canonical entity.
        """
        
        if not entities:
            raise ValueError("Cannot merge empty entity list.")
        
        canonical_id = primary_id or entities[0].get("id", "merged_entity")
        timestamp = datetime.now().isoformat()
        
        all_names = self._collect_field_variations(entities, ["full_name", "first_name", "last_name"])
        all_emails = self._collect_field_variations(entities, ["email"])
        all_phones = self._collect_field_variations(entities, ["phone"])
        all_companies = self._collect_field_variations(entities, ["company"])
        all_titles = self._collect_field_variations(entities, ["title"])
        
        conflicts = self._detect_conflicts(entities)
        
        canonical_name = self._choose_canonical_value(all_names)
        
        other_fields = self._collect_other_fields(
            entities,
            exclude=["full_name", "first_name", "last_name", "email", "phone", "company", "title", "id", "source"]
        )
        
        merged = MergedEntity(
            canonical_id=canonical_id,
            canonical_name=canonical_name,
            all_names=all_names,
            all_emails=all_emails,
            all_phones=all_phones,
            all_companies=all_companies,
            all_titles=all_titles,
            other_fields=other_fields,
            source_records=entities,
            merge_timestamp=timestamp,
            conflicts=conflicts
        )
        
        self.merge_history.append(merged)
        return merged
    
    def _collect_field_variations(
        self, 
        entities: List[Dict], 
        field_names: List[str]
        ) -> List[Dict[str, Any]]:
        """
        Collects all variations of specified fields from the entities.
        """
        variations = []
        seen_values = set()
        for entity in entities:
            for field_name in field_names:
                if field_name in entity and entity[field_name]:
                    value = entity[field_name]
                    
                    if value not in seen_values:
                        seen_values.add(value)
                        variations.append({
                          "value": value,
                          "field": field_name,
                          "source": entity.get("source", "unknown"),
                          "record_id": entity.get("id", "unknown")
                        })
                        
        return variations
    
    def _detect_conflicts(self, entities: List[Dict]) -> List[Dict[str, Any]]:
        """
        Identifies conflicting values for the same field.
        """
        conflicts = []
        field_values = {}
        
        for entity in entities:
            for key, value in entity.items():
                if key in ["id", "source"] or not value:
                    continue
                
                if key not in field_values:
                    field_values[key] = []
                    
                field_values[key].append({
                    "value": value,
                    "source": entity.get("source", "unknown"),
                    "record_id": entity.get("id", "unknown")
                })
                
        for field, values in field_values.items():
            unique_values = set(v["value"] for v in values)
            if len(unique_values) > 1:
                conflicts.append({
                    "field": field,
                    "values": list(unique_values)
                })
                
        return conflicts
    
    def _choose_canonical_value(self, variations: List[Dict]) -> str:
        """
        Selects the canonical value from variations.
        """
        
        if not variations:
            return "Unknown"
        
        # Prefer full names over abbreviated
        full_names = [var for var in variations if '.' not in var["value"]]
        if full_names:
            return full_names[0]["value"]
        
        return variations[0]["value"]
    
    def _collect_other_fields(
        self, 
        entities: List[Dict], 
        exclude: List[str]
        ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Collects all other fields not explicitly handled.
        """
        other_fields = {}
        
        for entity in entities:
            for key, value in entity.items():
                if key in exclude or not value:
                    continue
                
                if key not in other_fields:
                    other_fields[key] = []
                    
                other_fields[key].append({
                    "value": value,
                    "source": entity.get("source", "unknown"),
                    "record_id": entity.get("id", "unknown")
                })
        
        return other_fields
    

if __name__ == "__main__":
    strategy = MergeStrategy()
    
    entities = [
        {
            'id': 'contact_1',
            'full_name': 'Sarah Chen',
            'email': 'sarah.chen@acme.com',
            'company': 'Acme Corp',
            'source': 'email'
        },
        {
            'id': 'contact_2',
            'full_name': 'S. Chen',
            'title': 'VP Engineering',
            'company': 'Acme Corp',
            'source': 'calendar'
        },
        {
            'id': 'contact_3',
            'full_name': 'Sarah Chen',
            'phone': '+1-555-0123',
            'linkedin': 'linkedin.com/in/sarachen',
            'source': 'linkedin'
        }
    ]
    
    merged = strategy.merge_entities(entities)
    
    print("MERGED ENTITY:")
    print(json.dumps(merged.to_dict(), indent=2))