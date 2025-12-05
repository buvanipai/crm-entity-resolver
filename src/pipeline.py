"""
End-to-end entity resolution pipeline.

Takes raw contact data, identifies duplicates, merges them,
and outputs a clean deduplicated dataset.
"""

import json
from typing import List, Dict, Tuple
from entity_resolver import EntityResolver
from merge_strategy import MergeStrategy
from datetime import datetime


class EntityResolutionPipeline:
    """
    Complete pipeline for contact deduplication.
    """
    
    def __init__(self, confidence_threshold: float = 0.7):
        self.resolver = EntityResolver()
        self.merger = MergeStrategy()
        self.confidence_threshold = confidence_threshold
        
    def find_duplicates(self, contacts: List[Dict]) -> List[Tuple[Dict, Dict, float]]:
        """
        Finds all the potential duplicate pairs in contact list.
        """
        
        duplicates = []
        n = len(contacts)
        
        print(f"Scanning {n} contacts for duplicates...")
        print(f"Total comparisons needed: {n * (n - 1) // 2}")
        
        compared = 0
        for i in range(n):
            for j in range(i + 1, n):
                compared += 1
                if compared % 50 == 0:
                    print(f"Progress: {compared} comparisons done.")
                
                decision = self.resolver.should_merge(contacts[i], contacts[j])
                
                if compared <= 3:
                    print(f"Comparison {compared}:")
                    print(f"Contact A: {contacts[i]}")
                    print(f"Contact B: {contacts[j]}")
                    print(f"Should Merge: {decision.should_merge}, Confidence: {decision.confidence:.2f}")
                    print(f"Reasoning: {decision.reasoning[:100]}\n")
                
                if decision.should_merge and decision.confidence >= self.confidence_threshold:
                    duplicates.append((contacts[i], contacts[j], decision.confidence))
                    
        print(f"Found {len(duplicates)} duplicate pairs.")
        return duplicates
    
    def deduplicate(self, contacts: List[Dict]) -> Tuple[List[Dict], Dict]:
        """
        Runs the full deduplication pipeline.
        """
        start_time = datetime.now()
        
        duplicate_pairs = self.find_duplicates(contacts)
        
        merge_groups = self._build_merge_groups(duplicate_pairs)
        
        merged_entities = []
        for group in merge_groups:
            merged = self.merger.merge_entities(group)
            merged_entities.append(merged.to_dict())
            
        merged_ids = set()
        for group in merge_groups:
            for contact in group:
                merged_ids.add(contact['id'])
                
        unique_contacts = [c for c in contacts if c['id'] not in merged_ids]
        
        end_time = datetime.now()
        
        stats = {
            'original_count': len(contacts),
            'duplicate_pairs_found': len(duplicate_pairs),
            'merge_groups': len(merge_groups),
            'final_count': len(merged_entities) + len(unique_contacts),
            'reduction': len(contacts) - (len(merged_entities) + len(unique_contacts)),
            'processing_time': str(end_time - start_time)
        }
        
        return merged_entities + unique_contacts, stats
    
    def _build_merge_groups(self, duplicate_pairs: List[Tuple[Dict, Dict, float]]) -> List[List[Dict]]:
        """
        Groups transitive duplicates together.
        """
        
        id_to_contacts = {}
        parent = {}
        
        def find(x):
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]
        
        def union(x, y):
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py
                
        for entity_a, entity_b, _ in duplicate_pairs:
            id_a, id_b = entity_a['id'], entity_b['id']
            id_to_contacts[id_a] = entity_a
            id_to_contacts[id_b] = entity_b
            if id_a not in parent:
                parent[id_a] = id_a
            if id_b not in parent:
                parent[id_b] = id_b
            union(id_a, id_b)
            
            
        groups_dict = {}
        for contact_id in parent:
            root = find(contact_id)
            if root not in groups_dict:
                groups_dict[root] = []
            groups_dict[root].append(id_to_contacts[contact_id])
            
        return list(groups_dict.values())
    
    
if __name__ == "__main__":
    with open("data/contacts.json", "r") as f:
        contacts = json.load(f)
        
    with open("data/ground_truth.json", "r") as f:
        ground_truth = json.load(f)
        
    contact_lookup = {c['id']: c for c in contacts}
    
    test_contacts = []
    seen_ids = set()
    for gt in ground_truth[:10]:
        if gt['entity_a_id'] not in seen_ids:
            test_contacts.append(contact_lookup[gt['entity_a_id']])
            seen_ids.add(gt['entity_a_id'])
        if gt['entity_b_id'] not in seen_ids:
            test_contacts.append(contact_lookup[gt['entity_b_id']])
            seen_ids.add(gt['entity_b_id'])
        
    print(f"Testing pipeline on {len(test_contacts)} contacts\n")
    
    pipeline = EntityResolutionPipeline(confidence_threshold=0.5)
    deduplicated_contacts, stats = pipeline.deduplicate(test_contacts)
    
    print("\n" + "=" * 40)
    print("DEDUPLICATION RESULTS")
    print("=" * 40 + "\n")
    
    for key, value in stats.items():
        print(f"{key}: {value}")
    
    with open("results/deduplicated_contacts.json", "w") as f:
        json.dump(deduplicated_contacts, f, indent=2)
        
    with open("results/deduplication_stats.json", "w") as f:
        json.dump(stats, f, indent=2)
        
    print("Deduplication complete. Results saved to results/ directory.")