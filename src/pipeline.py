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
import sys

log_file = open("results/pipeline_log.txt", "w")


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
        
        print(f"Scanning {n} contacts for duplicates...", file=log_file)
        
        compared = 0
        batch_size = 8
        pairs_to_compare = []
        pair_contacts = []
        
        blocks = {}
        
        for i, contact in enumerate(contacts):
            key = contact.get('company', 'unknown').lower()
            blocks.setdefault(key, []).append((i, contact))
            
        pairs_to_compare = []
        pair_contacts = []
        for block in blocks.values():
            for i in range(len(block)):
                for j in range(i + 1, len(block)):
                    idx_a, contact_a = block[i]
                    idx_b, contact_b = block[j]
                    pairs_to_compare.append((contact_a, contact_b))
                    pair_contacts.append((idx_a, idx_b))

        print(f"Total comparisons needed: {len(pairs_to_compare)}", file=log_file)
        
        for batch_start in range(0, len(pairs_to_compare), batch_size):
            batch_pairs = pairs_to_compare[batch_start:batch_start + batch_size]
            batch_indices = pair_contacts[batch_start:batch_start + batch_size]
            
            decisions = self.resolver.should_merge(pairs=batch_pairs)
            
            if not isinstance(decisions, list):
                decisions = [decisions]
                
            for decision, (i, j) in zip(decisions, batch_indices):
                compared += 1
                
                if compared % 50 == 0:
                    print(f"Progress: {compared} comparisons done.", file=log_file)
                    
                if compared <= 3:
                    print(f"Comparison {compared}:", file=log_file)
                    print(f"Contact A: {contacts[i]}", file=log_file)
                    print(f"Contact B: {contacts[j]}", file=log_file)
                    print(f"Should Merge: {decision.should_merge}, Confidence: {decision.confidence:.2f}", file=log_file)
                    print(f"Reasoning: {decision.reasoning[:100]}\n", file=log_file)
                
                if decision.should_merge and decision.confidence >= self.confidence_threshold:
                    duplicates.append((contacts[i], contacts[j], decision.confidence))
                    
        print(f"Found {len(duplicates)} duplicate pairs.", file=log_file)
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
        
    # contact_lookup = {c['id']: c for c in contacts}
    
    # test_contacts = []
    # seen_ids = set()
    # for gt in ground_truth[:10]:
    #     if gt['entity_a_id'] not in seen_ids:
    #         test_contacts.append(contact_lookup[gt['entity_a_id']])
    #         seen_ids.add(gt['entity_a_id'])
    #     if gt['entity_b_id'] not in seen_ids:
    #         test_contacts.append(contact_lookup[gt['entity_b_id']])
    #         seen_ids.add(gt['entity_b_id'])
        
    # print(f"Testing pipeline on {len(test_contacts)} contacts\n")
    
    pipeline = EntityResolutionPipeline(confidence_threshold=0.5)
    # deduplicated_contacts, stats = pipeline.deduplicate(test_contacts)
    deduplicated_contacts, stats = pipeline.deduplicate(contacts)
    
    print("\n" + "=" * 40)
    print("DEDUPLICATION RESULTS")
    print("=" * 40 + "\n")
    
    for key, value in stats.items():
        print(f"{key}: {value}")
    
    with open("results/deduplicated_contacts.json", "w") as f:
        json.dump(deduplicated_contacts, f, indent=2)
        
    with open("results/deduplication_stats.json", "w") as f:
        json.dump(stats, f, indent=2)
        
    print("Deduplication complete. Results saved to results/ directory.", file=log_file)