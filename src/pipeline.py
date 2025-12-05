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
import time

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
        batch_size = 6
        pairs_to_compare = []
        pair_contacts = []
        
        blocks = {}
        
        for i, contact in enumerate(contacts):
            key = contact.get('company', 'unknown').lower()
            blocks.setdefault(key, []).append((i, contact))
            
        # print(f"n[DEBUG] Blocking Summary Created {len(blocks)} blocks.", file=log_file)
        # if len(blocks) < 2:
        #     print(f"n[WARNING] Blocking created less than 2 blocks. Check blocking strategy.", file=log_file)
            
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
        
        print(f"[CONFIRM] Total pairs to compare after blocking: {len(pairs_to_compare)}. Proceed with API calls? (y/n)")
        proceed = input().strip().lower()
        if proceed != 'y':
            print("Aborting duplicate search.")
            return []
        
        for batch_start in range(0, len(pairs_to_compare), batch_size):
            time.sleep(2.5)
            batch_pairs = pairs_to_compare[batch_start:batch_start + batch_size]
            batch_indices = pair_contacts[batch_start:batch_start + batch_size]
            
            decisions = self.resolver.should_merge(pairs=batch_pairs)
            
            if not isinstance(decisions, list):
                decisions = [decisions]
                
            for decision, (i, j) in zip(decisions, batch_indices):
                compared += 1
                
                if compared % 50 == 0:
                    print(f"Progress: {compared} comparisons done.", file=log_file)
                if compared % 100 == 0:
                    print(f"Progress: {compared} comparisons done.")
                    
                if compared % 10 == 0:
                    print(f"Comparison {compared}:", file=log_file)
                    print(f"Contact A: {contacts[i]}", file=log_file)
                    print(f"Contact B: {contacts[j]}", file=log_file)
                    print(f"Should Merge: {decision.should_merge}, Confidence: {decision.confidence:.2f}", file=log_file)
                    print(f"Reasoning: {decision.reasoning[:100]}\n", file=log_file)
                
                if decision.should_merge and decision.confidence >= self.confidence_threshold:
                    name_a_str = contacts[i].get('first_name') or contacts[i].get('full_name') or ""
                    name_b_str = contacts[j].get('first_name') or contacts[j].get('full_name') or ""
                    
                    parts_a = name_a_str.split()
                    parts_b = name_b_str.split()
                    
                    # Only compare if both have names (skips email_only records)
                    if parts_a and parts_b:
                        if parts_a[0].lower() != parts_b[0].lower():
                             print(f"[SUSPICIOUS MERGE]: {name_a_str} <-> {name_b_str} ({decision.confidence})", file=log_file)

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
        
        adj = {}
        id_map = {}
        
        for ea, eb, _ in duplicate_pairs:
            id_a, id_b = ea['id'], eb['id']
            id_map[id_a] = ea
            id_map[id_b] = eb
            
            if id_a not in adj: adj[id_a] = []
            if id_b not in adj: adj[id_b] = []
            
            adj[id_a].append(id_b)
            adj[id_b].append(id_a)
        
        groups = []
        visited = set()
        
        # print(f"\n[DEBUG] Clustering: Processing {len(id_map)} linked entities.", file=log_file)
        
        for uid in adj:
            if uid not in visited:
                component = []
                stack = [uid]
                visited.add(uid)
                while stack:
                    node = stack.pop()
                    component.append(id_map[node])
                    for neighbor in adj[node]:
                        if neighbor not in visited:
                            visited.add(neighbor)
                            stack.append(neighbor)
                
                groups.append(component)
                # [DEBUG 3: CLUSTER SIZE - SAFE VERSION]
                # Fallback to ID if full_name is missing (e.g. email_only records)
                names = [c.get('full_name') or c.get('email') or c['id'] for c in component]
                print(f"  -> Formed Group of {len(component)}: {names}", file=log_file)
                
        return groups
    

if __name__ == "__main__":
    with open("data/contacts.json", "r") as f:
        contacts = json.load(f)
        
    with open("data/ground_truth.json", "r") as f:
        ground_truth = json.load(f)
        
    # num_test_contacts = 20
    # contacts = contacts[:num_test_contacts]
       
    pipeline = EntityResolutionPipeline(confidence_threshold=0.5)
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