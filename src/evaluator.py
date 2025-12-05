"""
Evaluation framework for entity resolution system.

Compares LLM-based matching against ground truth labels,
computing standard metrics and providing error analysis.
"""

from typing import Dict, List, Tuple
import json
from dataclasses import dataclass
import numpy as np
from sklearn.metrics import precision_score, recall_score, f1_score, accuracy_score, confusion_matrix
from entity_resolver import EntityResolver
import time


@dataclass
class EvaluationMetrics:
    """
    Container for evaluation metrics.
    """
    precision: float
    recall: float
    f1_score: float
    accuracy: float
    true_positives: int
    true_negatives: int
    false_positives: int
    false_negatives: int
    total_predictions: int
    avg_confidence: float
    
    def to_dict(self) -> Dict:
        return {
            'precision': round(self.precision, 4),
            'recall': round(self.recall, 4),
            'f1_score': round(self.f1_score, 4),
            'accuracy': round(self.accuracy, 4),
            'true_positives': self.true_positives,
            'true_negatives': self.true_negatives,
            'false_positives': self.false_positives,
            'false_negatives': self.false_negatives,
            'total_predictions': self.total_predictions,
            'avg_confidence': round(self.avg_confidence, 4)
        }
    
    def __str__(self) -> str:
        return f"""
        Evaluation Metrics:
        ==================
        Precision: {self.precision:.2%}
        Recall:    {self.recall:.2%}
        F1 Score:  {self.f1_score:.2%}
        Accuracy:  {self.accuracy:.2%}

        Confusion Matrix:
        TP: {self.true_positives}  FN: {self.false_negatives}
        FP: {self.false_positives}  TN: {self.true_negatives}

        Avg Confidence: {self.avg_confidence:.2f}
        Total Predictions: {self.total_predictions}
        """
        
class Evaluator:
    """
    Evaluates entity resolution system against ground truth labels.
    """
    def __init__(self, resolver: EntityResolver):
        self.resolver = resolver
        self.predictions = []
        self.errors = []
        
    def evaluate(self, contacts: List[Dict], ground_truth: List[Dict], sample_size: int = None) -> EvaluationMetrics:
        """
        Evaluates resolver on labeled dataset.
        """
        print(f"Starting evaluation on {len(ground_truth)} pairs...")
        
        if sample_size and sample_size < len(ground_truth):
            import random
            ground_truth = random.sample(ground_truth, sample_size)
            print(f"Sampled {sample_size} pairs for evaluation.")
            
        contact_lookup = {c['id']: c for c in contacts}
        
        y_true = []
        y_pred = []
        confidences = []
        
        eval_pairs = []
        eval_ground_truth = []
        
        for i, gt in enumerate(ground_truth):
            entity_a = contact_lookup.get(gt['entity_a_id'])
            entity_b = contact_lookup.get(gt['entity_b_id'])
            
            if not entity_a or not entity_b:
                print(f"Warning: Missing contact for pair {gt['entity_a_id']}, {gt['entity_b_id']}")
                continue
            
            eval_pairs.append((entity_a, entity_b))
            eval_ground_truth.append(gt)
            
        batch_size = 6
        for batch_start in range(0, len(eval_pairs), batch_size):
            if (batch_start // batch_size + 1) % 5 == 0:
                print(f"Progress: {batch_start}/{len(eval_pairs)} pairs evaluated.")
                
            batch_pairs = eval_pairs[batch_start:batch_start + batch_size]
            batch_gt = eval_ground_truth[batch_start:batch_start + batch_size]
            
            decisions = self.resolver.should_merge(pairs=batch_pairs)
            if not isinstance(decisions, list):
                decisions = [decisions]
                
            for decision, gt in zip(decisions, batch_gt):
                y_true.append(gt['is_match'])
                y_pred.append(decision.should_merge)
                confidences.append(decision.confidence)
        
                if gt['is_match'] != decision.should_merge:
                    self.errors.append({
                        'entity_a': entity_a,
                        'entity_b': entity_b,
                        'ground_truth': gt['is_match'],
                        'prediction': decision.should_merge,
                        'confidence': decision.confidence,
                        'reasoning': decision.reasoning
                    })
            
        precision = precision_score(y_true, y_pred)
        recall = recall_score(y_true, y_pred)
        f1 = f1_score(y_true, y_pred)
        accuracy = accuracy_score(y_true, y_pred)
        
        tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()
        
        metrics = EvaluationMetrics(
            precision=precision,
            recall=recall,
            f1_score=f1,
            accuracy=accuracy,
            true_positives=int(tp),
            true_negatives=int(tn),
            false_positives=int(fp),
            false_negatives=int(fn),
            total_predictions=len(y_true),
            avg_confidence=float(np.mean(confidences)) if confidences else 0.0
        )
        
        print("\n" + "=" * 40)
        print(metrics)
        
        return metrics
    
    def analyze_errors(self, top_n: int = 10) -> List[Dict]:
        """
        Analyzes the most confident errors.
        """
        sorted_errors = sorted(
            self.errors, 
            key=lambda x: x['confidence'], 
            reverse=True
        )
        return {
            'total_errors': len(self.errors),
            'top_errors': sorted_errors[:top_n]
            }
        
    def get_baseline_comparison(
        self,
        contacts: List[Dict],
        ground_truth: List[Dict]
        ) -> Dict:
        """
        Compares LLM resolver against a simple rule-based baseline (e.g., exact email match).
        """
        
        contact_lookup = {c['id']: c for c in contacts}
        
        y_true = []
        y_pred_baseline = []
        
        for gt in ground_truth:
            entity_a = contact_lookup.get(gt['entity_a_id'])
            entity_b = contact_lookup.get(gt['entity_b_id'])
            
            if not entity_a or not entity_b:
                continue
            
            name_match = (
                entity_a.get('full_name', '').lower() == entity_b.get('full_name', '').lower()
            )
            
            company_match = (
                entity_a.get('company', '').lower() == entity_b.get('company', '').lower()
            )
            
            baseline_pred = name_match and company_match
            
            y_true.append(gt['is_match'])
            y_pred_baseline.append(baseline_pred)
            
        baseline_f1 = f1_score(y_true, y_pred_baseline)
        baseline_precision = precision_score(y_true, y_pred_baseline)
        baseline_recall = recall_score(y_true, y_pred_baseline)
        
        return {
            'baseline_f1_score': round(baseline_f1, 4),
            'baseline_precision': round(baseline_precision, 4),
            'baseline_recall': round(baseline_recall, 4)
        }
            
            
if __name__ == "__main__":
    with open("data/contacts.json", "r") as f:
        contacts = json.load(f)
        
    with open("data/ground_truth.json", "r") as f:
        ground_truth = json.load(f)
        
    resolver = EntityResolver()
    evaluator = Evaluator(resolver)
    
    # print("Running evaluation on 20 sample pairs...\n")
    # metrics = evaluator.evaluate(contacts, ground_truth, sample_size=20)
    
    print("Running full evaluation...\n")
    metrics = evaluator.evaluate(contacts, ground_truth)
    
    with open("results/evaluation_results.json", "w") as f:
        json.dump(metrics.to_dict(), f, indent=2)
        
    print("\n Results saved to results/evaluation_results.json")
    
    print("\n Comparing against rule-based baseline...\n")
    baseline_metrics = evaluator.get_baseline_comparison(contacts, ground_truth)
    print(f"\n Baseline F1 Score: {baseline_metrics['baseline_f1_score']:.2%}")
    print(f"LLM F1 Score: {metrics.f1_score:.2%}")
    print(f"Improvement: {(metrics.f1_score - baseline_metrics['baseline_f1_score']):.2%}\n")
    
    if evaluator.errors:
        print("\nTop 3 Most Confident Errors:")
        error_analysis = evaluator.analyze_errors(top_n=3)
        for i, error in enumerate(error_analysis['top_errors'], 1):
            print(f"Error {i} (Confidence: {error['confidence']:.2f}):")
            print(f"Ground Truth: {error['ground_truth']}, Predicted: {error['prediction']}")
            print(f"Reasoning: {error['reasoning'][:150]}...")