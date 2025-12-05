"""
Entity Resolution using LLM-based reasoning.

This module implements few-shot learning for contact deduplication,
using OpenAI's GPT models to make nuanced matching decisions that
rule-based systems struggle with.
"""

import os
from typing import Dict, List, Tuple
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold
from dotenv import load_dotenv
import json
from dataclasses import dataclass
import time
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

load_dotenv()


@dataclass
class MatchDecision:
    """
    Structured output from entity matching decision.
    
    Attributes:
        should_merge (bool): Whether the entities should be merged.
        confidence (float): Confidence level of the decision (0-1).
        reasoning (str): Explanation of the decision.
        evidence_for (List[str]): Evidence supporting the merge.
        evidence_against (List[str]): Evidence against the merge.
    """
        
    should_merge: bool
    confidence: float
    reasoning: str
    evidence_for: List[str]
    evidence_against: List[str]
    
    def to_dict(self) -> Dict:
        return {
            "should_merge": self.should_merge,
            "confidence": self.confidence,
            "reasoning": self.reasoning,
            "evidence_for": self.evidence_for,
            "evidence_against": self.evidence_against,
        }
        
class EntityResolver:
    """
    LLM-based entity resolution with OpenAI client.
    """
    
    def __init__(self, model: str = "gemini-2.0-flash-lite"):
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY environment variable not set.")
        
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(
            model,
            safety_settings={
                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
            }
        )
        self.few_shot_examples = self._create_few_shot_examples()
        
    def _create_few_shot_examples(self) -> List[Dict]:
        """
        Create few-shot examples for the prompt.
        """
        return [
             {
                "entity_a": {
                    "full_name": "Sarah Chen",
                    "email": "sarah.chen@acme.com",
                    "company": "Acme Corp"
                },
                "entity_b": {
                    "full_name": "S. Chen",
                    "title": "VP Engineering",
                    "company": "Acme Corp"
                },
                "decision": {
                    "should_merge": True,
                    "confidence": 0.9,
                    "reasoning": "Same last name (Chen), same company (Acme Corp). 'S.' is standard abbreviation for Sarah. Email domain matches company.",
                    "evidence_for": ["Same last name", "Same company", "Name abbreviation pattern"],
                    "evidence_against": ["Missing email in entity_b"]
                }
            },
            {
                "entity_a": {
                    "full_name": "Michael Johnson",
                    "email": "mjohnson@techcorp.com",
                    "company": "TechCorp"
                },
                "entity_b": {
                    "full_name": "Michael Johnson",
                    "email": "mike.j@designco.com",
                    "title": "Designer",
                    "company": "DesignCo"
                },
                "decision": {
                    "should_merge": False,
                    "confidence": 0.85,
                    "reasoning": "Same name but different companies and completely different email domains. Michael Johnson is a common name. No other matching identifiers.",
                    "evidence_for": ["Same full name"],
                    "evidence_against": ["Different companies", "Different email domains", "Common name (high collision risk)"]
                }
            },
            {
                "entity_a": {
                    "full_name": "Robert Smith",
                    "phone": "+1-555-0123",
                    "company": "DataCo"
                },
                "entity_b": {
                    "full_name": "Bob Smith",
                    "email": "bob.smith@dataco.com",
                    "company": "DataCo"
                },
                "decision": {
                    "should_merge": True,
                    "confidence": 0.95,
                    "reasoning": "Bob is standard nickname for Robert. Same last name, same company. Email follows naming pattern (bob.smith matches Bob Smith).",
                    "evidence_for": ["Nickname match (Bob=Robert)", "Same last name", "Same company", "Email matches name pattern"],
                    "evidence_against": []
                }
            }
        ]
    
    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=60),
        stop=stop_after_attempt(3),
        retry=retry_if_exception_type(Exception)
    )
    def _call_llm(self, prompt: str) -> str:
        """
        Makes LLM call with retry logic,
        """
        response = self.model.generate_content(
            prompt,
            generation_config=genai.GenerationConfig(temperature=0.1)
        )
        return response.text
        
        
    def should_merge(self, entity_a: Dict = None, entity_b: Dict = None, pairs: List[Tuple[Dict, Dict]] = None) -> MatchDecision:
        """
        Determine if two entities should be merged.
        """
        if pairs is None:
            pairs = [(entity_a, entity_b)]
            
        prompt = self._build_prompt(pairs)
        
        # print(f"\n=== DEBUG: Last 1000 chars of prompt ===\n{prompt[-1000:]}\n=== END DEBUG ===\n")
        
        try:
            content = self._call_llm(prompt)            
                        
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0]
            elif "```" in content:
                content = content.split("```")[1].split("```")[0]
                
            result = json.loads(content.strip())
            
            if not isinstance(result, list):
                result = [result]
                
            decisions = [
                MatchDecision(
                    should_merge=r["should_merge"],
                    confidence=r["confidence"],
                    reasoning=r["reasoning"],
                    evidence_for=r.get("evidence_for", []),
                    evidence_against=r.get("evidence_against",[])
                )
                for r in result
            ]
            
            return decisions[0] if len(decisions) == 1 else decisions
            
        except Exception as e:
            
            error = MatchDecision(False, 0.0, f"Error: {str(e)}", [], [])
            return error if len(pairs) == 1 else [error] * len(pairs)
            
    def _build_prompt(self, pairs: List[Tuple[Dict, Dict]]) -> str:
        """
        Constructs the few-shot prompt for entity matching.
        """
        
        example_text = ""
        
        for i, example in enumerate(self.few_shot_examples):
            example_text += f"Example {i+1}:\n"
            example_text += f"Entity A: {json.dumps(example['entity_a'], indent=2)}\n"
            example_text += f"Entity B: {json.dumps(example['entity_b'], indent=2)}\n"
            example_text += f"Decision: {json.dumps(example['decision'], indent=2)}\n"
        
        pairs_text = ""
        for i, (a, b) in enumerate(pairs):
            pairs_text += f"Pair {i+1}:\nEntity A:\n{json.dumps(a, indent=2)}\nEntity B:\n{json.dumps(b, indent=2)}\n"
        
        prompt = f"""
        
        You are an expert at entity resolution for CRM systems. Your task is to determine if two contact records represent the same person.
        
        Consider these signals:
        - Name matching (including nicknames, initials, abbreviations)
        - Email addresses (domain, username patterns)
        - Company names
        - Phone numbers
        - Job titles and locations
        - Common name collision risk (e.g., "John Smith" is high risk)

        Here are examples of how to reason through matches:
        {example_text}

        Now analyze these {len(pairs)} pair(s):

        {pairs_text}

        Return JSON array with {len(pairs)} decision(s) in this format:
        [{{
            "should_merge": true or false,
            "confidence": 0.0 to 1.0,
            "reasoning": "step-by-step explanation",
            "evidence_for": ["list", "of", "supporting", "signals"],
            "evidence_against": ["list", "of", "contradicting", "signals"]
        }}]

        Think step-by-step and be precise about confidence scoring.
        
        """
        
        return prompt
    
if __name__ == "__main__":
    resolver = EntityResolver()
    
    # Test Case 1: Nickname variation
    entity_a = {
        "full_name": "Jennifer Martinex",
        "email": "jennifer.martinez@dataco.com",
        "company": "DataCo"
    }
    
    entity_b = {
        "full_name": "Jenny Martinez",
        "phone": "+1-555-6789",
        "company": "DataCo"
    }
    
    print("Test Case 1: Nickname variation")
    print("Entity A:", entity_a)
    print("Entity B:", entity_b)
    decision = resolver.should_merge(entity_a, entity_b)
    print(f"Decision: {json.dumps(decision.to_dict(), indent=2)}\n")
    print("-" * 50)
    
    # Test Case 2: False Positive
    entity_c = {
        "full_name": "John Smith",
        "email": "john.smith@companyA.com",
        "company": "Company A"
    }
    
    entity_d = {
        "full_name": "John Smith",
        "email": "john.smith@companyB.com",
        "company": "Company B"
    }
    
    print("Test Case 2: False Positive")
    print("Entity C:", entity_c)
    print("Entity D:", entity_d)
    decision = resolver.should_merge(entity_c, entity_d)
    print(f"Decision: {json.dumps(decision.to_dict(), indent=2)}\n")
    print("-" * 50)