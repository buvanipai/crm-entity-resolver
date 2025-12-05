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
        Constructs the prompt with INLINE examples to enforce strict matching.
        """
        
        pairs_text = ""
        for i, (a, b) in enumerate(pairs):
            pairs_text += f"Target Pair {i+1}:\nEntity A: {json.dumps(a)}\nEntity B: {json.dumps(b)}\n\n"
        
        prompt = f"""
        You are a cynical Data Integrity Auditor. Your goal is to REJECT false matches.
        
        Task: Analyze the following {len(pairs)} pair(s) and determine if they are the EXACT SAME individual.
        
        CRITICAL RULES (Trumps all other evidence)
        1. Different First Names = DIFFERENT PEOPLE. (e.g., "Michael" vs "Michelle").
           - Exception: Common nicknames (Robert -> Bob) are allowed.
        2. Family Rule: Sharing a Company + Last Name is NOT enough (could be siblings/spouses).
        3. Location Conflict: Different cities usually mean different people.
        
        EXAMPLES (Study these "Hard Negatives")
        
        [EXAMPLE 1: DO NOT MERGE]
        Entity A: {{"full_name": "Michael Chen", "company": "Google", "email": "m.chen@google.com"}}
        Entity B: {{"full_name": "Michelle Chen", "company": "Google", "email": "michelle.c@google.com"}}
        Decision: {{
            "should_merge": false,
            "confidence": 0.98,
            "reasoning": "Same company and last name, but First Names (Michael vs Michelle) are distinctly different. Distinct emails."
        }}

        [EXAMPLE 2: MERGE]
        Entity A: {{"full_name": "Robert Smith", "company": "Salesforce"}}
        Entity B: {{"full_name": "Bob Smith", "company": "Salesforce Inc"}}
        Decision: {{
            "should_merge": true,
            "confidence": 0.95,
            "reasoning": "Bob is a standard nickname for Robert. Company matches. No conflicting info."
        }}

        YOUR ANALYSIS
        Analyze the input pairs below. Output JSON array.
        
        {pairs_text}
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