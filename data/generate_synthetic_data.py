from faker import Faker
import pandas as pd
import random
import json
from datetime import datetime

fake = Faker()
Faker.seed(42)
random.seed(42)

def generate_synthetic_data(contact_id):
    """
    Generates one realistic contact records (the 'true' person)
    """

    first_name = fake.first_name()
    last_name = fake.last_name()
    company = fake.company()
    
    contact = {
        'id': f"base_{contact_id}",
        'first_name': first_name,
        'last_name': last_name,
        'full_name': f"{first_name} {last_name}",
        'email': f"{first_name.lower()}.{last_name.lower()}@{company.lower().replace(' ', '').replace(',', '')}.com",
        'phone': fake.phone_number(),
        'company': company,
        'title': fake.job(),
        'linkedin': f"linkedin.com/in/{first_name.lower()}{last_name.lower()}",
        'location': fake.city() + ", " + fake.state_abbr()
    }
    
    return contact


def create_variations(base_contact, num_variations=3):
    """
    Creates messy variations of the same person
    (like how they appear in email vs linkedin vs calendar)
    """
    variations = []
    first = base_contact['first_name']
    last = base_contact['last_name']
    
    nicknames = {
        'Robert': 'Bob', 'William': 'Bill', 'Richard': 'Dick',
        'Michael': 'Mike', 'Christopher': 'Chris', 'Matthew': 'Matt',
        'Joshua': 'Josh', 'Daniel': 'Dan', 'David': 'Dave',
        'James': 'Jim', 'Joseph': 'Joe', 'Thomas': 'Tom',
        'Elizabeth': 'Liz', 'Jennifer': 'Jen', 'Jessica': 'Jess',
        'Katherine': 'Kate', 'Margaret': 'Meg', 'Susan': 'Sue'
    }
    
    variation_types = [
        'email_only',
        'abbreviated_name',
        'linkedin_only'
        'nickname',
        'typo',
        'phone_only',
        'different_email',
        'missing_company'
    ]
    
    if num_variations is None:
        num_variations = random.randint(2, 5)
    
    random.shuffle(variation_types)
    selected_types = variation_types[:num_variations]
    
    for i, var_type in enumerate(selected_types):
        var_id = f"{base_contact['id']}_v{i+1}"
        
        if var_type == 'email_only':
            variations.append({
                'id': var_id,
                'email': base_contact['email'],
                'company': base_contact['company'],
                'source': 'email'
            })
            
        elif var_type == 'abbreviated_name':
            variations.append({
                'id': var_id,
                'full_name': f"{first[0]}. {last}",
                'title': base_contact['title'],
                'company': base_contact['company'],
                'source': 'calendar'
            })
            
        elif var_type == 'linkedin_only':
            variations.append({
                'id': var_id,
                'full_name': base_contact['full_name'],
                'linkedin': base_contact['linkedin'],
                'location': base_contact['location'],
                'source': 'linkedin'
            })
        
        elif var_type == 'nickname':
            nickname = nicknames.get(first, first)
            variations.append({
                'id': var_id,
                'full_name': f"{nickname} {last}",
                'email': base_contact['email'],
                'source': 'informal_contact'
            })
            
        elif var_type == 'typo':
            typo_last = last
            if len(last) > 3:
                pos = random.randint(1, len(last)-2)
                typo_last = last[:pos] + last[pos] + last[pos] + last[pos+1:]  # Double a letter
            variations.append({
                'id': var_id,
                'full_name': f"{first} {typo_last}",
                'email': base_contact['email'].replace(last.lower(), typo_last.lower()),
                'company': base_contact['company'],
                'source': 'manual_entry'
            })
            
        elif var_type == 'phone_only':
            variations.append({
                'id': var_id,
                'full_name': base_contact['full_name'],
                'phone': base_contact['phone'],
                'source': 'phone_contact'
            })
        
        elif var_type == 'different_email':
            personal_email = f"{first.lower()}{random.randint(1,99)}@gmail.com"
            variations.append({
                'id': var_id,
                'full_name': base_contact['full_name'],
                'email': personal_email,
                'source': 'personal_contact'
            })
            
        elif var_type == 'missing_company':
            variations.append({
                'id': var_id,
                'full_name': base_contact['full_name'],
                'title': base_contact['title'],
                'location': base_contact['location'],
                'source': 'business_card'
            })
            
        elif var_type == 'middle_initial':
            # Add a middle initial
            middle = fake.random_uppercase_letter()
            variations.append({
                'id': var_id,
                'full_name': f"{first} {middle}. {last}",
                'company': base_contact['company'],
                'title': base_contact['title'],
                'source': 'formal_document'
            })
        
        elif var_type == 'job_change':
            # Same person, different company (they changed jobs)
            new_company = fake.company()
            variations.append({
                'id': var_id,
                'full_name': base_contact['full_name'],
                'email': f"{first.lower()}.{last.lower()}@{new_company.lower().replace(' ', '').replace(',', '')}.com",
                'company': new_company,
                'title': fake.job(),
                'source': 'recent_update'
            })
            
    return variations


def generate_false_positive(base_contact, fp_id):
    """
    Generates a DIFFERENT person with similar attributes
    (to test if the LLM incorrectly merges them)
    """
    first = base_contact['first_name']
    last = base_contact['last_name']
    
    fp_types = ['same_name_diff_company', 'similar_name_same_company', 'same_last_name']
    fp_type = random.choice(fp_types)
    
    if fp_type == 'same_name_diff_company':
        
        return {
            'id': f"fp_{fp_id}",
            'full_name': base_contact['full_name'],
            'email': f"{first.lower()}.{last.lower()}@{fake.company().lower().replace(' ', '').replace(',', '')}.com",
            'company': fake.company(),
            'title': fake.job(),
            'location': fake.city() + ", " + fake.state_abbr(),
            'source': 'different_person'
        }
        
    elif fp_type == 'similar_name_same_company':
        similar_first = first[:-1] if len(first) > 4 else first + 'a'
        return {
            'id': f"fp_{fp_id}",
            'full_name': f"{similar_first} {last}",
            'email': f"{similar_first.lower()}.{last.lower()}@{base_contact['company'].lower().replace(' ', '').replace(',', '')}.com",
            'company': base_contact['company'],
            'title': fake.job(),
            'source': 'different_person'
        }
    
    else: # same_last_name
        return {
            'id': f"fp_{fp_id}",
            'full_name': f"{fake.first_name()} {last}",
            'email': f"{fake.first_name().lower()}.{last.lower()}@{base_contact['company'].lower().replace(' ', '').replace(',', '')}.com",
            'company': base_contact['company'],
            'title': fake.job(),
            'location': base_contact['location'],
            'source': 'different_person'
        }
        
        
def generate_full_dataset(num_base_contacts=50):
    """
    Generates complete dataset with:
    - Base contacts
    - Their variations (positive matches)
    - False positives (negative matches)
    - Ground truth labels
    """
    
    all_records = []
    ground_truth = []
    
    for i in range(num_base_contacts):
        base = generate_synthetic_data(i)
        variations = create_variations(base)
        
        num_false_positives = random.randint(0, 2)
        
        num_false_positives = random.randint(0, 2)
        false_positives = [generate_false_positive(base, f"{i}_{j}") for j in range(num_false_positives)]
        
        all_records.append(base)
        all_records.extend(variations)
        all_records.extend(false_positives)
        
        base_id = base['id']
        for var in variations:
            ground_truth.append({
                'entity_a_id': base_id,
                'entity_b_id': var['id'],
                'is_match': True,
                'match_type': 'same_person'
            })
            
        for fp in false_positives:
            ground_truth.append({
                'entity_a_id': base_id,
                'entity_b_id': fp['id'],
                'is_match': False,
                'match_type': 'different_person'
            })
            
    return all_records, ground_truth
        

if __name__ == "__main__":
    print("Generating full dataset.")
    records, ground_truth = generate_full_dataset(num_base_contacts=10)
    
    print(f"Generated {len(records)} total records")
    print(f"Generated {len(ground_truth)} ground truth labels")
    print(f"  - Positive matches: {sum(1 for gt in ground_truth if gt['is_match'])}")
    print(f"  - Negative matches: {sum(1 for gt in ground_truth if not gt['is_match'])}")
    
    with open("data/contacts.json", "w") as f:
        json.dump(records, f, indent=2)
        
    with open("data/ground_truth.json", "w") as f:
        json.dump(ground_truth, f, indent=2)
        
    print("\n Saved to data/contacts.json and data/ground_truth.json")