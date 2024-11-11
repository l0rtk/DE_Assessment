from typing import Dict,List
import pandas as pd
import json

class FeatureExtractor:
    def __init__(self, input_file: str):
        self.input_file = input_file
        self.df = None
        self.features_df = None
        
    def load_data(self) -> None:
        self.df = pd.read_csv(self.input_file)
        self.df['application_date'] = pd.to_datetime(self.df['application_date'])
        
    def parse_contracts(self, contracts_json: str) -> List[Dict]:
        if pd.isna(contracts_json) or not contracts_json:
            return []
        try:
            return json.loads(contracts_json)
        except json.JSONDecodeError:
            return []
            
    def calculate_basic_metrics(self, contracts: List[Dict]) -> Dict[str, float]:
        return {
            'total_contracts': len(contracts),
            'unique_banks': len({c['bank'] for c in contracts if c.get('bank')}),
            'contract_value_sum': sum(float(c['summa']) for c in contracts if c.get('summa') and c['summa'] != '""'),
            'loan_amount_sum': sum(float(c['loan_summa']) for c in contracts if c.get('loan_summa') and c['loan_summa'] != '""')
        }
        