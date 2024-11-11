from typing import Dict,List, Any
import pandas as pd
import json
from .utils import parse_date
from datetime import datetime
from .utils import calculate_shannon_diversity
import numpy as np

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
        
    def calculate_temp_metrics(self, contracts: List[Dict]) -> Dict[str, float]:
        dates = []
        for contract in contracts:
            if contract.get('claim_date'):
                try:
                    date = parse_date(contract['claim_date'])
                    dates.append(date)
                except ValueError:
                    continue
                    
        if not dates:
            return {
                'days_since_first_contract': 0,
                'days_since_last_contract': 0,
                'contract_frequency': 0,
                'active_months': 0
            }
            
        first_date = min(dates)
        last_date = max(dates)
        unique_months = len(set((d.year, d.month) for d in dates))
        
        return {
            'days_since_first_contract': (datetime.now() - first_date).days,
            'days_since_last_contract': (datetime.now() - last_date).days,
            'contract_frequency': (last_date - first_date).days / len(dates) if len(dates) > 1 else 0,
            'active_months': unique_months
        }

    def calculate_advanced_metrics(self, contracts: List[Dict]) -> Dict[str, float]:
        # Bank diversity
        bank_counts = {}
        for contract in contracts:
            bank = contract.get('bank', '')
            bank_counts[bank] = bank_counts.get(bank, 0) + 1
            
        # Contract values by bank
        bank_values = {}
        for contract in contracts:
            if contract.get('summa') and contract['summa'] != '""':
                bank = contract.get('bank', '')
                value = float(contract['summa'])
                if bank in bank_values:
                    bank_values[bank].append(value)
                else:
                    bank_values[bank] = [value]
                    
        # Calculate metrics
        metrics = {
            'bank_diversity': calculate_shannon_diversity(list(bank_counts.values())) if bank_counts else 0,
            'completed_ratio': len([c for c in contracts if c.get('contract_id') and c.get('contract_date')]) / len(contracts) if contracts else 0,
            'value_per_bank': np.mean([np.mean(values) for values in bank_values.values()]) if bank_values else 0,
            'max_single_value': max([float(c['summa']) for c in contracts if c.get('summa') and c['summa'] != '""'], default=0)
        }
        
        return metrics


    def extract_features(self, row: pd.Series) -> Dict[str, Any]:
        contracts = self.parse_contracts(row['contracts'])
        
        features = {
            'id': row['id'],
            **self.calculate_basic_metrics(contracts),
            **self.calculate_temp_metrics(contracts),
            **self.calculate_advanced_metrics(contracts)
        }
        
        return features
        
    def process_data(self) -> None:
        if self.df is None:
            self.load_data()
            
        features_list = []
        for _, row in self.df.iterrows():
            features = self.extract_features(row)
            features_list.append(features)
        self.features_df = pd.DataFrame(features_list)
