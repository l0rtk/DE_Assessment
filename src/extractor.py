from typing import Dict, List, Any
import pandas as pd
import json
import numpy as np

class FeatureExtractor:
    def __init__(self, input_file: str):
        self.input_file = input_file
        self.df = None
        self.features_df = None
        
    def load_data(self) -> None:
        self.df = pd.read_csv(self.input_file)
        # self.df['application_date'] = pd.to_datetime(self.df['application_date'])
        self.df['application_date'] = pd.to_datetime(self.df['application_date'], format='ISO8601')

        
    def parse_contracts(self, contracts_json: str) -> List[Dict]:
        if pd.isna(contracts_json) or not contracts_json:
            return []
        try:
            return json.loads(contracts_json)
        except json.JSONDecodeError:
            return []

    def calculate_tot_claim_cnt_l180d(self, contracts: List[Dict], application_date: pd.Timestamp) -> int:
        if not contracts:
            return -3

        claim_count = 0
        for contract in contracts:
            if contract.get('claim_date'):
                try:
                    claim_date = pd.to_datetime(contract['claim_date'], format='%d.%m.%Y')
                    days_diff = (application_date - claim_date).days
                    if 0 <= days_diff <= 180:
                        claim_count += 1
                except ValueError:
                    continue

        return claim_count if claim_count > 0 else -3


    def calculate_disb_bank_loan_wo_tbc(self, contracts: List[Dict]) -> float:
        if not contracts:
            return -3

        excluded_banks = ['LIZ', 'LOM', 'MKO', 'SUG']
        total_summa = 0
        has_loans = False

        for contract in contracts:
            # Check if it's a valid bank loan (not in excluded banks)
            if (contract.get('bank') and 
                contract['bank'] not in excluded_banks and 
                contract.get('contract_date') and 
                contract['contract_date'] != '""'):
                
                # Get loan_summa if available
                if contract.get('loan_summa') and contract['loan_summa'] != '""':
                    try:
                        summa = float(contract['loan_summa'])
                        total_summa += summa
                        has_loans = True
                    except (ValueError, TypeError):
                        continue

        if not has_loans:
            return -3
        return total_summa if total_summa > 0 else -3
            
    def calculate_basic_metrics(self, contracts: List[Dict]) -> Dict[str, Any]:
        metrics = {
            'total_contracts': len(contracts),
            'unique_banks': 0,
            'filled_contracts': 0,
            'empty_contracts': 0, 
            'total_summa': 0,
            'total_loan_summa': 0,
            'count_with_summa': 0,
            'count_with_loan': 0
        }
        
        bank_set = set()
        
        for contract in contracts:
            if type(contract) == dict:
                if any(contract.get(field) and contract[field] != '""' 
                    for field in ['contract_id', 'summa', 'loan_summa']):
                    metrics['filled_contracts'] += 1
                else:
                    metrics['empty_contracts'] += 1
                
                if contract.get('bank') and contract['bank'] != '""':
                    bank_set.add(contract['bank'])
                
                if contract.get('summa') and contract['summa'] != '""':
                    try:
                        summa = float(contract['summa'])
                        metrics['total_summa'] += summa
                        metrics['count_with_summa'] += 1
                    except (ValueError, TypeError):
                        pass
                
                if contract.get('loan_summa') and contract['loan_summa'] != '""':
                    try:
                        loan = float(contract['loan_summa'])
                        metrics['total_loan_summa'] += loan
                        metrics['count_with_loan'] += 1
                    except (ValueError, TypeError):
                        pass
        
        metrics['unique_banks'] = len(bank_set)
        
        metrics['avg_summa'] = (metrics['total_summa'] / metrics['count_with_summa'] 
                              if metrics['count_with_summa'] > 0 else 0)
        metrics['avg_loan_summa'] = (metrics['total_loan_summa'] / metrics['count_with_loan']
                                   if metrics['count_with_loan'] > 0 else 0)
        
        return metrics
    
    def calculate_temporal_metrics(self, contracts: List[Dict]) -> Dict[str, Any]:
        metrics = {
            'unique_dates': 0,
            'earliest_date': None,
            'latest_date': None,
            'date_range_days': 0,
            'unique_months': 0
        }
        
        dates = []
        for contract in contracts:
            if type(contract) == dict:
                if contract.get('claim_date'):
                    try:
                        date = pd.to_datetime(contract['claim_date'], format='%d.%m.%Y')
                        dates.append(date)
                    except ValueError:
                        continue
        
        if dates:
            metrics['unique_dates'] = len(set(dates))
            metrics['earliest_date'] = min(dates)
            metrics['latest_date'] = max(dates)
            metrics['date_range_days'] = (metrics['latest_date'] - metrics['earliest_date']).days
            metrics['unique_months'] = len(set((d.year, d.month) for d in dates))
        
        return metrics
    
    def calculate_bank_metrics(self, contracts: List[Dict]) -> Dict[str, Any]:
        metrics = {
            'bank_contract_counts': {},  
            'bank_total_summa': {},      
            'bank_total_loans': {}       
        }
        
        for contract in contracts:
            if type(contract) == dict:
                bank = contract.get('bank', '')
                if bank and bank != '""':
                    metrics['bank_contract_counts'][bank] = metrics['bank_contract_counts'].get(bank, 0) + 1
                    
                    if contract.get('summa') and contract['summa'] != '""':
                        try:
                            summa = float(contract['summa'])
                            metrics['bank_total_summa'][bank] = metrics['bank_total_summa'].get(bank, 0) + summa
                        except (ValueError, TypeError):
                            pass
                            
                    if contract.get('loan_summa') and contract['loan_summa'] != '""':
                        try:
                            loan = float(contract['loan_summa'])
                            metrics['bank_total_loans'][bank] = metrics['bank_total_loans'].get(bank, 0) + loan
                        except (ValueError, TypeError):
                            pass
        
        if metrics['bank_contract_counts']:
            metrics['max_contracts_per_bank'] = max(metrics['bank_contract_counts'].values())
            metrics['min_contracts_per_bank'] = min(metrics['bank_contract_counts'].values())
            metrics['avg_contracts_per_bank'] = np.mean(list(metrics['bank_contract_counts'].values()))
        else:
            metrics['max_contracts_per_bank'] = 0
            metrics['min_contracts_per_bank'] = 0
            metrics['avg_contracts_per_bank'] = 0
            
        return metrics

    def extract_features(self, row: pd.Series) -> Dict[str, Any]:
        contracts = self.parse_contracts(row['contracts'])
        
        features = {
            'id': row['id'],
            **self.calculate_basic_metrics(contracts),
            **self.calculate_temporal_metrics(contracts),
            **self.calculate_bank_metrics(contracts)
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
        
    def save_features(self, output_file: str = 'contract_features.csv') -> None:
        if self.features_df is None:
            raise ValueError("No features to save. Run process_data() first.")
        self.features_df.to_csv(output_file, index=False)