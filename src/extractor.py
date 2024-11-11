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

    def calculate_day_sinlastloan(self, contracts: List[Dict], application_date: pd.Timestamp) -> int:
        if not contracts:
            return -3

        latest_loan_date = None
        has_loans = False

        for contract in contracts:
            if contract.get('contract_date') and contract.get('summa') and contract['summa'] != '""':
                try:
                    contract_date = pd.to_datetime(contract['contract_date'], format='%d.%m.%Y')
                    if latest_loan_date is None or contract_date > latest_loan_date:
                        latest_loan_date = contract_date
                        has_loans = True
                except ValueError:
                    continue

        if not has_loans:
            return -3

        if latest_loan_date:
            days_since = (application_date - latest_loan_date).days
            return days_since
        return -3
            

    def extract_features(self, row: pd.Series) -> Dict[str, Any]:
        """Extract all features for a single row"""
        contracts = self.parse_contracts(row['contracts'])
        
        features = {
            'id': row['id'],
            'tot_claim_cnt_l180d': self.calculate_tot_claim_cnt_l180d(contracts, row['application_date']),
            'disb_bank_loan_wo_tbc': self.calculate_disb_bank_loan_wo_tbc(contracts),
            'day_sinlastloan': self.calculate_day_sinlastloan(contracts, row['application_date'])
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