import argparse
from src.extractor import FeatureExtractor

def main():
    parser = argparse.ArgumentParser(description='Extract features from contract data')
    parser.add_argument('--input', required=True, help='Input CSV file path')
    parser.add_argument('--output', default='contract_features.csv', help='Output CSV file path')
    
    args = parser.parse_args()
    
    # Initialize and run feature extraction
    extractor = FeatureExtractor(args.input)
    extractor.process_data()
    extractor.save_features(args.output)
    
    print(f"Features extracted and saved to {args.output}")

if __name__ == '__main__':
    main()