from src.extractor import FeatureExtractor



extractor = FeatureExtractor('data.csv')
extractor.process_data()
extractor.save_features('result.csv')