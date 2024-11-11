# DE_Assessment

# contract feature extraction tool

extracts features from contract data in csv files. helps analyze claims, loans and other contract stuff.

## what it does

pulls out these features from your contract data:

- how many claims in last 180 days
- total loans (except for some specific banks)
- days since last loan happened

## getting started

1. clone this repo
2. install stuff you need:

```bash
pip install -r requiremnts.txt
```

## how to use it

just run it like this:

```bash
python main.py --input data.csv --output results.csv
```

you need:

- `--input`: where your csv file is
- `--output`: where to save results (if you dont specify, saves as 'contract_features.csv')

## about the input file

your csv needs these columns:

- id
- application_date (in ISO8601)
- contracts (json string with contract info)

the contracts json should look kinda like this:

```json
{
  "claim_date": "dd.mm.yyyy",
  "contract_date": "dd.mm.yyyy",
  "bank": "bankname",
  "loan_summa": "amount",
  "summa": "amount"
}
```

## files in project

```
├── main.py            # main script
├── src/
│   ├── extractor.py   # does the feature extraction
│   └── utils.py       # helper functions and stuff
```

## about the features

tot_claim_cnt_l180d:

- counts claims in last 180 days
- gives -3 if it finds nothing

disb_bank_loan_wo_tbc:

- adds up loans but skips some banks (LIZ, LOM, MKO, SUG)
- gives -3 if theres no loans

day_sinlastloan:

- figures out how many days since last loan
- gives -3 if it cant find any loans

## error stuff

- bad json = empty list
- if dates are weird it skips them
- if something breaks it puts -3
- tells you progress every 100 rows

## what you need installed

- pandas (for data stuff)
- numpy (for math)
- json (for json obvs)
- typing (for type hints)
- argparse (for command line args)

## fyi

- dates should be dd.mm.yyyy
- times get timezone stuff removed
- output csv doesnt have index numbers
