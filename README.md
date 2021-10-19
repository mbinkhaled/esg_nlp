# Analyze Firms’ ESG Commitment from Texts

The project performs Environmental, Social, and Governance (ESG) commitment analysis based on texts extracted from companies' 10-K/10-Q filings

## Environment Setup

Navigate to the project root folder and run the following command:
```bash
pip install -r requirements.txt
```

## Usage

In 4. good_vs_bad.ipynb notebook, run cells to generate the analysis within a sector. There are two `TODO` cells to change, one for sector/score_type, and the other for ngram parameters. It produces csv files that indicate good and bad ngrams in the industry based on the 10-K/10-Q filings.