# Semantic-Scholar-PDF-Processing
Pipeline for extracting text and keywords from a given Semantic Scholar search query 


## Run whole pipeline:
```python parse_results_reorg.py --all --query "emotion speech" --keywords "valence,arousal"```

## Run step-by-step
```python parse_results_reorg.py --fetch --query "emotion speech"
python parse_results_reorg.py --extract-dois
python parse_results_reorg.py --resolve-urls
python parse_results_reorg.py --download
python parse_results_reorg.py --extract-text
python parse_results_reorg.py --analyze --keywords "valence,arousal"
python parse_results_reorg.py --write-csv --keywords "valence,arousal"```