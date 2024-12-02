## Parse and analyze wikipedia dumps with Spark

### Prerequisites

- [Conda or Miniconda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html)

```bash
conda env create -f environment.yaml

conda activate wikipedia-parser
```

### Run

```bash
python3 main.py

# or

spark-submit --packages com.databricks:spark-xml_2.12:0.18.0 main.py
```
