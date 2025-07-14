# TripleXtract

Pipeline for automated extraction of species–gene–trait triples in plants from scientific literature.

TripleXtract uses a [dual license](https://github.com/VIB-PSB/TripleXtract/blob/main/LICENSE) to offer the distribution of the software under a proprietary model as well as an open source model.


## Pipeline description

1. **Metadata collection**: species, gene and trait identifiers; PLAZA orthology information; ...
2. **Triple extract**: text mining to identify species-gene-trait triples
3. **Export**: filtering and export of collected triples


## Installation

### Python dependencies

Install into a virtual environment using:

```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
### MySQL database

Create a MySQL database with the schema at [data/database/db_schema.sql](https://github.com/VIB-PSB/TripleXtract/blob/main/data/database/db_schema.sql).

### Configuration file

Copy and edit the template: `config/template.cfg` → `config/config.cfg`

This file controls which steps run and specifies all input/output paths. Full details are described in the [Configuration file wiki](https://github.com/VIB-PSB/TripleXtract/wiki/Configuration-file).


## Usage

To run the full pipeline:

`python3 ./main.py ./config/config.cfg`

Some options in the config file can be overridden on the command line. For a complete list, run:

`python3 ./main.py --help`

To execute only selected steps, enable the corresponding flags in `config.cfg` (all flags = `yes` runs the entire pipeline). Execution order requirements are documented [here](https://github.com/VIB-PSB/TripleXtract/wiki/Pipeline-execution).


## Output files

Descriptions of generated files—including custom GAF triples, evidence records, and MINI-EX priors—are available in the [Output files wiki](https://github.com/VIB-PSB/TripleXtract/wiki/Output-files).


## Contact and support

Should you have any questions or suggestions, please send an e-mail to klaas.vandepoele@psb.vib-ugent.be.

Should you encounter a bug, please [open an issue](https://github.com/VIB-PSB/TripleXtract/issues).
