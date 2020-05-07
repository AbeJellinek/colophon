# Colophon

Colophon traverses the database compiled by the wonderful [Unpaywall](https://unpaywall.org/) project for articles on a topic of your choosing and exports the results as MARC records, ready to be imported into a library database. Great for libraries without [many] journal subscriptions.

## Installation

Clone the repository and use [pip](https://pip.pypa.io/en/stable/) to install the requirements. Colophon requires Python 3.

```bash
$ git clone https://github.com/AbeJellinek/colophon.git
$ cd colophon
$ pip install -r requirements.txt
```

## Usage

```bash
$ python3 colophon.py --help
usage: colophon.py [-h] [-f FILTER] [-d DATASET] [-o OUTPUT_FILE]

Process Unpaywall data and output MARC.

optional arguments:
  -h, --help      show this help message and exit
  -f FILTER       specify path to a file containing paper title regex
  -d DATASET      specify path to the Unpaywall dataset in GZIP format
  -o OUTPUT_FILE  specify path of the MRC file to output to
```

## Contributing
Pull requests are welcome.

## License
[BSD](https://opensource.org/licenses/BSD-3-Clause).
