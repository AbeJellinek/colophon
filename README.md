# Colophon

Colophon traverses the database compiled by the wonderful [Unpaywall](https://unpaywall.org/) project for articles on a topic of your choosing and exports the results as MARC records, ready to be imported into a library database. Great for libraries without [many] journal subscriptions.

## Installation

Clone the repository and use [pip](https://pip.pypa.io/en/stable/) to install the requirements. Colophon requires Python 3.

```
$ git clone https://github.com/AbeJellinek/colophon.git
$ cd colophon
$ pip install -r requirements.txt
```

## Usage

```
$ python3 colophon.py --help
usage: colophon.py [-h] {download,dl,filter,marc} ...

Filter Unpaywall data for library use.

positional arguments:
  {download,dl,filter,marc}

optional arguments:
  -h, --help            show this help message and exit
```

Colophon has three main modules: `download`, `filter`, and `marc`. `download` downloads the latest Unpaywall dataset, `filter` filters it based on regex files that you pass in with `-p`, and `marc` converts the filtered CSV produced by `filter` into [MARC 21](https://www.loc.gov/marc/bibliographic/) data that can be imported into most major library software backends.

For help with one of these modules, run `python3 colophon.py <module> --help`, i.e. `python3 colophon.py filter --help`.

## Contributing
Pull requests are welcome.

## License
[BSD](https://opensource.org/licenses/BSD-3-Clause).
