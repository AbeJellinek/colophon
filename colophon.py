# Copyright (c) 2020 Abe Jellinek
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the author not the names of its contributors may
#       be used to endorse or promote products derived from this software
#       without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL ABE JELLINEK BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


import os
import os.path
import sys
import requests
import regex as re
import errno
import gzip
import json
import argparse
import unicodedata
import shutil
import csv

from tqdm import tqdm
from datetime import datetime
from pymarc import Record, Field


URL_BASE = 'https://unpaywall-data-snapshots.s3-us-west-2.amazonaws.com/'
FIELD_NAMES = ['Primary Author', 'Title', 'Year', 'Journal', 'PDF', 'DOI', 'Full JSON']

title_splitter = re.compile(r'([:;\\/\p{Pd},.])')
filters = []


def prompt(question, default=True):
    choices = '[Y/n]' if default else '[y/N]'
    default_choice = 'Y' if default else 'N'
    user_entered = input(f'{question} {choices} ').strip().lower()

    while user_entered and user_entered != 'y' and user_entered != 'n':
        user_entered = input(' ' * max(len(question) - 3, 0) + \
            f' ?? {choices} (or press enter for {default_choice}) ').strip().lower()

    if not user_entered:
        return default
    elif user_entered == 'y':
        return True
    else:
        return False

def latest_dataset():
    r = requests.get(URL_BASE)
    manifest = r.text

    match = re.search('(?s:.*)<Key>([^<]+)</Key><LastModified>([^<]+)</LastModified><ETag>[^<]+</ETag><Size>([0-9]+)</Size>', \
        manifest)

    if not match:
        return

    path = URL_BASE + match.group(1)
    last_modified = datetime.strptime(match.group(2), '%Y-%m-%dT%H:%M:%S.%fZ')
    size = int(match.group(3))

    return path, last_modified, size

def run_download(args):
    local_data_path = args.path

    path, last_modified, size = latest_dataset()
    if path:
        size_in_gb = size / 1073741824

        print(f'Dataset found. Last update: {last_modified:%d %b %Y}.')

        if prompt(f'Download this {size_in_gb:1.1f} GB dataset?', default=True):
            if os.path.isfile(local_data_path) \
                and not prompt('Output file exists! Replace?', default=False):
                sys.exit(0)

            try:
                os.makedirs(os.path.dirname(local_data_path))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise

            with requests.get(path, stream=True) as response:
                if response.ok:
                    with open(local_data_path, 'wb') as handle:
                        with tqdm(
                            unit='B', unit_scale=True, unit_divisor=1024, miniters=1,
                            total=size, smoothing=0
                        ) as pbar:
                            for chunk in response.iter_content(chunk_size=8192):
                                handle.write(chunk)
                                pbar.update(len(chunk))
                    print('Done! Proceeding...')
                else:
                    print(f'ERROR: Download failed with status code {response.status_code}.', file=sys.stderr)
                    sys.exit(1)
        else:
            sys.exit(0)
    else:
        print('ERROR: No dataset found online.', file=sys.stderr)
        sys.exit(99)

def format_author(author, reverse=True):
    if 'given' in author:
        if reverse:
            return f"{author.get('family', '')}, {author.get('given')}"
        else:
            return f"{author['given']} {author.get('family', '')}"
    else:
        return f"{author.get('family', 'Unknown')}"

def format_authors(authors):
    if not authors:
        return ''

    first_author = format_author(authors[0], reverse=True)
    rest = [format_author(author, reverse=False) for author in authors[1:]]

    if len(rest) == 0:
        return first_author
    elif len(rest) == 1:
        return f'{first_author} and {rest[0]}'
    else:
        return f"{first_author}, {', '.join(rest[:-1])}, and {rest[-1]}"

def to_csv(obj, json):
    return {
        'Primary Author': format_author(obj['z_authors'][0], reverse=True) \
            if obj['z_authors'] else 'Unknown',
        'Title': obj['title'],
        'Year': obj['year'],
        'Journal': obj['journal_name'],
        'PDF': obj['best_oa_location']['url'],
        'DOI': obj['doi_url'],
        'Full JSON': json
    }

def to_marc(obj):
    primary_author = format_author(obj['z_authors'][0], reverse=True) if obj['z_authors'] else None

    split_title = [part.strip() for part in title_splitter.split(obj['title'])]

    if len(split_title) > 2:
        primary_title = f'{split_title[0]} {split_title[1]}'
        remainder_of_title = f'{" ".join(split_title[2:])} /'
    else:
        primary_title = f'{" ".join(split_title)} /'
        remainder_of_title = ''

    record = Record()
    record.leader.type_of_record = 'a'
    record.leader.coding_scheme = 'a'
    record.bibliographic_level = 'm'
    record.cataloging_form = 'a'
    # record.add_field(Field(
    #     tag = '041',
    #     indicators = ['#', '#'],
    #     subfields = [
    #         'a', 'Eng'
    #     ]))

    if primary_author:
        record.add_field(Field(
            tag = '100',
            indicators = ['0', ' '],
            subfields = [
                'a', primary_author
            ]))

    if remainder_of_title:
        record.add_field(Field(
            tag = '245',
            indicators = ['0', '0'],
            subfields = [
                'a', primary_title,
                'b', remainder_of_title,
                'c', format_authors(obj['z_authors'])
            ]))
    else:
        record.add_field(Field(
            tag = '245',
            indicators = ['0', '0'],
            subfields = [
                'a', f"{obj['title']} /",
                'c', format_authors(obj['z_authors'])
            ]))

    record.add_field(Field(
            tag = '260',
            indicators = [' ', ' '],
            subfields = [
                'b', obj['publisher'],
                'c', str(obj['year'])
            ]))

    record.add_field(Field(
            tag = '500',
            indicators = [' ', ' '],
            subfields = [
                'a', f"Article from {obj['journal_name']}.",
            ]))

    record.add_field(Field(
            tag = '856',
            indicators = ['4', '0'],
            subfields = [
                'u', obj['best_oa_location']['url'],
                'y', 'View article as PDF'
            ]))

    record.add_field(Field(
            tag = '856',
            indicators = [' ', ' '],
            subfields = [
                'u', obj['doi_url'],
                'y', 'DOI'
            ]))

    # print(str(record))
    return record.as_marc()

def strip_diacritics(s):
   return ''.join(c for c in unicodedata.normalize('NFD', s)
                  if unicodedata.category(c) != 'Mn')

def stream_to_csv(stream):
    for line in stream:
        obj = json.loads(line)

        oa_location = obj['best_oa_location']

        if oa_location is None or obj['title'] is None:
            continue

        title_normalized = strip_diacritics(obj['title']).lower()

        if any(pattern.findall(title_normalized) for pattern in filters):
            yield to_csv(obj, line)

def stream_to_marc(stream):
    for row in stream:
        obj = json.loads(row['Full JSON'])
        yield to_marc(obj)

def run_filter(args):
    for filename in args.pattern:
        with open(filename, 'r') as file:
            filters.append(re.compile(file.read().strip()))

    if args.output_file and os.path.isfile(args.output_file):
        if not prompt('Output file exists! Overwrite?', default=False):
            sys.exit(1)

    downloaded = os.path.isfile(args.dataset)

    if not downloaded:
        python_command = os.path.basename(sys.executable)
        print('ERROR: No downloaded dataset found. Can be downloaded with:', file=sys.stderr)
        print(f'    {python_command} {sys.argv[0]} dl', file=sys.stderr)
        sys.exit(1)

    with gzip.open(args.dataset, 'rt') as stream:
        if not args.output_file or args.output_file == '-':
            writer = csv.DictWriter(sys.stdout, fieldnames=FIELD_NAMES)
            writer.writeheader()

            for line in stream_to_csv(stream):
                writer.writerow(line)
        else:
            with open(args.output_file, 'w', encoding='utf-8') as out:
                writer = csv.DictWriter(out, fieldnames=FIELD_NAMES)
                writer.writeheader()

                # total here is just an estimate:
                for line in stream_to_csv(tqdm(stream, unit=' articles', total=114164038, smoothing=0)):
                    writer.writerow(line)

def run_marc(args):
    with open(args.csv) as csv_file:
        if not args.output_file or args.output_file == '-':
            reader = csv.DictReader(csv_file, fieldnames=FIELD_NAMES)
            next(reader) # read through header
            for marc in stream_to_marc(reader):
                sys.stdout.buffer.write(marc)
        else:
            with open(args.output_file, 'wb') as out:
                reader = csv.DictReader(csv_file, fieldnames=FIELD_NAMES)
                next(reader) # read through header
                for marc in stream_to_marc(reader):
                    out.write(marc)

def main():
    parser = argparse.ArgumentParser(description='Filter Unpaywall data for library use.')
    subparsers = parser.add_subparsers()

    parser_dl = subparsers.add_parser('download', aliases=['dl'])
    parser_dl.add_argument('-o', dest='path', default='data/unpaywall_snapshot.jsonl.gz',
        help='store in the specified location [optional, default location recommended]')
    parser_dl.set_defaults(func=run_download)

    parser_filter = subparsers.add_parser('filter')
    parser_filter.add_argument('-p', action='append', dest='pattern', default=['filters/jordan'],
        help='specify path to a file containing paper title regex (repeat for OR)')
    parser_filter.add_argument('-d', dest='dataset', default='data/unpaywall_snapshot.jsonl.gz',
        help='specify path to the Unpaywall dataset in GZIP format')
    parser_filter.add_argument('-o', dest='output_file',
        help='output to specified CSV file [optional, default: stdout]')
    parser_filter.set_defaults(func=run_filter)

    parser_marc = subparsers.add_parser('marc')
    parser_marc.add_argument('csv', help='input CSV file to process')
    parser_marc.add_argument('-o', dest='output_file',
        help='output to specified MARC file [optional, default: stdout]')
    parser_marc.set_defaults(func=run_marc)

    if len(sys.argv) < 2:
        parser.print_usage()
        sys.exit(1)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
