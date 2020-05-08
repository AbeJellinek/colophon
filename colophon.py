# Copyright (c) 2020 Abe Jellinek
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * The name of the author may not be used to endorse or promote products
#       derived from this software without specific prior written permission.

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
import requests
import regex as re
import errno
import gzip
import json
import argparse

from tqdm import tqdm
from datetime import datetime
from pymarc import Record, Field


URL_BASE = 'https://unpaywall-data-snapshots.s3-us-west-2.amazonaws.com/'

title_splitter = re.compile(r'([:;\\/\p{Pd},.])')
filters = []

def latest_dataset():
    r = requests.get(URL_BASE)
    manifest = r.text

    match = re.search('(?s:.*)<Key>([^<]+)</Key><LastModified>([^<]+)</LastModified><ETag>[^<]+</ETag><Size>([0-9]+)</Size>', \
        manifest)

    if not match:
        return

    path = URL_BASE + match.group(1)
    last_modified = datetime.strptime(match.group(2), '%Y-%m-%dT%H:%M:%S.%fZ')
    size_in_gb = int(match.group(3)) / 1_000_000_000

    return path, last_modified, size_in_gb

def prompt_download(local_data_path):
    print('No local Unpaywall dataset found. Searching online...')
    path, last_modified, size_in_gb = latest_dataset()
    if path:
        print(f'Dataset found. Last update: {last_modified:%d %b %Y}.')
        if input(f'Download this {size_in_gb:1.1f} GB dataset now? [Y/n] ').lower() != 'n':
            response = requests.get(path, stream=True)
            if response.ok:
                with open(local_data_path, 'wb') as handle:
                    with tqdm(
                        unit='B', unit_scale=True, unit_divisor=1000,
                        miniters=1,
                        total=int(response.headers.get('content-length', 0))
                    ) as pbar:
                        for chunk in response.iter_content(chunk_size=4096):
                            handle.write(chunk)
                            pbar.update(len(chunk))
                print('Done! Proceeding...')
            else:
                print(f'Non-OK response: status code {response.status_code}')
                os.exit(1)
        else:
            os.exit(1)
    else:
        print('ERROR: No dataset found online.')
        os.exit(99)

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

def process_entry(line):
    obj = json.loads(line)

    oa_location = obj['best_oa_location']

    if oa_location is None or obj['title'] is None:
        return

    title_lower = obj['title'].lower()

    if any(pattern.findall(title_lower) for pattern in filters):
        return to_marc(obj)

def main():
    parser = argparse.ArgumentParser(description='Process Unpaywall data and output MARC.')
    parser.add_argument('-f', action='append', dest='filter', default=['filters/jordan'],
                        help='specify path to a file containing paper title regex')
    parser.add_argument('-d', dest='dataset', default='data/unpaywall_snapshot.jsonl.gz',
                        help='specify path to the Unpaywall dataset in GZIP format')
    parser.add_argument('-o', dest='output_file', default='out.mrc',
                        help='specify path of the MRC file to output to')

    args = parser.parse_args()

    local_data_path = args.dataset

    for filename in args.filter:
        with open(filename, 'r') as file:
            filters.append(re.compile(file.read().strip()))

    print()
    print('Colophon 1.0 by Abe Jellinek <jellinek@berkeley.edu>')
    print('Checking data...')

    if os.path.isfile(args.output_file):
        if input('Output file exists! Overwrite? [y/N] ').lower() != 'y':
            os.exit(1)

    downloaded = os.path.isfile(local_data_path)

    if not downloaded:
        if not os.path.exists(os.path.dirname(local_data_path)):
            try:
                os.makedirs(os.path.dirname(local_data_path))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise

        prompt_download(local_data_path)

    print('Unpaywall dataset ready. Reading...')
    print()

    with gzip.open(local_data_path, 'rt') as stream, open(args.output_file, 'wb') as out:
        # total here is just an estimate:
        for line in tqdm(stream, unit=' articles', total=26078206, smoothing=0):
            marc = process_entry(line)
            if marc:
                out.write(marc)


if __name__ == '__main__':
    main()
