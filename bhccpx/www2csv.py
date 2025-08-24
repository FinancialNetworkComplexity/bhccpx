import os
import argparse
from configparser import ConfigParser
import importlib.util
import zipfile
import logging
from typing import Literal
import bhc_datautil
import xml2csv

def get_zip_files(file_type):
	base_names = [
		'ATTRIBUTES_ACTIVE.zip',
		'ATTRIBUTES_BRANCHES.zip',
		'ATTRIBUTES_CLOSED.zip',
		'RELATIONSHIPS.zip',
		'TRANSFORMATIONS.zip'
	]
	prefix = 'XML_' if file_type == 'xml' else 'CSV_'
	return [prefix + name for name in base_names]


def extract_files_from_zip(zip_path, extract_to, file_type, logger=logging) -> list[str]:
	"""Extract files of specified type from zip archive and return list of extracted files."""
	extracted_files = []
	with zipfile.ZipFile(zip_path, 'r') as zf:
		for member in zf.namelist():
			if file_type == 'xml' and member.lower().endswith('.xml'):
				zf.extract(member, extract_to)
				extracted_files.append(member)
				logger.info('Extracted %s to %s', member, extract_to)
			elif file_type == 'csv' and member.lower().endswith('.csv'):
				zf.extract(member, extract_to)
				extracted_files.append(member)
				logger.info('Extracted %s to %s', member, extract_to)
			else:
				logger.warning('Skipped %s (not a %s file)', member, file_type)
	return extracted_files


def process_files(format: Literal['csv', 'xml'], config: ConfigParser, logger: logging):
	"""Extract files from zip archives and convert XML to CSV if needed"""
	data_dir = config.get('DEFAULT', 'datadir')
	zip_files = get_zip_files(format)
	
	for zip_filename in zip_files:
		zip_path = os.path.join(data_dir, zip_filename)
		if not os.path.exists(zip_path):
			logger.warning('Zip file not found: %s', zip_path)
			continue
		extracted_files = extract_files_from_zip(zip_path, data_dir, format, logger)
		if format == 'xml':
			for extracted_file in extracted_files:
				xml2csv.parse_nic_file(config, extracted_file, logger)

def main():
	parser = argparse.ArgumentParser(description='Extract CSV or XML files from zip archives')
	parser.add_argument('--format', choices=['csv', 'xml'], default='csv', help='File format to process (default: csv)')
	args = parser.parse_args()
	config = bhc_datautil.read_config()
	logger = logging.getLogger("www2csv")
	process_files(args.format, config, logger)

if __name__ == '__main__':
	main()
