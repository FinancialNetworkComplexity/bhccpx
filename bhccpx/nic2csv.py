import os
import argparse
from configparser import ConfigParser
import zipfile
import logging
import bhc_datautil
import xml2csv


def extract_files_from_zip(zip_path, extract_to, logger=logging) -> list[str]:
	"""Extract all csv and xml files from zip archive and return list of extracted files."""
	extracted_files = []
	with zipfile.ZipFile(zip_path, 'r') as zf:
		for member in zf.namelist():
			if member.lower().endswith('.xml') or member.lower().endswith('.csv'):
				zf.extract(member, extract_to)
				extracted_files.append(member)
				logger.info('Extracted %s to %s', member, extract_to)
			else:
				logger.warning('Skipped %s (not a csv or xml file)', member)
	return extracted_files


def process_files(zipfiles: list[str], config: ConfigParser, logger: logging):
	"""Extract files from zip archives and convert XML->CSV if needed"""
	for zip_filename in zipfiles:
		if os.path.isabs(zip_filename):
			zip_path = zip_filename
		else:
			zip_path = os.path.join(config.get('DEFAULT', 'datadir'), zip_filename)
		
		if not os.path.exists(zip_path):
			logger.warning('Zip file not found: %s', zip_path)
			continue
			
		extracted_files = extract_files_from_zip(zip_path, config.get('DEFAULT', 'datadir'), logger)
		for extracted_file in extracted_files:
			if extracted_file.lower().endswith('.xml'):
				xml2csv.parse_nic_file(config, extracted_file, logger)


def main():
	parser = argparse.ArgumentParser(description='Extract CSV or XML files from zip archives')
	parser.add_argument('zipfiles', nargs='+', help='List of zip files to process')
	args = parser.parse_args()
	config = bhc_datautil.read_config()
	logger = logging.getLogger("www2csv")

	process_files(args.zipfiles, config, logger)

if __name__ == '__main__':
	main()
