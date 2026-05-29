import os
import glob
import argparse
from configparser import ConfigParser
import zipfile
import logging
import bhc_datautil
import xml2csv

logger = logging.getLogger("nic2csv")


def extract_files_from_zip(zip_path, extract_to) -> list[str]:
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


def process_files(zipfiles: list[str], config: ConfigParser):
	"""Extract files from zip archives and convert XML->CSV if needed"""
	for zip_filename in zipfiles:
		if os.path.isabs(zip_filename):
			zip_path_pattern = zip_filename
		else:
			zip_path_pattern = os.path.join(config.get('DEFAULT', 'datadir'), zip_filename)
		
		matches = glob.glob(zip_path_pattern)
		if not matches:
			logger.warning('Zip file not found: %s', zip_path_pattern)
			continue
			
		for zip_path in matches:
			extracted_files = extract_files_from_zip(zip_path, config.get('DEFAULT', 'datadir'))
			for extracted_file in extracted_files:
				if extracted_file.lower().endswith('.xml'):
					xml2csv.parse_nic_file(config, extracted_file)


def process(config, *zipfiles: str):
	if zipfiles:
		process_files(list(zipfiles), config)
	else:
		logger.warning("No zipfiles provided to nic2csv process")

def main():
	parser = argparse.ArgumentParser(description='Extract CSV or XML files from zip archives')
	parser.add_argument('zipfiles', nargs='+', help='List of zip files to process')
	args = parser.parse_args()
	config = bhc_datautil.read_config()
	process(config, *args.zipfiles)

if __name__ == '__main__':
	main()
