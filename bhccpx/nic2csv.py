import os
import glob
from configparser import ConfigParser
import zipfile
import logging
import xml2csv

logger = logging.getLogger("nic2csv")


def extract_files_from_zip(zip_path: str, extract_to) -> list[str]:
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


def process_files(zipfile_globs: list[str], config: ConfigParser):
	"""Extract files from zip archives and convert XML->CSV if needed"""
	for zipfile_glob in zipfile_globs:
		if os.path.isabs(zipfile_glob):
			zipfile_glob_qualified = zipfile_glob
		else:
			zipfile_glob_qualified = os.path.join(config.get('DEFAULT', 'datadir'), zipfile_glob)
		
		matches = glob.glob(zipfile_glob_qualified)
		if not matches:
			logger.warning('Zip file not found: %s', zipfile_glob_qualified)
			continue
			
		for zipfile in matches:
			extracted_files = extract_files_from_zip(zipfile, config.get('DEFAULT', 'datadir'))
			for extracted_file in extracted_files:
				if extracted_file.lower().endswith('.xml'):
					xml2csv.parse_nic_file(config, extracted_file)


def process(config: ConfigParser, *zipfiles: str):
	if zipfiles:
		process_files(list(zipfiles), config)
	else:
		logger.warning("No zipfiles provided to nic2csv process")

def main():
	import argparse
	import bhc_datautil

	parser = argparse.ArgumentParser(description='Extract CSV or XML files from zip archives')
	parser.add_argument('zipfiles', nargs='+', help='List of zip files to process')
	args = parser.parse_args()
	config = bhc_datautil.read_config()
	
	process(config, *args.zipfiles)

if __name__ == '__main__':
	main()
