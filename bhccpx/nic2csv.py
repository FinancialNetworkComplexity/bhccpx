import os
import argparse
from configparser import ConfigParser
import zipfile
import datetime
import logging
from typing import Literal
import bhc_datautil
import xml2csv

def get_base_zipfiles(file_type):
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
	base_zipfiles = get_base_zipfiles(format)

	latest_files_map: dict[str, datetime.datetime] = {}
	for base_zip in base_zipfiles:
		result: tuple[str, datetime.datetime] = choose_latest_zipefile(base_zip, config, logger)
		if result is None:
			logger.error('Could not find latest zip file for %s', base_zip)
			return
		latest_files_map[result[0]] = result[1]

	# Check if all dates are equal
	dates = set(latest_files_map.values())
	if len(dates) != 1:
		logger.error('Zip file dates are not equal: %s', str(latest_files_map))
		return
	logger.info('Using zip files from date: %s', list(dates)[0].strftime('%Y%m%d'))
	zipfiles = list(latest_files_map.keys())
	
	for zip_filename in zipfiles:
		zip_path = os.path.join(data_dir, zip_filename)
		if not os.path.exists(zip_path):
			logger.warning('Zip file not found: %s', zip_path)
			continue
		extracted_files = extract_files_from_zip(zip_path, data_dir, format, logger)
		if format == 'xml':
			for extracted_file in extracted_files:
				xml2csv.parse_nic_file(config, extracted_file, logger)


def get_zip_date(zip_path: str) -> str:
	with zipfile.ZipFile(zip_path, 'r') as f:
		info = f.infolist()[0]
		dt = datetime.datetime(*info.date_time)
		return dt.strftime('%Y%m%d')


def timestamp_all_zipfiles(config: ConfigParser, logger=logging):
	"""Rename all zip files to include their internal date if not already present."""
	data_dir = config.get('DEFAULT', 'datadir')
	zip_files = get_base_zipfiles('csv') + get_base_zipfiles('xml')
	for zip_filename in zip_files:
		zip_path = os.path.join(data_dir, zip_filename)
		if not os.path.exists(zip_path):
			continue
		zip_date = get_zip_date(zip_path)
		if zip_date not in zip_filename:
			new_zip_filename = zip_filename.replace('.zip', f'_{zip_date}.zip')
			new_zip_path = os.path.join(data_dir, new_zip_filename)
			os.rename(zip_path, new_zip_path)
			logger.debug('Renamed %s to %s', zip_filename, new_zip_filename)
		else:
			logger.debug('Zip file %s already has date in name', zip_filename)


def choose_latest_zipefile(base_zipfile: str, config: ConfigParser, logger=logging) -> tuple[str, datetime.datetime] | None:
	"""Choose the latest zip file based on date in filename."""
	data_dir = config.get('DEFAULT', 'datadir')
	files = [f for f in os.listdir(data_dir) if f.startswith(base_zipfile.replace('.zip', '_')) and f.endswith('.zip')]
	if not files:
		logger.warning('No zip files found for base name: %s', base_zipfile)
		return None
	
	latest_date = None
	latest_file = None
	for file in files:
		try:
			date_str = file.split('_')[-1].replace('.zip', '')
			if len(date_str) == 8 and date_str.isdigit():
				date_obj = datetime.datetime.strptime(date_str, '%Y%m%d')
				if latest_date is None or date_obj > latest_date:
					latest_date = date_obj
					latest_file = file
		except:
			continue
	
	if latest_file is None:
		logger.warning('No files with valid date format found for base name: %s', base_zipfile)
		return None
	logger.debug('Chosen latest zip file: %s', latest_file)
	return latest_file, latest_date


def main():
	parser = argparse.ArgumentParser(description='Extract CSV or XML files from zip archives')
	parser.add_argument('--format', choices=['csv', 'xml'], default='csv', help='File format to process (default: csv)')
	parser.add_argument('--skip-timestamp-rename', type=bool, default=False, help='Whether to skip timestamp-rename all zipfiles (default: False)')
	args = parser.parse_args()
	config = bhc_datautil.read_config()
	logger = logging.getLogger("www2csv")

	if not args.skip_timestamp_rename:
		timestamp_all_zipfiles(config, logger)

	process_files(args.format, config, logger)

if __name__ == '__main__':
	main()
