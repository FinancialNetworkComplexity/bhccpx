"""Unpacks raw NIC download archives and converts their contents to CSV.

FFIEC publishes NIC snapshots as ZIP archives holding either CSV or XML
data files. This module extracts the members of those archives into the
``datadir`` folder specified in the configuration file. XML files are
handed to :mod:`xml2csv` for conversion to CSV, and non-data members
in the archive are skipped

Usage::

    python nic2csv.py <zipfile> [<zipfile> ...] [-c CONFIG] [-p key:value]

Archive names may be globs, and are resolved relative to ``datadir`` unless
absolute. Settings are read from the ``[nic2csv]`` section of ``BHCCPX.ini``.
"""

import os
import glob
from configparser import ConfigParser
import zipfile
import logging

from xml2csv import parse_nic_file

logger = logging.getLogger("nic2csv")


def extract_files_from_zip(zip_path: str, extract_to: str) -> list[str]:
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
    """Extract files from zip archives and convert XML->CSV if needed.

    Processes a list of glob patterns pointing to zip archives. For each matched
    zip file, extracts its contents to the configured data directory and
    automatically converts any XML files to CSV format using the NIC file parser.

    Args:
        zipfile_globs (list[str]): List of glob patterns or file paths pointing to
            zip archives. Patterns can be absolute paths or relative paths (which
            will be resolved relative to the 'datadir' from config).
            Example: ['data/export_*.zip', '/absolute/path/archive.zip']

        config (ConfigParser): Configuration object with at least a 'DEFAULT'
            section containing 'datadir' key specifying the base directory for
            relative path resolution and file extraction.
            Expected structure:
                [DEFAULT]
                datadir = /path/to/data

    Returns:
        None

    Raises:
        No exceptions raised directly; logs warnings for unmatched glob patterns.

    Notes:
        - Unmatched glob patterns generate a warning log but don't halt execution.
        - Only .xml files (case-insensitive) are automatically processed for
          conversion.
        - Requires external functions: extract_files_from_zip() and
          parse_nic_file() to be defined.

    Example:
        >>> from configparser import ConfigParser
        >>>
        >>> # Set up configuration
        >>> config = ConfigParser()
        >>> config['DEFAULT'] = {'datadir': '/home/user/data'}
        >>>
        >>> # Process multiple zip files matching patterns
        >>> zipfile_patterns = [
        ...     'exports/monthly_*.zip',
        ...     '/archive/backup_2024.zip'
        ... ]
        >>> process_files(zipfile_patterns, config)

        # This will:
        # 1. Find all zip files matching 'exports/monthly_*.zip' relative to /home/user/data
        # 2. Find /archive/backup_2024.zip (absolute path)
        # 3. Extract contents to /home/user/data
        # 4. Automatically convert any .xml files to .csv format
    """
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
					parse_nic_file(config, extracted_file)


def process(config: ConfigParser, *zipfiles: str):
	if zipfiles:
		process_files(list(zipfiles), config)
	else:
		logger.warning("No zipfiles provided to nic2csv process")

def main():
	import argparse
	from bhc_datautil import add_common_args, get_config

	parser = argparse.ArgumentParser(description='Extract CSV or XML files from zip archives')
	parser.add_argument('zipfiles', nargs='+', help='List of zip files to process')
	add_common_args(parser)
	args = parser.parse_args()
	config = get_config(args, 'nic2csv')
	
	process(config, *args.zipfiles)

if __name__ == '__main__':
	main()
