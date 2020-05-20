#!/usr/bin/python3
from oco2peak import nc4_convert
import pandas as pd
#input_dir = r'/media/NAS-Divers/dev/datasets/OCO2/nc4/'
input_dir = r'/media/NAS-Divers/dev/datasets/OCO2/nc4-v10/'
output_dir = r'../../../datasets/OCO2/csv-v10/'

years_months = nc4_convert.get_pattern_yearmonth()
nc4_convert.process_files(input_dir, output_dir, years_months)