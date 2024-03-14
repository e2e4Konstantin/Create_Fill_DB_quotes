
import configparser
import os
import io

file_name = "config.yaml"

config = configparser.ConfigParser()
config['PostgresSQL'] = {
    'host': '172.16.49.193',
    'port': '5432',
    'dbname': 'postgres',
    'user': 'read_write',
    'password': 'read_write',
    }

config['Constants'] = {}
const = config['Constants']
const['TEST_LIMIT'] = int('10')



with open(file_name, 'w') as file:
    config.write(file)
