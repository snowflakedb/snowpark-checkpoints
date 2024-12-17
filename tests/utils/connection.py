import configparser
import os

def connectionConfig():
    configfile_name = os.path.join(os.path.dirname(__file__), "config.ini")
    config = configparser.ConfigParser()
    config.read(configfile_name)
    return config