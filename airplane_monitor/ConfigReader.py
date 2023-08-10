from pathlib import Path
from configparser import ConfigParser


# config file should be in parent to src folder
class ConfigReader:
    def __init__(self, config_path: str | None = None):
        # if None, check ../config.ini
        if config_path is None:
            self.config_path = Path(__file__).resolve().parents[1] / "config.ini"
        else:
            self.config_path = Path(config_path)

        # initialize reader
        self.config = ConfigParser()
        self.config.read(self.config_path)

    @property
    def db_path_raw(self):
        return self.config["default"]["db_path_raw"]

    @property
    def db_path_agg(self):
        return self.config["default"]["db_path_agg"]

    @property
    def timezone(self):
        return self.config["app"]["time_zone"]

    @property
    def base_url(self):
        return self.config["app"]["base_url"]
