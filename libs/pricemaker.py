#!/usr/bin/env python3

from libs.ppsettings import pp_settings
from datetime import datetime as dt, timedelta
import pandas as pd
import numpy as np

class PriceMaker:
    update_from_path = ""
    date_for_update = ""

    def __init__(self,_update_from_path,_date_for_update):
        self.update_from_path = _update_from_path
        self.date_for_update = _date_for_update
        self.update_time = self.update_from_path.split('_')[1][:5]

        settings = pp_settings()

        self.update_to_path = settings['dailypri']['path']
        self.update_to_columns = settings['dailypri']['format']
        self.update_from_path = settings['amfi']['path']['pp'] + self.update_from_path + ".csv"

