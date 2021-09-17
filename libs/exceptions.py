#!/usr/bin/env python3

class PpException(Exception):

    e_code = ""
    e_type = ""
    e_desc = ""
    e_value = ""

    def __init__(self, _e_type,_e_code,_e_value):
        self.e_type = _e_type
        self.e_code = _e_code
        self.e_value = _e_value

class RequestError(PpException):
    # Constructor or Initializer 
    def __init__(self, _e_code,_e_value): 
        super().__init__("request", _e_code,_e_value)
        PpException.e_desc = {"401":"ConnectionError","402":"HTTPError","403":"Timeout",
                                "404":"TooManyRedirects","405":"RequestException"}.get(_e_code)

class ParseError(PpException):
    # Constructor or Initializer 
    def __init__(self, _e_code): 
        super().__init__("parse", _e_code)
        PpException.e_desc = {"401":"ConnectionError","402":"HTTPError","403":"Timeout",
                                "404":"TooManyRedirects","405":"RequestException"}.get(_e_code)
class SaveError(PpException):
    # Constructor or Initializer 
    def __init__(self, _e_code): 
        super().__init__("save", _e_code)
        PpException.e_desc = {"401":"ConnectionError","402":"HTTPError","403":"Timeout",
                                "404":"TooManyRedirects","405":"RequestException"}.get(_e_code)

class CopyError(PpException):
    # Constructor or Initializer 
    def __init__(self, _e_code,_e_value): 
        super().__init__("save", _e_code,_e_value)
        PpException.e_desc = {"701":"SourceNotFound","702":"DataFrameError","703":"CopyError"}.get(_e_code)
