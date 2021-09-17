#!/usr/bin/env python3

import os


def list_of_files(dirPath, extension=""):
    if not extension:
        return os.listdir(dirPath)

    return [file for file in os.listdir(dirPath) if file.endswith("." + extension)]


def create_dir_if_not(dirName):
    if not os.path.exists(dirName):
        os.mkdir(dirName)
