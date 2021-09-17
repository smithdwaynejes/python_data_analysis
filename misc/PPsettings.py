import json

def ppsettings():
	with open("/var/prism/config/settings.json") as f:
		return json.load(f)
