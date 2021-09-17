import json

def pp_settings():
	with open("/var/prism/config/settings.json") as f:
		return json.load(f)
