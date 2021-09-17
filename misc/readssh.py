#!/usr/bin/env python3


from libs.ppreadssh import param_ssh
from libs.ppsettings import pp_settings

# if len(sys.argv) < 4: 
        # print('Not enough arguments\n>>hostname,username,password required.')
# else:
# settings = ppsettings()
# address=settings['central-host']
# username=settings['central-user']
# password=settings['central-pass']

address='172.20.55.111'
username='prism'
password='pp@555portfolio'

ssh_client = param_ssh(address,username,password)
sftp_client = ssh_client.open_sftp()
# remote_file = sftp_client.get('/var/prism/data/Reports/doc_view/amoacc_1549513851224.pdf','amoacc_1549513851224.pdf')

# print(remote_file >> "amoacc_1549513851224.pdf")
# try:
	# for line in remote_file:
		# print(line)
# finally:
	# remote_file.close()
	

sftp_client.close()

	
