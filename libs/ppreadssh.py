#!/usr/bin/python
import paramiko
import socket
#A variable 'e',this will act as a flag. You can also define it inside the function itself.
e=0

#A function I have defined that takes arguments addr as hostname,user as username,passwd as password
def param_ssh(addr,user,passwd):

	global e              #Making use of variable e in the function
       #Assigning the values
	username=str(user)
	password=str(passwd)
	address=str(addr)
	#just a print line
	
	# print("Connecting to server...")

#defining an object for SSHClient(). client will act as a handler here. You can name it anything
	client = paramiko.SSHClient()

#This handles the missing Keys while connecting to the server. It will add the host keys locally if not present.
	client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	try:
		ip=socket.gethostbyname(address)  #getting IP address
		client.connect(ip, username=username, password=password, look_for_keys=False)   # Making connection 
		
	except Exception as r:
		e=1   #if exception is raised value of e will turn 1. Showing failue message
		print("[-]"+str(r)+" \nWrong credentials "+username+":"+password)
	
	#if the value of e stays 0 that means the connection has been made successfully then below lines will run	
	if(e==0):
		# print("Connected Successfully with "+username+":"+password)
		# stdin,stdout,stderr=client.exec_command('whoami')  #creating stdin,stdout,stderr command execution
		# outlines=stdout.readlines()  #reading the terminal output
		# resp=''.join(outlines)
		# print(resp)

		pass
	
	return client
#checker for arguments

