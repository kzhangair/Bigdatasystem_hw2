#-*-coding:utf-8-*-

import socket
import sys
import struct
HEAD_STRUCT = '128sII'
info_size = struct.calcsize(HEAD_STRUCT)
buffer_size = 1024
#addr = '219.223.181.197'
addr = '192.168.0.103'
port = 12306

#Create a TCP/IP socket
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#Bind the socket to the port
server_address = (addr, port)
print >>sys.stderr, 'starting up on %s port %s' % server_address
sock.bind(server_address)
#Listen for incoming connections
sock.listen(1)

while True :
    #Wait for a connection
    print >>sys.stderr, 'waiting for a connection'
    connection, client_address = sock.accept()
    try:
        print >>sys.stderr, 'connection from', client_address
        #Receive the data in small chunks and retransmit it
        file_info = connection.recv(info_size)
        file_name, file_name_len, request_type = struct.unpack(HEAD_STRUCT, file_info)
        file_name = file_name[:file_name_len]
        if request_type == 0 :
            print 'Requested File Name is: %s' % file_name
            fopen = open(file_name, 'rb')
            count = 0
            print 'Begin to send file...'
            for slice in fopen:
                connection.send(slice)
            print >>sys.stderr, 'sent...'
            fopen.close()
        elif request_type == 1:
            print 'Upload File Name is: %s' % file_name
            fopen = open(file_name, 'wb')
            print 'Begin to receive file...'
            one_slice = connection.recv(buffer_size)
            while one_slice:
                fopen.write(one_slice)
                fopen.flush()
                one_slice = connection.recv(buffer_size)
                if not one_slice:
                    print >> sys.stderr, 'one_slice empty!!!'
            print 'File receive complete...'
            fopen.close()
    except socket.errno, e:
        print "Socket error: %s" % str(e)
    finally:
        #Clean up the connection
        connection.close()
