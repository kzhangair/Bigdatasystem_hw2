import socket
import sys
import struct
import time

HEAD_STRUCT = '128sII'

def DownloadFile(addr, port, file_name):
    buffer_size = 1024

    #Create a TCP/IP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    #Connect the socket to the port where the server is listening
    server_address = (addr, port)
    print >>sys.stderr, 'connecting to %s port %s' % server_address
    sock.connect(server_address)

    try:
        #Send file_name to data_node
        print >>sys.stderr, 'Get %s from data_node' % file_name
        sent_info = struct.pack(HEAD_STRUCT, file_name, len(file_name), 0) #DownloadFile = 0
        sock.send(sent_info)

        #Look for the response
        fopen = open(file_name, 'wb')
        one_slice = sock.recv(buffer_size)
        while one_slice :
            fopen.write(one_slice)
            fopen.flush()
            time.sleep(0.1)
            one_slice = sock.recv(buffer_size)
            if not one_slice:
                print >>sys.stderr, 'one_slice empty!!!'
    except socket.errno, e:
            print "Socket error: %s" % str(e)
    finally:
        fopen.close()
        print >>sys.stderr, 'closing socket'
    sock.close()

def UploadFile(addr, port, file_name):
    #Create a TCP/IP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    #Connect the socket to the port where the server is listening
    server_address = (addr, port)
    print >>sys.stderr, 'connecting to %s port %s' % server_address
    sock.connect(server_address)

    try:
        # Send file_name to data_node
        print >> sys.stderr, 'Upload %s to data_node' % file_name
        sent_info = struct.pack(HEAD_STRUCT, file_name, len(file_name), 1)
        sock.send(sent_info)

        fopen = open(file_name, 'rb')
        print >> sys.stderr, 'Begin to send file...'
        for slice in fopen:
            sock.send(slice)
        print >> sys.stderr, 'sent...'
    except socket.errno, e:
        print "Socket error: %s" % str(e)
    finally:
        fopen.close()
        print >>sys.stderr, 'closing socket'
    sock.close()

def DFSSave(file_name, local_path):
    record_f = open('dfs_name_record', 'a')
    record_f.write(file_name+' ')
    fr = open(local_path, 'rb')
    chunk_size = 64 * 1024 * 1024
    chunk_num = 0
    chunk = fr.read(chunk_size)
    while chunk :
        chunk_num = chunk_num + 1
        chunk_name = file_name + '_' + str(chunk_num)
        print  'current chunk is %s' % chunk_name
        fw = open(chunk_name, 'wb')
        fw.write(chunk)
        fw.close()
        chunk = fr.read(chunk_size)
    fr.close()
    record_f.write(str(chunk_num))
    record_f.write('\n')
    record_f.close()
    #Upload chunks
    for i in range(1, chunk_num+1):
        upload_file_name = file_name + '_' + str(i)
        data_node_id = i%3
        if data_node_id == 0:
            UploadFile('thumm02', 12306, upload_file_name)
            UploadFile('thumm03', 12306, upload_file_name)
        elif data_node_id == 1:
            UploadFile('thumm02', 12306, upload_file_name)
            UploadFile('thumm04', 12306, upload_file_name)
        elif data_node_id == 2:
            UploadFile('thumm03', 12306, upload_file_name)
            UploadFile('thumm04', 12306, upload_file_name)
    print 'File Save Complete!'

def DFSLoad(file_name, local_path):
    