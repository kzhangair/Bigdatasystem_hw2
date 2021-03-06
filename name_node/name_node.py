import socket
import sys
import struct
import os
import time

HEAD_STRUCT = '128sII'

def DownloadFile(addr, port, file_name):
    try:
        buffer_size = 1024

        #Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        #Connect the socket to the port where the server is listening
        server_address = (addr, port)
        print >>sys.stderr, 'connecting to %s port %s' % server_address
        sock.connect(server_address)

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
            one_slice = sock.recv(buffer_size)
            if not one_slice:
                print >>sys.stderr, 'one_slice empty!!!'
        fopen.close()
        print >>sys.stderr, 'closing socket'
        sock.close()
        return True
    except:
        print  "There is some error!"
        return False

def UploadFile(addr, port, file_name):
    try:
        #Create a TCP/IP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        #Connect the socket to the port where the server is listening
        server_address = (addr, port)
        print >>sys.stderr, 'connecting to %s port %s' % server_address
        sock.connect(server_address)
        # Send file_name to data_node
        print >> sys.stderr, 'Upload %s to data_node' % file_name
        sent_info = struct.pack(HEAD_STRUCT, file_name, len(file_name), 1)
        sock.send(sent_info)

        fopen = open(file_name, 'rb')
        print >> sys.stderr, 'Begin to send file...'
        for slice in fopen:
            sock.send(slice)
        print >> sys.stderr, 'sent...'
        fopen.close()
        print >>sys.stderr, 'closing socket'
        sock.close()
        return True
    except:
        print  "There is some error!"
        return False

def isFileNameInRecord(file_name):
    chunk_num = -1
    try:
        record_f = open('dfs_name_record', 'r')
    except:
        return False, chunk_num
    line = record_f.readline()
    while line:
        strList = line.split()
        if strList[0] == file_name:
            chunk_num = int(strList[1])
            break
        else:
            line = record_f.readline()
    record_f.close()
    if chunk_num == -1 :
        return False, chunk_num
    else :
        return True, chunk_num

def DFSSave(file_name, local_path):
    start = time.clock()
    isRecorded, chunk_num = isFileNameInRecord(file_name)
    if isRecorded:
        print "%s already in DFS!" % file_name
        return
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
        os.remove(upload_file_name)
    print 'File Save Complete!'
    elapsed = (time.clock() - start)
    print "DFSSave Time used: %d seconds" % elapsed

def DFSLoad(file_name, local_path):
    start = time.clock()
    isRecorded, chunk_num = isFileNameInRecord(file_name)
    if not isRecorded:
        print "No Such File in DFS!"
        return
    print "Chunk Number of %s is %d" % (file_name, chunk_num)
    fw = open(local_path, 'wb')
    for i in range(1, chunk_num+1):
        download_file_name = file_name + "_" + str(i)
        data_node_id = i%3
        if data_node_id == 0:
            result = DownloadFile('thumm02', 12306, download_file_name)
            if not result:
                DownloadFile('thumm03', 12306, download_file_name)
        elif data_node_id == 1:
            result = DownloadFile('thumm04', 12306, download_file_name)
            if not result:
                DownloadFile('thumm02', 12306, download_file_name)
        elif data_node_id == 2:
            result = DownloadFile('thumm03', 12306, download_file_name)
            if not result:
                DownloadFile('thumm04', 12306, download_file_name)
        fo = open(download_file_name, 'rb')
        for data in fo:
            fw.write(data)
        fo.close()
        os.remove(download_file_name)
    fw.close()
    print 'File load Complete!'
    elapsed = (time.clock() - start)
    print "DFSLoad Time used: %d seconds" % elapsed