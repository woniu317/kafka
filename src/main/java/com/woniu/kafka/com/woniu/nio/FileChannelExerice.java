package com.woniu.kafka.com.woniu.nio;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileChannelExerice {
    public static void main(String[] args) throws IOException {
        FileChannelExerice fileChannelExerice = new FileChannelExerice();
        fileChannelExerice.bytesBuffer();
    }

    public void bytesBuffer() throws IOException {
        RandomAccessFile randomAccessFile = new RandomAccessFile("./resource/random.txt","rw");
        FileChannel channel = randomAccessFile.getChannel();
        ByteBuffer buf = ByteBuffer.allocate(8);
        int bytesRead = channel.read(buf);
        while (bytesRead != -1) {
            System.out.println("Read " + bytesRead);
            buf.flip();

            while (buf.hasRemaining()) {
                System.out.println((char)buf.get());
            }

            buf.clear();
            bytesRead = channel.read(buf);
        }
    }
}
