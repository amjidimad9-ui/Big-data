package edu.supmti;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {

    public static void main(String[] args) throws IOException {

        if (args.length != 3) {
            System.out.println("Usage: HadoopFileStatus <hdfs_path> <old_name> <new_name>");
            System.exit(1);
        }

        String directory = args[0];
        String oldName = args[1];
        String newName = args[2];

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path filepath = new Path(directory + "/" + oldName);

        if (!fs.exists(filepath)) {
            System.out.println("File does not exist !");
            System.exit(1);
        }

        FileStatus status = fs.getFileStatus(filepath);

        System.out.println("----- FILE INFO -----");
        System.out.println("Name: " + status.getPath().getName());
        System.out.println("Size: " + status.getLen() + " bytes");
        System.out.println("Owner: " + status.getOwner());
        System.out.println("Permission: " + status.getPermission());
        System.out.println("Replication: " + status.getReplication());
        System.out.println("Block Size: " + status.getBlockSize());

        System.out.println("\n----- BLOCK LOCATIONS -----");
        BlockLocation[] blocks = fs.getFileBlockLocations(status, 0, status.getLen());
        for (BlockLocation block : blocks) {
            System.out.println("Block offset: " + block.getOffset());
            System.out.println("Block length: " + block.getLength());
            System.out.print("Hosts: ");
            for (String host : block.getHosts()) {
                System.out.print(host + " ");
            }
            System.out.println("\n");
        }

        // Rename file
        Path newFile = new Path(directory + "/" + newName);
        fs.rename(filepath, newFile);
        System.out.println("Fichier renommé → " + newName);

        fs.close();
    }
}
