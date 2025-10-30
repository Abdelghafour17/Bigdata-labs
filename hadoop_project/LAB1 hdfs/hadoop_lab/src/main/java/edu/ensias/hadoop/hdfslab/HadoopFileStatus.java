package edu.ensias.hadoop.hdfslab;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
    public static void main(String[] args) {
        
        // 1. Vérification des arguments (3 arguments requis : dir, old_name, new_name)
        if (args.length != 3) {
            System.out.println("Usage: hadoop jar <jarname> <classpath> <dir> <old_filename> <new_filename>");
            System.out.println("Example: hadoop jar hadoop-exe.jar edu.ensias.hdfslab.HadoopFileStatus /user/root/input purchases.txt achats.txt");
            System.exit(1);
        }

        // Récupération des arguments
        String dirPath = args[0];
        String oldFileName = args[1];
        String newFileName = args[2];
        
        Configuration conf = new Configuration();
        FileSystem fs;
        try {
            fs = FileSystem.get(conf);
            Path filepath = new Path(dirPath, oldFileName); // Utilise les arguments
            
            if(!fs.exists(filepath)){
                System.out.println("File " + oldFileName + " does not exist in " + dirPath);
                System.exit(1);
            }
            
            FileStatus status = fs.getFileStatus(filepath);
            
            // Affichage des informations
            System.out.println(Long.toString(status.getLen()) + " bytes");
            System.out.println("File Name: " + filepath.getName());
            System.out.println("File Size: " + status.getLen());
            System.out.println("File owner: " + status.getOwner());
            System.out.println("File permission: " + status.getPermission());
            System.out.println("File Replication: " + status.getReplication());
            System.out.println("File Block Size: " + status.getBlockSize());
            
            BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
            for(BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                System.out.println("Block offset: " + blockLocation.getOffset());
                System.out.println("Block length: " + blockLocation.getLength());
                System.out.print("Block hosts: ");
                for (String host : hosts) {
                    System.out.print(host + " ");
                }
                System.out.println();
            }
            
            // Renommage du fichier
            fs.rename(filepath, new Path(dirPath, newFileName));
            System.out.println("File renamed successfully from " + oldFileName + " to " + newFileName + "!");
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}