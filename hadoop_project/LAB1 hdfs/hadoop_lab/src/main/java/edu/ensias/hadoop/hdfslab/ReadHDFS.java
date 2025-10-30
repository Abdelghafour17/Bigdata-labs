package edu.ensias.hadoop.hdfslab;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadHDFS {
    public static void main(String[] args) {
        
        // Vérification des arguments (1 argument requis: chemin_complet_fichier)
        if (args.length != 1) {
            System.out.println("Usage: hadoop jar <jarname> <classpath> <file_path_on_hdfs>");
            System.out.println("Example: hadoop jar hadoop-exe.jar edu.ensias.hdfslab.ReadHDFS /user/root/input/achats.txt");
            System.exit(1);
        }

        String hdfsPath = args[0];
        Configuration conf = new Configuration();
        
        try {
            // 1. Connexion au FileSystem
            FileSystem fs = FileSystem.get(conf);
            Path filePath = new Path(hdfsPath);

            // 2. Vérifier l'existence du fichier
            if (!fs.exists(filePath)) {
                System.out.println("Fichier HDFS non trouvé: " + hdfsPath);
                System.exit(1);
            }

            // 3. Ouvrir le flux de lecture du fichier HDFS
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(filePath)))) {
                String line;
                System.out.println("--- Contenu du fichier HDFS (" + hdfsPath + ") ---");
                
                // 4. Lire et afficher chaque ligne
                while ((line = br.readLine()) != null) {
                    System.out.println(line);
                }
                System.out.println("--- Fin du contenu ---");
            }
        
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}