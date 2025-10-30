Ce laboratoire a permis de maîtriser les fondamentaux du Big Data en déployant un cluster Hadoop 3 via Docker Compose, essentiel pour le stockage et le traitement de données distribuées.

 Le projet s'est articulé autour de deux axes principaux : l'interaction avec le système de fichiers distribué HDFS (Hadoop Distributed File System) en utilisant l'API Java, et l'implémentation de l'algorithme de comptage de mots (WordCount). Le Job WordCount a été développé et exécuté avec succès de deux manières différentes : en utilisant l'API MapReduce Java native (dans un projet Maven séparé) et en exploitant Hadoop Streaming pour intégrer des scripts Python externes comme Mapper et Reducer. Cette approche a validé la capacité à gérer le cycle de vie complet du traitement Big Data, de la soumission du Job au cluster YARN à la lecture du résultat final sur HDFS.

**
 Hadoop_project : contient les 2 premiers LABS **
 
