#!/usr/bin/env python
import sys

# L'entrée vient de l'entrée standard (STDIN)
for line in sys.stdin:
    line = line.strip() # Enlève les espaces blancs au début/fin
    words = line.split() # Sépare la ligne en mots
    
    for word in words:
        # Écrit le résultat sur la sortie standard (STDOUT) au format (clé\tvaleur)
        print('%s\t%s' % (word, 1))