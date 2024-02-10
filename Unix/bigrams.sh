#!/usr/bin/env bash
# This program should take an fileIn as first parameter
# It takes the input text file and outputs the 5 most common word bigrams (https://en.wikipedia.org/wiki/Bigram) in the file.
# Your solution should be case insensitive.

# example: when `./bigrams.sh ../data/myBook/01-chapter1.txt' is ran the output should look like this:
#   3 the     little
#   3 little  blind
#   3 blind   text
#   2 the     word
#   2 the     copy

fileIn="$1"

# Clean the file's text from punctiation, upper -> lower and split by spaces
cleanText=$(cat "$fileIn" | tr -d '[:punct:]' | tr '[:upper:]' '[:lower:]' | tr -s '[:space:]' '\n')

# Get the first word of the list and extract it
word_1=$(head -n 1 <<< "$cleanText")

# Remove the first word from the list
cleanText="${cleanText#*$'\n'}"

# Setup a variable for biagrams
biagrams=""

# Read the word_2 from the content and output the word_1 and word_2 and update word_1 with word_2
while read -r word_2; do
    biagrams+="$word_1 $word_2\n"
    word_1="$word_2"
done <<< "$cleanText"

# Sort the biagram so that simillar are together then get the number of occurences then sort by the number of occurences in reverse
# and output the top 5 places
echo -e "$biagrams" | sort | uniq -c | sort -rn | head -n 5

