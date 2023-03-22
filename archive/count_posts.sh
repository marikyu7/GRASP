# Create a script that iterates over an array of json files and counts in the number of json objects in each file.
# The script should output the number of posts in each file and the total number of posts in all files.
# The script should be able to handle any number of json files in the directory.


#!/bin/bash

# create a array of directories
directories=("personality/extrovert" "personality/introvert" "personality/intuitive" "personality/sensing" "personality/thinking" "personality/feeling" "personality/judging" "personality/perceiving")

# Create a variable to store the total number of posts
total_posts=0

for dir in ${directories[@]}; do

    # Create an array of json files
    json_files=("$dir"/*.json)


    # Iterate over the array of json file



s
    for file in ${json_files[@]}; do

    # Count the number of posts in each file
    posts=$(cat $file | grep '^\s\s},'| wc -l )

    # Add the number of posts in each file to the total number of posts
    total_posts=$((total_posts + posts))

    done

done

# Output the total number of posts
echo "There are $total_posts posts in total"
