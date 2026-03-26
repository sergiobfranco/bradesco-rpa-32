@ECHO OFF
autoflake --in-place --remove-unused-variables --remove-all-unused-imports --recursive ./
pipreqs ./ --force

