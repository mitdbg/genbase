# this command registers madlib against a postgreSQL database. Pass in the database name
# as a command line argument
/usr/local/madlib/bin/madpack -p postgres -c $USER@$HOST/$1 install