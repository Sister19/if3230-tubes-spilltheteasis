server leader command:  
`python3 server.py localhost 15000`

server follower command:  
`python3 server.py localhost 15001 localhost 15000`  
`python3 server.py localhost 15002 localhost 15000`

client command:  
`python3 client.py localhost 3000 localhost 15000`