Octopus
=======

Octopus is a tool for recording events and logs. You can use it to collect logs, parse them, and store them for later use (like, for searching). 

Firstly, this tool is built for recording search events, search click, and Search filer [facet].


How to install ?

run the following commands :

cd $home/octopus

./octopus-webapp


To make sure everything is working fine.

localhost:8080/ws/ping

To log the events 

localhost:8080/ws/record?event=search&q=ramzi&...   // you will find all parameter inside table mapping
