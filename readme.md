# Home assistant database compactor
This is a simple script to compact the home assistant database. It will remove all states which are unchanged from the
database. It will also vacuum the database to reclaim the space.

## Installation
In order to use script you need to have the following installed:

system package **py3-psycopg2** through your AppDaemon config.

## Configuration
Copy app.yaml.example to the app.yaml and fill postgres database credentials
