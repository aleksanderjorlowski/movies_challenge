#MOVIES CHALLENGE

**Table of Contents**

[TOC]

##Description:

> The task is to simulate situation when you run some data transformations based on data stored in different environment than your transformation script runs.
> To achieve this please follow below steps:

> 1. Create two docker containers  - one with configured relational database and second from which you'll run your data transformations  in your preferable >programming language.
> 2. Prepare docker compose that allows to run such setup.
>     Download an appropriate publicly available, non-commercial data set from (https://grouplens.org/datasets/movielens/latest/  ml-latest-small.zip) and import it >to your database.

>*Note: this data set is not provided by ET&S and it is up to the candidate to access this for non-commercial purposes

> 3. Connect to  database form your ‘analytics’ container and answer below questions:

>1. How many movies are in data set ?
> 2. What is the most common genre of movie?
> 3. What are top 10 movies with highest rate ?
> 4. What are 5 most often rating users ?
> 5. When was done first and last rate included in data set and what was the rated movie tittle?
> 6. Find all movies released in 1990

>Write README.md containing detailed documentation of your solution.

##Solution:

Docker compose is prepared in order to create and run setup in two containers from scratch. One for the MSSQL Server 2017, second for the Python script.

###Docker_compose file:
    version: '3.4'

    services:  
    db:
    image: mcr.microsoft.com/mssql/server:2017-latest
    environment:
     - SA_PASSWORD=Pass@word
     - ACCEPT_EULA=Y
    ports:
     - "1401:1433"

    script:
    build: .
    network_mode: host
    volumes:
     - .:/usr/src/app
    depends_on:
    - db

Service db: downloads the SQL2017 image from Microsoft website and run it in container, mapping the default 1433 port into 1401 port of the host, setting password for SA.

Service script: builds the "data transformation" image based on the dockerfile in project directory, and binding the directory to the container, what allows changes to be made in real time (during developement process). Connection with the DB container is established by using the host network mode.

Docker compose can set the order of services start. I tried to set the python service to start automatically after db is up, however, this idea does not work... Python script doesnt wait for db to start. This problem is not solved by now. As the workaround the python script has to be manually started from the bash command of the script conainer (explained later).

###Dockerfile:

    FROM python:3

    WORKDIR /usr/src/app

    COPY . /usr/src/app

    # install FreeTDS and dependencies
    RUN apt-get update \
     && apt-get install unixodbc -y \
     && apt-get install unixodbc-dev -y \
     && apt-get install freetds-dev -y \
     && apt-get install freetds-bin -y \
     && apt-get install tdsodbc -y \
     && apt-get install --reinstall build-essential -y

    # Install pyodbc dependencies
    RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
    RUN curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list
    RUN apt-get update
    RUN ACCEPT_EULA=Y apt-get -y install msodbcsql17
    RUN apt-get -y install unixodbc-dev

    RUN pip install --no-cache-dir -r requirements.txt

    RUN echo "[FreeTDS]\n\
    Description = FreeTDS Driver\n\
    Driver = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\n\
    Setup = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so" >> /etc/odbcinst.ini

    CMD tail -f /dev/null

Dockerfile contains all the commands to assemble an image for script(python) container. All the project files (even the CSV files!) will be included into docker image. All the install and run commands in this file are mostly result of my web research to install and run the necessary ODBC driver inside of the linux script container.

Additionally, the "requirementrs.txt" file contains information for pip to install pyodbc driver and pandas library

`CMD tail -f /dev/null` command at the end of the file runs the script container in continous mode, allowing to start the main.py manually form the bash terminal

##How does it work - step by step instruction
###Creating docker containers

Assuming, the docker is installed on the host mashine, but there is no docker image nor containers yet, the first step is to enter the terminal (I use Powershell), go to the project directory and use the `docker compose up` command. This creates script image and both containers and runs them. Now is time to wait for db to go up...

After this is done, it is good to verify, for example by connection from host SSMS.

###Starting the python script

Because dockerfile contains `CMD tail -f /dev/null` instead of the run command, the script has to be started manually. It can be done in terminal window.

In terminal window (new):
`docker exec -ti movies_script_1 bash`

opens the linux container command line, where the script can be started using:
`python main.py`

###Creating database, creating tables, loading data into SQL server

From now everything is happening automatically.

Script reads the csv files (from the script container), then creates the Pandas Dataframes containing the Data.

```python
data = pd.read_csv(r'./CSV/movies.csv')
df_movies = pd.DataFrame(data, columns=['movieId', 'title', 'genres'])

# print(df_movies.info())

data = pd.read_csv(r'./CSV/links.csv')
df_links = pd.DataFrame(data, columns=['movieId', 'imdbId', 'tmdbId'])
df_links.fillna(0, inplace=True)

# print(df_links.info())

data = pd.read_csv(r'./CSV/ratings.csv')
df_ratings = pd.DataFrame(data, columns=['userId', 'movieId', 'rating', 'timestamp'])
df_ratings['timestamp'] = pd.to_datetime(df_ratings['timestamp'], unit='s')

# print(df_ratings.info())

data = pd.read_csv(r'./CSV/tags.csv')
df_tags = pd.DataFrame(data, columns=['userId', 'movieId', 'tag', 'timestamp'])
df_tags['timestamp'] = pd.to_datetime(df_tags['timestamp'], unit='s')
```
Worth to mention: script use the `df_links.fillna(0, inplace=True)` function to fill the blanks in links table, that was nessecary to load the data to SQL

Second important thing - converting the timestamp format data into the nicer timestamp format `df_tags['timestamp'] = pd.to_datetime(df_tags['timestamp'], unit='s')`, which later can be used for more convinient data handling.

SQL connection establish and first promt:

    server = 'localhost,1401'
    database = 'stock_db'
    username = 'sa'
    password = 'Pass@word'
    
    db_create_choice = input("Want to create db? [Y/N]:\n")
    
    if db_create_choice == "Y":
    
        cnxn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';UID=' + username + ';PWD=' + password,
            autocommit=True)
        cursor = cnxn.cursor()
    
        cursor.execute('CREATE DATABASE stock_db')
        print("Database stock_db created")
    
    else:
        print("Connecting to stock_db...")

Script needs the database to push the data. If the database does not exist yet (for example, we run the script for a first time), user can select "Y". The db will automatically be created.
If the database already exist, this step can be skipped by any other user input (for example "N")

Connection with sql database using ODBC:

    cnxn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    cursor = cnxn.cursor()
    
    print("Connection to db established")

Creation of the tables:

    db_create_choice = input("Want to create tables? [Y/N]:\n")
    
    if db_create_choice == "Y":
    
        cursor.execute('CREATE TABLE movies (movieId int, title nvarchar(1000), genres nvarchar(1000))')
        print("table movies created")
        cursor.execute('CREATE TABLE links (movieId int, imdbId nvarchar(1000), tmdbId nvarchar(1000))')
        print("table links created")
        cursor.execute('CREATE TABLE ratings (userId int, movieId int, rating int, timestamp datetime)')
        print("table ratings created")
        cursor.execute('CREATE TABLE tags (userId int, movieId int, tag nvarchar(1000), timestamp datetime)')
        print("table tags created")
        cnxn.commit()
    
    else:
        print("Table create skipped...")
    
    db_create_choice = input("Want to fill the tables with Data? [Y/N]:\n")

Same situation like with sql DB creation. User is asked to create the tables, or skip this step if they already exist. Now, with SQL structure ready for data, the next step is:

Data pushing (it can also be skipped if our Data is already in the SQL server tables)

        print("Data pushing into tables. This can take a minute...")
    
        for row in df_movies.itertuples():
            cursor.execute('''
                        INSERT INTO stock_db.dbo.movies (movieId, title, genres)
                        VALUES (?,?,?)
                        ''',
                           row.movieId,
                           row.title,
                           row.genres
                           )
            print(row)
        cnxn.commit()
    
        print("movies.csv pushed into database")
    
        for row in df_links.itertuples():
            cursor.execute('''
                    INSERT INTO stock_db.dbo.links (movieId, imdbId, tmdbId)
                            VALUES (?,?,?)
                            ''',
                           row.movieId,
                           row.imdbId,
                           row.tmdbId,
                           )
            print(row)
        cnxn.commit()
    
        # print("links.csv pushed into database")
    
        for row in df_ratings.itertuples():
            cursor.execute('''
                        INSERT INTO stock_db.dbo.ratings (userId, MovieId, rating, timestamp)
                        VALUES (?,?,?,?)
                        ''',
                           row.userId,
                           row.movieId,
                           row.rating,
                           row.timestamp
                           )
            print(row)
        cnxn.commit()
    
        print("ratings.csv pushed into database")
    
        for row in df_tags.itertuples():
            cursor.execute('''
                        INSERT INTO stock_db.dbo.tags (userId, MovieId, tag, timestamp)
                        VALUES (?,?,?,?)
                        ''',
                           row.userId,
                           row.movieId,
                           row.tag,
                           row.timestamp
                           )
            print(row)
        cnxn.commit()
    
        print("tags.csv pushed into database")
    
    else:
        print("Data push skipped...")

At this moment, there is SQL Server up ad running, including database containing all the data from the CSV files.

###Playing with the data

Now, there is time to answer some questions:

    def get_mode_select():
        mode_select = input("_____________________________________________________________\n"
                            "Want would you like to know? [enter the number]:\n\n"                      
                            "1 - How many movies are in data set ?:\n"
                            "2 - What is the most common genre of movie?:\n"
                            "3 - What are top 10 movies with highest rate?:\n"
                            "4 - What are 5 most often rating users?:\n"
                            "5 - When was done first and last rate included in data set and what was the rated movie tittle?:\n"
                            "6 - Find all movies released in 1990:\n\n"
                            "0 - EXIT PROGRAM\n"
                            "___________________________________________________________________\n\n")
        return mode_select

Above get_mode_select() function returns the input form the user. The answers are shown accordingly based on the SQL querries via ODBC.

    while selected != "0":
    
        selected=get_mode_select()
    
        if selected == "1":
    
            # cursor.execute("SELECT COUNT (movieId) FROM dbo.movies")
            # rows = cursor.fetchall()
    
            # for row in cursor:
            #     print (row)
    
            query = "SELECT COUNT (movieId) FROM dbo.movies"
            cursor.execute(query)
            print("\nNumber of movies in the data set:\n")
            record = cursor.fetchall()
            for row in record:
                print(row[0], "\n")
    
            # selected = get_mode_select()
    
        elif selected == "2":
    
            query = "SELECT top 1 genres, COUNT(genres) AS value_occurrence FROM dbo.movies " \
                    "GROUP BY genres ORDER BY value_occurrence DESC"
            cursor.execute(query)
            print("Most common movie genre:")
            record = cursor.fetchall()
            for row in record:
                print(row[0], "\n")
    
            # selected = get_mode_select()
    
        elif selected == "3":
    
            query = "SELECT top 10 M.movieId, M.title, R.rating FROM dbo.movies as M " \
                    "JOIN dbo.ratings as R ON M.movieId = R.movieId ORDER BY rating DESC"
            cursor.execute(query)
            print("Top 10 movies by rank are:\n")
            record = cursor.fetchall()
            for row in record:
                print(row[1])
            print("\n...however, there are many more movies with this score:\n")
    
            # selected = get_mode_select()
    
        elif selected == "4":
    
            query = "SELECT top 5 userId, COUNT(userId) AS value_occurrence FROM dbo.ratings " \
                    "GROUP BY userId ORDER BY value_occurrence DESC"
            cursor.execute(query)
            print("The 5 most rating users are (userId, number of ratings):\n")
            record = cursor.fetchall()
            for row in record:
                print("UserId: ", row[0])
                print("Number of ratings made by this user: ", row[1], "\n")
    
            # selected = get_mode_select()
    
        elif selected == "5":
    
            query = "SELECT TOP (1) R.movieId, R.timestamp, M.title FROM stock_db.dbo.ratings as R " \
                    "JOIN dbo.movies as M ON  M.movieId = R.movieId ORDER BY timestamp ASC"
            cursor.execute(query)
            print("The first rating ever done:\n")
            record = cursor.fetchall()
            for row in record:
                print("Rating time: ", row[1])
                print("Title: ", row[2], "\n")
    
            query = "SELECT TOP (1) R.movieId, R.timestamp, M.title FROM stock_db.dbo.ratings as R " \
                    "JOIN dbo.movies as M ON  M.movieId = R.movieId ORDER BY timestamp DESC"
            cursor.execute(query)
            record = cursor.fetchall()
            print("The last rating ever done:\n")
            for row in record:
                print("Rating time: ", row[1])
                print("Title: ", row[2], "\n")
    
            # selected = get_mode_select()
    
        elif selected == "6":
    
            query = "SELECT title FROM dbo.movies WHERE title LIKE '%(1990)' ORDER BY title ASC"
            cursor.execute(query)
            print("All of the movies realised in 1990:\n")
            record = cursor.fetchall()
            for row in record:
                print("Title: ", row[0])
    
            # selected = get_mode_select()
    
        elif selected == "0":
            print("Bye bye\n\n")
            # selected = get_mode_select()
    
        else:
            print("Wrong choice")
    
            # selected = get_mode_select()

##End note:

The author is aware, the solution does not stick fully to best practices. Lack of available time was the main factor... All of the challenge requirements was fulfilled and this matters most. :)

What else could be done:

1. The whole script runs in main.py file. I wish I had more time to split it between functions and files.
2. It could be possible to run main.py automatically after db is up, so the bash manual start could be eliminated.
