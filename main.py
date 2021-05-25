import pandas as pd
import pyodbc

f = open('logo.txt', 'r')
file_contents = f.read()
print(file_contents)
f.close()

print("Parsing CSV files into DataFrames")

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

# print(df_tags.info())

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

cnxn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = cnxn.cursor()

print("Connection to db established")

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

if db_create_choice == "Y":

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


def get_mode_select():
    mode_select = input("_____________________________________________________________________________\n"
                        "Want would you like to know? [enter the number]:\n\n"                      
                        "1 - How many movies are in data set ?:\n"
                        "2 - What is the most common genre of movie?:\n"
                        "3 - What are top 10 movies with highest rate?:\n"
                        "4 - What are 5 most often rating users?:\n"
                        "5 - When was done first and last rate included in data set and what was the rated movie tittle?:\n"
                        "6 - Find all movies released in 1990:\n\n"
                        "0 - EXIT PROGRAM\n"
                        "_____________________________________________________________________________\n\n")
    return mode_select


selected = ""

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
