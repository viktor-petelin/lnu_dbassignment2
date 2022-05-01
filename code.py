import mysql.connector
from mysql.connector import Error

import csv


name_database = "dota2_table"
player_file_name = "Player.csv"
match_file_name = "Match.csv"
team_file_name = "Team.csv"
tournament_file_name = "Tournament.csv"

def create_database():
    conn = mysql.connector.connect(host='localhost', user='root', password='root')
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE " + name_database)
    conn.close()

    conn = mysql.connector.connect(host='localhost', user='root', database=name_database, password='root')
    cursor = conn.cursor()
    players_sql = '''CREATE TABLE player (
  nickname varchar(50),
  first_name varchar(50),
  last_name varchar(50),
  born int,
  total_winnings int,
  country varchar(50),
  PRIMARY KEY (nickname))'''
    cursor.execute(players_sql)

    teams_sql = '''CREATE TABLE team (
  name varchar(50),
  id varchar(50),
  rating int DEFAULT 5000,
  location varchar(50),
  web varchar(50),
  PRIMARY KEY (id),
  player1 varchar(50),
  player2 varchar(50),
  player3 varchar(50),
  player4 varchar(50),
  player5 varchar(50),
  FOREIGN KEY (player1) REFERENCES player (nickname),
  FOREIGN KEY (player2) REFERENCES player (nickname),
  FOREIGN KEY (player3) REFERENCES player (nickname),
  FOREIGN KEY (player4) REFERENCES player (nickname),
  FOREIGN KEY (player5) REFERENCES player (nickname))'''
    cursor.execute(teams_sql)

    tournaments_sql = '''CREATE TABLE tournament (
  name varchar(50),
  country varchar(50),
  city varchar(50),
  prize varchar(50),
  start DATETIME,
  finish DATETIME,
  sponsor varchar(50),
  winner varchar(50) NULL,
  FOREIGN KEY (winner) REFERENCES team (id),
  PRIMARY KEY (name))'''
    cursor.execute(tournaments_sql)

    matches_sql = '''CREATE TABLE match_game (
  id int,
  date_time DATETIME,
  result varchar(50),
  place varchar(50),
  round varchar(50),
  team1 varchar(50) NULL,
  team2 varchar(50) NULL,
  winner varchar(50) NULL,
  tournament_name varchar(50),
  PRIMARY KEY (id),
  FOREIGN KEY (team1) REFERENCES team (id),
  FOREIGN KEY (team2) REFERENCES team (id),
  FOREIGN KEY (winner) REFERENCES team (id),
  FOREIGN KEY (tournament_name) REFERENCES tournament (name))'''
    cursor.execute(matches_sql)

    cursor.execute('''CREATE VIEW matches_tournament AS
    SELECT m.id, m.date_time, m.result, m.place, m.round, m.team1, m.team2, m.winner, 
    t.name, t.country, t.city, t.prize, t.start, t.finish, t.sponsor
    FROM match_game AS m, tournament AS t
    WHERE m.tournament_name = t.name''')

    cursor.close()

    load_data_players(conn, player_file_name)
    load_data_teams(conn, team_file_name)
    load_data_tournaments(conn, tournament_file_name)
    load_data_matches(conn, match_file_name)


def load_data_players(conn, data_file_name):
    cursor = conn.cursor()

    with open(data_file_name, 'r') as file:
        heading = next(file)
        csv_data = csv.reader(file)
        for row in csv_data:

            # print(row)
            cursor.execute('''INSERT INTO player (nickname, first_name, 
            last_name, born, total_winnings, country)
            VALUES(%s, %s, %s, %s, %s, %s)''', row)
    
    conn.commit()
    cursor.close()
    
def load_data_matches(conn, data_file_name):
    cursor = conn.cursor()

    with open(data_file_name, 'r') as file:
        heading = next(file)
        csv_data = csv.reader(file)
        for row in csv_data:

            # print(row)
            cursor.execute('''INSERT INTO match_game (id, date_time, 
            result, place, round, team1, team2, winner, 
            tournament_name)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)''', row)
    
    conn.commit()
    cursor.close()
def load_data_teams(conn, data_file_name):
    cursor = conn.cursor()

    with open(data_file_name, 'r') as file:
        heading = next(file)
        csv_data = csv.reader(file)
        for row in csv_data:

            # print(row)
            cursor.execute('''INSERT INTO team (name, id, 
            rating, location, web, player1, 
            player2, player3, player4, player5)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)''', row)
    
    conn.commit()
    cursor.close()

def load_data_tournaments(conn, data_file_name):
    cursor = conn.cursor()

    with open(data_file_name, 'r') as file:
        heading = next(file)
        csv_data = csv.reader(file)
        for row in csv_data:
            
            # print(row)
            cursor.execute('''INSERT INTO tournament (name, country, 
            city, prize, start, finish, sponsor, winner)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s)''', row)
    
    conn.commit()
    cursor.close()
    

def connect():
    """ Connect to MySQL database """
    try:
        conn = mysql.connector.connect(host='localhost',
                                       database=name_database,
                                       user='root',
                                       password='root')
        return conn

    except Error as e:
        print(e)
        return None
 

def print_menu():
    print("1. List all players sort by total winnings.")
    print("2. Search for players by name of the team.")
    print("3. List teams ordered by matches won.")
    print("4. List matches for the player.")
    print("q. Quit")
    print("----------")
    x = input("Please choose one option: ")
    return x

def list_players_total_winnings(conn):
    cursor = conn.cursor()

    cursor.execute("SELECT nickname, total_winnings FROM player ORDER BY total_winnings DESC")

    result = cursor.fetchall()

    for x in result:
        print(x)
    

def players_in_team(conn):
    cursor = conn.cursor()

    team = input("Enter the name of the team: ")
    cursor.execute('''SELECT player.nickname, COUNT(player.nickname)
    FROM player left outer join team AS t1 ON (player.nickname = t1.player1
    OR player.nickname = t1.player2
    OR player.nickname = t1.player3
    OR player.nickname = t1.player4
    OR player.nickname = t1.player5)
    WHERE t1.name = %s
    GROUP BY player.nickname
    ORDER BY player.nickname''', [team])

    result = cursor.fetchall()

    for x in result:
        print(x)
    if len(result) < 1:
        print("There isn't info about the team!")
    
def won_matches_team(conn):
    cursor = conn.cursor()
    cursor.execute('''SELECT team.name, COUNT(match_game.winner) AS won
    FROM team LEFT OUTER JOIN match_game ON team.id = match_game.winner
    GROUP BY team.name
    ORDER BY won DESC''')

    result = cursor.fetchall()

    for x in result:
        print(x)
    if len(result) < 1:
        print("There isn't info about the height with hight more than !")

def players_matches(conn):
    cursor = conn.cursor()

    player = input("Enter the name of the team: ")
    cursor.execute('''SELECT player.nickname, t1.id, match_game.team1, match_game.team2, match_game.result, match_game.date_time
    FROM player left outer join team AS t1 ON (player.nickname = t1.player1
    OR player.nickname = t1.player2
    OR player.nickname = t1.player3
    OR player.nickname = t1.player4
    OR player.nickname = t1.player5) 
    left outer join match_game ON (t1.id = match_game.team1 OR
    t1.id = match_game.team2)
    WHERE player.nickname = %s
    ORDER BY player.nickname''', [player])

    result = cursor.fetchall()

    for x in result:
        print(x)
    if len(result) < 1:
        print("There isn't info about the height with hight more than !")    





        
if __name__ == '__main__':
    conn = connect()

    if conn is None:
        create_database()
        conn = connect()
        
    print('Connected to MySQL database')
    print(conn.database)

    while True:
        x = print_menu()
        if x == "1":
            list_players_total_winnings(conn)
        elif x == "2":
            players_in_team(conn)
        elif x == "3":
            won_matches_team(conn)
        elif x == "4":
            players_matches(conn)
        else:
            x = print_menu
        input("\n Please ENTER to continue: ")
    
    conn.close()
