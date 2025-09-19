from db import create_mssql_connection

connection = create_mssql_connection()
cursor = connection.cursor()

#cursor.close()

# def user_login(username, password):
#     try:
#         cursor.execute("SELECT * FROM user_Credential(nolock) WHERE Name = ? AND Pwd = ?", (username, password))
#         result = cursor.fetchone()
#         return result is not None
#     except Exception as e:
#         print(f"Error during login: {e}")
#         return False

# def user_login(username, password):
#     try:
#         cursor.execute(
#             "SELECT Id, Name FROM user_Credential (nolock) WHERE Name = ? AND Pwd = ?",
#             (username, password)
#         )
#         result = cursor.fetchone()
#         if result:
#             # result[0] = Id (int), result[1] = Name (string)
#             return int(result[0]), result[1]
#         return None, None
#     except Exception as e:
#         print(f"Error during login: {e}")
#         return None, None

def user_login(username, password):
    """
    Return (user_id:int, username:str) on success, else (None, None).
    """
    try:
        # adjust column names if different; I'm using Id and Name as example
        cursor.execute(
            "SELECT Id, Name FROM user_Credential (nolock) WHERE Name = ? AND Pwd = ?",
            (username, password)
        )
        row = cursor.fetchone()
        if row:
            try:
                uid = int(row[0])
            except Exception:
                uid = row[0]
            name = row[1] if len(row) > 1 else username
            return uid, name
        return None, None
    except Exception as e:
        print(f"Error during login: {e}")
        return None, None

def User_Exist(Email,name):
    try:
        cursor.execute("SELECT * FROM user_Credential (nolock) WHERE Email = ? and name=?", (Email,name))
        result = cursor.fetchone()
        return result is not None
    except Exception as e:
        print(f"Error checking user existence: {e}")
        return False

def user_register(username,Email,password):
    try:
        cursor.execute("INSERT INTO user_Credential (Name, Email,Pwd) VALUES (?, ?,?)", (username, Email,password))
        connection.commit()
        return True
    except Exception as e:
        print(f"Error registering user: {e}")
        return False



def User_event_Log(user_id, Brand, Dealer, Location, Missing_file,
                   Startdate, Enddate, Category, MissingPeriod):
    try:
        sql = """
        INSERT INTO Log_user
        (user_id, Brand, Dealer, Location, Missing_file, Startdate, Enddate, Category, MissingPeriod)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(sql, (
            user_id,
            Brand,
            Dealer,
            Location,
            Missing_file,
            Startdate,
            Enddate,
            Category,
            MissingPeriod
        ))
        connection.commit()
        return True
    except Exception as e:
        print(f"Error logging user event: {e}")
        return False
