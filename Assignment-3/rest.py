from flask import Flask
from flask_restful import Api, Resource, reqparse
from flask_cors import CORS
import psycopg2

app = Flask(__name__)
api = Api(app)
CORS(app)

class Post(Resource):
    def get(self, postid):
        #####################################################################################3
        #### Important -- This is the how the connection must be done for autograder to work
        ### But on your local machine, you may need to remove "host=..." part if this doesn't work
        #####################################################################################3
        conn = psycopg2.connect("host=127.0.0.1 dbname=stackexchange user=root password=root")
        cur = conn.cursor()

        cur.execute("select id, posttypeid, title, AcceptedAnswerID, creationdate from posts where id = %s" % (postid))
        ans = cur.fetchall()
        if len(ans) == 0:
            cur.close()
            conn.close()
            return "Post Not Found", 404
        else:
            ret = {"id": ans[0][0], "PostTypeID": ans[0][1], "Title": str(ans[0][2]), "AcceptedAnswerID": str(ans[0][3]), "CreationDate": str(ans[0][4])}
            cur.close()
            conn.close()
            return ret, 200


class Dashboard(Resource):
    # Return some sort of a summary of the data -- we will use the "name" attribute to decide which of the dashboards to return
    # 
    # Here the goal is to return the top 100 users using the reputation -- this will be returned as an array in increasing order of Rank
    # Use PostgreSQL default RANK function (that does sparse ranking), followed by a limit 100 to get the top 100 
    #
    # FORMAT: {"Top 100 Users by Reputation": [{"ID": "...", "DisplayName": "...", "Reputation": "...", "Rank": "..."}, {"ID": "...", "DisplayName": "...", "Reputation": "...", "Rank": "..."}, ]
    def get(self, name):
        if name == "top100users":
            conn = psycopg2.connect("host=127.0.0.1 dbname=stackexchange user=root password=root")
            cur = conn.cursor()

            cur.execute("""select id, displayname, reputation, rank () over (order by reputation desc) as rank 
                            from users 
                            order by rank asc
                            limit 100;""" )
            ans = cur.fetchall()
            if len(ans) == 0:
                cur.close()
                conn.close()
                return "Post Not Found", 404
            top100 = []
            for i in range(100):
                top100.append({"ID": ans[i][0], 
                                "DisplayName": ans[i][1], 
                                "Reputation": str(ans[i][2]), 
                                "Rank": str(ans[i][3])
                })
            ret = {"Top 100 Users by Reputation": top100}
            cur.close()
            conn.close()
            return ret, 200
        else:
            return "Unknown Dashboard Name", 404

class User(Resource):
    # Return all the info about a specific user, including the titles of the user's posts as an array
    # The titles array must be sorted in the increasing order by the title.
    # Remove NULL titles if any
    # FORMAT: {"ID": "...", "DisplayName": "...", "CreationDate": "...", "Reputation": "...", "PostTitles": ["posttitle1", "posttitle2", ...]}
    def get(self, userid):
        # Add your code to construct "ret" using the format shown below
        # Post Titles must be sorted in alphabetically increasing order
        # CreationDate should be of the format: "2007-02-04" (this is what Python str() will give you)

        # Add your code to check if the userid is already present in the database
        
        conn = psycopg2.connect("host=127.0.0.1 dbname=stackexchange user=root password=root")
        cur = conn.cursor()
        user_query = """select id, displayname, creationdate, reputation 
                        from users
                        where id = %s
                        """ % (userid)
        cur.execute(user_query)
        user_ans = cur.fetchall()
        exists_user = len(user_ans) > 0
    
        if not exists_user:
            cur.close()
            conn.close()
            return "User not found", 404
        else:
            cur.execute("""select title 
                        from posts 
                        where owneruserid = %s
                        """ % (userid))
            posts_ans = cur.fetchall()
            numPosts = len(posts_ans)
            postTitles = []
            for i in range(numPosts):
                postTitle = str(posts_ans[i][0])
                if postTitle != "None":
                    postTitles.append(postTitle)
            postTitles.sort()
            ret = {"ID": user_ans[0][0], "DisplayName": user_ans[0][1], "CreationDate": str(user_ans[0][2]), "Reputation": user_ans[0][3], "PostTitles": postTitles}
            conn.commit()
            cur.close()
            conn.close()
            return ret, 200

    # Add a new user into the database, using the information that's part of the POST request
    # We have provided the code to parse the POST payload
    # If the "id" is already present in the database, a FAILURE message should be returned
    def post(self, userid):
        parser = reqparse.RequestParser()
        parser.add_argument("reputation")
        parser.add_argument("creationdate")
        parser.add_argument("displayname")
        parser.add_argument("upvotes")
        parser.add_argument("downvotes")
        args = parser.parse_args()
        print("Data received for new user with id {}".format(userid))
        print(args)

        # Add your code to check if the userid is already present in the database
        conn = psycopg2.connect("host=127.0.0.1 dbname=stackexchange user=root password=root")
        cur = conn.cursor()

        cur.execute("select id from users where id = %s;" % (userid))
        user_ans = cur.fetchall()
        exists_user = len(user_ans) > 0

        if exists_user:
            return "FAILURE -- Userid must be unique", 201
        else:
            # Add your code to insert the new tuple into the database
            insert_sql = """insert into users (id, reputation, creationdate, displayname, views, upvotes, downvotes) 
            values (%s, %s, %s, %s, %s, %s, %s)
            """
           
            cur.execute(insert_sql,  (userid, args["reputation"], args["creationdate"], args["displayname"], 0, args["upvotes"], args["downvotes"]))
            conn.commit()
            cur.close()
            conn.close()
            return "SUCCESS", 201

    # Delete the user with the specific user id from the database
    def delete(self, userid):
        # Add your code to check if the userid is present in the database
        conn = psycopg2.connect("host=127.0.0.1 dbname=stackexchange user=root password=root")
        cur = conn.cursor()

        cur.execute("""select id from users 
                        where id = %s
                        """ % (userid))
        user_ans = cur.fetchall()
        exists_user = len(user_ans) > 0
    
        if exists_user:
            # Add your code to delete the user from the user table
            # If there are corresponding entries in "badges" table for that userid, those should be deleted
            # For posts, comments, votes, set the appropriate userid fields to -1 (since that content should not be deleted)
            cur.execute("""select userid from badges 
                        where userid = %s
                        """ % (userid))
            
            badge_ans = cur.fetchall()
            exists_badge = len(badge_ans) > 0
            if exists_badge:
                cur.execute("""delete from badges 
                        where userid = %s
                        """ % (userid))
            conn.commit()

            cur.execute(f"select owneruserid from posts where owneruserid = {userid};")
            t_ans = cur.fetchall()
            if len(t_ans) > 0: # if have, change userid to -1
                cur.execute(f"update posts set owneruserid = {-1} where owneruserid = {userid};") 
                assert(cur.rowcount > 0)
                conn.commit()

            cur.execute(f"select lasteditoruserid from posts where lasteditoruserid = {userid};")
            t_ans = cur.fetchall()
            if len(t_ans) > 0: # if have, change userid to -1
                cur.execute(f"update posts set lasteditoruserid = {-1} where lasteditoruserid = {userid};") 
                print("hello")
                assert(cur.rowcount > 0)
                conn.commit()

            cur.execute(f"select userid from comments where userid = {userid};")
            t_ans = cur.fetchall()
            if len(t_ans) > 0: # if have, change userid to -1
                cur.execute(f"update comments set userid = {-1} where userid = {userid};") 
                assert(cur.rowcount > 0)
                conn.commit()
            
            cur.execute(f"select userid from votes where userid = {userid};")
            t_ans = cur.fetchall()
            if len(t_ans) > 0: # if have, change userid to -1
                cur.execute(f"update votes set userid = {-1} where userid = {userid};") 
                assert(cur.rowcount > 0)
                conn.commit()

            cur.execute("delete from users where id = %s;"% (userid))
            assert(cur.rowcount == 1)
            conn.commit()
            cur.close()
            conn.close()
            return "SUCCESS", 201
        else:
            cur.close()
            conn.close()
            return "FAILURE -- Unknown Userid", 404
      
api.add_resource(User, "/user/<int:userid>")
api.add_resource(Post, "/post/<int:postid>")
api.add_resource(Dashboard, "/dashboard/<string:name>")

app.run(debug=True, host="0.0.0.0", port=5000)
